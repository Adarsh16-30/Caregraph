//! Supervision of the embedding worker pool (production-hardening P2).
//!
//! Before the pool, one dead Python worker failed every later mutation until
//! the whole server was restarted, and nothing noticed a hung worker. These
//! tests run the real worker (`ml/embedding_server.py`, real trained model, real
//! torch) and break it on purpose:
//!
//! * kill the worker process → the very next request still succeeds (the pool
//!   waits for the replacement), the pid changes, `restarts` increments;
//! * a request past its deadline → `Timeout`, the stuck worker is killed, and
//!   the pool serves again;
//! * repeated worker deaths → the circuit breaker opens and requests fail fast
//!   with `Unavailable` instead of queueing behind a crashlooping worker, then
//!   recover after the cooldown;
//! * a request the *model* rejects (bad target index) → an error, but the worker
//!   is NOT killed and the breaker is untouched.

use std::process::Command;
use std::time::{Duration, Instant};

use caregraph::embedding::worker::pool::PoolConfig;
use caregraph::embedding::EmbeddingModel;
use caregraph::CareGraphError;

const MODEL: &str = "diabetes130_graphsage";
/// Width of the structural node-type one-hot features (`resolver::NODE_TYPES`).
const FEATURE_DIM: usize = 6;

fn kill_pid(pid: u32) {
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .output();
    }
    #[cfg(not(windows))]
    {
        let _ = Command::new("kill").args(["-9", &pid.to_string()]).output();
    }
}

fn ring(n: usize) -> (Vec<Vec<f32>>, Vec<Vec<u32>>) {
    let features = (0..n)
        .map(|i| {
            let mut row = vec![0.0f32; FEATURE_DIM];
            row[i % FEATURE_DIM] = 1.0;
            row
        })
        .collect();
    let (mut src, mut dst) = (Vec::new(), Vec::new());
    for i in 0..n {
        let j = (i + 1) % n;
        src.extend([i as u32, j as u32]);
        dst.extend([j as u32, i as u32]);
    }
    (features, vec![src, dst])
}

fn tiny(model: &EmbeddingModel) -> caregraph::Result<Vec<Vec<f32>>> {
    let (f, e) = ring(8);
    model.forward(&f, &e, &[0, 3])
}

fn fast_cfg() -> PoolConfig {
    PoolConfig {
        workers: 1,
        backoff_min: Duration::from_millis(50),
        forward_timeout: Duration::from_secs(120),
        spawn_timeout: Duration::from_secs(120),
        ..PoolConfig::default()
    }
}

fn wait_until(what: &str, limit: Duration, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < limit, "timed out waiting for: {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_killed_worker_is_respawned_and_the_next_request_succeeds() {
    let model = EmbeddingModel::spawn_with(MODEL, fast_cfg()).expect("spawn");
    let before = tiny(&model).expect("baseline forward");
    let pid_before = model.worker_pid().expect("a live worker");

    kill_pid(pid_before);

    // No sleeping, no polling for health first: the request issued right after
    // the crash must itself succeed, because the pool waits for the
    // replacement and retries once.
    let after = tiny(&model).expect("request issued right after a worker crash must succeed");
    assert_eq!(before.len(), after.len());
    for (b, a) in before.iter().flatten().zip(after.iter().flatten()) {
        assert_eq!(
            b.to_bits(),
            a.to_bits(),
            "a respawned worker must compute identically"
        );
    }

    let stats = model.stats();
    assert!(
        stats.restarts >= 1,
        "restart counter must record the respawn: {stats:?}"
    );
    assert_ne!(
        model.worker_pid(),
        Some(pid_before),
        "a new process must be serving"
    );
}

#[test]
fn a_request_past_its_deadline_times_out_kills_the_worker_and_the_pool_recovers() {
    let model = EmbeddingModel::spawn_with(MODEL, fast_cfg()).expect("spawn");
    tiny(&model).expect("baseline");
    let pid_before = model.worker_pid().expect("pid");

    // Large enough that one forward pass takes far longer than the deadline.
    let (f, e) = ring(300_000);
    let err = model
        .forward_with_deadline(&f, &e, &[0], Duration::from_millis(20))
        .expect_err("a 20ms deadline cannot cover a 300k-node forward pass");
    assert!(
        matches!(err, CareGraphError::Timeout(_)),
        "expected Timeout, got {err:?}"
    );
    assert_eq!(model.stats().timeouts, 1);

    // The stuck worker was killed, not left wedged on the pipe.
    wait_until(
        "worker replaced after timeout",
        Duration::from_secs(120),
        || model.worker_pid().is_some_and(|p| p != pid_before),
    );
    tiny(&model).expect("pool serves normally after recovering from a timeout");
}

#[test]
fn a_crashlooping_worker_opens_the_breaker_and_requests_fail_fast_then_recover() {
    let cfg = PoolConfig {
        breaker_failures: 2,
        breaker_cooldown: Duration::from_secs(3),
        ..fast_cfg()
    };
    let model = EmbeddingModel::spawn_with(MODEL, cfg).expect("spawn");
    tiny(&model).expect("baseline");

    for _ in 0..2 {
        wait_until("a live worker to kill", Duration::from_secs(120), || {
            model.worker_pid().is_some()
        });
        kill_pid(model.worker_pid().unwrap());
        wait_until("death to be recorded", Duration::from_secs(30), || {
            model.worker_pid().is_none()
        });
    }

    wait_until("breaker to open", Duration::from_secs(5), || {
        model.stats().breaker_open
    });
    let started = Instant::now();
    let err = tiny(&model).expect_err("an open breaker must refuse");
    assert!(
        matches!(err, CareGraphError::Unavailable(_)),
        "expected Unavailable, got {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "an open breaker must fail fast, took {:?}",
        started.elapsed()
    );

    // After the cooldown and a respawn the pool serves again.
    wait_until("worker back", Duration::from_secs(120), || {
        model.worker_pid().is_some() && !model.stats().breaker_open
    });
    tiny(&model).expect("recovered after the breaker's cooldown");
}

#[test]
fn a_request_the_model_rejects_does_not_kill_the_worker_or_trip_the_breaker() {
    let model = EmbeddingModel::spawn_with(MODEL, fast_cfg()).expect("spawn");
    tiny(&model).expect("baseline");
    let pid = model.worker_pid().expect("pid");

    let (f, e) = ring(8);
    // Target row 999 does not exist: the worker answers with an ERROR frame.
    let err = model
        .forward(&f, &e, &[999])
        .expect_err("an out-of-range target index must be rejected");
    assert!(
        !matches!(
            err,
            CareGraphError::Unavailable(_) | CareGraphError::Timeout(_)
        ),
        "a model error is not an availability failure: {err:?}"
    );

    let stats = model.stats();
    assert_eq!(
        stats.restarts, 0,
        "a rejected request must not restart the worker"
    );
    assert!(!stats.breaker_open);
    assert_eq!(
        model.worker_pid(),
        Some(pid),
        "the same worker keeps serving"
    );
    tiny(&model).expect("worker still serves after rejecting a bad request");
}
