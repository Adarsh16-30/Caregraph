//! Probe endpoints and the bounded drain sequence.

use std::sync::Arc;
use std::time::{Duration, Instant};

use caregraph::embedding::worker::pool::PoolStats;
use caregraph::metrics_server::router;
use caregraph::server::shutdown::{run_until_shutdown, DrainOutcome};
use caregraph::server::{Health, ModelStatus, PoolCollector, ServerLimits};
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn http_get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status line");
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn stats(healthy: usize, breaker_open: bool) -> PoolStats {
    PoolStats {
        workers: 2,
        healthy,
        restarts: 4,
        timeouts: 0,
        worker_failures: 1,
        breaker_open,
    }
}

async fn serve_probes(
    health: Arc<Health>,
    registry: Registry,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(registry, health);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, task)
}

#[tokio::test]
async fn readiness_follows_startup_the_breaker_and_draining() {
    let breaker = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let probe_breaker = breaker.clone();
    let health = Health::new(
        Arc::new(move || {
            vec![ModelStatus {
                name: "graphsage".into(),
                stats: stats(2, probe_breaker.load(std::sync::atomic::Ordering::SeqCst)),
            }]
        }),
        Arc::new(|| true),
    );
    let registry = Registry::new();
    PoolCollector::register(health.clone(), &registry).unwrap();
    let (addr, server) = serve_probes(health.clone(), registry).await;

    assert_eq!(http_get(addr, "/healthz").await.0, 200);
    assert_eq!(http_get(addr, "/startupz").await.0, 503);
    assert_eq!(http_get(addr, "/readyz").await.0, 503);

    health.mark_started();
    assert_eq!(http_get(addr, "/startupz").await.0, 200);
    assert_eq!(http_get(addr, "/readyz").await.0, 200);

    breaker.store(true, std::sync::atomic::Ordering::SeqCst);
    let (code, body) = http_get(addr, "/readyz").await;
    assert_eq!(code, 503);
    assert!(body.contains("circuit breaker open"), "{body}");
    breaker.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(http_get(addr, "/readyz").await.0, 200);

    health.begin_draining();
    let (code, body) = http_get(addr, "/readyz").await;
    assert_eq!(code, 503);
    assert!(body.contains("draining"), "{body}");
    assert_eq!(
        http_get(addr, "/healthz").await.0,
        200,
        "a draining process is still alive"
    );

    let (code, metrics) = http_get(addr, "/metrics").await;
    assert_eq!(code, 200);
    assert!(
        metrics.contains("worker_restarts_total{model=\"graphsage\"} 4"),
        "{metrics}"
    );
    assert!(
        metrics.contains("worker_healthy{model=\"graphsage\"} 2"),
        "{metrics}"
    );
    server.abort();
}

#[tokio::test]
async fn liveness_fails_when_the_database_stops_answering() {
    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let probe = alive.clone();
    let health = Health::new(
        Arc::new(Vec::new),
        Arc::new(move || probe.load(std::sync::atomic::Ordering::SeqCst)),
    );
    let (addr, server) = serve_probes(health, Registry::new()).await;
    assert_eq!(http_get(addr, "/healthz").await.0, 200);
    alive.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(http_get(addr, "/healthz").await.0, 503);
    server.abort();
}

fn limits(prestop_ms: u64, drain_ms: u64) -> ServerLimits {
    ServerLimits {
        prestop_delay: Duration::from_millis(prestop_ms),
        drain_deadline: Duration::from_millis(drain_ms),
        ..ServerLimits::default()
    }
}

fn idle_health() -> Arc<Health> {
    Health::new(Arc::new(Vec::new), Arc::new(|| true))
}

#[tokio::test]
async fn a_signal_marks_not_ready_then_waits_for_in_flight_work() {
    let health = idle_health();
    health.mark_started();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = finished.clone();

    let started = Instant::now();
    let outcome = run_until_shutdown(
        move |stop| async move {
            stop.await;
            // In-flight request still running when the stop arrives.
            tokio::time::sleep(Duration::from_millis(300)).await;
            done.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        tokio::time::sleep(Duration::from_millis(50)),
        &health,
        &limits(100, 5_000),
    )
    .await
    .unwrap();

    assert_eq!(outcome, DrainOutcome::Clean);
    assert!(
        finished.load(std::sync::atomic::Ordering::SeqCst),
        "the in-flight work was allowed to finish"
    );
    assert!(!health.readiness().ready, "readiness flipped off");
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "signal delay + pre-stop delay + in-flight work"
    );
}

#[tokio::test]
async fn a_server_that_never_drains_is_abandoned_at_the_deadline() {
    let health = idle_health();
    let started = Instant::now();
    let outcome = run_until_shutdown(
        |stop| async move {
            stop.await;
            // A connection that never closes.
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(())
        },
        std::future::ready(()),
        &health,
        &limits(0, 300),
    )
    .await
    .unwrap();
    assert_eq!(outcome, DrainOutcome::DeadlineExceeded);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "exit is bounded by the drain deadline, not the stuck request"
    );
}

#[tokio::test]
async fn a_server_that_exits_on_its_own_is_reported_without_waiting_for_a_signal() {
    let health = idle_health();
    let outcome = run_until_shutdown(
        |_stop| async { Ok(()) },
        std::future::pending::<()>(),
        &health,
        &limits(0, 1_000),
    )
    .await
    .unwrap();
    assert_eq!(outcome, DrainOutcome::ServerExited);
    assert!(!health.is_draining());
}
