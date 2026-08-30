//! "Kill the process mid-write, verify no partial state" — Rule 5's actual
//! claim, tested against the actual failure mode rather than assumed from
//! `atomic_commit.rs`'s single `store.write(batch)` call reading correctly.
//!
//! # What "mid-write" means here, and why a fixed delay can't find it
//!
//! `caregraph-fault-injection-worker` prints `MODEL_READY` the instant its
//! embedding model subprocess is up, then makes exactly one
//! `AtomicCommitter::commit` call and prints `DONE`. Everything before
//! `MODEL_READY` is Python/PyTorch import cost — hundreds of milliseconds,
//! and killing there proves nothing about atomicity. The commit call itself
//! — resolve, one small forward pass, one `WriteBatch` write — is on the
//! order of a few milliseconds. So each iteration waits for `MODEL_READY`,
//! then races a short, randomised delay (0-8ms) against the worker's own
//! progress: short enough that many iterations land before, during, or just
//! after the commit call itself, not during the slow, uninteresting part of
//! the worker's lifetime. See "Killing fast, cleaning up separately" below
//! for why the delay window (0-8ms) is only half of what makes the kill
//! land close to the commit — the other half is not spending that budget on
//! process-spawn overhead in the kill itself.
//!
//! # The invariant
//!
//! Each iteration seeds a fresh database with two bare nodes and no edge, no
//! embedding. After the worker is killed (or finishes and exits on its own),
//! the only two states that are not a Rule 5 violation:
//!   - **fully uncommitted**: the edge does not exist as of `ts`, and neither
//!     node has an embedding as of `ts`.
//!   - **fully committed**: the edge exists as of `ts`, and both nodes have
//!     an embedding as of `ts`.
//!
//! Anything else — the edge landed without embeddings, or an embedding
//! landed without the edge — is exactly the non-atomic state Rule 5
//! forbids, and fails the test.
//!
//! # Killing fast, cleaning up separately
//!
//! The worker spawns its own `ml/embedding_server.py` child, and on Windows
//! killing a process does not propagate to its children with no job object
//! grouping them — so a plain kill would leak one `python.exe` per
//! iteration. Reaping that child correctly is not, however, on the critical
//! path: shelling out to `taskkill` to do it *before* killing the worker
//! would add exactly the process-spawn latency that risks losing the race
//! against the worker's own commit call. So the timing-critical kill is a
//! direct `Child::kill()` (one syscall) against the worker alone, and the
//! worker's Python child — whose pid it prints on startup, see
//! `PYTHON_PID` below — is reaped afterward on a detached thread that adds
//! no latency to the race itself.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::record::NodeValue;
use caregraph::temporal::{TemporalIndex, TemporalWriter};
use caregraph::types::{EdgeType, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

const ITERATIONS: u32 = 100;
const SRC: NodeId = NodeId(1);
const DST: NodeId = NodeId(2);
const EDGE_TYPE: EdgeType = EdgeType::DiagnosedWith;
const MODEL_ID: &str = "diabetes130_graphsage";
/// Generous: covers Python interpreter start plus a cold PyTorch import.
const MODEL_READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Generous: only reached if a kill was somehow missed entirely.
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);

/// xorshift64* — same generator this crate's other benchmark/test harnesses
/// use, for a reproducible jitter sequence.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    FullyCommitted,
    FullyUncommitted,
}

/// Fresh database, two bare nodes, no edge, no embedding. The seeding
/// `RocksKv` handle is dropped before returning so the worker can open its
/// own — RocksDB allows one open handle per directory at a time.
fn seed_fresh_db() -> TempDir {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb for seeding");
    let writer = TemporalWriter::new(&store).expect("writer");
    let mut batch = WriteBatch::default();
    writer.put_node(
        &mut batch,
        SRC,
        Timestamp(0),
        &NodeValue::new("Patient", json!({})),
    );
    writer.put_node(
        &mut batch,
        DST,
        Timestamp(0),
        &NodeValue::new("Condition", json!({})),
    );
    drop(writer);
    store.write(batch).expect("seed commit");
    drop(store);
    dir
}

/// Best-effort, non-timing-critical cleanup of the worker's own Python
/// child, once its pid is known (from the `PYTHON_PID` line). Spawned onto a
/// detached thread so it never adds latency to the actual kill below — the
/// race this test cares about is against the worker process, not against
/// tidying up after it.
fn cleanup_python_child(pid: u32) {
    std::thread::spawn(move || {
        #[cfg(windows)]
        {
            let _ = Command::new("taskkill")
                .args(["/F", "/PID", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        #[cfg(not(windows))]
        {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    });
}

/// Run one iteration: seed, spawn the worker, race a kill against its
/// commit, verify the resulting database state. Returns the outcome and
/// whether this iteration actually killed the worker (vs. it finishing
/// naturally before the kill delay elapsed).
fn run_one_iteration(iteration: u32, rng: &mut Rng) -> (Outcome, bool) {
    let dir = seed_fresh_db();
    let db_path = dir.path().join("caregraph");
    let ts = 1_000_000 + iteration as u64;

    let worker_exe = env!("CARGO_BIN_EXE_caregraph-fault-injection-worker");
    let mut child = Command::new(worker_exe)
        .arg("--db")
        .arg(&db_path)
        .arg("--model")
        .arg(MODEL_ID)
        .arg("--src")
        .arg(SRC.as_u64().to_string())
        .arg("--dst")
        .arg(DST.as_u64().to_string())
        .arg("--edge-type")
        .arg("DiagnosedWith")
        .arg("--ts")
        .arg(ts.to_string())
        .stdout(Stdio::piped())
        // A killed worker's own Python child routinely raises writing to a
        // now-closed stdout pipe — expected noise from a successful kill,
        // not a failure; silenced here so 100 iterations don't flood the
        // test log with it.
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn fault_injection_worker");

    let stdout = child.stdout.take().expect("piped stdout");

    // Reader thread: the main thread needs to sleep for the jittered delay
    // without blocking on a line that may never come (the process could die
    // before printing anything at all).
    let (tx, rx) = mpsc::channel::<String>();
    let reader = std::thread::spawn(move || {
        let mut lines = BufReader::new(stdout).lines();
        while let Some(Ok(line)) = lines.next() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let mut saw_model_ready = false;
    let mut saw_done = false;
    let mut python_pid: Option<u32> = None;
    let start = Instant::now();
    while start.elapsed() < MODEL_READY_TIMEOUT {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) if line == "MODEL_READY" => {
                saw_model_ready = true;
                break;
            }
            Ok(line) => {
                if let Some(rest) = line.strip_prefix("PYTHON_PID ") {
                    python_pid = rest.trim().parse().ok();
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break, // died before printing anything
        }
    }

    let mut killed = false;
    if saw_model_ready {
        let jitter_micros = rng.below(8_000);
        std::thread::sleep(Duration::from_micros(jitter_micros));

        // Non-blocking: DONE may already have arrived during the jitter.
        match rx.try_recv() {
            Ok(line) if line == "DONE" => saw_done = true,
            _ => {
                // A direct syscall (TerminateProcess on Windows), not a
                // shelled-out `taskkill` — the point is to land this as
                // close as possible to the worker's own commit call, and
                // spawning a whole new process to do the killing would add
                // exactly the latency that risks losing the race.
                let _ = child.kill();
                killed = true;
                if let Some(python_pid) = python_pid {
                    cleanup_python_child(python_pid);
                }
            }
        }
    }

    // Drain any remaining output (bounded by EXIT_TIMEOUT) so `DONE` is
    // observed if the worker finished right as the kill was issued.
    if !saw_done {
        let deadline = Instant::now() + EXIT_TIMEOUT;
        while Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(line) if line == "DONE" => {
                    saw_done = true;
                    break;
                }
                Ok(_) => continue,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !killed {
                        continue;
                    }
                    break;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    let _ = child.wait();
    let _ = reader.join();

    // Reopen fresh with a short retry loop: on Windows, releasing the RocksDB
    // LOCK file after a killed process's handle closes can lag the process
    // actually disappearing from the OS's own bookkeeping by a beat.
    let mut store = None;
    for _ in 0..20 {
        match RocksKv::open(&db_path) {
            Ok(s) => {
                store = Some(s);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    let store = store.expect("reopen database after worker exit");
    let index = TemporalIndex::new(&store);

    let edge_present = index
        .edge_as_of(SRC, EDGE_TYPE, DST, Timestamp(ts))
        .expect("edge_as_of")
        .is_some();
    let src_embedding = index
        .embedding_as_of(SRC, Timestamp(ts))
        .expect("embedding_as_of src");
    let dst_embedding = index
        .embedding_as_of(DST, Timestamp(ts))
        .expect("embedding_as_of dst");

    let outcome = match (
        edge_present,
        src_embedding.is_some(),
        dst_embedding.is_some(),
    ) {
        (false, false, false) => Outcome::FullyUncommitted,
        (true, true, true) => Outcome::FullyCommitted,
        (edge, src, dst) => panic!(
            "iteration {iteration}: non-atomic state — edge_present={edge} \
             src_embedding_present={src} dst_embedding_present={dst} \
             (killed={killed}, saw_done={saw_done})"
        ),
    };

    (outcome, killed)
}

#[test]
fn atomic_commit_survives_a_kill_at_any_point_in_the_workers_lifetime() {
    let mut rng = Rng::new(0xFA07_1E5EC0FFEEu64);
    let mut committed = 0u32;
    let mut uncommitted = 0u32;
    let mut killed_count = 0u32;

    for i in 0..ITERATIONS {
        let (outcome, killed) = run_one_iteration(i, &mut rng);
        match outcome {
            Outcome::FullyCommitted => committed += 1,
            Outcome::FullyUncommitted => uncommitted += 1,
        }
        if killed {
            killed_count += 1;
        }
    }

    eprintln!(
        "{ITERATIONS} iterations: {killed_count} killed, {committed} fully committed, \
         {uncommitted} fully uncommitted, 0 non-atomic (a violation would have panicked above)"
    );

    // The invariant itself is checked per-iteration inside run_one_iteration
    // (a violation panics there, failing this test immediately). This is a
    // sanity check on the harness, not the invariant: if nothing was ever
    // actually killed, the race never had a chance to manifest and the run
    // proves nothing.
    assert!(
        killed_count > 0,
        "no iteration was actually killed — the race never exercised anything"
    );
}
