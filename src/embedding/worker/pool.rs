//! Supervised pool of `embedding_server.py` worker processes.
//!
//! What this replaces: one Python child per model behind two `Mutex`es, no
//! respawn, no deadline. A single dead worker failed every later mutation until
//! the whole server restarted, and the separate stdin/stdout locks even let two
//! concurrent callers swap each other's responses.
//!
//! Model: N *slot threads*. Each slot thread exclusively owns one Python
//! process (its pipes are never shared, so request/response pairing is correct
//! by construction), pulls jobs from one shared bounded queue (natural load
//! balancing across idle slots), and respawns its process with exponential
//! backoff when it dies. On every (re)spawn the checkpoint's SHA-256 and
//! self-reported architecture are re-verified against the manifest.
//!
//! Failure handling:
//! * **Deadline.** A caller that times out kills the worker handling its job
//!   (the pipe state is unknown mid-frame, so killing is the only safe
//!   recovery); the slot thread observes EOF and respawns.
//! * **Retry.** A job whose worker *died* is retried once; a timeout is not
//!   retried (a 2-minute forward pass is not repeated blindly).
//! * **Circuit breaker.** K worker failures within a window open the breaker:
//!   requests fail fast with `Unavailable` until a cooldown elapses.
//! * **Model errors** (the worker answered `ERROR`, e.g. a malformed request)
//!   do not kill the worker and do not count toward the breaker.
//!
//! There is deliberately no active PING: reading a pong needs a deadline on a
//! blocking pipe read, which std cannot give. Idle crashes are caught by
//! polling `try_wait`; an idle *hang* is caught by the next request's deadline.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::embedding::worker::protocol::{self, ty, PROTOCOL_VERSION};
use crate::error::{CareGraphError, Result};

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub workers: usize,
    pub forward_timeout: Duration,
    pub attribution_timeout: Duration,
    pub queue_depth: usize,
    pub breaker_failures: u32,
    pub breaker_window: Duration,
    pub breaker_cooldown: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// How long a spawned worker may take to import torch and report ready.
    pub spawn_timeout: Duration,
    /// `torch.set_num_threads` per worker; 0 leaves torch's own default.
    pub torch_threads: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            workers: 1,
            forward_timeout: Duration::from_secs(120),
            attribution_timeout: Duration::from_secs(900),
            queue_depth: 256,
            breaker_failures: 5,
            breaker_window: Duration::from_secs(60),
            breaker_cooldown: Duration::from_secs(15),
            backoff_min: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            spawn_timeout: Duration::from_secs(180),
            torch_threads: 0,
        }
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

impl PoolConfig {
    /// `CAREGRAPH_WORKERS`, `CAREGRAPH_TORCH_THREADS`,
    /// `CAREGRAPH_FORWARD_TIMEOUT_SECS`, `CAREGRAPH_ATTRIBUTION_TIMEOUT_SECS`,
    /// `CAREGRAPH_WORKER_QUEUE_DEPTH`, `CAREGRAPH_BREAKER_FAILURES`,
    /// `CAREGRAPH_BREAKER_COOLDOWN_SECS`.
    pub fn from_env() -> Self {
        let mut c = PoolConfig::default();
        if let Some(v) = env_u64("CAREGRAPH_WORKERS") {
            c.workers = v.max(1) as usize;
        }
        if let Some(v) = env_u64("CAREGRAPH_TORCH_THREADS") {
            c.torch_threads = v as usize;
        }
        if let Some(v) = env_u64("CAREGRAPH_FORWARD_TIMEOUT_SECS") {
            c.forward_timeout = Duration::from_secs(v);
        }
        if let Some(v) = env_u64("CAREGRAPH_ATTRIBUTION_TIMEOUT_SECS") {
            c.attribution_timeout = Duration::from_secs(v);
        }
        if let Some(v) = env_u64("CAREGRAPH_WORKER_QUEUE_DEPTH") {
            c.queue_depth = v.max(1) as usize;
        }
        if let Some(v) = env_u64("CAREGRAPH_BREAKER_FAILURES") {
            c.breaker_failures = v.max(1) as u32;
        }
        if let Some(v) = env_u64("CAREGRAPH_BREAKER_COOLDOWN_SECS") {
            c.breaker_cooldown = Duration::from_secs(v);
        }
        c
    }

    /// Refuse a configuration that oversubscribes the CPU: N workers each
    /// running `torch_threads` intra-op threads on fewer cores thrashes.
    pub fn validate(&self) -> Result<()> {
        if self.workers == 0 {
            return Err(CareGraphError::Unavailable(
                "CAREGRAPH_WORKERS must be >= 1".into(),
            ));
        }
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        if self.torch_threads > 0 && self.workers * self.torch_threads > cpus {
            return Err(CareGraphError::Io(std::io::Error::other(format!(
                "{} workers x {} torch threads = {} exceeds the {cpus} available CPUs",
                self.workers,
                self.torch_threads,
                self.workers * self.torch_threads
            ))));
        }
        Ok(())
    }
}

/// What a freshly spawned worker must be, checked on every spawn.
#[derive(Debug, Clone)]
pub struct Expected {
    pub model_id: String,
    pub architecture: String,
    pub is_associative: bool,
    /// Lower-case hex SHA-256 of `model.pt`, from the manifest.
    pub model_sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Handshake {
    pub ready: bool,
    #[serde(default)]
    pub protocol: u32,
    #[serde(default)]
    pub architecture: Option<String>,
    #[serde(default)]
    pub is_associative: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PoolStats {
    pub workers: usize,
    pub healthy: usize,
    pub restarts: u64,
    pub timeouts: u64,
    pub worker_failures: u64,
    pub breaker_open: bool,
}

enum JobError {
    Died(String),
    Model(String),
}

struct Job {
    payload: Vec<u8>,
    reply: Sender<std::result::Result<Vec<u8>, JobError>>,
    cancelled: Arc<AtomicBool>,
    /// The child currently running this job, so a timed-out caller can kill it.
    assigned: Arc<Mutex<Option<Arc<Mutex<Child>>>>>,
}

#[derive(Default)]
struct Breaker {
    failures: VecDeque<Instant>,
    open_until: Option<Instant>,
}

struct Shared {
    model_dir: PathBuf,
    script: PathBuf,
    python: String,
    expected: Expected,
    cfg: PoolConfig,
    jobs_rx: Receiver<Job>,
    shutdown: AtomicBool,
    healthy: AtomicUsize,
    restarts: AtomicU64,
    timeouts: AtomicU64,
    failures: AtomicU64,
    breaker: Mutex<Breaker>,
    pids: Mutex<Vec<Option<u32>>>,
    children: Mutex<Vec<Option<Arc<Mutex<Child>>>>>,
}

impl Shared {
    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut b = self.breaker.lock().unwrap_or_else(|e| e.into_inner());
        b.failures.push_back(now);
        while b
            .failures
            .front()
            .is_some_and(|t| now.duration_since(*t) > self.cfg.breaker_window)
        {
            b.failures.pop_front();
        }
        if b.failures.len() as u32 >= self.cfg.breaker_failures {
            b.open_until = Some(now + self.cfg.breaker_cooldown);
            tracing::error!(
                model = %self.expected.model_id,
                failures = b.failures.len(),
                cooldown_secs = self.cfg.breaker_cooldown.as_secs(),
                "embedding circuit breaker OPEN"
            );
        }
    }

    fn record_success(&self) {
        let mut b = self.breaker.lock().unwrap_or_else(|e| e.into_inner());
        b.failures.clear();
        b.open_until = None;
    }

    fn breaker_open(&self) -> bool {
        let b = self.breaker.lock().unwrap_or_else(|e| e.into_inner());
        b.open_until.is_some_and(|t| Instant::now() < t)
    }
}

struct Live {
    child: Arc<Mutex<Child>>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    pid: u32,
}

fn sha256_hex(path: &Path) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    let digest = Sha256::digest(&bytes);
    Ok(digest.iter().map(|b| format!("{b:02x}")).collect())
}

fn start_child(shared: &Arc<Shared>, idx: usize) -> Result<(Live, Handshake)> {
    let ckpt = shared.model_dir.join("model.pt");
    if !shared.expected.model_sha256.is_empty() {
        let actual = sha256_hex(&ckpt)?;
        if !actual.eq_ignore_ascii_case(&shared.expected.model_sha256) {
            return Err(CareGraphError::Io(std::io::Error::other(format!(
                "{}: model.pt SHA-256 is {actual} but the manifest records {} — refusing to \
                 serve a checkpoint that is not the one that was deployed",
                shared.expected.model_id, shared.expected.model_sha256
            ))));
        }
    }

    let mut cmd = Command::new(&shared.python);
    cmd.arg(&shared.script)
        .arg(&shared.model_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if shared.cfg.torch_threads > 0 {
        cmd.env(
            "CAREGRAPH_TORCH_THREADS",
            shared.cfg.torch_threads.to_string(),
        );
    }
    let mut child = cmd.spawn().map_err(CareGraphError::Io)?;
    let pid = child.id();
    let stdin = child.stdin.take().expect("piped stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("piped stdout"));
    let stderr = child.stderr.take().expect("piped stderr");
    let child = Arc::new(Mutex::new(child));

    // Worker stderr becomes structured log lines, never raw inherited output.
    {
        let model = shared.expected.model_id.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(|l| l.ok()) {
                if !line.trim().is_empty() {
                    tracing::warn!(target: "embedding_worker", model = %model, worker = idx, "{line}");
                }
            }
        });
    }

    // Watchdog: a worker that never reports ready (import hang) is killed.
    let ready = Arc::new(AtomicBool::new(false));
    {
        let child = child.clone();
        let ready = ready.clone();
        let limit = shared.cfg.spawn_timeout;
        std::thread::spawn(move || {
            let start = Instant::now();
            while start.elapsed() < limit {
                if ready.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            if !ready.load(Ordering::Relaxed) {
                let _ = child.lock().map(|mut c| c.kill());
            }
        });
    }

    let (t, payload) = protocol::read_frame(&mut stdout).map_err(|e| {
        CareGraphError::Io(std::io::Error::other(format!(
            "worker exited or timed out before reporting ready: {e}"
        )))
    })?;
    ready.store(true, Ordering::Relaxed);
    let kill = |child: &Arc<Mutex<Child>>| {
        let _ = child.lock().map(|mut c| c.kill());
    };
    if t != ty::HANDSHAKE {
        kill(&child);
        return Err(CareGraphError::Io(std::io::Error::other(
            "first frame from the worker was not a handshake",
        )));
    }
    let hs: Handshake = serde_json::from_slice(&payload)?;
    let fail = |msg: String| -> CareGraphError { CareGraphError::Io(std::io::Error::other(msg)) };
    let check = || -> Result<()> {
        if !hs.ready {
            return Err(fail("worker did not report ready".into()));
        }
        if hs.protocol != PROTOCOL_VERSION {
            return Err(fail(format!(
                "worker speaks protocol {}, this build speaks {PROTOCOL_VERSION}",
                hs.protocol
            )));
        }
        if let Some(arch) = &hs.architecture {
            if arch != &shared.expected.architecture {
                return Err(fail(format!(
                    "{}: manifest declares architecture {:?} but the worker loaded model.pt \
                     as {arch:?}",
                    shared.expected.model_id, shared.expected.architecture
                )));
            }
        }
        if let Some(assoc) = hs.is_associative {
            if assoc != shared.expected.is_associative {
                return Err(fail(format!(
                    "{}: manifest says is_associative={} but the checkpoint self-reports {assoc}",
                    shared.expected.model_id, shared.expected.is_associative
                )));
            }
        }
        Ok(())
    };
    if let Err(e) = check() {
        kill(&child);
        return Err(e);
    }
    Ok((
        Live {
            child,
            stdin,
            stdout,
            pid,
        },
        hs,
    ))
}

fn jitter(base: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    base + base.mul_f64((nanos % 250) as f64 / 1000.0)
}

fn slot_main(shared: Arc<Shared>, idx: usize, first_ready: Sender<Result<Handshake>>) {
    let mut first = Some(first_ready);
    let mut backoff = shared.cfg.backoff_min;
    while !shared.shutdown.load(Ordering::Relaxed) {
        let (mut live, hs) = match start_child(&shared, idx) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(model = %shared.expected.model_id, worker = idx, error = %e, "worker spawn failed");
                if let Some(tx) = first.take() {
                    let _ = tx.send(Err(e));
                    return; // a pool that cannot start at all is a startup error
                }
                shared.record_failure();
                std::thread::sleep(jitter(backoff));
                backoff = (backoff * 2).min(shared.cfg.backoff_max);
                continue;
            }
        };
        backoff = shared.cfg.backoff_min;
        shared.pids.lock().unwrap_or_else(|e| e.into_inner())[idx] = Some(live.pid);
        shared.children.lock().unwrap_or_else(|e| e.into_inner())[idx] = Some(live.child.clone());
        shared.healthy.fetch_add(1, Ordering::Relaxed);
        if let Some(tx) = first.take() {
            let _ = tx.send(Ok(hs));
        } else {
            tracing::info!(model = %shared.expected.model_id, worker = idx, pid = live.pid, "worker respawned");
        }

        'serve: loop {
            match shared.jobs_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(job) => {
                    if job.cancelled.load(Ordering::Relaxed) {
                        continue;
                    }
                    *job.assigned.lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(live.child.clone());
                    if let Err(e) =
                        protocol::write_frame(&mut live.stdin, ty::FORWARD, &job.payload)
                    {
                        let _ = job
                            .reply
                            .send(Err(JobError::Died(format!("write failed: {e}"))));
                        break 'serve;
                    }
                    match protocol::read_frame(&mut live.stdout) {
                        Ok((ty::FORWARD_OK, payload)) => {
                            let _ = job.reply.send(Ok(payload));
                        }
                        Ok((ty::ERROR, msg)) => {
                            let _ = job.reply.send(Err(JobError::Model(
                                String::from_utf8_lossy(&msg).into_owned(),
                            )));
                        }
                        Ok((other, _)) => {
                            let _ = job.reply.send(Err(JobError::Died(format!(
                                "unexpected frame type {other:#x}"
                            ))));
                            break 'serve;
                        }
                        Err(e) => {
                            let _ = job
                                .reply
                                .send(Err(JobError::Died(format!("worker died mid-request: {e}"))));
                            break 'serve;
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    let exited = live
                        .child
                        .lock()
                        .map(|mut c| c.try_wait().map(|s| s.is_some()).unwrap_or(true))
                        .unwrap_or(true);
                    if exited {
                        tracing::error!(model = %shared.expected.model_id, worker = idx, "worker died while idle");
                        break 'serve;
                    }
                    if shared.shutdown.load(Ordering::Relaxed) {
                        break 'serve;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break 'serve,
            }
        }

        shared.healthy.fetch_sub(1, Ordering::Relaxed);
        shared.pids.lock().unwrap_or_else(|e| e.into_inner())[idx] = None;
        let _ = live.child.lock().map(|mut c| {
            let _ = c.kill();
            let _ = c.wait();
        });
        if shared.shutdown.load(Ordering::Relaxed) {
            return;
        }
        shared.restarts.fetch_add(1, Ordering::Relaxed);
        shared.record_failure();
        std::thread::sleep(jitter(backoff));
        backoff = (backoff * 2).min(shared.cfg.backoff_max);
    }
}

pub struct WorkerPool {
    shared: Arc<Shared>,
    jobs_tx: Option<Sender<Job>>,
    handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    /// Starts `cfg.workers` slot threads and blocks until every slot's first
    /// worker has reported ready (or any of them fails, which is an error).
    pub fn spawn(
        model_dir: PathBuf,
        script: PathBuf,
        python: String,
        expected: Expected,
        cfg: PoolConfig,
    ) -> Result<(Self, Handshake)> {
        cfg.validate()?;
        let (jobs_tx, jobs_rx) = bounded::<Job>(cfg.queue_depth);
        let n = cfg.workers;
        let shared = Arc::new(Shared {
            model_dir,
            script,
            python,
            expected,
            cfg,
            jobs_rx,
            shutdown: AtomicBool::new(false),
            healthy: AtomicUsize::new(0),
            restarts: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            breaker: Mutex::new(Breaker::default()),
            pids: Mutex::new(vec![None; n]),
            children: Mutex::new(vec![None; n]),
        });
        let mut pool = WorkerPool {
            shared: shared.clone(),
            jobs_tx: Some(jobs_tx),
            handles: Vec::new(),
        };
        let (ready_tx, ready_rx) = bounded::<Result<Handshake>>(n);
        for idx in 0..n {
            let s = shared.clone();
            let tx = ready_tx.clone();
            pool.handles.push(
                std::thread::Builder::new()
                    .name(format!("embed-worker-{idx}"))
                    .spawn(move || slot_main(s, idx, tx))
                    .map_err(CareGraphError::Io)?,
            );
        }
        drop(ready_tx);
        let mut first_hs = None;
        for _ in 0..n {
            match ready_rx.recv() {
                Ok(Ok(hs)) => first_hs = Some(hs),
                Ok(Err(e)) => return Err(e), // Drop of `pool` tears the rest down
                Err(_) => {
                    return Err(CareGraphError::Io(std::io::Error::other(
                        "a worker slot exited before reporting ready",
                    )))
                }
            }
        }
        Ok((pool, first_hs.expect("n >= 1")))
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            workers: self.shared.cfg.workers,
            healthy: self.shared.healthy.load(Ordering::Relaxed),
            restarts: self.shared.restarts.load(Ordering::Relaxed),
            timeouts: self.shared.timeouts.load(Ordering::Relaxed),
            worker_failures: self.shared.failures.load(Ordering::Relaxed),
            breaker_open: self.shared.breaker_open(),
        }
    }

    pub fn worker_pids(&self) -> Vec<u32> {
        self.shared
            .pids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    pub fn forward_timeout(&self) -> Duration {
        self.shared.cfg.forward_timeout
    }

    pub fn attribution_timeout(&self) -> Duration {
        self.shared.cfg.attribution_timeout
    }

    /// Submit one FORWARD payload and wait up to `timeout`. A worker that died
    /// is retried once; a timeout is not.
    pub fn submit(&self, payload: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        match self.submit_once(&payload, timeout) {
            Err(Failure::Died(first)) => {
                tracing::warn!(model = %self.shared.expected.model_id, error = %first, "worker died; waiting for a replacement, then retrying once");
                // A single-worker pool has zero healthy workers until the
                // respawn finishes (torch import takes seconds). Wait for it,
                // bounded by the caller's own deadline, instead of failing a
                // request that a replacement could serve moments later.
                let deadline = Instant::now() + timeout;
                while self.shared.healthy.load(Ordering::Relaxed) == 0
                    && !self.shared.breaker_open()
                    && Instant::now() < deadline
                {
                    std::thread::sleep(Duration::from_millis(100));
                }
                self.submit_once(&payload, timeout)
                    .map_err(Failure::into_error)
            }
            other => other.map_err(Failure::into_error),
        }
    }

    fn submit_once(
        &self,
        payload: &[u8],
        timeout: Duration,
    ) -> std::result::Result<Vec<u8>, Failure> {
        if self.shared.breaker_open() {
            return Err(Failure::Unavailable(format!(
                "{}: circuit breaker open after repeated worker failures",
                self.shared.expected.model_id
            )));
        }
        if self.shared.healthy.load(Ordering::Relaxed) == 0 {
            return Err(Failure::Unavailable(format!(
                "{}: no healthy embedding workers (restarting)",
                self.shared.expected.model_id
            )));
        }
        let (reply_tx, reply_rx) = bounded(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let assigned = Arc::new(Mutex::new(None));
        let job = Job {
            payload: payload.to_vec(),
            reply: reply_tx,
            cancelled: cancelled.clone(),
            assigned: assigned.clone(),
        };
        match self.jobs_tx.as_ref().expect("pool alive").try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(Failure::Unavailable(format!(
                    "{}: embedding queue full ({} waiting)",
                    self.shared.expected.model_id, self.shared.cfg.queue_depth
                )))
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(Failure::Unavailable("embedding pool is shut down".into()))
            }
        }
        match reply_rx.recv_timeout(timeout) {
            Ok(Ok(bytes)) => {
                self.shared.record_success();
                Ok(bytes)
            }
            Ok(Err(JobError::Model(m))) => Err(Failure::Model(m)),
            Ok(Err(JobError::Died(m))) => {
                // The slot thread records the failure itself when it respawns.
                Err(Failure::Died(m))
            }
            Err(_) => {
                cancelled.store(true, Ordering::Relaxed);
                if let Some(child) = assigned.lock().unwrap_or_else(|e| e.into_inner()).take() {
                    let _ = child.lock().map(|mut c| c.kill());
                }
                self.shared.timeouts.fetch_add(1, Ordering::Relaxed);
                Err(Failure::Timeout(timeout))
            }
        }
    }
}

enum Failure {
    Died(String),
    Timeout(Duration),
    Model(String),
    Unavailable(String),
}

impl Failure {
    fn into_error(self) -> CareGraphError {
        match self {
            Failure::Died(m) => CareGraphError::Unavailable(format!("embedding worker died: {m}")),
            Failure::Timeout(d) => CareGraphError::Timeout(format!(
                "embedding forward pass exceeded {}s; the worker was killed and is restarting",
                d.as_secs()
            )),
            Failure::Model(m) => {
                CareGraphError::Io(std::io::Error::other(format!("embedding_server.py: {m}")))
            }
            Failure::Unavailable(m) => CareGraphError::Unavailable(m),
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Relaxed);
        self.jobs_tx.take(); // disconnects the queue
        for c in self
            .shared
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .flatten()
        {
            let _ = c.lock().map(|mut c| c.kill()); // unblocks any in-flight read
        }
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}
