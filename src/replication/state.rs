//! A node's replication state: its role, its epoch, how far a follower has
//! applied, which followers a leader has, and the synchronous-acknowledgement
//! gate.
//!
//! # Roles
//!
//! | Role | Writes | Serves the stream | Applies the stream |
//! |---|---|---|---|
//! | `Standalone` | yes | no | no |
//! | `Leader` | yes | yes | no |
//! | `Follower` | no | no | yes |
//! | `Fenced` | no | no | no |
//!
//! A **fenced** node is a deposed leader. It refuses writes and refuses to stream
//! or to follow: its database may hold writes the new leader never saw, so it
//! must be re-bootstrapped from a checkpoint of the current leader before it
//! rejoins. The role is persisted beside the database so a restart cannot
//! resurrect it.
//!
//! # Synchronous acknowledgement
//!
//! With `min_acks = 1` a write is acknowledged to its client only after a standby
//! confirms it has durably applied it. If **no** standby is connected and caught
//! up when the write arrives, the leader degrades to asynchronous (counted,
//! exported as `replication_sync_degraded`, meant to alert) instead of refusing
//! every write while the standby is down. If a standby *was* eligible and does not
//! confirm in time, or disappears while the leader waits, the write is durable on
//! the leader but its replication is unconfirmed: the client gets
//! `DEADLINE_EXCEEDED` and retries with the same `request_id`, which is
//! idempotent.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Context;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Standalone,
    Leader,
    Follower,
    /// Handed over by the operator: refuses writes, still streams so the
    /// standby can finish catching up, must be re-bootstrapped to rejoin.
    Demoted,
    /// Deposed without ceremony (a newer epoch was discovered): refuses writes
    /// and the stream.
    Fenced,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Standalone => "standalone",
            Role::Leader => "leader",
            Role::Follower => "follower",
            Role::Demoted => "demoted",
            Role::Fenced => "fenced",
        }
    }

    pub fn parse(s: &str) -> Option<Role> {
        match s.to_ascii_lowercase().as_str() {
            "standalone" => Some(Role::Standalone),
            "leader" => Some(Role::Leader),
            "follower" => Some(Role::Follower),
            "demoted" => Some(Role::Demoted),
            "fenced" => Some(Role::Fenced),
            _ => None,
        }
    }
}

/// Why a write was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRefusal {
    /// A read replica.
    Follower,
    /// A deposed leader awaiting re-bootstrap.
    Fenced,
}

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub role: Role,
    pub replica_id: String,
    /// Standby confirmations a write waits for. 0 is asynchronous replication.
    pub min_acks: usize,
    /// Longest a write waits for them.
    pub ack_timeout: Duration,
    /// A standby more than this many sequence numbers behind is not counted as
    /// a synchronous standby.
    pub sync_lag_threshold: u64,
    /// A follower reports not-ready when this many sequence numbers behind.
    pub max_lag_for_ready: u64,
    pub leader_addr: Option<String>,
    /// Where the role is persisted. `None` keeps it in memory only (tests).
    pub state_file: Option<PathBuf>,
    /// Scratch directory, on the database's own volume, where `FetchCheckpoint`
    /// builds the copy it streams. `None` disables serving checkpoints.
    pub bootstrap_dir: Option<PathBuf>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        ReplicationConfig {
            role: Role::Standalone,
            replica_id: "replica".to_string(),
            min_acks: 0,
            ack_timeout: Duration::from_secs(5),
            sync_lag_threshold: 10_000,
            max_lag_for_ready: 10_000,
            leader_addr: None,
            state_file: None,
            bootstrap_dir: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StateFile {
    role: Role,
    epoch: u64,
}

#[derive(Debug, Clone, Copy)]
struct Progress {
    acked: u64,
    last_seen: Instant,
}

struct Mutable {
    role: Role,
    followers: HashMap<String, Progress>,
}

/// How a synchronous wait ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// Enough standbys confirmed (or replication is asynchronous).
    Acked,
    /// No standby was eligible when the write arrived: proceeded asynchronously.
    Degraded,
    /// An eligible standby did not confirm in time or went away.
    Unconfirmed,
}

struct Metrics {
    epoch: IntGauge,
    applied: IntGauge,
    lag: IntGauge,
    followers: IntGauge,
    degraded: IntGauge,
    needs_bootstrap: IntGauge,
    batches_applied: IntCounter,
    degraded_writes: IntCounter,
    ack_timeouts: IntCounter,
    ack_wait: Histogram,
}

pub struct ReplicationState {
    cfg: ReplicationConfig,
    inner: Mutex<Mutable>,
    epoch: AtomicU64,
    /// Follower: newest applied sequence. Leader: not used (the database knows).
    applied: AtomicU64,
    /// Follower: newest leader sequence reported by the stream.
    leader_seq: AtomicU64,
    needs_bootstrap: AtomicBool,
    connected_to_leader: AtomicBool,
    rx_count: AtomicU64,
    last_rx: Mutex<Option<Instant>>,
    acks: Notify,
    applied_changed: Notify,
    /// Held while a follower applies a batch, so a promotion can be certain no
    /// batch is half-applied when it takes over.
    apply_lock: tokio::sync::Mutex<()>,
    /// Set while a checkpoint is being streamed to a new replica.
    transferring: AtomicBool,
    metrics: Metrics,
}

/// The one checkpoint-transfer slot; released on drop.
pub struct TransferSlot(std::sync::Arc<ReplicationState>);

impl Drop for TransferSlot {
    fn drop(&mut self) {
        self.0.transferring.store(false, Ordering::Release);
    }
}

impl ReplicationState {
    /// `epoch` is the highest epoch recorded in the database. A persisted role
    /// file takes precedence over `cfg.role` (a promoted standby stays leader
    /// across restarts) unless replication is disabled.
    pub fn open(cfg: ReplicationConfig, epoch: u64) -> anyhow::Result<std::sync::Arc<Self>> {
        let mut role = cfg.role;
        let mut epoch = epoch;
        if cfg.role != Role::Standalone {
            if let Some(path) = &cfg.state_file {
                match std::fs::read(path) {
                    Ok(bytes) => {
                        let file: StateFile = serde_json::from_slice(&bytes)
                            .with_context(|| format!("reading {}", path.display()))?;
                        if file.role != cfg.role {
                            tracing::warn!(
                                configured = cfg.role.name(),
                                persisted = file.role.name(),
                                "the persisted replication role overrides the configured one"
                            );
                        }
                        role = file.role;
                        epoch = epoch.max(file.epoch);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
                }
            }
        }
        let metrics = Metrics {
            epoch: IntGauge::new("replication_epoch", "Leadership epoch this node knows")?,
            applied: IntGauge::new(
                "replication_applied_sequence",
                "Follower: newest applied sequence number",
            )?,
            lag: IntGauge::new(
                "replication_lag_sequences",
                "Follower: sequence numbers behind the leader",
            )?,
            followers: IntGauge::new(
                "replication_connected_followers",
                "Leader: followers currently streaming",
            )?,
            degraded: IntGauge::new(
                "replication_sync_degraded",
                "1 when the last write proceeded without a synchronous standby",
            )?,
            needs_bootstrap: IntGauge::new(
                "replication_needs_bootstrap",
                "1 when this follower must be re-bootstrapped from a checkpoint",
            )?,
            batches_applied: IntCounter::new(
                "replication_batches_applied_total",
                "Follower: batches applied from the leader",
            )?,
            degraded_writes: IntCounter::new(
                "replication_degraded_writes_total",
                "Writes acknowledged without a synchronous standby",
            )?,
            ack_timeouts: IntCounter::new(
                "replication_ack_timeouts_total",
                "Writes whose standby confirmation did not arrive in time",
            )?,
            ack_wait: Histogram::with_opts(
                HistogramOpts::new(
                    "replication_ack_wait_seconds",
                    "Time a write waited for a standby confirmation",
                )
                .buckets(vec![
                    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ]),
            )?,
        };
        metrics.epoch.set(epoch as i64);
        Ok(std::sync::Arc::new(ReplicationState {
            cfg,
            inner: Mutex::new(Mutable {
                role,
                followers: HashMap::new(),
            }),
            epoch: AtomicU64::new(epoch),
            applied: AtomicU64::new(0),
            leader_seq: AtomicU64::new(0),
            needs_bootstrap: AtomicBool::new(false),
            connected_to_leader: AtomicBool::new(false),
            rx_count: AtomicU64::new(0),
            last_rx: Mutex::new(None),
            acks: Notify::new(),
            applied_changed: Notify::new(),
            apply_lock: tokio::sync::Mutex::new(()),
            transferring: AtomicBool::new(false),
            metrics,
        }))
    }

    /// A node with replication disabled.
    pub fn standalone() -> std::sync::Arc<Self> {
        Self::open(ReplicationConfig::default(), 0).expect("no state file is read")
    }

    pub fn register_metrics(&self, registry: &Registry) -> prometheus::Result<()> {
        let m = &self.metrics;
        registry.register(Box::new(m.epoch.clone()))?;
        registry.register(Box::new(m.applied.clone()))?;
        registry.register(Box::new(m.lag.clone()))?;
        registry.register(Box::new(m.followers.clone()))?;
        registry.register(Box::new(m.degraded.clone()))?;
        registry.register(Box::new(m.needs_bootstrap.clone()))?;
        registry.register(Box::new(m.batches_applied.clone()))?;
        registry.register(Box::new(m.degraded_writes.clone()))?;
        registry.register(Box::new(m.ack_timeouts.clone()))?;
        registry.register(Box::new(m.ack_wait.clone()))?;
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, Mutable> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn apply_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.apply_lock
    }

    pub fn config(&self) -> &ReplicationConfig {
        &self.cfg
    }

    pub fn role(&self) -> Role {
        self.lock().role
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn persist(&self, role: Role) {
        let Some(path) = &self.cfg.state_file else {
            return;
        };
        let file = StateFile {
            role,
            epoch: self.epoch(),
        };
        let tmp = path.with_extension("tmp");
        let result = serde_json::to_vec(&file)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(&tmp, bytes))
            .and_then(|_| std::fs::rename(&tmp, path));
        if let Err(e) = result {
            // A role that cannot be remembered is a safety problem (a fenced
            // node could come back as leader), so say so loudly.
            tracing::error!(error = %e, path = %path.display(), "could not persist the replication role");
        }
    }

    /// `Ok` when this node may accept a client write.
    pub fn writable(&self) -> Result<(), WriteRefusal> {
        match self.role() {
            Role::Standalone | Role::Leader => Ok(()),
            Role::Follower => Err(WriteRefusal::Follower),
            Role::Demoted | Role::Fenced => Err(WriteRefusal::Fenced),
        }
    }

    /// Takes the single checkpoint-transfer slot, or `None` if one is running.
    pub fn begin_checkpoint_transfer(self: &std::sync::Arc<Self>) -> Option<TransferSlot> {
        self.transferring
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| TransferSlot(self.clone()))
    }

    /// Whether this node may serve the replication stream.
    pub fn serves_stream(&self) -> bool {
        matches!(self.role(), Role::Leader | Role::Demoted)
    }

    pub fn note_epoch(&self, epoch: u64) {
        self.epoch.fetch_max(epoch, Ordering::AcqRel);
        self.metrics.epoch.set(self.epoch() as i64);
    }

    /// Makes this node the leader of `epoch`.
    pub fn become_leader(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Release);
        self.metrics.epoch.set(epoch as i64);
        self.needs_bootstrap.store(false, Ordering::Release);
        self.metrics.needs_bootstrap.set(0);
        self.lock().role = Role::Leader;
        self.persist(Role::Leader);
    }

    /// The operator hands leadership over: writes stop now, the stream stays
    /// up until the standby has caught up. Persisted, like a fence.
    pub fn demote(&self) {
        tracing::warn!("this node was demoted by the operator");
        self.lock().role = Role::Demoted;
        self.persist(Role::Demoted);
        self.acks.notify_waiters();
    }

    /// Fences this node: it refuses writes and the stream from now on, across
    /// restarts.
    pub fn fence(&self, reason: &str) {
        tracing::error!(
            reason,
            "this node is fenced: it refuses writes until re-bootstrapped"
        );
        self.lock().role = Role::Fenced;
        self.persist(Role::Fenced);
        self.acks.notify_waiters();
    }

    // ---- follower progress -------------------------------------------------

    pub fn set_applied(&self, seq: u64) {
        self.applied.store(seq, Ordering::Release);
        self.metrics.applied.set(seq as i64);
        self.refresh_lag();
        self.applied_changed.notify_waiters();
    }

    pub fn applied(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    pub fn note_batch_applied(&self, seq_after: u64, leader_seq: u64) {
        self.metrics.batches_applied.inc();
        self.leader_seq.fetch_max(leader_seq, Ordering::AcqRel);
        self.set_applied(seq_after);
    }

    pub fn note_leader_seq(&self, leader_seq: u64) {
        self.leader_seq.store(leader_seq, Ordering::Release);
        self.refresh_lag();
    }

    pub fn leader_seq(&self) -> u64 {
        self.leader_seq.load(Ordering::Acquire)
    }

    fn refresh_lag(&self) {
        self.metrics.lag.set(self.lag() as i64);
    }

    pub fn lag(&self) -> u64 {
        self.leader_seq().saturating_sub(self.applied())
    }

    pub fn set_connected(&self, connected: bool) {
        self.connected_to_leader.store(connected, Ordering::Release);
    }

    /// Records that a message (batch or heartbeat) arrived from the leader.
    pub fn touch_connection(&self) {
        self.rx_count.fetch_add(1, Ordering::Relaxed);
        *self.last_rx.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }

    pub fn messages_received(&self) -> u64 {
        self.rx_count.load(Ordering::Relaxed)
    }

    /// Connected, and the leader has been heard from recently (it heartbeats
    /// every second even when idle).
    fn connection_fresh(&self) -> bool {
        self.connected_to_leader.load(Ordering::Acquire)
            && self
                .last_rx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some_and(|t| t.elapsed() < Duration::from_secs(5))
    }

    pub fn mark_needs_bootstrap(&self, reason: &str) {
        tracing::error!(
            reason,
            "this follower must be re-bootstrapped from a leader checkpoint"
        );
        self.needs_bootstrap.store(true, Ordering::Release);
        self.metrics.needs_bootstrap.set(1);
    }

    pub fn needs_bootstrap(&self) -> bool {
        self.needs_bootstrap.load(Ordering::Acquire)
    }

    /// Whether this node should receive read traffic.
    pub fn ready_for_reads(&self) -> bool {
        match self.role() {
            Role::Standalone | Role::Leader => true,
            Role::Demoted | Role::Fenced => false,
            Role::Follower => {
                !self.needs_bootstrap()
                    && self.connection_fresh()
                    && self.lag() <= self.cfg.max_lag_for_ready
            }
        }
    }

    /// Why this node should not take traffic, if it should not.
    pub fn not_ready_reason(&self) -> Option<String> {
        if self.ready_for_reads() {
            return None;
        }
        Some(match self.role() {
            Role::Demoted | Role::Fenced => {
                format!(
                    "{}: must be re-bootstrapped before it serves again",
                    self.role().name()
                )
            }
            _ if self.needs_bootstrap() => {
                "replica must be re-bootstrapped from a leader checkpoint".to_string()
            }
            _ if !self.connection_fresh() => "replica is not connected to the leader".to_string(),
            _ => format!(
                "replica is {} sequence numbers behind the leader (limit {})",
                self.lag(),
                self.cfg.max_lag_for_ready
            ),
        })
    }

    /// Waits until this node has applied at least `seq` (read-your-writes on a
    /// replica). Returns whether it did within `timeout`.
    pub async fn wait_applied(&self, seq: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let notified = self.applied_changed.notified();
            tokio::pin!(notified);
            // Register before checking, or a notification between the check
            // and the wait is lost.
            notified.as_mut().enable();
            if self.applied() >= seq || self.role() != Role::Follower {
                return true;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    // ---- leader side -------------------------------------------------------

    /// Registers a streaming follower; it counts until the guard drops.
    pub fn follower_connected(
        self: &std::sync::Arc<Self>,
        id: &str,
        applied: u64,
    ) -> FollowerGuard {
        self.lock().followers.insert(
            id.to_string(),
            Progress {
                acked: applied,
                last_seen: Instant::now(),
            },
        );
        self.metrics
            .followers
            .set(self.lock().followers.len() as i64);
        self.acks.notify_waiters();
        FollowerGuard {
            state: self.clone(),
            id: id.to_string(),
        }
    }

    pub fn record_ack(&self, id: &str, seq: u64) {
        let mut inner = self.lock();
        if let Some(p) = inner.followers.get_mut(id) {
            p.acked = p.acked.max(seq);
            p.last_seen = Instant::now();
        }
        drop(inner);
        self.acks.notify_waiters();
    }

    pub fn follower_count(&self) -> usize {
        self.lock().followers.len()
    }

    /// `(id, acked sequence)` for each connected follower.
    pub fn followers(&self) -> Vec<(String, u64)> {
        let mut v: Vec<_> = self
            .lock()
            .followers
            .iter()
            .map(|(id, p)| (id.clone(), p.acked))
            .collect();
        v.sort();
        v
    }

    fn assess(&self, seq: u64, leader_latest: u64, started: bool) -> Assessment {
        if self.cfg.min_acks == 0 {
            return Assessment::Satisfied;
        }
        let inner = self.lock();
        let eligible: Vec<&Progress> = inner
            .followers
            .values()
            .filter(|p| leader_latest.saturating_sub(p.acked) <= self.cfg.sync_lag_threshold)
            .collect();
        if eligible.len() < self.cfg.min_acks {
            return if started {
                Assessment::Lost
            } else {
                Assessment::Degraded
            };
        }
        let confirmed = eligible.iter().filter(|p| p.acked >= seq).count();
        if confirmed >= self.cfg.min_acks {
            Assessment::Satisfied
        } else {
            Assessment::Waiting
        }
    }

    /// Waits for the configured number of standbys to confirm `seq`. `latest` is
    /// read afresh each round, so a standby that falls too far behind stops
    /// counting.
    pub async fn wait_acked(&self, seq: u64, latest: impl Fn() -> u64) -> AckOutcome {
        let started_at = Instant::now();
        let deadline = started_at + self.cfg.ack_timeout;
        let mut started = false;
        let outcome = loop {
            let notified = self.acks.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.assess(seq, latest(), started) {
                Assessment::Satisfied => break AckOutcome::Acked,
                Assessment::Degraded => break AckOutcome::Degraded,
                Assessment::Lost => break AckOutcome::Unconfirmed,
                Assessment::Waiting => started = true,
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break AckOutcome::Unconfirmed;
            };
            // Also wake periodically: a follower that stops acking without
            // disconnecting changes nothing observable otherwise.
            let _ = tokio::time::timeout(remaining.min(Duration::from_millis(250)), notified).await;
        };
        match outcome {
            AckOutcome::Acked => {
                self.metrics.degraded.set(0);
                if self.cfg.min_acks > 0 {
                    self.metrics
                        .ack_wait
                        .observe(started_at.elapsed().as_secs_f64());
                }
            }
            AckOutcome::Degraded => {
                self.metrics.degraded.set(1);
                self.metrics.degraded_writes.inc();
            }
            AckOutcome::Unconfirmed => self.metrics.ack_timeouts.inc(),
        }
        outcome
    }
}

enum Assessment {
    Satisfied,
    Degraded,
    Waiting,
    Lost,
}

/// Keeps a follower registered on the leader while its stream is open.
pub struct FollowerGuard {
    state: std::sync::Arc<ReplicationState>,
    id: String,
}

impl Drop for FollowerGuard {
    fn drop(&mut self) {
        let mut inner = self.state.lock();
        inner.followers.remove(&self.id);
        let n = inner.followers.len();
        drop(inner);
        self.state.metrics.followers.set(n as i64);
        self.state.acks.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn leader(min_acks: usize, timeout_ms: u64) -> Arc<ReplicationState> {
        ReplicationState::open(
            ReplicationConfig {
                role: Role::Leader,
                min_acks,
                ack_timeout: Duration::from_millis(timeout_ms),
                sync_lag_threshold: 100,
                ..ReplicationConfig::default()
            },
            1,
        )
        .unwrap()
    }

    #[test]
    fn only_a_leader_or_standalone_node_accepts_writes() {
        let s = ReplicationState::open(
            ReplicationConfig {
                role: Role::Follower,
                ..ReplicationConfig::default()
            },
            1,
        )
        .unwrap();
        assert_eq!(s.writable(), Err(WriteRefusal::Follower));
        s.become_leader(2);
        assert_eq!((s.writable(), s.epoch()), (Ok(()), 2));
        s.fence("test");
        assert_eq!(s.writable(), Err(WriteRefusal::Fenced));
        assert!(!s.serves_stream() && !s.ready_for_reads());
        assert!(ReplicationState::standalone().writable().is_ok());
    }

    #[tokio::test]
    async fn asynchronous_replication_never_waits() {
        let s = leader(0, 50);
        assert_eq!(s.wait_acked(10, || 10).await, AckOutcome::Acked);
    }

    #[test]
    fn only_one_checkpoint_transfer_runs_at_a_time_and_the_slot_is_released() {
        let s = ReplicationState::standalone();
        let first = s.begin_checkpoint_transfer().expect("the slot is free");
        assert!(
            s.begin_checkpoint_transfer().is_none(),
            "a second transfer must wait"
        );
        drop(first);
        assert!(
            s.begin_checkpoint_transfer().is_some(),
            "dropping the slot, however the transfer ended, frees it"
        );
    }

    #[tokio::test]
    async fn with_no_standby_connected_writes_degrade_to_asynchronous() {
        let s = leader(1, 50);
        assert_eq!(s.wait_acked(10, || 10).await, AckOutcome::Degraded);
    }

    #[tokio::test]
    async fn a_write_waits_for_the_standby_to_confirm_it() {
        let s = leader(1, 2_000);
        let _guard = s.follower_connected("b", 5);
        let waiter = {
            let s = s.clone();
            tokio::spawn(async move { s.wait_acked(10, || 10).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "must still be waiting: nothing acked yet"
        );
        s.record_ack("b", 9);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "an ack for 9 does not confirm 10");
        s.record_ack("b", 10);
        assert_eq!(waiter.await.unwrap(), AckOutcome::Acked);
    }

    #[tokio::test]
    async fn an_eligible_standby_that_stays_silent_times_the_write_out() {
        let s = leader(1, 100);
        let _guard = s.follower_connected("b", 5);
        let t = Instant::now();
        assert_eq!(s.wait_acked(10, || 10).await, AckOutcome::Unconfirmed);
        assert!(t.elapsed() >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn a_standby_that_disconnects_mid_wait_ends_the_wait_as_unconfirmed() {
        let s = leader(1, 5_000);
        let guard = s.follower_connected("b", 5);
        let waiter = {
            let s = s.clone();
            tokio::spawn(async move { s.wait_acked(10, || 10).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(guard);
        let t = Instant::now();
        assert_eq!(waiter.await.unwrap(), AckOutcome::Unconfirmed);
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "no waiting out the full timeout"
        );
    }

    #[tokio::test]
    async fn a_standby_far_behind_does_not_count_as_synchronous() {
        let s = leader(1, 100);
        let _guard = s.follower_connected("b", 5);
        // Leader is at 5_000; the standby's last ack was 5: 4_995 behind > 100.
        assert_eq!(s.wait_acked(5_000, || 5_000).await, AckOutcome::Degraded);
    }

    #[tokio::test]
    async fn a_replica_read_waits_until_it_has_applied_the_requested_sequence() {
        let s = ReplicationState::open(
            ReplicationConfig {
                role: Role::Follower,
                ..ReplicationConfig::default()
            },
            1,
        )
        .unwrap();
        s.set_applied(5);
        assert!(s.wait_applied(5, Duration::from_millis(10)).await);
        assert!(!s.wait_applied(8, Duration::from_millis(50)).await);
        let waiter = {
            let s = s.clone();
            tokio::spawn(async move { s.wait_applied(8, Duration::from_secs(2)).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        s.set_applied(8);
        assert!(waiter.await.unwrap());
    }

    #[test]
    fn a_persisted_role_survives_a_restart_and_overrides_the_configured_one() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ReplicationConfig {
            role: Role::Follower,
            state_file: Some(dir.path().join("role.json")),
            ..ReplicationConfig::default()
        };
        let first = ReplicationState::open(cfg.clone(), 1).unwrap();
        first.become_leader(2);
        let second = ReplicationState::open(cfg.clone(), 1).unwrap();
        assert_eq!((second.role(), second.epoch()), (Role::Leader, 2));
        second.fence("deposed");
        let third = ReplicationState::open(cfg, 1).unwrap();
        assert_eq!(third.role(), Role::Fenced);
    }
}
