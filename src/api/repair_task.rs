//! Runs the embedding repair worker (`embedding::repair_worker`) inside the server.
//!
//! One background thread. It only works while this node accepts writes (a
//! standalone node or the leader), rebuilds its queue from `CF_REPAIR` each time
//! it becomes writable (start-up, and promotion of a standby), and holds one
//! write permit while it repairs, so a demotion that waits for writes to finish
//! also waits for any repair in flight.
//!
//! It gives way to client writes: while any mutation is in flight it waits, unless
//! it has not run for [`MAX_STARVATION`], so a write-heavy period cannot grow the
//! backlog without bound but also cannot starve the model workers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::CareGraphApi;
use crate::embedding::repair_worker::{run_pass, RepairModel, RepairParams};
use crate::types::ModelKind;

/// Longest the worker defers to client writes before it takes a turn anyway.
const MAX_STARVATION: Duration = Duration::from_secs(2);
/// Pause after a pass in which the embedding service was unavailable.
const OUTAGE_BACKOFF: Duration = Duration::from_secs(2);
/// How often the queue is rebuilt from `CF_REPAIR`, so a node dropped after
/// repeated failures, or marked by a write this process never saw queued, is
/// picked up again without a restart.
const RESCAN_EVERY: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone)]
pub struct RepairWorkerConfig {
    /// Pause between passes, and how long an idle worker sleeps.
    pub interval: Duration,
    pub params: RepairParams,
}

impl Default for RepairWorkerConfig {
    fn default() -> Self {
        RepairWorkerConfig {
            interval: Duration::from_millis(200),
            params: RepairParams::default(),
        }
    }
}

/// Stops the worker thread when dropped (or on [`RepairHandle::stop`]).
pub struct RepairHandle {
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl RepairHandle {
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for RepairHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl CareGraphApi {
    /// Start the repair worker. From this call on, mutations that defer or fail
    /// to refresh nodes also queue them for it.
    pub fn spawn_repair_worker(&self, cfg: RepairWorkerConfig) -> RepairHandle {
        let inner = self.inner.clone();
        inner.repair_active.store(true, Ordering::SeqCst);
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();

        let join = std::thread::Builder::new()
            .name("repair-worker".to_string())
            .spawn(move || {
                let mut was_writable = false;
                let mut last_pass: Option<Instant> = None;
                let mut last_scan = Instant::now();
                while !flag.load(Ordering::SeqCst) {
                    let writable = inner.repl.writable().is_ok();
                    if writable && (!was_writable || last_scan.elapsed() >= RESCAN_EVERY) {
                        last_scan = Instant::now();
                        match inner.repair_queue.rebuild(&inner.store) {
                            Ok(0) => {}
                            Ok(n) => {
                                tracing::info!(nodes = n, "repair queue rebuilt from CF_REPAIR")
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "rebuilding the repair queue failed")
                            }
                        }
                        inner
                            .metrics
                            .repair_pending
                            .set(inner.repair_queue.len() as i64);
                    }
                    was_writable = writable;
                    if !writable {
                        std::thread::sleep(cfg.interval);
                        continue;
                    }

                    inner.repair_queue.wait(cfg.interval);
                    if inner.repair_queue.is_empty() {
                        continue;
                    }

                    let in_flight = inner
                        .write_capacity
                        .saturating_sub(inner.write_permits.available_permits());
                    let starved = last_pass.map_or(true, |t| t.elapsed() >= MAX_STARVATION);
                    if in_flight > 0 && !starved {
                        std::thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                    let Ok(permit) = inner.write_permits.clone().try_acquire_owned() else {
                        std::thread::sleep(Duration::from_millis(20));
                        continue;
                    };
                    // Re-check under the permit: a demotion that has not yet
                    // started waiting on permits has already changed the role.
                    if inner.repl.writable().is_err() {
                        continue;
                    }

                    let mut models = Vec::new();
                    if let Some(m) = &inner.graphsage_model {
                        models.push(RepairModel {
                            model: m,
                            kind: ModelKind::GraphSAGE,
                        });
                    }
                    if let Some(m) = &inner.gat_model {
                        models.push(RepairModel {
                            model: m,
                            kind: ModelKind::GAT,
                        });
                    }
                    let stats = run_pass(
                        &inner.store,
                        &inner.repair_queue,
                        &inner.metrics,
                        &models,
                        &cfg.params,
                    );
                    drop(permit);
                    last_pass = Some(Instant::now());
                    if stats.repaired + stats.failed + stats.superseded > 0 {
                        tracing::debug!(?stats, pending = inner.repair_queue.len(), "repair pass");
                    }
                    std::thread::sleep(if stats.unavailable > 0 {
                        OUTAGE_BACKOFF
                    } else {
                        cfg.interval
                    });
                }
            })
            .expect("spawning the repair worker thread");

        RepairHandle {
            stop,
            join: Some(join),
        }
    }
}
