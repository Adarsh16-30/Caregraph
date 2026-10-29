//! Liveness, readiness and startup state, plus the worker-pool gauges.
//!
//! Probes are plain HTTP on the metrics port rather than gRPC: kubelet gRPC
//! probes cannot present a client certificate, and the gRPC listener requires
//! one in production.
//!
//! - `/healthz`  liveness: the process is up and the database answers a read.
//! - `/readyz`   readiness: startup finished, not draining, and every deployed
//!   embedding model has a healthy worker and a closed circuit breaker.
//! - `/startupz` startup: models are spawned and the service is built.
//!
//! Readiness is what gates traffic. A node whose workers are all down is alive
//! (restarting it would not help while the supervisor is already respawning)
//! but must not receive writes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};

use crate::embedding::worker::pool::PoolStats;

/// One deployed model's health, as reported by its worker pool.
#[derive(Debug, Clone)]
pub struct ModelStatus {
    pub name: String,
    pub stats: PoolStats,
}

type ModelProbe = Arc<dyn Fn() -> Vec<ModelStatus> + Send + Sync>;
type DbProbe = Arc<dyn Fn() -> bool + Send + Sync>;

pub struct Health {
    started: AtomicBool,
    draining: AtomicBool,
    models: ModelProbe,
    db: DbProbe,
}

/// A readiness verdict with the reasons it is negative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Readiness {
    pub ready: bool,
    pub reasons: Vec<String>,
}

impl Health {
    pub fn new(models: ModelProbe, db: DbProbe) -> Arc<Self> {
        Arc::new(Health {
            started: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            models,
            db,
        })
    }

    pub fn mark_started(&self) {
        self.started.store(true, Ordering::SeqCst);
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Flips readiness off ahead of shutdown so traffic drains away first.
    pub fn begin_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    pub fn live(&self) -> bool {
        (self.db)()
    }

    pub fn readiness(&self) -> Readiness {
        let mut reasons = Vec::new();
        if !self.is_started() {
            reasons.push("startup has not finished".to_string());
        }
        if self.is_draining() {
            reasons.push("draining for shutdown".to_string());
        }
        for m in (self.models)() {
            if m.stats.breaker_open {
                reasons.push(format!("model {}: circuit breaker open", m.name));
            } else if m.stats.healthy == 0 {
                reasons.push(format!("model {}: no healthy worker", m.name));
            }
        }
        Readiness {
            ready: reasons.is_empty(),
            reasons,
        }
    }

    pub fn model_statuses(&self) -> Vec<ModelStatus> {
        (self.models)()
    }
}

/// Exposes each pool's state as Prometheus series, read when scraped so the
/// numbers are never stale and no background updater has to be kept alive.
pub struct PoolCollector {
    health: Arc<Health>,
    workers: IntGaugeVec,
    healthy: IntGaugeVec,
    breaker_open: IntGaugeVec,
    restarts: IntCounterVec,
    timeouts: IntCounterVec,
    failures: IntCounterVec,
    /// Last absolute values seen, to turn the pools' running totals into
    /// counter increments.
    seen: Mutex<std::collections::HashMap<String, (u64, u64, u64)>>,
}

impl PoolCollector {
    pub fn register(health: Arc<Health>, registry: &Registry) -> prometheus::Result<()> {
        let labels = &["model"];
        let c = PoolCollector {
            health,
            workers: IntGaugeVec::new(
                Opts::new(
                    "workers_configured",
                    "Embedding workers configured per model",
                ),
                labels,
            )?,
            healthy: IntGaugeVec::new(
                Opts::new(
                    "worker_healthy",
                    "Embedding workers currently healthy per model",
                ),
                labels,
            )?,
            breaker_open: IntGaugeVec::new(
                Opts::new(
                    "breaker_open",
                    "1 when the model's circuit breaker is open and requests fail fast",
                ),
                labels,
            )?,
            restarts: IntCounterVec::new(
                Opts::new("worker_restarts_total", "Embedding worker respawns"),
                labels,
            )?,
            timeouts: IntCounterVec::new(
                Opts::new(
                    "worker_timeouts_total",
                    "Forward passes that exceeded their deadline",
                ),
                labels,
            )?,
            failures: IntCounterVec::new(
                Opts::new("worker_failures_total", "Embedding worker deaths"),
                labels,
            )?,
            seen: Mutex::new(Default::default()),
        };
        registry.register(Box::new(c))
    }

    fn refresh(&self) {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        for m in self.health.model_statuses() {
            let l = &[m.name.as_str()];
            self.workers
                .with_label_values(l)
                .set(m.stats.workers as i64);
            self.healthy
                .with_label_values(l)
                .set(m.stats.healthy as i64);
            self.breaker_open
                .with_label_values(l)
                .set(i64::from(m.stats.breaker_open));
            let prev = seen.entry(m.name.clone()).or_insert((0, 0, 0));
            self.restarts
                .with_label_values(l)
                .inc_by(m.stats.restarts.saturating_sub(prev.0));
            self.timeouts
                .with_label_values(l)
                .inc_by(m.stats.timeouts.saturating_sub(prev.1));
            self.failures
                .with_label_values(l)
                .inc_by(m.stats.worker_failures.saturating_sub(prev.2));
            *prev = (m.stats.restarts, m.stats.timeouts, m.stats.worker_failures);
        }
    }
}

impl Collector for PoolCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut d = Vec::new();
        d.extend(self.workers.desc());
        d.extend(self.healthy.desc());
        d.extend(self.breaker_open.desc());
        d.extend(self.restarts.desc());
        d.extend(self.timeouts.desc());
        d.extend(self.failures.desc());
        d
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.refresh();
        let mut out = Vec::new();
        out.extend(self.workers.collect());
        out.extend(self.healthy.collect());
        out.extend(self.breaker_open.collect());
        out.extend(self.restarts.collect());
        out.extend(self.timeouts.collect());
        out.extend(self.failures.collect());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(healthy: usize, breaker_open: bool) -> PoolStats {
        PoolStats {
            workers: 2,
            healthy,
            restarts: 0,
            timeouts: 0,
            worker_failures: 0,
            breaker_open,
        }
    }

    fn health_with(models: Vec<ModelStatus>) -> Arc<Health> {
        Health::new(Arc::new(move || models.clone()), Arc::new(|| true))
    }

    #[test]
    fn not_ready_until_started_and_ready_when_models_are_healthy() {
        let h = health_with(vec![ModelStatus {
            name: "m".into(),
            stats: stats(2, false),
        }]);
        assert!(!h.readiness().ready);
        h.mark_started();
        assert!(h.readiness().ready);
    }

    #[test]
    fn draining_open_breaker_and_dead_workers_each_make_it_unready() {
        let h = health_with(vec![ModelStatus {
            name: "a".into(),
            stats: stats(0, false),
        }]);
        h.mark_started();
        assert!(h.readiness().reasons[0].contains("no healthy worker"));

        let h = health_with(vec![ModelStatus {
            name: "b".into(),
            stats: stats(2, true),
        }]);
        h.mark_started();
        assert!(h.readiness().reasons[0].contains("breaker"));

        let h = health_with(vec![]);
        h.mark_started();
        assert!(h.readiness().ready);
        h.begin_draining();
        assert!(!h.readiness().ready);
        assert!(h.live(), "draining does not make the process unhealthy");
    }

    #[test]
    fn pool_gauges_appear_in_the_registry_and_counters_do_not_double_count() {
        let h = health_with(vec![ModelStatus {
            name: "m".into(),
            stats: PoolStats {
                restarts: 3,
                ..stats(1, false)
            },
        }]);
        let registry = Registry::new();
        PoolCollector::register(h, &registry).unwrap();
        for _ in 0..2 {
            let families = registry.gather();
            let restarts = families
                .iter()
                .find(|f| f.name() == "worker_restarts_total")
                .expect("restart counter exported");
            assert_eq!(restarts.get_metric()[0].counter.value(), 3.0);
        }
    }
}
