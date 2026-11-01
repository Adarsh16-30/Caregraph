//! `BenchmarkLogger` stage — the Prometheus series
//! `observability/prometheus/recording-rules.yaml` has been waiting on since
//! Phase 1. The no-silent-fallback guarantee is this counter existing and being real:
//! `incremental_fallback_total` must go up exactly when a fallback happens,
//! never as an estimate.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};

pub struct EmbeddingMetrics {
    pub mutations_total: IntCounter,
    pub incremental_fallback_total: IntCounter,
    pub mutation_latency_seconds: Histogram,
    pub embedding_update_latency_seconds: Histogram,
    /// Time a computed mutation waited for its turn to commit (P3).
    pub commit_wait_seconds: Histogram,
    /// Mutations recomputed because a concurrent commit touched their read set.
    pub mutation_conflict_retries_total: IntCounter,
    /// Nodes whose update a mutation left for the repair worker (hub deferral).
    /// Not a fallback: each has a durable marker and is refreshed shortly after.
    pub embedding_updates_deferred_total: IntCounter,
    /// Nodes waiting in the repair queue right now.
    pub repair_pending: IntGauge,
    /// Nodes the repair worker has refreshed.
    pub repairs_completed_total: IntCounter,
    /// Repair batches that failed (the nodes are retried, then left marked).
    pub repair_failures_total: IntCounter,
    /// Time to refresh one repair batch.
    pub repair_batch_seconds: Histogram,
}

impl EmbeddingMetrics {
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let mutations_total = IntCounter::new("mutations_total", "Structural mutations processed")?;
        let incremental_fallback_total = IntCounter::new(
            "incremental_fallback_total",
            "Mutations whose embedding update fell back to full recompute",
        )?;
        let mutation_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "mutation_latency_seconds",
            "End-to-end mutation pipeline latency",
        ))?;
        let embedding_update_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "embedding_update_latency_seconds",
            "Embedding recomputation latency, incremental or fallback",
        ))?;

        let commit_wait_seconds = Histogram::with_opts(HistogramOpts::new(
            "commit_wait_seconds",
            "Time a computed mutation waited for earlier timestamps to commit",
        ))?;
        let mutation_conflict_retries_total = IntCounter::new(
            "mutation_conflict_retries_total",
            "Mutations recomputed because a concurrent commit touched their read set",
        )?;

        let embedding_updates_deferred_total = IntCounter::new(
            "embedding_updates_deferred_total",
            "Node embedding updates a mutation deferred to the repair worker (hub deferral)",
        )?;
        let repair_pending = IntGauge::new(
            "repair_pending",
            "Nodes waiting for the repair worker to refresh their embedding",
        )?;
        let repairs_completed_total = IntCounter::new(
            "repairs_completed_total",
            "Node embeddings refreshed by the repair worker",
        )?;
        let repair_failures_total = IntCounter::new(
            "repair_failures_total",
            "Repair batches that failed and were retried or left marked",
        )?;
        let repair_batch_seconds = Histogram::with_opts(HistogramOpts::new(
            "repair_batch_seconds",
            "Time for the repair worker to refresh one batch of nodes",
        ))?;

        registry.register(Box::new(embedding_updates_deferred_total.clone()))?;
        registry.register(Box::new(repair_pending.clone()))?;
        registry.register(Box::new(repairs_completed_total.clone()))?;
        registry.register(Box::new(repair_failures_total.clone()))?;
        registry.register(Box::new(repair_batch_seconds.clone()))?;
        registry.register(Box::new(mutations_total.clone()))?;
        registry.register(Box::new(commit_wait_seconds.clone()))?;
        registry.register(Box::new(mutation_conflict_retries_total.clone()))?;
        registry.register(Box::new(incremental_fallback_total.clone()))?;
        registry.register(Box::new(mutation_latency_seconds.clone()))?;
        registry.register(Box::new(embedding_update_latency_seconds.clone()))?;

        Ok(EmbeddingMetrics {
            mutations_total,
            incremental_fallback_total,
            mutation_latency_seconds,
            embedding_update_latency_seconds,
            commit_wait_seconds,
            mutation_conflict_retries_total,
            embedding_updates_deferred_total,
            repair_pending,
            repairs_completed_total,
            repair_failures_total,
            repair_batch_seconds,
        })
    }
}
