//! `BenchmarkLogger` stage (PRD 4.1) — the Prometheus series
//! `observability/prometheus/recording-rules.yaml` has been waiting on since
//! Phase 1. Rule 7's enforcement is this counter existing and being real:
//! `incremental_fallback_total` must go up exactly when a fallback happens,
//! never as an estimate.

use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

pub struct EmbeddingMetrics {
    pub mutations_total: IntCounter,
    pub incremental_fallback_total: IntCounter,
    pub mutation_latency_seconds: Histogram,
    pub embedding_update_latency_seconds: Histogram,
}

impl EmbeddingMetrics {
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let mutations_total =
            IntCounter::new("mutations_total", "Structural mutations processed")?;
        let incremental_fallback_total = IntCounter::new(
            "incremental_fallback_total",
            "Mutations whose embedding update fell back to full recompute (Rule 7)",
        )?;
        let mutation_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "mutation_latency_seconds",
            "End-to-end mutation pipeline latency",
        ))?;
        let embedding_update_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "embedding_update_latency_seconds",
            "Embedding recomputation latency, incremental or fallback",
        ))?;

        registry.register(Box::new(mutations_total.clone()))?;
        registry.register(Box::new(incremental_fallback_total.clone()))?;
        registry.register(Box::new(mutation_latency_seconds.clone()))?;
        registry.register(Box::new(embedding_update_latency_seconds.clone()))?;

        Ok(EmbeddingMetrics {
            mutations_total,
            incremental_fallback_total,
            mutation_latency_seconds,
            embedding_update_latency_seconds,
        })
    }
}
