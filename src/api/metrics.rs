//! Query-path metrics (Phase 7) — the series
//! `observability/prometheus/recording-rules.yaml` has been waiting on since
//! Phase 1 alongside `embedding::metrics::EmbeddingMetrics`'s mutation-path
//! series (that module's doc explains the shared history). These two back
//! the read side: `point_in_time_query_seconds` (Section 1's `as_of` reads)
//! and `traversal_latency_seconds` (Section 1's bounded traversal), labeled
//! by `max_hops` so the recording rule's `{max_hops="2"}` selector has a
//! real dimension to filter on rather than matching every traversal ever
//! served regardless of hop count.
//!
//! `similarity_delta_seconds` (Phase 9, Feature 3) follows the same
//! registration pattern for the new two-timestamp similarity diff query.

use prometheus::{Histogram, HistogramOpts, HistogramVec, Registry};

pub struct ApiMetrics {
    pub point_in_time_query_seconds: Histogram,
    pub traversal_latency_seconds: HistogramVec,
    pub similarity_delta_seconds: Histogram,
}

impl ApiMetrics {
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let point_in_time_query_seconds = Histogram::with_opts(HistogramOpts::new(
            "point_in_time_query_seconds",
            "Point-in-time snapshot read latency (Section 1)",
        ))?;
        let traversal_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "traversal_latency_seconds",
                "Bounded traversal latency (Section 1), labeled by requested max_hops",
            ),
            &["max_hops"],
        )?;
        let similarity_delta_seconds = Histogram::with_opts(HistogramOpts::new(
            "similarity_delta_seconds",
            "Point-in-time similarity delta query latency (Phase 9, Feature 3)",
        ))?;

        registry.register(Box::new(point_in_time_query_seconds.clone()))?;
        registry.register(Box::new(traversal_latency_seconds.clone()))?;
        registry.register(Box::new(similarity_delta_seconds.clone()))?;

        Ok(ApiMetrics {
            point_in_time_query_seconds,
            traversal_latency_seconds,
            similarity_delta_seconds,
        })
    }
}
