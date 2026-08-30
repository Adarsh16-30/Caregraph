//! `run_mutation_pipeline` (PRD 4.3) — resolve, aggregate, persist, count.
//!
//! "Persist" is `AtomicCommitter::commit` (`atomic_commit.rs`): the
//! structural mutation and its embedding update land in one `WriteBatch`,
//! not two separately-durable writes, regardless of which model is active —
//! `AtomicCommitter` itself dispatches between the associative
//! (GraphSAGE/GCN) and GAT-constrained aggregations (`gat_incremental.rs`).
//! This module owns orchestration and metrics only — timing the call,
//! counting mutations and fallbacks (Rule 7) — not the write path itself.

use std::time::Instant;

use crate::embedding::atomic_commit::AtomicCommitter;
use crate::embedding::metrics::EmbeddingMetrics;
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::state::{GraphMutation, MutationContext};
use crate::error::Result;
use crate::storage::RocksKv;
use crate::temporal::record::EdgeValue;
use crate::types::ModelKind;

// `associative::full_recompute` exists for the correctness test — proving the
// incremental path's output is exact by comparing it against an unbounded
// forward pass — and is not called from this pipeline. On failure this phase
// gives up on the mutation's embedding update rather than silently retrying
// against the whole graph: an affected node keeps its pre-mutation embedding,
// stale rather than wrong, and `ctx.fallback` records that plainly so the
// caller (and Rule 7's counter) can see it.

/// `edge_value` carries the properties for an `AddEdge`; ignored for a
/// `RemoveEdge`. `store` is concrete `RocksKv`, not generic over `KvStore` —
/// `AtomicCommitter` needs `TemporalWriter`'s real column-family handles to
/// stage a `WriteBatch`, same reason `TemporalWriter` itself is concrete.
///
/// Eight parameters is genuinely what this orchestration step depends on —
/// the mutation, its properties, which model is active, where to read and
/// write, where to record metrics, and the two independent tuning caps
/// `AtomicCommitter` needs — not incidental sprawl to hide behind a bag-of-
/// fields struct that would just move the same count one level down.
#[allow(clippy::too_many_arguments)]
pub fn run_mutation_pipeline(
    mutation: GraphMutation,
    edge_value: &EdgeValue,
    active_model: ModelKind,
    store: &RocksKv,
    model: &EmbeddingModel,
    metrics: &EmbeddingMetrics,
    fanout_cap: usize,
    max_expanded_nodes: usize,
) -> Result<MutationContext> {
    let start = Instant::now();
    metrics.mutations_total.inc();

    let embed_start = Instant::now();
    let committer = AtomicCommitter::new(store)?;
    let ctx = committer.commit(
        mutation,
        edge_value,
        active_model,
        model,
        fanout_cap,
        max_expanded_nodes,
    )?;
    metrics
        .embedding_update_latency_seconds
        .observe(embed_start.elapsed().as_secs_f64());

    if ctx.fallback {
        metrics.incremental_fallback_total.inc();
    }

    metrics
        .mutation_latency_seconds
        .observe(start.elapsed().as_secs_f64());

    Ok(ctx)
}
