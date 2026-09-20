//! `run_mutation_pipeline` (PRD 4.3) — resolve, aggregate, persist, count.
//!
//! "Persist" is `AtomicCommitter::commit` (`atomic_commit.rs`): the
//! structural mutation and its embedding update land in one `WriteBatch`,
//! not two separately-durable writes, regardless of which model is active —
//! `AtomicCommitter` itself dispatches between the associative
//! (GraphSAGE/GCN) and staged aggregations (`staged_incremental.rs`) based on
//! the active model's own manifest (Phase 9), not a hardcoded model list.
//! This module owns orchestration and metrics only — timing the call,
//! counting mutations and fallbacks (Rule 7) — not the write path itself.
//!
//! # Feeding the cap controller (Phase 9, Feature 2)
//!
//! This is the one place in production that both knows the real
//! embedding-update duration and runs once per mutation, so it is where
//! [`CapController::observe`] is fed — never from `metrics`, whose
//! `Histogram` has no quantile accessor to read a duration back out of. The
//! duration fed in is `ctx.embedding_duration`, which `AtomicCommitter::commit`
//! times narrowly around dispatch alone — excluding resolution/patching,
//! excluding attribution (Feature 1), and excluding the batch write — so the
//! controller only ever reacts to the latency its own caps actually govern.
//! `embedding_update_latency_seconds` is fed the same value, for the same
//! reason: that metric predates attribution, and its recording rule targets
//! the incremental-computation p95 miss specifically, not a request that
//! opted into an explanation of its own result.

use std::sync::Mutex;
use std::time::Instant;

use crate::embedding::atomic_commit::AtomicCommitter;
use crate::embedding::caps::CapController;
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
/// `caps` replaces what were, before Phase 9, two fixed `fanout_cap`/
/// `max_expanded_nodes` parameters: this function now reads the pair
/// currently in force, commits with it, and feeds the real duration back in —
/// the read-commit-feedback cycle is entirely local to this one call, so a
/// caller only has to hold the lock, never manage the controller's state.
///
/// `explain` requests an integrated-gradients attribution for the mutation's
/// two endpoints (Feature 1) — see `AtomicCommitter::commit`'s own doc for
/// why this is opt-in per call rather than a server-wide default.
#[allow(clippy::too_many_arguments)]
pub fn run_mutation_pipeline(
    mutation: GraphMutation,
    edge_value: &EdgeValue,
    active_model: ModelKind,
    store: &RocksKv,
    model: &EmbeddingModel,
    metrics: &EmbeddingMetrics,
    caps: &Mutex<CapController>,
    explain: bool,
) -> Result<MutationContext> {
    let start = Instant::now();
    metrics.mutations_total.inc();

    let (rung_caps, cap_rung) = caps.lock().unwrap_or_else(|e| e.into_inner()).current();

    let committer = AtomicCommitter::new(store)?;
    let ctx = committer.commit(
        mutation,
        edge_value,
        active_model,
        model,
        rung_caps.fanout_cap,
        rung_caps.max_expanded_nodes,
        cap_rung,
        explain,
    )?;
    metrics
        .embedding_update_latency_seconds
        .observe(ctx.embedding_duration.as_secs_f64());
    caps.lock()
        .unwrap_or_else(|e| e.into_inner())
        .observe(ctx.embedding_duration);

    if ctx.fallback {
        metrics.incremental_fallback_total.inc();
    }

    metrics
        .mutation_latency_seconds
        .observe(start.elapsed().as_secs_f64());

    Ok(ctx)
}
