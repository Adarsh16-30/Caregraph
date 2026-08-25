//! `run_mutation_pipeline` (PRD 4.3) — resolve, aggregate, persist, count.
//!
//! Not yet what PRD 9.2's `atomic_commit` names: the structural mutation must
//! already be durably written before this runs (resolution reads the graph
//! `KvStore::scan_from` sees, and an uncommitted `WriteBatch` is invisible to
//! reads), so embeddings land in their own write here, after the mutation's.
//! Two real, separately-durable writes rather than one atomic one. Rule 5
//! does not gate until Phase 5, when `AtomicCommitter` merges them into a
//! single `WriteBatch` — this module's shape does not need to change for
//! that, only `persist_embeddings` does.

use std::time::Instant;

use crate::embedding::associative::incremental_aggregate;
use crate::embedding::metrics::EmbeddingMetrics;
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::state::{GraphMutation, MutationContext};
use crate::error::Result;
use crate::storage::{cf, KvStore};
use crate::temporal::keys::encode_embedding_key;
use crate::types::ModelKind;

// `associative::full_recompute` exists for the correctness test — proving the
// incremental path's output is exact by comparing it against an unbounded
// forward pass — and is not called from this pipeline. On failure this phase
// gives up on the mutation's embedding update rather than silently retrying
// against the whole graph: an affected node keeps its pre-mutation embedding,
// stale rather than wrong, and `ctx.fallback` records that plainly so the
// caller (and Rule 7's counter) can see it. Phase 5's GAT path is where an
// automatic full-recompute fallback is expected to actually fire.

pub fn run_mutation_pipeline<S: KvStore + ?Sized>(
    mutation: GraphMutation,
    active_model: ModelKind,
    store: &S,
    model: &EmbeddingModel,
    metrics: &EmbeddingMetrics,
    fanout_cap: usize,
    max_expanded_nodes: usize,
) -> Result<MutationContext> {
    let start = Instant::now();
    metrics.mutations_total.inc();

    let mut ctx = MutationContext::new(mutation, active_model);

    match active_model {
        ModelKind::GraphSAGE | ModelKind::GCN => {
            let embed_start = Instant::now();
            incremental_aggregate(&mut ctx, store, model, fanout_cap, max_expanded_nodes)?;
            metrics
                .embedding_update_latency_seconds
                .observe(embed_start.elapsed().as_secs_f64());
        }
        ModelKind::GAT => {
            // GATUpdatePath is Phase 5. Reaching here is a caller error, not a
            // data condition — fail loudly rather than silently routing GAT
            // through the associative path, which would be exactly the wrong
            // math for a non-associative aggregator.
            panic!("GAT incremental path is Phase 5; not implemented");
        }
    }

    if ctx.fallback {
        metrics.incremental_fallback_total.inc();
    }

    persist_embeddings(&ctx, store)?;

    metrics
        .mutation_latency_seconds
        .observe(start.elapsed().as_secs_f64());

    Ok(ctx)
}

fn persist_embeddings<S: KvStore + ?Sized>(ctx: &MutationContext, store: &S) -> Result<()> {
    let ts = ctx.mutation.timestamp();
    for (node, embedding) in &ctx.embeddings_after {
        let key = encode_embedding_key(*node, ts);
        store.put(cf::CF_EMBEDDINGS, &key, &embedding.serialize())?;
    }
    Ok(())
}
