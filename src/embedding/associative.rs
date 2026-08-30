//! `IncrementalAggregator` (PRD 4.1) — the associative-model update path for
//! GraphSAGE and GCN.
//!
//! "Incremental" here means bounded-subgraph recompute, not a literal delta
//! update to a stored running mean. A 2-layer message-passing model's output
//! for node `v` is a pure function of `v`'s 2-hop neighbourhood, so
//! recomputing the forward pass restricted to
//! [`AffectedSubgraphResolver`](crate::embedding::resolver::AffectedSubgraphResolver)'s
//! resolved subgraph is *exact* — not an approximation of a full recompute,
//! mathematically identical to one for every node the resolver named as
//! affected. What makes it incremental is that it never reads or feeds the
//! rest of the graph into that forward pass.

use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::resolver::{build_model_input, AffectedSubgraphResolver, ResolvedSubgraph};
use crate::embedding::state::MutationContext;
use crate::error::Result;
use crate::storage::KvStore;
use crate::types::{ComputationPath, Embedding, ModelKind, Timestamp};

/// Resolve, then run the model over exactly the resolved subgraph.
///
/// Mutates `ctx.affected`, `ctx.truncation`, and `ctx.embeddings_after`.
/// Sets `ctx.fallback` and leaves `embeddings_after` empty on any failure —
/// callers must check it, and the caller is `pipeline::run_mutation_pipeline`,
/// which counts every fallback (Rule 7) rather than treating an empty result
/// as "nothing to do".
///
/// Thin wrapper over [`aggregate_over_subgraph`]: resolves against whatever
/// is currently committed, then delegates. `atomic_commit.rs` cannot use this
/// directly — it needs to patch one not-yet-committed edge into the resolved
/// subgraph first — so it calls `aggregate_over_subgraph` itself instead.
pub fn incremental_aggregate<S: KvStore + ?Sized>(
    ctx: &mut MutationContext,
    store: &S,
    model: &EmbeddingModel,
    fanout_cap: usize,
    max_expanded_nodes: usize,
) -> Result<()> {
    let resolver = AffectedSubgraphResolver::new(store, fanout_cap, max_expanded_nodes);
    let subgraph = resolver.resolve(ctx.mutation)?;
    let as_of = ctx.mutation.timestamp();
    aggregate_over_subgraph(ctx, store, model, subgraph, as_of)
}

/// Run the model over an already-resolved subgraph and fill in `ctx`.
///
/// Split out from [`incremental_aggregate`] so `atomic_commit.rs` can resolve
/// against pre-mutation state, patch in the one edge the mutation itself
/// changes (a not-yet-committed write is invisible to the resolver's own
/// reads — see `atomic_commit.rs`'s module doc), and only then run the
/// forward pass this function performs.
pub fn aggregate_over_subgraph<S: KvStore + ?Sized>(
    ctx: &mut MutationContext,
    store: &S,
    model: &EmbeddingModel,
    subgraph: ResolvedSubgraph,
    as_of: Timestamp,
) -> Result<()> {
    debug_assert!(
        ctx.active_model.is_associative(),
        "the associative path is GraphSAGE/GCN; GAT routes to gat_incremental_update (Phase 5)"
    );

    ctx.truncation = subgraph.truncation;

    match run_forward_pass(store, model, &subgraph, ctx.active_model, as_of) {
        Ok(embeddings) => {
            ctx.affected = subgraph.affected;
            ctx.embeddings_after = embeddings;
            ctx.fallback = false;
            Ok(())
        }
        Err(_) => {
            // Rule 7: a failure here is logged and counted by the caller, never
            // silently treated as "no update needed". `pipeline.rs` increments
            // `incremental_fallback_total` when it sees this flag.
            ctx.fallback = true;
            ctx.embeddings_after.clear();
            Ok(())
        }
    }
}

/// Run the model over a resolved subgraph and label the outputs with their
/// model identity and computation path (Rule 3's per-embedding provenance).
fn run_forward_pass<S: KvStore + ?Sized>(
    store: &S,
    model: &EmbeddingModel,
    subgraph: &ResolvedSubgraph,
    active_model: ModelKind,
    as_of: crate::types::Timestamp,
) -> Result<Vec<(crate::types::NodeId, Embedding)>> {
    let (features, edge_index, local) =
        build_model_input(store, &subgraph.nodes, &subgraph.edges, as_of)?;

    let targets: Vec<usize> = subgraph
        .affected
        .iter()
        .filter_map(|n| local.get(n).copied())
        .collect();

    let vectors = model.forward(&features, &edge_index, &targets)?;

    Ok(subgraph
        .affected
        .iter()
        .zip(vectors)
        .map(|(&node, vector)| {
            (
                node,
                Embedding::new(
                    vector,
                    model.model_id.clone(),
                    active_model,
                    ComputationPath::Associative,
                ),
            )
        })
        .collect())
}

/// The deliberately slow path: run the model over the *entire* graph as of
/// `as_of`, returning embeddings only for `targets`. Exists as the reference
/// a correctness test compares the incremental path against — proof that
/// bounding the forward pass to the resolved subgraph is exact, not that it
/// is close. Not called from `pipeline.rs`: on failure that pipeline gives up
/// on the mutation rather than silently retrying against the whole graph, so
/// this function's only caller today is the correctness test.
pub fn full_recompute<S: KvStore + ?Sized>(
    store: &S,
    model: &EmbeddingModel,
    all_nodes: &[crate::types::NodeId],
    all_edges: &[(crate::types::NodeId, crate::types::NodeId)],
    targets: &[crate::types::NodeId],
    active_model: ModelKind,
    as_of: crate::types::Timestamp,
) -> Result<Vec<(crate::types::NodeId, Embedding)>> {
    let (features, edge_index, local) = build_model_input(store, all_nodes, all_edges, as_of)?;
    let target_idx: Vec<usize> = targets
        .iter()
        .filter_map(|n| local.get(n).copied())
        .collect();
    let vectors = model.forward(&features, &edge_index, &target_idx)?;

    Ok(targets
        .iter()
        .zip(vectors)
        .map(|(&node, vector)| {
            (
                node,
                Embedding::new(
                    vector,
                    model.model_id.clone(),
                    active_model,
                    ComputationPath::Fallback,
                ),
            )
        })
        .collect())
}
