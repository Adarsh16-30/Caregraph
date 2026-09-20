//! Staged incremental update path for non-associative aggregations (PRD 4.1,
//! Contribution 4). Originally written as `gat_incremental.rs`, hardcoded to
//! `ModelKind::GAT` — renamed and generalized for Phase 9's manifest-driven
//! dispatch (see [`crate::embedding::manifest`]), which decides associative
//! vs. staged from a model's own declared `is_associative` flag rather than a
//! hand-maintained `match` on a fixed enum of known architectures.
//!
//! # Why this module exists, and why the mechanism inside it is the same
//!
//! GraphSAGE's mean is associative in the literal sense — mean = sum / count,
//! and either term can absorb one neighbour's contribution as an independent
//! delta. An attention aggregation's weight for every surviving edge is a
//! softmax over *all* of a node's neighbours, so adding or removing one
//! neighbour renormalizes every other edge's weight at once — there is no
//! independent per-neighbour term to add or remove. That is what
//! `is_associative: false` in a model's manifest is naming, for whichever
//! architecture declares it, not specifically GAT.
//!
//! That property does not, however, mean a non-associative model needs a
//! different bounded subgraph or a different notion of "affected" — an
//! attention layer still only ever reads a node's own immediate neighbours
//! (the softmax is local to each node, not global), so the same 2-hop
//! dependency argument `associative.rs`'s module doc makes for GraphSAGE holds
//! here too: `v`'s output is a pure function of `v`'s 2-hop neighbourhood, and
//! recomputing the forward pass over
//! [`AffectedSubgraphResolver`](crate::embedding::resolver::AffectedSubgraphResolver)'s
//! resolved subgraph is exact for either aggregation function. "Staged" is
//! reinterpreted here the same way `atomic_commit.rs` reinterprets PRD 9.2's
//! pseudocode: not a literal multi-phase algorithm, but the real property
//! Rule 7 cares about — every embedding this path produces is tagged with
//! whichever non-associative [`ComputationPath`] the caller passes in, never
//! [`ComputationPath::Associative`], so the audit trail can always tell which
//! aggregation actually produced a given vector.

use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::resolver::{build_model_input, ResolvedSubgraph};
use crate::embedding::state::MutationContext;
use crate::error::Result;
use crate::storage::KvStore;
use crate::types::{ComputationPath, Embedding, ModelKind, NodeId, Timestamp};

/// Run the model over an already-resolved (and mutation-patched) subgraph and
/// fill in `ctx`, tagging every embedding with `computation_path`.
///
/// `model_kind`/`computation_path` are supplied by the caller (derived from
/// the model's manifest — see `atomic_commit.rs`) rather than hardcoded here,
/// so this function serves any non-associative architecture a manifest
/// declares, not only GAT.
///
/// Mirrors [`associative::aggregate_over_subgraph`](crate::embedding::associative::aggregate_over_subgraph)
/// deliberately: `atomic_commit.rs` resolves once and patches once regardless
/// of which model is active, then dispatches to this function or that one
/// purely to get the right [`ComputationPath`] tag on the result — the
/// resolution and patching work is identical either way (see resolver.rs's
/// `patch_subgraph_for_mutation`), so duplicating it here would only be a
/// second place for the same bug to be introduced.
pub fn staged_incremental_update<S: KvStore + ?Sized>(
    ctx: &mut MutationContext,
    store: &S,
    model: &EmbeddingModel,
    subgraph: ResolvedSubgraph,
    as_of: Timestamp,
    model_kind: ModelKind,
    computation_path: ComputationPath,
) -> Result<()> {
    debug_assert!(
        computation_path != ComputationPath::Associative,
        "staged_incremental_update is the non-associative path; \
         GraphSAGE/GCN use associative::aggregate_over_subgraph"
    );

    ctx.truncation = subgraph.truncation;

    match run_forward_pass(store, model, &subgraph, as_of, model_kind, computation_path) {
        Ok(embeddings) => {
            ctx.affected = subgraph.affected;
            ctx.embeddings_after = embeddings;
            ctx.fallback = false;
            Ok(())
        }
        Err(_) => {
            // Rule 7: same honesty contract as the associative path — a
            // failure here is counted by the caller (`pipeline.rs`), never
            // silently treated as "no update needed".
            ctx.fallback = true;
            ctx.embeddings_after.clear();
            Ok(())
        }
    }
}

fn run_forward_pass<S: KvStore + ?Sized>(
    store: &S,
    model: &EmbeddingModel,
    subgraph: &ResolvedSubgraph,
    as_of: Timestamp,
    model_kind: ModelKind,
    computation_path: ComputationPath,
) -> Result<Vec<(NodeId, Embedding)>> {
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
                Embedding::new(vector, model.model_id.clone(), model_kind, computation_path),
            )
        })
        .collect())
}
