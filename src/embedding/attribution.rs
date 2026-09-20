//! Integrated-gradients edge attribution (Phase 9, Feature 1) — the "why did
//! this embedding change, and which edges drove it" record that folds into
//! [`crate::embedding::meta::CommitMeta::attribution`].
//!
//! # Why this lives as a separate call, after the ordinary forward pass
//!
//! `associative::aggregate_over_subgraph`/`staged_incremental::staged_incremental_update`
//! already ran the real "post-mutation" forward pass to produce
//! `ctx.embeddings_after` — this module does not touch that. It issues one
//! *additional* [`EmbeddingModel::forward_with_attribution`] call, over the
//! same node/feature set, once the caller (`atomic_commit.rs`) knows
//! attribution was actually requested. The extra plain forward pass this
//! costs (a few milliseconds — see `docs/benchmark_report.md`'s attribution
//! section) is negligible next to the IG computation itself
//! (measured ≈160ms GraphSAGE / ≈550ms GAT at m=16, endpoints only), so
//! reusing the exact `run_forward_pass` internals of the associative/staged
//! paths was not worth the coupling it would add to two already-diverging
//! aggregation implementations.
//!
//! # Why only the mutation's own endpoints, never all affected nodes
//!
//! `docs/benchmark_report.md` §7.6 records a 521-node affected set from a
//! single real mutation; attributing every affected node would multiply an
//! already-measured latency cost (on top of a p95 already over target) by
//! two orders of magnitude. `atomic_commit.rs` only ever asks this module to
//! attribute `mutation.endpoints()` — see its own doc for why that boundary
//! is drawn where it is.

use std::collections::HashMap;

use crate::embedding::meta::{Attribution, EdgeAttribution};
use crate::embedding::model_bridge::{AttributionRequest, EmbeddingModel};
use crate::embedding::resolver::build_model_input;
use crate::error::Result;
use crate::storage::KvStore;
use crate::types::{NodeId, Timestamp};

/// Default integrated-gradients step count. Not tunable per-request today —
/// `docs/benchmark_report.md`'s convergence study found GraphSAGE's
/// completeness residual non-monotone in step count (ReLU kinks) and GAT's
/// smoothly decreasing (ELU), so a caller-supplied step count would let a
/// client trade accuracy for latency without a principled way to choose
/// where — `CAREGRAPH_ATTRIBUTION_STEPS` overrides this server-wide instead.
pub const DEFAULT_STEPS: u32 = 16;
/// Edges kept in a stored [`Attribution`] record before folding the rest into
/// `rest_sum`/`rest_count` — see [`Attribution`]'s own doc for the storage-size
/// argument.
pub const DEFAULT_TOP_K: usize = 20;

/// Compute an [`Attribution`] record for each of `targets` whose embedding
/// actually changed, explaining the change between `edges_before` (the
/// pre-mutation local subgraph) and `edges_after` (the same subgraph with the
/// mutation's one edge applied) — both over the identical `nodes` ordering,
/// which is what makes a row index mean the same node on both sides.
///
/// Returns one entry per target with a non-degenerate (`delta_norm > 0`)
/// change; a target whose embedding did not move at all is simply absent
/// from the map, not an error and not a fabricated zero-attribution record.
#[allow(clippy::too_many_arguments)]
pub fn attribute_targets<S: KvStore + ?Sized>(
    store: &S,
    model: &EmbeddingModel,
    nodes: &[NodeId],
    edges_before: &[(NodeId, NodeId)],
    edges_after: &[(NodeId, NodeId)],
    targets: &[NodeId],
    as_of: Timestamp,
    steps: u32,
    top_k: usize,
) -> Result<HashMap<NodeId, Attribution>> {
    let (features, edge_index_after, local) = build_model_input(store, nodes, edges_after, as_of)?;
    // `local` depends only on `nodes`' order, which is identical for both
    // calls — so this second call cannot produce a different mapping, only
    // (cheaply) re-read the same node-type one-hot rows and re-symmetrise a
    // different edge list. See this module's doc for why that small, bounded
    // redundancy was chosen over threading `local` through as a precondition.
    let (_features_before, edge_index_before, local_before) =
        build_model_input(store, nodes, edges_before, as_of)?;
    debug_assert_eq!(
        local, local_before,
        "attribute_targets: the same `nodes` slice must produce the same row mapping \
         regardless of which edge list build_model_input was given"
    );

    let target_rows: Vec<usize> = targets
        .iter()
        .filter_map(|n| local.get(n).copied())
        .collect();
    if target_rows.is_empty() {
        return Ok(HashMap::new());
    }

    let (_embeddings, raw_entries) = model.forward_with_attribution(
        &features,
        &edge_index_after,
        &target_rows,
        AttributionRequest {
            targets: &target_rows,
            edge_index_before: &edge_index_before,
            steps,
            top_k,
        },
    )?;

    let row_to_node: HashMap<usize, NodeId> = local.iter().map(|(&n, &i)| (i, n)).collect();
    let model_sha256 = model.manifest.model_sha256.clone();

    let mut out = HashMap::new();
    for entry in raw_entries {
        // A target the worker reported no attribution for either had a
        // zero-norm delta (handled explicitly by
        // `embedding_server.py::_attribute_one_target`, not an error) or —
        // defensively — was a row this map does not recognise; either way,
        // absence here means "nothing to explain", never a fabricated entry.
        if entry.delta_norm == 0.0 && entry.edges.is_empty() && entry.edges_total == 0 {
            continue;
        }
        let Some(&node) = row_to_node.get(&entry.target_index) else {
            continue;
        };

        let mut top: Vec<EdgeAttribution> = entry
            .edges
            .iter()
            .filter_map(|&(u_row, v_row, value)| {
                let src = *row_to_node.get(&(u_row as usize))?;
                let dst = *row_to_node.get(&(v_row as usize))?;
                let (src, dst) = if src.as_u64() <= dst.as_u64() {
                    (src, dst)
                } else {
                    (dst, src)
                };
                Some(EdgeAttribution {
                    src: src.as_u64(),
                    dst: dst.as_u64(),
                    value,
                })
            })
            .collect();
        top.sort_by(|a, b| {
            b.value
                .abs()
                .partial_cmp(&a.value.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Exact by construction: `rest_sum` is defined by the worker as the
        // sum of every edge excluded from `top`, so `sum(top) + rest_sum`
        // reconstructs the worker's own full sum without re-deriving it from
        // data this side never received (only the top-k edges cross the
        // wire — see `Attribution`'s own doc on why storing every edge
        // forever was rejected).
        let sum_attr: f32 = top.iter().map(|e| e.value).sum::<f32>() + entry.rest_sum;

        out.insert(
            node,
            Attribution {
                method: "integrated_gradients".to_string(),
                steps,
                rule: "midpoint".to_string(),
                baseline: "zero_edge_mask_post_softmax_self_loops_pinned_to_1".to_string(),
                direction: "unit(z_after - z_before)".to_string(),
                model_sha256: model_sha256.clone(),
                delta_norm: entry.delta_norm,
                baseline_delta: entry.baseline_delta,
                sum_attr,
                completeness_residual: entry.completeness_residual,
                edges_total: entry.edges_total,
                top,
                rest_sum: entry.rest_sum,
                rest_count: entry.rest_count,
            },
        );
    }
    Ok(out)
}
