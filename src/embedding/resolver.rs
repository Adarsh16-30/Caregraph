//! `AffectedSubgraphResolver` (PRD 4.1) — which nodes' embeddings a mutation
//! touches, and what a 2-layer model needs to recompute them correctly.
//!
//! # Why a mutation on a hub is expensive, honestly
//!
//! GraphSAGE's mean aggregation makes this hop-through-a-hub-again lesson
//! resurface at a second layer of the system. Adding one edge to a node
//! changes the *denominator* of every mean that node is aggregated into: for
//! a hub with 54,000 neighbours, that is 54,000 embeddings that are now,
//! exactly, at floating-point precision, different from what they were —
//! not an approximation, a property of averaging. `Truncation` on the query
//! path exists because a cap can silently drop data; this resolver caps for
//! the same honesty reason, but the thing being bounded is real mathematical
//! impact, not a client's result budget.
//!
//! Two capped scans, mirroring [`crate::graph::limits::TraversalLimits`]:
//!
//! 1. **Affected set** — the mutation's two endpoints plus their live
//!    neighbours (all edge types, both directions). These are the nodes whose
//!    *reported* embedding changes.
//! 2. **Receptive field** — each affected node's own neighbours. A 2-layer
//!    model's output for node `v` depends only on `v`'s 2-hop neighbourhood,
//!    so this is exactly what recomputing the affected set's embeddings
//!    needs, and nothing more.
//!
//! Both stay under the fan-out cap. A node beyond the cap keeps its stale
//! embedding until it is itself directly mutated — recorded in
//! [`ResolutionTruncation`], never silently treated as up to date.

use std::collections::{HashMap, HashSet};

use crate::embedding::state::{GraphMutation, ResolutionTruncation};
use crate::error::Result;
use crate::storage::KvStore;
use crate::temporal::TemporalIndex;
use crate::types::{NodeId, Timestamp};

/// The induced subgraph a 2-layer model needs to recompute the affected set.
pub struct ResolvedSubgraph {
    /// Nodes whose embedding is reported as updated.
    pub affected: Vec<NodeId>,
    /// Every node touched while building the affected set's receptive field,
    /// affected nodes included. Feature-matrix rows come from this set.
    pub nodes: Vec<NodeId>,
    /// Edges among `nodes`, as (src, dst) — direction is not meaningful to an
    /// undirected mean aggregator, so it is not carried past this point.
    pub edges: Vec<(NodeId, NodeId)>,
    pub truncation: ResolutionTruncation,
}

/// Live neighbours of `node` across every edge type, both directions, capped
/// at `cap` **total per direction** — the same unit the query-path fan-out
/// cap bounds, reused here for the same reason.
///
/// # Why two calls per node, not twelve
///
/// The first version of this function called
/// [`TemporalIndex::edges_as_of_limited`] once per `EdgeType` per direction —
/// 6 edge types × 2 directions = 12 separate RocksDB scans for every single
/// node the resolver visited. Measured on the real clinical graph
/// (`docs/benchmark_report.md` §7.5), a mutation touching a moderate-degree
/// reference node (affected-set size ~500, well under the fan-out cap of
/// 5,000 but still substantial) drove incremental latency to within 2-4x of
/// full recompute — the target is 5x. The gap wasn't the forward pass; it was
/// several thousand small per-node RocksDB round trips, each paying seek
/// overhead independent of how little data it returned.
///
/// [`TemporalIndex::all_edges_as_of_limited`] and its incoming counterpart
/// scan a node's entire key range — every edge type — in one pass, because
/// edge type sorts immediately after `src_id` in the key layout and so every
/// edge type's data for one node is already one contiguous byte range. Two
/// scans per node instead of twelve.
///
/// One honest consequence: the cap used to bound each edge type
/// independently (up to `cap` `DiagnosedWith` neighbours *and* up to `cap`
/// `PrescribedMedication` neighbours from the same node). It now bounds the
/// combined total per direction. A node with high degree spread across
/// several edge types surfaces fewer total neighbours than it used to under
/// the same `cap` value — a tighter bound, not a looser one, and still
/// recorded in [`ResolutionTruncation`] exactly as before whenever it binds.
fn capped_neighbors<S: KvStore + ?Sized>(
    index: &TemporalIndex<'_, S>,
    node: NodeId,
    as_of: Timestamp,
    cap: usize,
) -> Result<(Vec<NodeId>, bool, usize)> {
    let mut neighbors = HashSet::new();
    let mut capped = false;
    let mut dropped = 0usize;
    let probe = cap.saturating_add(1);

    let out = index.all_edges_as_of_limited(node, as_of, probe)?;
    if out.len() == probe {
        capped = true;
        dropped += out.len() - cap;
    }
    neighbors.extend(out.into_iter().take(cap).map(|e| e.dst));

    let inc = index.all_incoming_edges_as_of_limited(node, as_of, probe)?;
    if inc.len() == probe {
        capped = true;
        dropped += inc.len() - cap;
    }
    neighbors.extend(inc.into_iter().take(cap).map(|e| e.src));

    Ok((neighbors.into_iter().collect(), capped, dropped))
}

pub struct AffectedSubgraphResolver<'a, S: KvStore + ?Sized> {
    index: TemporalIndex<'a, S>,
    /// Fan-out cap for both resolution stages. Reuses
    /// [`crate::graph::limits::TraversalLimits::max_neighbors_per_node`]'s
    /// default rather than inventing a second number to keep in sync.
    cap: usize,
    /// Backstop on how many nodes ring two will fetch edges *for* — distinct
    /// from `cap`, which only bounds how many edges come *from* any one node.
    /// Capping per-node fan-out does not cap how many affected nodes there
    /// can be, and a mutation touching a 521-neighbour reference node still
    /// leaves 521 nodes for ring two to visit. Reuses
    /// [`crate::graph::limits::TraversalLimits::max_expanded_nodes`]'s default
    /// for the same reason `cap` reuses its sibling — this is the same
    /// "bounded in work, not just in fan-out" property, one layer up.
    max_expanded_nodes: usize,
}

impl<'a, S: KvStore + ?Sized> AffectedSubgraphResolver<'a, S> {
    pub fn new(store: &'a S, cap: usize, max_expanded_nodes: usize) -> Self {
        AffectedSubgraphResolver {
            index: TemporalIndex::new(store),
            cap,
            max_expanded_nodes,
        }
    }

    pub fn resolve(&self, mutation: GraphMutation) -> Result<ResolvedSubgraph> {
        let as_of = mutation.timestamp();
        let (src, dst) = mutation.endpoints();

        let mut truncation = ResolutionTruncation::default();
        let mut affected: HashSet<NodeId> = HashSet::from([src, dst]);

        for endpoint in [src, dst] {
            let (neighbors, capped, dropped) =
                capped_neighbors(&self.index, endpoint, as_of, self.cap)?;
            if capped {
                truncation.fanout_capped = true;
                truncation.neighbors_dropped += dropped;
            }
            affected.extend(neighbors);
        }

        // Receptive field. Computing h2(a) for an affected node `a` needs
        // h1(n) for every neighbour n of a — and h1(n) is itself a function of
        // n's *own* edges, not just of n existing as a feature row. So this
        // has to run twice: once to fetch every affected node's edges (ring
        // one), and again over the *new* nodes ring one discovered, so their
        // own edges are present too (ring two). A single pass over `affected`
        // alone leaves ring-two nodes as isolated feature rows in the
        // subgraph — present, but with none of their real edges — which
        // silently corrupts their h1 and therefore every affected node that
        // has them as a neighbour. Nodes discovered *by* ring two (ring
        // three) only ever serve as a raw-feature input to a ring-two node's
        // h1; nothing downstream reads their own h1, so their edges are never
        // needed and the expansion stops there.
        let mut nodes: HashSet<NodeId> = affected.clone();
        let mut edge_set: HashSet<(NodeId, NodeId)> = HashSet::new();
        let mut fetched: HashSet<NodeId> = HashSet::new();

        let mut ring = affected.clone();
        for ring_number in 0..2 {
            let mut next_ring = HashSet::new();
            for node in ring {
                if !fetched.insert(node) {
                    continue;
                }
                // The budget applies from ring two onward only. Ring one *is*
                // `affected` — the nodes this call reports embeddings for —
                // and every one of them must get its own edges fetched or its
                // computed embedding is wrong, not merely stale (isolated in
                // the subgraph looks identical to having no neighbours at
                // all). Ring two is what actually needs bounding: it is
                // whatever ring one's neighbours turned out to be, sized by
                // the graph's own structure rather than by the mutation, and
                // it is what produced a 49,165-node receptive field from a
                // 521-node affected set on the real clinical graph before
                // this bound existed (`docs/benchmark_report.md` §7.6).
                if ring_number > 0 && fetched.len() > self.max_expanded_nodes {
                    // Stay in the loop rather than returning early: nodes
                    // already queued in `ring` after this one still get their
                    // `fetched` membership recorded (so a later duplicate
                    // sighting is a no-op, not a second skipped attempt), but
                    // none of them are expanded either. The budget is a hard
                    // stop on further RocksDB work, not a soft slowdown.
                    truncation.expansion_capped = true;
                    continue;
                }
                let (neighbors, capped, dropped) =
                    capped_neighbors(&self.index, node, as_of, self.cap)?;
                if capped {
                    truncation.fanout_capped = true;
                    truncation.neighbors_dropped += dropped;
                }
                for other in neighbors {
                    if nodes.insert(other) {
                        next_ring.insert(other);
                    }
                    let pair = if node.as_u64() <= other.as_u64() {
                        (node, other)
                    } else {
                        (other, node)
                    };
                    edge_set.insert(pair);
                }
            }
            ring = next_ring;
        }

        Ok(ResolvedSubgraph {
            affected: affected.into_iter().collect(),
            nodes: nodes.into_iter().collect(),
            edges: edge_set.into_iter().collect(),
            truncation,
        })
    }
}

/// Node-type one-hot dimensions. Must match `ml/train_graphsage.py::NODE_TYPES`
/// exactly, in the same order — this is the interface contract between the
/// Rust feature builder and the trained model's input layer.
pub const NODE_TYPES: [&str; 6] =
    ["Patient", "Condition", "Medication", "Provider", "LabResult", "Encounter"];

/// Build the dense feature matrix and local-index edge list a forward pass
/// needs, for an arbitrary node set. Structural features only — no clinical
/// attributes — for the same reason the trainer avoids them: the model must
/// learn from the graph, not have the answer handed to it as an input.
pub fn build_model_input<S: KvStore + ?Sized>(
    store: &S,
    nodes: &[NodeId],
    edges: &[(NodeId, NodeId)],
    as_of: Timestamp,
) -> Result<(Vec<Vec<f32>>, Vec<Vec<u32>>, HashMap<NodeId, usize>)> {
    let index = TemporalIndex::new(store);
    let local: HashMap<NodeId, usize> = nodes.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    let mut features = vec![vec![0.0f32; NODE_TYPES.len()]; nodes.len()];
    for (i, &node) in nodes.iter().enumerate() {
        if let Some(n) = index.node_as_of(node, as_of)? {
            if let Some(dim) = NODE_TYPES.iter().position(|t| *t == n.node_type) {
                features[i][dim] = 1.0;
            }
        }
    }

    // Symmetric, matching the trainer's undirected message passing.
    let mut src_idx = Vec::with_capacity(edges.len() * 2);
    let mut dst_idx = Vec::with_capacity(edges.len() * 2);
    for &(a, b) in edges {
        if let (Some(&ia), Some(&ib)) = (local.get(&a), local.get(&b)) {
            src_idx.push(ia as u32);
            dst_idx.push(ib as u32);
            src_idx.push(ib as u32);
            dst_idx.push(ia as u32);
        }
    }

    Ok((features, vec![src_idx, dst_idx], local))
}
