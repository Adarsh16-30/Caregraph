//! `AffectedSubgraphResolver` — which nodes' embeddings a mutation
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
use crate::types::{EdgeType, NodeId, Timestamp};

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
    /// The nodes whose adjacency lists this resolution actually read. A
    /// mutation's output depends on exactly these adjacencies (node types are
    /// immutable through the API), so this is the precise *read set* the
    /// ordered-commit sequencer checks against concurrently committed writes.
    pub fetched: HashSet<NodeId>,
    /// Nodes whose embedding the mutation changes but which are **not**
    /// recomputed now (hub deferral, see [`crate::embedding::deferral`]). They
    /// are disjoint from `affected`; the caller stages a repair marker for each
    /// in the mutation's own batch. Empty when deferral is off.
    pub deferred: Vec<NodeId>,
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
/// One node's capped neighbour list: `(neighbours, fanout_capped, dropped)`.
type Expansion = (Vec<NodeId>, bool, usize);

/// Upper bound on scan threads per resolve; several mutations resolve at once
/// under the worker pool, so one resolve must not claim every core.
const MAX_RESOLVE_THREADS: usize = 4;

fn capped_neighbors<S: KvStore + ?Sized>(
    index: &TemporalIndex<'_, S>,
    node: NodeId,
    as_of: Timestamp,
    cap: usize,
) -> Result<Expansion> {
    let mut neighbors = HashSet::new();
    let mut capped = false;
    let mut dropped = 0usize;
    let probe = cap.saturating_add(1);

    let out = index.all_neighbor_ids_as_of_limited(node, as_of, false, probe)?;
    if out.len() == probe {
        capped = true;
        dropped += out.len() - cap;
    }
    neighbors.extend(out.into_iter().take(cap));

    let inc = index.all_neighbor_ids_as_of_limited(node, as_of, true, probe)?;
    if inc.len() == probe {
        capped = true;
        dropped += inc.len() - cap;
    }
    neighbors.extend(inc.into_iter().take(cap));

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
    /// A node with more than this many live neighbours is a hub whose update is
    /// deferred (see [`crate::embedding::deferral`]). `None` disables deferral.
    hub_degree: Option<usize>,
}

impl<'a, S: KvStore + ?Sized> AffectedSubgraphResolver<'a, S> {
    /// Deferral follows the process-wide setting
    /// ([`crate::embedding::deferral::hub_degree`], off unless configured);
    /// [`Self::with_hub_degree`] overrides it for one resolver.
    pub fn new(store: &'a S, cap: usize, max_expanded_nodes: usize) -> Self {
        AffectedSubgraphResolver {
            index: TemporalIndex::new(store),
            cap,
            max_expanded_nodes,
            hub_degree: crate::embedding::deferral::hub_degree(),
        }
    }

    pub fn with_hub_degree(mut self, hub_degree: Option<usize>) -> Self {
        self.hub_degree = hub_degree;
        self
    }

    fn is_hub(&self, degree: usize) -> bool {
        self.hub_degree.is_some_and(|h| degree > h)
    }

    /// [`capped_neighbors`] for every node, in the order given. The scans are
    /// independent point reads that each pay RocksDB seek latency, so a ring of
    /// hundreds of nodes is split across a few scoped threads; small rings stay
    /// on the calling thread, where a spawn would cost more than it saves.
    fn expand_all(&self, nodes: &[NodeId], as_of: Timestamp) -> Result<Vec<Expansion>> {
        const MIN_PER_THREAD: usize = 48;
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(MAX_RESOLVE_THREADS)
            .min(nodes.len() / MIN_PER_THREAD);
        if threads <= 1 {
            return nodes
                .iter()
                .map(|&n| capped_neighbors(&self.index, n, as_of, self.cap))
                .collect();
        }
        let chunk = nodes.len().div_ceil(threads);
        std::thread::scope(|scope| {
            let handles: Vec<_> = nodes
                .chunks(chunk)
                .map(|part| {
                    scope.spawn(move || {
                        part.iter()
                            .map(|&n| capped_neighbors(&self.index, n, as_of, self.cap))
                            .collect::<Result<Vec<Expansion>>>()
                    })
                })
                .collect();
            let mut out = Vec::with_capacity(nodes.len());
            for h in handles {
                out.extend(h.join().expect("resolver scan thread panicked")?);
            }
            Ok(out)
        })
    }

    /// Resolve the subgraph a mutation needs. With hub deferral on, a hub among
    /// the endpoints or their neighbours is read (it is an input) but not
    /// recomputed, and the nodes whose update that postpones are returned in
    /// [`ResolvedSubgraph::deferred`] instead of `affected`.
    pub fn resolve(&self, mutation: GraphMutation) -> Result<ResolvedSubgraph> {
        let as_of = mutation.timestamp();
        let (src, dst) = mutation.endpoints();

        let mut truncation = ResolutionTruncation::default();
        let mut affected: HashSet<NodeId> = HashSet::from([src, dst]);
        // Hubs: read in full (their neighbours become raw-feature rows) but not
        // recomputed, and their neighbours are not expanded further on their
        // account. `deferred` is every node whose update moves to the repair worker.
        let mut hubs: HashSet<NodeId> = HashSet::new();
        let mut deferred: HashSet<NodeId> = HashSet::new();

        for endpoint in [src, dst] {
            let (neighbors, capped, dropped) =
                capped_neighbors(&self.index, endpoint, as_of, self.cap)?;
            if capped {
                truncation.fanout_capped = true;
                truncation.neighbors_dropped += dropped;
            }
            if self.is_hub(neighbors.len()) {
                // Every neighbour of a hub endpoint has a changed embedding, but
                // recomputing them is exactly the expensive part.
                hubs.insert(endpoint);
                deferred.extend(neighbors);
            } else {
                affected.extend(neighbors);
            }
        }

        self.expand_rings(affected, hubs, deferred, truncation, as_of, true)
    }

    /// Resolve the subgraph needed to recompute exactly `targets` as of `as_of`.
    /// Used by the repair worker: it never defers (it is the deferred work) and
    /// reports every target as affected.
    pub fn resolve_targets(
        &self,
        targets: &[NodeId],
        as_of: Timestamp,
    ) -> Result<ResolvedSubgraph> {
        let affected: HashSet<NodeId> = targets.iter().copied().collect();
        self.expand_rings(
            affected,
            HashSet::new(),
            HashSet::new(),
            ResolutionTruncation::default(),
            as_of,
            false,
        )
    }

    fn expand_rings(
        &self,
        mut affected: HashSet<NodeId>,
        mut hubs: HashSet<NodeId>,
        mut deferred: HashSet<NodeId>,
        mut truncation: ResolutionTruncation,
        as_of: Timestamp,
        defer: bool,
    ) -> Result<ResolvedSubgraph> {
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
        // Nodes already queued for the next ring. Tracked apart from `nodes`
        // because a hub's neighbours enter `nodes` as feature rows without being
        // queued: if a later expansion finds one of them, it must still queue it.
        let mut queued: HashSet<NodeId> = HashSet::new();

        let mut ring: Vec<NodeId> = affected.iter().copied().collect();
        for ring_number in 0..2 {
            // Sorted so the budget below always spends itself on the same
            // nodes for the same graph state; a `HashSet`'s order differs per
            // process and made *which* ring-two nodes were skipped arbitrary.
            ring.sort();
            let unfetched: Vec<NodeId> = ring
                .iter()
                .copied()
                .filter(|n| fetched.insert(*n))
                .collect();

            // The budget applies from ring two onward only. Ring one *is*
            // `affected` — the nodes this call reports embeddings for — and
            // every one of them must get its own edges fetched or its
            // computed embedding is wrong, not merely stale (isolated in the
            // subgraph looks identical to having no neighbours at all). Ring
            // two is what actually needs bounding: it is whatever ring one's
            // neighbours turned out to be, sized by the graph's own structure
            // rather than by the mutation, and it is what produced a
            // 49,165-node receptive field from a 521-node affected set on the
            // real clinical graph before this bound existed
            // (`docs/benchmark_report.md` §7.6). Nodes past the budget are
            // still recorded in `fetched` (a later duplicate sighting is a
            // no-op) but are not expanded: the budget is a hard stop on
            // further RocksDB work, not a soft slowdown.
            let expand: &[NodeId] = if ring_number > 0 {
                let already = fetched.len() - unfetched.len();
                let allowed = self.max_expanded_nodes.saturating_sub(already);
                if unfetched.len() > allowed {
                    truncation.expansion_capped = true;
                }
                &unfetched[..unfetched.len().min(allowed)]
            } else {
                &unfetched
            };

            let expanded = self.expand_all(expand, as_of)?;

            let mut next_ring = Vec::new();
            for (&node, (neighbors, capped, dropped)) in expand.iter().zip(expanded) {
                if capped {
                    truncation.fanout_capped = true;
                    truncation.neighbors_dropped += dropped;
                }
                // A hub in the affected set is read like any other node, but is
                // not recomputed now and does not pull its neighbours into the
                // next ring: nothing computed synchronously depends on their
                // own edges.
                let is_hub = ring_number == 0
                    && defer
                    && (hubs.contains(&node) || self.is_hub(neighbors.len()));
                if is_hub {
                    hubs.insert(node);
                }
                for other in neighbors {
                    nodes.insert(other);
                    if !is_hub && !fetched.contains(&other) && queued.insert(other) {
                        next_ring.push(other);
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

        // A hub in the affected set is deferred, not computed. Anything that is
        // computed now needs no repair, so it leaves `deferred`.
        affected.retain(|n| !hubs.contains(n));
        deferred.extend(hubs.iter().copied());
        deferred.retain(|n| !affected.contains(n));

        // Phase 9 (Feature 1): sorted, not left in `HashSet` iteration order.
        // Two reasons, not one: (1) an attribution record is meant to be
        // re-derivable from the same mutation replayed against the same
        // starting state, and a `HashSet`'s iteration order is not stable
        // across process runs; (2) less obviously, `edge_index` column order
        // is the order a forward pass's scatter/mean aggregation sums
        // neighbour messages in, and float32 addition is not associative — an
        // unstable order means the model's own output could differ in its
        // last bits between two runs of the identical mutation, which would
        // make the exactness tests' tolerances (`docs/benchmark_report.md`
        // §7.4) load-bearing against noise this sort removes for free.
        let mut affected: Vec<NodeId> = affected.into_iter().collect();
        affected.sort();
        let mut deferred: Vec<NodeId> = deferred.into_iter().collect();
        deferred.sort();
        let mut nodes: Vec<NodeId> = nodes.into_iter().collect();
        nodes.sort();
        let mut edges: Vec<(NodeId, NodeId)> = edge_set.into_iter().collect();
        edges.sort();

        Ok(ResolvedSubgraph {
            affected,
            nodes,
            edges,
            truncation,
            fetched,
            deferred,
        })
    }
}

/// Node-type one-hot dimensions. Must match `ml/train_graphsage.py::NODE_TYPES`
/// exactly, in the same order — this is the interface contract between the
/// Rust feature builder and the trained model's input layer.
pub const NODE_TYPES: [&str; 6] = [
    "Patient",
    "Condition",
    "Medication",
    "Provider",
    "LabResult",
    "Encounter",
];

/// `(node features, symmetric edge index as [src_idx, dst_idx], node id ->
/// local row index)` — named for clippy's `type_complexity` lint and to give
/// the shape a name at each of [`build_model_input`]'s call sites.
pub type ModelInput = (Vec<Vec<f32>>, Vec<Vec<u32>>, HashMap<NodeId, usize>);

/// Build the dense feature matrix and local-index edge list a forward pass
/// needs, for an arbitrary node set. Structural features only — no clinical
/// attributes — for the same reason the trainer avoids them: the model must
/// learn from the graph, not have the answer handed to it as an input.
pub fn build_model_input<S: KvStore + ?Sized>(
    store: &S,
    nodes: &[NodeId],
    edges: &[(NodeId, NodeId)],
    as_of: Timestamp,
) -> Result<ModelInput> {
    let index = TemporalIndex::new(store);
    let local: HashMap<NodeId, usize> = nodes.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    let cache = store.node_type_cache();
    let mut features = vec![vec![0.0f32; NODE_TYPES.len()]; nodes.len()];
    for (i, &node) in nodes.iter().enumerate() {
        if let Some(dim) = cache.and_then(|c| c.get(node, as_of)) {
            if let Some(slot) = features[i].get_mut(dim as usize) {
                *slot = 1.0;
            }
            continue;
        }
        if let Some(n) = index.node_as_of(node, as_of)? {
            let dim = NODE_TYPES.iter().position(|t| *t == n.node_type);
            if let Some(dim) = dim {
                features[i][dim] = 1.0;
                if let Some(c) = cache {
                    c.insert(node, n.timestamp, dim as u8);
                }
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

/// Patch the one edge a mutation changes directly into a subgraph resolved
/// from *pre*-mutation state — shared by every commit path in
/// `atomic_commit.rs` (associative and GAT alike), since none of them can
/// see the mutation itself when they call [`AffectedSubgraphResolver::resolve`]:
/// the structural write hasn't landed yet (see `atomic_commit.rs`'s module
/// doc for why not landing it yet is exactly the point).
///
/// Generic over `S` for the same reason [`build_model_input`] is — real
/// callers always use `RocksKv`, but nothing here is RocksDB-specific.
pub fn patch_subgraph_for_mutation<S: KvStore + ?Sized>(
    subgraph: &mut ResolvedSubgraph,
    index: &TemporalIndex<'_, S>,
    mutation: GraphMutation,
) -> Result<()> {
    let (src, dst) = mutation.endpoints();
    let pair = if src.as_u64() <= dst.as_u64() {
        (src, dst)
    } else {
        (dst, src)
    };

    match mutation {
        GraphMutation::AddEdge { .. } => {
            // src and dst are always present in `subgraph.nodes` — resolve()
            // seeds `affected` with both endpoints, and `nodes` starts from
            // `affected`. Only the edge between them is missing, because
            // resolve() read the graph before this mutation landed. The
            // model's own topology is edge-type-agnostic (`build_model_input`
            // symmetrises without regard to type), so adding the pair is
            // correct regardless of which of the six edge types this
            // mutation is, and regardless of which model will consume it.
            if !subgraph.edges.contains(&pair) {
                subgraph.edges.push(pair);
                // Keep the sorted invariant `resolve()` establishes (see its
                // own comment on why) — a plain `push` would otherwise leave
                // exactly one call's output in insertion order instead.
                subgraph.edges.sort();
            }
        }
        GraphMutation::RemoveEdge { edge_type, .. } => {
            // `pair` is already in `subgraph.edges` if resolve() discovered
            // src and dst as each other's neighbours — the edge being
            // removed was still live in the state resolve() read. Strip it
            // only if no OTHER edge type still connects the same two node
            // ids: two nodes related two different ways stay connected in
            // the model's topology after removing just one relationship,
            // and that case is cheap enough to check for that assuming it
            // away is not worth the risk.
            let ts = mutation.timestamp();
            let mut still_connected = false;
            for &other_type in EdgeType::ALL.iter() {
                if other_type == edge_type {
                    continue;
                }
                if index.edge_as_of(src, other_type, dst, ts)?.is_some() {
                    still_connected = true;
                    break;
                }
            }
            if !still_connected {
                subgraph.edges.retain(|&e| e != pair);
            }
        }
    }
    Ok(())
}
