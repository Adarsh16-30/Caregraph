//! Layer 3 — bounded, hop-limited point-in-time traversal (PRD Phase 3).
//!
//! Breadth-first expansion over the graph *as it stood at a timestamp*. Every
//! adjacency read goes through [`TemporalIndex`], so a traversal at `as_of`
//! sees exactly the edges that were live then — including edges since removed,
//! and excluding edges added later.
//!
//! # Bounding
//!
//! Three separate bounds apply, all enforced server-side (see
//! [`TraversalLimits`]): hop depth, total result size, and per-node fan-out.
//! The last is the one that matters on a real clinical graph, where shared
//! reference nodes make degree wildly skewed — see the `limits` module.
//!
//! When any bound binds, the result records which one and how often
//! ([`Truncation`]). Returning a partial answer that looks complete is the
//! failure this is designed to prevent: a similarity search over a silently
//! truncated neighbourhood produces plausible, wrong clinical output.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::Result;
use crate::graph::limits::TraversalLimits;
use crate::storage::KvStore;
use crate::temporal::index::{Edge, TemporalIndex};
use crate::types::{EdgeType, NodeId, Timestamp};

/// Which way to follow edges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Follow `CF_EDGES` only — patient to condition.
    Outgoing,
    /// Follow `CF_REVERSE` only — condition back to its patients.
    Incoming,
    /// Both. Required for any query of the form "patients similar to this one",
    /// which must go out to a condition and back down to its other patients.
    Both,
}

impl Direction {
    fn includes_outgoing(self) -> bool {
        matches!(self, Direction::Outgoing | Direction::Both)
    }

    fn includes_incoming(self) -> bool {
        matches!(self, Direction::Incoming | Direction::Both)
    }
}

/// A traversal request, before server-side clamping.
#[derive(Clone, Debug)]
pub struct TraversalRequest {
    pub start: NodeId,
    /// The graph is read as it stood at this timestamp.
    pub as_of: Timestamp,
    pub max_hops: u8,
    pub direction: Direction,
    /// Edge types to follow. Empty means all of [`EdgeType::ALL`].
    pub edge_types: Vec<EdgeType>,
    /// Optional restriction to edges whose live version was written inside
    /// `[from, to]`. See [`TraversalRequest::window`].
    pub window: Option<(Timestamp, Timestamp)>,
    /// Client-requested result budgets. Zero means "unspecified".
    pub max_nodes: usize,
    pub max_edges: usize,
}

impl TraversalRequest {
    /// A traversal of `max_hops` from `start`, as of a timestamp, following all
    /// edge types in both directions.
    pub fn new(start: NodeId, as_of: Timestamp, max_hops: u8) -> Self {
        TraversalRequest {
            start,
            as_of,
            max_hops,
            direction: Direction::Both,
            edge_types: Vec::new(),
            window: None,
            max_nodes: 0,
            max_edges: 0,
        }
    }

    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    pub fn edge_types(mut self, types: impl IntoIterator<Item = EdgeType>) -> Self {
        self.edge_types = types.into_iter().collect();
        self
    }

    /// Follow only edges whose live-at-`as_of` version was written within
    /// `[from, to]`.
    ///
    /// Note this filters on the *version* timestamp, not on existence: an edge
    /// created before the window and never modified is excluded, because
    /// nothing about it falls inside the window. The question this answers is
    /// "what changed here recently", not "what existed here".
    pub fn window(mut self, from: Timestamp, to: Timestamp) -> Self {
        self.window = Some((from, to));
        self
    }

    pub fn max_nodes(mut self, n: usize) -> Self {
        self.max_nodes = n;
        self
    }

    pub fn max_edges(mut self, n: usize) -> Self {
        self.max_edges = n;
        self
    }
}

/// A node reached by the traversal, with its distance from the start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VisitedNode {
    pub node_id: NodeId,
    /// Hops from the start node. The start itself is 0.
    pub hops: u8,
}

/// Why a result is incomplete, if it is.
///
/// Each field names a specific bound that bound, so a caller can tell an
/// exhausted traversal from a capped one — and, if capped, which cap to raise.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Truncation {
    /// The node result budget filled.
    pub hit_node_limit: bool,
    /// The edge result budget filled.
    pub hit_edge_limit: bool,
    /// The whole-traversal expansion backstop was reached.
    pub hit_expansion_limit: bool,
    /// How many nodes had their neighbour list cut short by the fan-out cap.
    /// Exact: a node either hit the cap or it did not.
    pub fanout_capped_nodes: usize,
    /// How many neighbours were dropped by that cap.
    ///
    /// A **lower bound** during traversal. The fan-out scan stops one past the
    /// cap, so it learns *that* more neighbours existed without learning how
    /// many — counting them exactly would mean reading the entire adjacency,
    /// which is the cost the cap exists to avoid. `fanout_capped_nodes > 0`
    /// with a small count here means "at least this many", not "this many".
    /// [`SnapshotReader`](crate::graph::SnapshotReader) scans exhaustively and
    /// does report an exact figure.
    pub fanout_dropped_neighbors: usize,
}

impl Truncation {
    /// True if any bound bound — i.e. the result is not the complete answer.
    pub fn is_truncated(&self) -> bool {
        self.hit_node_limit
            || self.hit_edge_limit
            || self.hit_expansion_limit
            || self.fanout_capped_nodes > 0
    }
}

/// The outcome of a bounded traversal.
#[derive(Clone, Debug)]
pub struct TraversalResult {
    pub start: NodeId,
    pub as_of: Timestamp,
    /// Reached nodes, ordered by hop distance then node id.
    pub nodes: Vec<VisitedNode>,
    pub edges: Vec<Edge>,
    /// The hop bound actually applied, after server-side clamping. May be lower
    /// than requested.
    pub effective_max_hops: u8,
    pub truncation: Truncation,
}

impl TraversalResult {
    /// Nodes at exactly `hops` distance.
    pub fn at_hop(&self, hops: u8) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes
            .iter()
            .filter(move |n| n.hops == hops)
            .map(|n| n.node_id)
    }
}

/// Bounded traversal over a point-in-time view of the graph.
pub struct Traverser<'a, S: KvStore + ?Sized> {
    index: TemporalIndex<'a, S>,
    limits: TraversalLimits,
}

impl<'a, S: KvStore + ?Sized> Traverser<'a, S> {
    pub fn new(store: &'a S, limits: TraversalLimits) -> Self {
        Traverser {
            index: TemporalIndex::new(store),
            limits,
        }
    }

    /// Run a breadth-first, hop-bounded traversal.
    ///
    /// BFS rather than DFS so that `hops` is the true shortest distance from
    /// the start: the first time a node is reached is via a shortest path, so
    /// the recorded distance is correct and never revised.
    pub fn traverse(&self, request: &TraversalRequest) -> Result<TraversalResult> {
        let max_hops = self.limits.clamp_hops(request.max_hops);
        let max_nodes = self.limits.clamp_result_nodes(request.max_nodes);
        let max_edges = self.limits.clamp_result_edges(request.max_edges);

        let edge_types: Vec<EdgeType> = if request.edge_types.is_empty() {
            EdgeType::ALL.to_vec()
        } else {
            let mut types = request.edge_types.clone();
            types.sort_by_key(|t| *t as u16);
            types.dedup();
            types
        };

        let mut truncation = Truncation::default();
        let mut distance: HashMap<NodeId, u8> = HashMap::new();
        let mut edges: Vec<Edge> = Vec::new();
        let mut seen_edges: HashSet<(NodeId, NodeId, u16)> = HashSet::new();
        let mut expanded = 0usize;

        distance.insert(request.start, 0);
        let mut frontier: VecDeque<(NodeId, u8)> = VecDeque::new();
        frontier.push_back((request.start, 0));

        'bfs: while let Some((node, hops)) = frontier.pop_front() {
            if hops >= max_hops {
                continue;
            }
            if expanded >= self.limits.max_expanded_nodes {
                truncation.hit_expansion_limit = true;
                break;
            }
            expanded += 1;

            for &edge_type in &edge_types {
                let mut neighbors: Vec<(Edge, NodeId)> = Vec::new();

                // One more than the cap: if a direction returns `probe` edges we
                // know the cap bound without having read the whole adjacency.
                // Reading `cap + 1` is what keeps a hop through a 54,000-degree
                // reference node O(cap) rather than O(degree) — see the
                // `limits` module for why that distinction decides the p95.
                let probe = self.limits.max_neighbors_per_node.saturating_add(1);
                let mut capped_direction = false;

                if request.direction.includes_outgoing() {
                    let out = self
                        .index
                        .edges_as_of_limited(node, edge_type, request.as_of, probe)?;
                    capped_direction |= out.len() == probe;
                    for edge in out {
                        let other = edge.dst;
                        neighbors.push((edge, other));
                    }
                }
                if request.direction.includes_incoming() {
                    let inc = self.index.incoming_edges_as_of_limited(
                        node,
                        edge_type,
                        request.as_of,
                        probe,
                    )?;
                    capped_direction |= inc.len() == probe;
                    for edge in inc {
                        let other = edge.src;
                        neighbors.push((edge, other));
                    }
                }

                // Time-window filter, applied before the fan-out cap so the cap
                // selects from the edges the caller actually asked about.
                if let Some((from, to)) = request.window {
                    neighbors.retain(|(edge, _)| edge.timestamp >= from && edge.timestamp <= to);
                }

                // Deterministic order, so that which neighbours survive the cap
                // is reproducible across runs rather than dependent on scan
                // order. A benchmark whose result set varies run to run is not
                // measuring anything stable.
                neighbors.sort_by_key(|(edge, other)| (*other, edge.timestamp));

                if capped_direction || neighbors.len() > self.limits.max_neighbors_per_node {
                    truncation.fanout_capped_nodes += 1;
                    truncation.fanout_dropped_neighbors += neighbors
                        .len()
                        .saturating_sub(self.limits.max_neighbors_per_node);
                    neighbors.truncate(self.limits.max_neighbors_per_node);
                }

                for (edge, other) in neighbors {
                    // Deduplicate: an edge reached from both endpoints, or via
                    // both CF_EDGES and CF_REVERSE, is still one edge.
                    let identity = (edge.src, edge.dst, edge.edge_type as u16);
                    if seen_edges.insert(identity) {
                        if edges.len() >= max_edges {
                            truncation.hit_edge_limit = true;
                            break 'bfs;
                        }
                        edges.push(edge);
                    }

                    if !distance.contains_key(&other) {
                        if distance.len() >= max_nodes {
                            truncation.hit_node_limit = true;
                            break 'bfs;
                        }
                        distance.insert(other, hops + 1);
                        if hops + 1 < max_hops {
                            frontier.push_back((other, hops + 1));
                        }
                    }
                }
            }
        }

        let mut nodes: Vec<VisitedNode> = distance
            .into_iter()
            .map(|(node_id, hops)| VisitedNode { node_id, hops })
            .collect();
        nodes.sort_by_key(|n| (n.hops, n.node_id));

        Ok(TraversalResult {
            start: request.start,
            as_of: request.as_of,
            nodes,
            edges,
            effective_max_hops: max_hops,
            truncation,
        })
    }
}
