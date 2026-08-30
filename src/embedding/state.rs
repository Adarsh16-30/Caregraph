//! Mutation and pipeline state (PRD Section 4.2).
//!
//! The PRD's `MutationContext` carries a live `WriteBatch` because Phase 5
//! commits the structural mutation and the embedding update in one atomic
//! write (Rule 5). That merge is Phase 5's job. Phase 4 writes embeddings to
//! `CF_EMBEDDINGS` in their own batch, separate from the mutation — real, not
//! atomic yet. `AtomicCommitter` upgrades this in Phase 5; nothing here should
//! need to change shape when it does.

use crate::types::{EdgeType, Embedding, ModelKind, NodeId, Timestamp};

/// The structural change that triggered this pipeline run.
#[derive(Clone, Copy, Debug)]
pub enum GraphMutation {
    AddEdge {
        src: NodeId,
        dst: NodeId,
        edge_type: EdgeType,
        ts: Timestamp,
    },
    RemoveEdge {
        src: NodeId,
        dst: NodeId,
        edge_type: EdgeType,
        ts: Timestamp,
    },
}

impl GraphMutation {
    pub fn timestamp(self) -> Timestamp {
        match self {
            GraphMutation::AddEdge { ts, .. } | GraphMutation::RemoveEdge { ts, .. } => ts,
        }
    }

    /// The two nodes this mutation touches. Resolution starts from here.
    pub fn endpoints(self) -> (NodeId, NodeId) {
        match self {
            GraphMutation::AddEdge { src, dst, .. }
            | GraphMutation::RemoveEdge { src, dst, .. } => (src, dst),
        }
    }
}

/// Why a node's embedding was, or was not, updated. Distinct from
/// [`crate::graph::traversal::Truncation`] but the same honesty rule: a bound
/// binding is recorded, never silently absorbed into "done".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResolutionTruncation {
    /// The affected-set fan-out cap bound at least one endpoint. Nodes beyond
    /// the cap keep their pre-mutation embedding until their own next update —
    /// they are not wrong, they are stale, and that distinction is the point
    /// of naming this field rather than omitting it.
    pub fanout_capped: bool,
    /// How many neighbours were left unresolved by that cap. A lower bound,
    /// for the same reason `Truncation::fanout_dropped_neighbors` is one: the
    /// scan stops at the cap rather than paying to count the rest.
    pub neighbors_dropped: usize,
    /// The receptive field's total-size backstop bound, distinct from
    /// `fanout_capped`. Capping each node's *own* fan-out does not cap how
    /// many affected nodes there can be: a mutation touching a reference node
    /// with 521 (capped) neighbours still has 521 nodes whose own neighbours
    /// ring two must fetch, and 521 × up to 512 each is no longer a small
    /// number. Measured on the real clinical graph before this field existed:
    /// one such mutation produced a 49,165-node, 101,675-edge receptive field
    /// from a 521-node affected set — see `docs/benchmark_report.md` §7.6.
    /// Nodes past this budget are simply never visited in ring two; their own
    /// edges are absent from the subgraph, same stale-not-wrong semantics as
    /// `fanout_capped`.
    pub expansion_capped: bool,
}

/// One mutation's pass through the pipeline (PRD 4.2).
pub struct MutationContext {
    pub mutation: GraphMutation,
    pub active_model: ModelKind,
    /// Nodes the resolver named as affected. Populated by
    /// `resolve_affected_subgraph`; empty until then.
    pub affected: Vec<NodeId>,
    pub truncation: ResolutionTruncation,
    pub embeddings_after: Vec<(NodeId, Embedding)>,
    pub fallback: bool,
}

impl MutationContext {
    pub fn new(mutation: GraphMutation, active_model: ModelKind) -> Self {
        MutationContext {
            mutation,
            active_model,
            affected: Vec::new(),
            truncation: ResolutionTruncation::default(),
            embeddings_after: Vec::new(),
            fallback: false,
        }
    }
}
