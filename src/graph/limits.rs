//! Server-side query limits (PRD Phase 3).
//!
//! These caps are enforced *regardless of what the client asks for*. A request
//! may lower a bound but never raise one — `clamp` takes the minimum, never the
//! maximum. A client cannot opt out of them.
//!
//! # Why a fan-out cap exists
//!
//! The clinical graph uses shared reference nodes: one `Condition` node for
//! Stage 3 CKD, referenced by every patient who has it
//! (`data/clinical_graph_schema.md`). That is what makes care-pathway
//! similarity meaningful, but it means degree is wildly skewed — a patient has
//! tens of edges, while a common condition has thousands.
//!
//! A 2-hop traversal from a patient therefore expands
//! `Patient → Condition → every other patient with that condition`. Without a
//! per-node fan-out cap, one hop through a common comorbidity pulls in a large
//! fraction of the cohort, and the Section 1 target of p95 < 50 ms for a 2-hop
//! traversal is not reachable on the full UKPDS graph at any constant factor.
//!
//! The cap is what makes the traversal genuinely *bounded* rather than
//! bounded-in-hops-but-unbounded-in-work. When it binds, the result says so —
//! see [`Truncation`](crate::graph::traversal::Truncation). A truncated result
//! reported as complete would be a silent correctness failure of exactly the
//! kind Rule 7 exists to prevent.

/// Hard server-side ceilings on traversal work and result size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraversalLimits {
    /// Maximum hop depth. The PRD benchmarks 2-hop; 4 leaves headroom for
    /// exploratory queries without permitting unbounded expansion.
    pub max_hops: u8,
    /// Maximum distinct nodes returned.
    pub max_result_nodes: usize,
    /// Maximum edges returned.
    pub max_result_edges: usize,
    /// Maximum neighbours expanded from any single node, per direction and
    /// edge type. Bounds the damage a high-degree reference node can do.
    pub max_neighbors_per_node: usize,
    /// Maximum nodes whose adjacency is expanded, across the whole traversal.
    /// A backstop for pathological shapes that slip past the other caps.
    pub max_expanded_nodes: usize,
}

impl Default for TraversalLimits {
    fn default() -> Self {
        TraversalLimits {
            max_hops: 4,
            max_result_nodes: 10_000,
            max_result_edges: 50_000,
            max_neighbors_per_node: 512,
            max_expanded_nodes: 5_000,
        }
    }
}

impl TraversalLimits {
    /// Limits for the Section 1 benchmark workload: 2-hop bounded traversal.
    pub fn benchmark() -> Self {
        TraversalLimits {
            max_hops: 2,
            ..Self::default()
        }
    }

    /// Reduce a client-requested hop count to the server ceiling.
    ///
    /// Takes the minimum. A client asking for 50 hops gets `max_hops`, not 50.
    pub fn clamp_hops(&self, requested: u8) -> u8 {
        requested.min(self.max_hops)
    }

    /// Reduce a client-requested node budget to the server ceiling.
    ///
    /// A request of 0 means "unspecified" and yields the server default rather
    /// than an empty result — otherwise a client could accidentally ask for
    /// nothing and receive a silently empty answer.
    pub fn clamp_result_nodes(&self, requested: usize) -> usize {
        if requested == 0 {
            self.max_result_nodes
        } else {
            requested.min(self.max_result_nodes)
        }
    }

    /// Reduce a client-requested edge budget to the server ceiling.
    pub fn clamp_result_edges(&self, requested: usize) -> usize {
        if requested == 0 {
            self.max_result_edges
        } else {
            requested.min(self.max_result_edges)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_cannot_raise_a_limit() {
        let limits = TraversalLimits::default();
        assert_eq!(limits.clamp_hops(200), limits.max_hops);
        assert_eq!(limits.clamp_result_nodes(usize::MAX), limits.max_result_nodes);
        assert_eq!(limits.clamp_result_edges(usize::MAX), limits.max_result_edges);
    }

    #[test]
    fn a_client_can_lower_a_limit() {
        let limits = TraversalLimits::default();
        assert_eq!(limits.clamp_hops(1), 1);
        assert_eq!(limits.clamp_result_nodes(10), 10);
        assert_eq!(limits.clamp_result_edges(10), 10);
    }

    #[test]
    fn an_unspecified_budget_falls_back_to_the_server_default() {
        let limits = TraversalLimits::default();
        assert_eq!(limits.clamp_result_nodes(0), limits.max_result_nodes);
        assert_eq!(limits.clamp_result_edges(0), limits.max_result_edges);
    }

    #[test]
    fn zero_hops_is_a_legitimate_request() {
        // "Just the start node" is meaningful, and must not be rewritten to a
        // default the way a zero result budget is.
        assert_eq!(TraversalLimits::default().clamp_hops(0), 0);
    }
}
