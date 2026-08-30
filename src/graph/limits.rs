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
//! When the cap binds, the result says so — see
//! [`Truncation`](crate::graph::traversal::Truncation). A truncated result
//! reported as complete would be a silent correctness failure of exactly the
//! kind Rule 7 exists to prevent.
//!
//! # How the cap bounds work, not just the result
//!
//! The cap is enforced *during* the adjacency scan, not after it. `traversal.rs`
//! asks the temporal index for at most `max_neighbors_per_node + 1` live
//! neighbours per direction, and the index stops walking once it has them
//! ([`edges_as_of_limited`](crate::temporal::TemporalIndex::edges_as_of_limited)).
//! A hop through a 54,000-degree reference node therefore costs O(cap), not
//! O(degree).
//!
//! This was not always true. The first implementation materialised the full
//! adjacency and sorted it before truncating, which made the cap bound the
//! result while leaving the work unbounded: sweeping it from 512 down to 32
//! moved p95 not at all (179-202 ms, no trend) while cutting the answer 14x.
//! That measurement is what motivated the bounded scan — see
//! `docs/benchmark_report.md` §4.
//!
//! The trade-off is *which* neighbours survive. Scanning backward through time
//! means the cap keeps the most recently updated neighbours rather than an
//! arbitrary subset. For care-pathway queries that is the more defensible
//! answer, but it is a behaviour change, not a pure optimisation.

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
        assert_eq!(
            limits.clamp_result_nodes(usize::MAX),
            limits.max_result_nodes
        );
        assert_eq!(
            limits.clamp_result_edges(usize::MAX),
            limits.max_result_edges
        );
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
