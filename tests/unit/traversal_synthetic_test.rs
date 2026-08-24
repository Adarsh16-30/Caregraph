//! Bounded point-in-time traversal (PRD Phase 3).
//!
//! Graph shapes are synthetic; the storage engine is a real on-disk RocksDB.

use caregraph::graph::{Direction, TraversalLimits, TraversalRequest, Traverser};
use caregraph::types::NodeId;

use crate::graph_fixture::{
    fixture, hub_fixture, ts, CKD, DX, METFORMIN, P1, P2, P3, RETINOPATHY, RX,
};

/// Nodes reached, as plain ids.
fn reached(result: &caregraph::graph::TraversalResult) -> Vec<u64> {
    result.nodes.iter().map(|n| n.node_id.as_u64()).collect()
}

fn at_hop(result: &caregraph::graph::TraversalResult, hops: u8) -> Vec<u64> {
    let mut ids: Vec<u64> = result.at_hop(hops).map(|n| n.as_u64()).collect();
    ids.sort_unstable();
    ids
}

// ---------------------------------------------------------------------------
// Hop bounding
// ---------------------------------------------------------------------------

#[test]
fn zero_hops_returns_only_the_start_node() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 0))
        .unwrap();

    assert_eq!(reached(&result), vec![P1.as_u64()]);
    assert!(result.edges.is_empty());
    assert!(!result.truncation.is_truncated(), "exhausted, not capped");
}

#[test]
fn one_hop_reaches_direct_neighbours_only() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 1))
        .unwrap();

    assert_eq!(at_hop(&result, 0), vec![P1.as_u64()]);
    assert_eq!(at_hop(&result, 1), vec![CKD.as_u64(), METFORMIN.as_u64()]);
    // P2 is two hops away and must not appear.
    assert!(!reached(&result).contains(&P2.as_u64()));
}

#[test]
fn two_hops_reaches_a_co_patient_through_the_shared_condition() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 2))
        .unwrap();

    // This is the query the system exists to serve: out to a shared condition,
    // back down to the other patients who have it.
    assert_eq!(at_hop(&result, 2), vec![P2.as_u64()]);
    // P3's component is disconnected from P1's and must not leak in.
    assert!(!reached(&result).contains(&P3.as_u64()));
    assert!(!reached(&result).contains(&RETINOPATHY.as_u64()));
}

#[test]
fn hop_distance_is_the_shortest_path_not_the_first_path_found() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 3))
        .unwrap();

    // P1 is reachable from itself via CKD -> P2 -> CKD, but it is the start and
    // must stay at distance 0.
    let p1 = result.nodes.iter().find(|n| n.node_id == P1).unwrap();
    assert_eq!(p1.hops, 0);
}

// ---------------------------------------------------------------------------
// Point-in-time semantics
// ---------------------------------------------------------------------------

#[test]
fn traversal_does_not_follow_edges_that_did_not_exist_yet() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    // P1's diagnosis lands at t=110; at t=105 it has not happened.
    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(105), 2))
        .unwrap();

    assert_eq!(reached(&result), vec![P1.as_u64()]);
}

#[test]
fn traversal_does_not_follow_a_retracted_edge() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    // P1 -DX-> CKD is retracted at t=200.
    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(250), 2))
        .unwrap();

    assert!(!reached(&result).contains(&CKD.as_u64()));
    assert!(
        !reached(&result).contains(&P2.as_u64()),
        "the path to P2 ran through the retracted edge"
    );
    // The prescription is untouched by that retraction.
    assert!(reached(&result).contains(&METFORMIN.as_u64()));
}

#[test]
fn a_retracted_edge_is_still_followed_when_querying_the_past() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    // The edge was retracted at t=200 but genuinely existed at t=150. This is
    // the case a hard delete would silently get wrong.
    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 2))
        .unwrap();

    assert!(reached(&result).contains(&CKD.as_u64()));
    assert!(reached(&result).contains(&P2.as_u64()));
}

// ---------------------------------------------------------------------------
// Direction and edge-type filtering
// ---------------------------------------------------------------------------

#[test]
fn outgoing_only_does_not_walk_back_down_to_co_patients() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 2).direction(Direction::Outgoing))
        .unwrap();

    assert!(reached(&result).contains(&CKD.as_u64()));
    assert!(
        !reached(&result).contains(&P2.as_u64()),
        "reaching P2 requires traversing CKD's incoming edges"
    );
}

#[test]
fn incoming_only_finds_every_patient_with_a_condition() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(CKD, ts(150), 1).direction(Direction::Incoming))
        .unwrap();

    assert_eq!(at_hop(&result, 1), vec![P1.as_u64(), P2.as_u64()]);
}

#[test]
fn edge_type_filtering_excludes_other_relationships() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 1).edge_types([DX]))
        .unwrap();

    assert!(reached(&result).contains(&CKD.as_u64()));
    assert!(
        !reached(&result).contains(&METFORMIN.as_u64()),
        "METFORMIN is reached by a PRESCRIBED edge, which was excluded"
    );
}

#[test]
fn several_edge_types_can_be_requested_together() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 1).edge_types([DX, RX, DX]))
        .unwrap();

    assert_eq!(at_hop(&result, 1), vec![CKD.as_u64(), METFORMIN.as_u64()]);
}

// ---------------------------------------------------------------------------
// Time windows
// ---------------------------------------------------------------------------

#[test]
fn a_time_window_selects_only_edges_written_inside_it() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    // P1 -DX-> CKD was written at t=110, P1 -RX-> METFORMIN at t=130.
    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 1).window(ts(125), ts(135)))
        .unwrap();

    assert_eq!(at_hop(&result, 1), vec![METFORMIN.as_u64()]);
}

#[test]
fn a_window_covering_nothing_yields_only_the_start() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 2).window(ts(141), ts(149)))
        .unwrap();

    assert_eq!(reached(&result), vec![P1.as_u64()]);
}

// ---------------------------------------------------------------------------
// Server-side limit enforcement
// ---------------------------------------------------------------------------

#[test]
fn the_server_clamps_a_hop_count_beyond_its_ceiling() {
    let (_dir, store) = fixture();
    let limits = TraversalLimits {
        max_hops: 1,
        ..TraversalLimits::default()
    };
    let traverser = Traverser::new(&store, limits);

    // The client asks for 200 hops; the server grants 1.
    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 200))
        .unwrap();

    assert_eq!(result.effective_max_hops, 1);
    assert!(!reached(&result).contains(&P2.as_u64()));
}

#[test]
fn a_client_cannot_raise_the_result_budget_past_the_server_ceiling() {
    let (_dir, store) = fixture();
    let limits = TraversalLimits {
        max_result_nodes: 2,
        ..TraversalLimits::default()
    };
    let traverser = Traverser::new(&store, limits);

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 2).max_nodes(usize::MAX))
        .unwrap();

    assert!(result.nodes.len() <= 2);
    assert!(result.truncation.hit_node_limit);
}

#[test]
fn hitting_a_limit_is_reported_rather_than_returned_as_a_complete_answer() {
    let (_dir, store) = hub_fixture(50);
    let limits = TraversalLimits {
        max_neighbors_per_node: 10,
        ..TraversalLimits::default()
    };
    let traverser = Traverser::new(&store, limits);

    let result = traverser
        .traverse(&TraversalRequest::new(CKD, ts(1000), 1).direction(Direction::Incoming))
        .unwrap();

    // 50 patients attached, 10 permitted. The caller must be able to tell that
    // more exist — silently returning 10 as if that were all of them is the
    // failure this guards against.
    //
    // The count is a lower bound by design: the scan stops one past the cap, so
    // it learns that more existed without reading all 50. Knowing the exact 40
    // would mean paying the O(degree) cost the cap is there to avoid.
    assert!(result.truncation.is_truncated());
    assert_eq!(result.truncation.fanout_capped_nodes, 1);
    assert!(
        result.truncation.fanout_dropped_neighbors >= 1,
        "at least one dropped neighbour must be reported, got {}",
        result.truncation.fanout_dropped_neighbors
    );
    assert_eq!(result.nodes.len(), 11, "the hub plus 10 spokes");
}

#[test]
fn a_capped_hop_does_not_read_the_whole_adjacency() {
    // The property that decides the p95 target: cost must scale with the cap,
    // not with the degree of the node being expanded. A 2,000-spoke hub read
    // through a cap of 10 must cost about what a 50-spoke hub costs, not 40x it.
    let (_dir, small) = hub_fixture(50);
    let (_dir2, large) = hub_fixture(2_000);
    let limits = TraversalLimits {
        max_neighbors_per_node: 10,
        ..TraversalLimits::default()
    };

    let request = TraversalRequest::new(CKD, ts(100_000), 1).direction(Direction::Incoming);

    let small_result = Traverser::new(&small, limits).traverse(&request).unwrap();
    let large_result = Traverser::new(&large, limits).traverse(&request).unwrap();

    // Same bounded answer from both, despite a 40x difference in degree.
    assert_eq!(small_result.nodes.len(), 11);
    assert_eq!(large_result.nodes.len(), 11);
    assert!(large_result.truncation.is_truncated());

    // If the scan were unbounded the larger hub would return every spoke it
    // read before truncating; bounding it means it never materialised them.
    assert_eq!(
        large_result.edges.len(),
        10,
        "a capped hop must yield exactly the cap, never the full adjacency"
    );
}

#[test]
fn an_untruncated_result_reports_itself_as_complete() {
    let (_dir, store) = hub_fixture(5);
    let limits = TraversalLimits {
        max_neighbors_per_node: 10,
        ..TraversalLimits::default()
    };
    let traverser = Traverser::new(&store, limits);

    let result = traverser
        .traverse(&TraversalRequest::new(CKD, ts(1000), 1).direction(Direction::Incoming))
        .unwrap();

    assert!(!result.truncation.is_truncated());
    assert_eq!(result.nodes.len(), 6);
}

#[test]
fn fanout_truncation_is_deterministic_across_runs() {
    let limits = TraversalLimits {
        max_neighbors_per_node: 7,
        ..TraversalLimits::default()
    };

    let mut runs = Vec::new();
    for _ in 0..3 {
        let (_dir, store) = hub_fixture(40);
        let traverser = Traverser::new(&store, limits);
        let result = traverser
            .traverse(&TraversalRequest::new(CKD, ts(1000), 1).direction(Direction::Incoming))
            .unwrap();
        runs.push(reached(&result));
    }

    // A benchmark whose result set varies run to run is not measuring anything
    // stable, so which neighbours survive the cap must be reproducible.
    assert_eq!(runs[0], runs[1]);
    assert_eq!(runs[1], runs[2]);
}

// ---------------------------------------------------------------------------
// Result shape
// ---------------------------------------------------------------------------

#[test]
fn an_edge_reached_from_both_endpoints_is_reported_once() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(P1, ts(150), 2))
        .unwrap();

    // P1 -DX-> CKD is discovered outgoing from P1 and again incoming to CKD.
    let dx_edges = result
        .edges
        .iter()
        .filter(|e| e.src == P1 && e.dst == CKD && e.edge_type == DX)
        .count();
    assert_eq!(dx_edges, 1);
}

#[test]
fn traversing_from_an_isolated_node_returns_just_that_node() {
    let (_dir, store) = fixture();
    let traverser = Traverser::new(&store, TraversalLimits::default());

    let result = traverser
        .traverse(&TraversalRequest::new(NodeId(999_999), ts(150), 3))
        .unwrap();

    assert_eq!(reached(&result), vec![999_999]);
    assert!(!result.truncation.is_truncated());
}
