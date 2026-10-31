//! Hub deferral in the affected-subgraph resolver (`embedding/deferral.rs`).
//!
//! Synthetic graph *shapes* on a real on-disk RocksDB (see `graph_fixture`): one
//! hub condition shared by many patients, plus a new patient with a small
//! neighbourhood. No model runs here; `tests/embedding/hub_deferral_repair_test.rs`
//! proves the deferred work, once repaired, equals what a synchronous update
//! computes. What these tests pin down is the resolver's bookkeeping, above all
//! the conservation property: nothing a synchronous update used to refresh is
//! lost, it is either refreshed now or deferred.

use std::collections::HashSet;

use caregraph::embedding::resolver::AffectedSubgraphResolver;
use caregraph::embedding::state::GraphMutation;
use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::record::EdgeValue;
use caregraph::temporal::TemporalWriter;
use caregraph::types::{EdgeType, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;

use super::graph_fixture::open;

const HUB: NodeId = NodeId(100_000);
const MED_A: NodeId = NodeId(200_001);
const MED_B: NodeId = NodeId(200_002);
const NEW_PATIENT: NodeId = NodeId(500);
const HUB_PATIENTS: u64 = 30;

const FANOUT: usize = 512;
const EXPANDED: usize = 1_500;

fn edge(
    writer: &TemporalWriter<'_>,
    batch: &mut WriteBatch,
    s: NodeId,
    t: EdgeType,
    d: NodeId,
    at: u64,
) {
    writer.put_edge(batch, s, t, d, Timestamp(at), &EdgeValue::new(json!({})));
}

/// `HUB_PATIENTS` patients diagnosed with `HUB`; a new patient on two small
/// medications (each shared with one hub patient), not yet linked to the hub.
fn graph() -> (tempfile::TempDir, RocksKv) {
    let (dir, store) = open();
    let writer = TemporalWriter::new(&store).expect("writer");
    let mut batch = WriteBatch::default();
    for i in 1..=HUB_PATIENTS {
        edge(
            &writer,
            &mut batch,
            NodeId(i),
            EdgeType::DiagnosedWith,
            HUB,
            i,
        );
    }
    edge(
        &writer,
        &mut batch,
        NEW_PATIENT,
        EdgeType::PrescribedMedication,
        MED_A,
        100,
    );
    edge(
        &writer,
        &mut batch,
        NEW_PATIENT,
        EdgeType::PrescribedMedication,
        MED_B,
        101,
    );
    edge(
        &writer,
        &mut batch,
        NodeId(1),
        EdgeType::PrescribedMedication,
        MED_A,
        102,
    );
    edge(
        &writer,
        &mut batch,
        NodeId(2),
        EdgeType::PrescribedMedication,
        MED_B,
        103,
    );
    drop(writer);
    store.write(batch).expect("commit graph");
    (dir, store)
}

fn join_hub() -> GraphMutation {
    GraphMutation::AddEdge {
        src: NEW_PATIENT,
        dst: HUB,
        edge_type: EdgeType::DiagnosedWith,
        ts: Timestamp(1_000),
    }
}

fn set(v: &[NodeId]) -> HashSet<NodeId> {
    v.iter().copied().collect()
}

#[test]
fn with_deferral_off_the_whole_hub_neighbourhood_is_affected() {
    let (_dir, store) = graph();
    let sub = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(None)
        .resolve(join_hub())
        .expect("resolve");
    let affected = set(&sub.affected);
    assert!(affected.contains(&HUB));
    assert!((1..=HUB_PATIENTS).all(|i| affected.contains(&NodeId(i))));
    assert!(sub.deferred.is_empty());
}

#[test]
fn a_hub_endpoint_and_its_neighbours_are_deferred_not_recomputed() {
    let (_dir, store) = graph();
    let sub = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(Some(10))
        .resolve(join_hub())
        .expect("resolve");

    let affected = set(&sub.affected);
    let deferred = set(&sub.deferred);

    // The new patient and its small neighbourhood stay synchronous.
    assert!(affected.contains(&NEW_PATIENT));
    assert!(affected.contains(&MED_A) && affected.contains(&MED_B));
    // The hub and everything hanging off it move to the repair worker.
    assert!(deferred.contains(&HUB));
    assert!(
        (3..=HUB_PATIENTS).all(|i| deferred.contains(&NodeId(i))),
        "hub neighbours must be deferred"
    );
    assert!(affected.is_disjoint(&deferred));
}

#[test]
fn nothing_a_synchronous_update_refreshed_is_lost() {
    let (_dir, store) = graph();
    let before = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(None)
        .resolve(join_hub())
        .expect("resolve");
    let after = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(Some(10))
        .resolve(join_hub())
        .expect("resolve");

    let now: HashSet<NodeId> = set(&after.affected)
        .union(&set(&after.deferred))
        .copied()
        .collect();
    let missing: Vec<NodeId> = before
        .affected
        .iter()
        .copied()
        .filter(|n| !now.contains(n))
        .collect();
    assert!(
        missing.is_empty(),
        "nodes refreshed before but neither refreshed nor deferred now: {missing:?}"
    );
}

#[test]
fn the_hub_is_still_read_so_its_neighbours_are_inputs_to_the_synchronous_nodes() {
    let (_dir, store) = graph();
    let sub = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(Some(10))
        .resolve(join_hub())
        .expect("resolve");
    // The hub's adjacency was read (it feeds the new patient's layer-2 value)...
    assert!(sub.fetched.contains(&HUB));
    // ...and its edges are in the subgraph, as raw-feature neighbours.
    let hub_edges = sub
        .edges
        .iter()
        .filter(|(a, b)| *a == HUB || *b == HUB)
        .count();
    assert_eq!(hub_edges as u64, HUB_PATIENTS);
}

#[test]
fn deferral_reads_far_fewer_adjacency_lists() {
    let (_dir, store) = graph();
    let off = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(None)
        .resolve(join_hub())
        .expect("resolve");
    let on = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(Some(10))
        .resolve(join_hub())
        .expect("resolve");
    assert!(
        on.fetched.len() * 2 < off.fetched.len(),
        "deferral should skip expanding the hub's neighbourhood: {} fetched with it, {} without",
        on.fetched.len(),
        off.fetched.len()
    );
}

#[test]
fn a_hub_that_is_only_a_neighbour_is_deferred_on_its_own() {
    let (_dir, store) = graph();
    let small = GraphMutation::AddEdge {
        src: NEW_PATIENT,
        dst: NodeId(7),
        edge_type: EdgeType::TreatedByProvider,
        ts: Timestamp(1_000),
    };
    // Neither endpoint is a hub (patient 7 has one neighbour, the new patient two),
    // but patient 7's one neighbour *is*.
    let off = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(None)
        .resolve(small)
        .expect("resolve");
    let on = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(Some(10))
        .resolve(small)
        .expect("resolve");
    // The hub is refreshed in the first run and deferred, alone, in the second.
    // Everything else is identical.
    assert_eq!(
        set(&off.affected),
        set(&on.affected)
            .union(&set(&on.deferred))
            .copied()
            .collect()
    );
    assert!(set(&on.deferred) == set(&[HUB]));
}

#[test]
fn resolve_targets_reports_exactly_the_requested_nodes_and_never_defers() {
    let (_dir, store) = graph();
    let targets = [NodeId(3), NodeId(4), HUB];
    let sub = AffectedSubgraphResolver::new(&store, FANOUT, EXPANDED)
        .with_hub_degree(Some(10))
        .resolve_targets(&targets, Timestamp(500))
        .expect("resolve");
    assert_eq!(set(&sub.affected), set(&targets));
    assert!(sub.deferred.is_empty());
    assert!(sub.fetched.contains(&HUB));
}
