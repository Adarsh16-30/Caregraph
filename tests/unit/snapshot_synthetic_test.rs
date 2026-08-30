//! Care-graph snapshot reconstruction (PRD Phase 3).
//!
//! Graph shapes are synthetic; the storage engine is a real on-disk RocksDB.

use caregraph::graph::{SnapshotReader, TraversalLimits};
use caregraph::storage::KvStore;
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::TemporalWriter;
use caregraph::types::NodeId;
use rocksdb::WriteBatch;
use serde_json::json;

use crate::graph_fixture::{fixture, open, ts, CKD, DX, METFORMIN, P1, RX};

#[test]
fn a_snapshot_holds_every_live_relationship_at_that_instant() {
    let (_dir, store) = fixture();
    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    let snapshot = reader
        .snapshot(P1, ts(150))
        .unwrap()
        .expect("P1 exists at t=150");

    assert_eq!(snapshot.subject.node_id, P1);
    assert_eq!(snapshot.subject.node_type, "Patient");
    assert_eq!(snapshot.related.len(), 2);

    let conditions: Vec<u64> = snapshot.targets_of(DX).map(|n| n.as_u64()).collect();
    let medications: Vec<u64> = snapshot.targets_of(RX).map(|n| n.as_u64()).collect();
    assert_eq!(conditions, vec![CKD.as_u64()]);
    assert_eq!(medications, vec![METFORMIN.as_u64()]);
}

#[test]
fn a_snapshot_reflects_a_retraction_that_happened_before_it() {
    let (_dir, store) = fixture();
    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    // P1 -DX-> CKD is retracted at t=200.
    let snapshot = reader.snapshot(P1, ts(250)).unwrap().unwrap();

    assert_eq!(snapshot.targets_of(DX).count(), 0);
    assert_eq!(snapshot.targets_of(RX).count(), 1);
}

#[test]
fn a_snapshot_before_a_retraction_still_shows_the_relationship() {
    let (_dir, store) = fixture();
    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    // The whole point of the system: asking what was true the day before.
    let before = reader.snapshot(P1, ts(199)).unwrap().unwrap();
    let after = reader.snapshot(P1, ts(200)).unwrap().unwrap();

    assert_eq!(before.targets_of(DX).count(), 1);
    assert_eq!(after.targets_of(DX).count(), 0);
}

#[test]
fn a_subject_that_did_not_exist_yet_has_no_snapshot() {
    let (_dir, store) = fixture();
    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    // P1's record is written at t=100.
    assert!(reader.snapshot(P1, ts(99)).unwrap().is_none());
}

#[test]
fn an_unknown_subject_has_no_snapshot() {
    let (_dir, store) = fixture();
    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    assert!(reader.snapshot(NodeId(999_999), ts(150)).unwrap().is_none());
}

#[test]
fn related_nodes_carry_the_state_they_had_at_the_snapshot_instant() {
    let (_dir, store) = open();
    let writer = TemporalWriter::new(&store).unwrap();
    let mut batch = WriteBatch::default();

    writer.put_node(
        &mut batch,
        P1,
        ts(100),
        &NodeValue::new("Patient", json!({})),
    );
    // The medication's label is corrected at t=300.
    writer.put_node(
        &mut batch,
        METFORMIN,
        ts(100),
        &NodeValue::new("Medication", json!({"label": "Metformin"})),
    );
    writer.put_node(
        &mut batch,
        METFORMIN,
        ts(300),
        &NodeValue::new("Medication", json!({"label": "Metformin hydrochloride"})),
    );
    writer.put_edge(
        &mut batch,
        P1,
        RX,
        METFORMIN,
        ts(110),
        &EdgeValue::new(json!({})),
    );
    store.write(batch).unwrap();

    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    // A snapshot is internally consistent: the label is the one that was
    // recorded then, not the one recorded now.
    let old = reader.snapshot(P1, ts(200)).unwrap().unwrap();
    let new = reader.snapshot(P1, ts(400)).unwrap().unwrap();

    assert_eq!(
        old.related[0].node.as_ref().unwrap().properties["label"],
        "Metformin"
    );
    assert_eq!(
        new.related[0].node.as_ref().unwrap().properties["label"],
        "Metformin hydrochloride"
    );
}

#[test]
fn an_edge_pointing_at_an_unwritten_node_is_surfaced_not_hidden() {
    let (_dir, store) = open();
    let writer = TemporalWriter::new(&store).unwrap();
    let mut batch = WriteBatch::default();

    writer.put_node(
        &mut batch,
        P1,
        ts(100),
        &NodeValue::new("Patient", json!({})),
    );
    // Edge written, target node never written — a loader bug.
    writer.put_edge(
        &mut batch,
        P1,
        DX,
        NodeId(100_777),
        ts(110),
        &EdgeValue::new(json!({})),
    );
    store.write(batch).unwrap();

    let reader = SnapshotReader::new(&store, TraversalLimits::default());
    let snapshot = reader.snapshot(P1, ts(150)).unwrap().unwrap();

    // Dropping the edge would hide the inconsistency; the caller should see it.
    assert_eq!(snapshot.related.len(), 1);
    let unresolved: Vec<u64> = snapshot.unresolved_targets().map(|n| n.as_u64()).collect();
    assert_eq!(unresolved, vec![100_777]);
}

#[test]
fn a_snapshot_can_be_restricted_to_one_relationship_type() {
    let (_dir, store) = fixture();
    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    let snapshot = reader
        .snapshot_of_types(P1, ts(150), &[RX])
        .unwrap()
        .unwrap();

    assert_eq!(snapshot.related.len(), 1);
    assert_eq!(snapshot.targets_of(RX).count(), 1);
}

#[test]
fn grouping_by_edge_type_partitions_every_relationship() {
    let (_dir, store) = fixture();
    let reader = SnapshotReader::new(&store, TraversalLimits::default());

    let snapshot = reader.snapshot(P1, ts(150)).unwrap().unwrap();
    let grouped = snapshot.by_edge_type();

    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[&DX].len(), 1);
    assert_eq!(grouped[&RX].len(), 1);
    let total: usize = grouped.values().map(|v| v.len()).sum();
    assert_eq!(total, snapshot.related.len());
}
