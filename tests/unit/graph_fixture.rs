//! A small graph with the shape that matters, shared by the Phase 3 tests.
//!
//! Two patients joined by a shared `Condition` reference node — the structure
//! that makes a 2-hop traversal meaningful, and the structure that makes
//! fan-out bounding necessary on the real graph. Node ids follow the namespaces
//! in `data/clinical_graph_schema.md`.
//!
//! Synthetic: these are graph *shapes* chosen to exercise specific code paths,
//! not stand-ins for clinical records. Nothing here feeds an evaluation path.

#![allow(dead_code)] // each test module uses a different subset

use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::TemporalWriter;
use caregraph::types::{EdgeType, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

pub const P1: NodeId = NodeId(1);
pub const P2: NodeId = NodeId(2);
pub const P3: NodeId = NodeId(3);
pub const CKD: NodeId = NodeId(100_001);
pub const RETINOPATHY: NodeId = NodeId(100_002);
pub const METFORMIN: NodeId = NodeId(200_001);

pub const DX: EdgeType = EdgeType::DiagnosedWith;
pub const RX: EdgeType = EdgeType::PrescribedMedication;

pub fn ts(t: u64) -> Timestamp {
    Timestamp(t)
}

/// Open a real RocksDB in a temp directory.
pub fn open() -> (TempDir, RocksKv) {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    (dir, store)
}

/// The shared fixture.
///
/// ```text
///   t=100  P1, P2, P3, CKD, RETINOPATHY, METFORMIN all created
///   t=110  P1 -DX-> CKD
///   t=120  P2 -DX-> CKD
///   t=130  P1 -RX-> METFORMIN
///   t=140  P3 -DX-> RETINOPATHY      (disconnected from P1/P2)
///   t=200  P1 -DX-> CKD  retracted
/// ```
///
/// So at t=150, P1 reaches P2 in two hops via CKD; at t=250 it no longer does.
pub fn fixture() -> (TempDir, RocksKv) {
    let (dir, store) = open();
    let writer = TemporalWriter::new(&store).expect("writer");
    let mut batch = WriteBatch::default();

    for (node, kind, props) in [
        (P1, "Patient", json!({"sex": "F", "treatment_arm": "conventional"})),
        (P2, "Patient", json!({"sex": "M", "treatment_arm": "intensive"})),
        (P3, "Patient", json!({"sex": "F", "treatment_arm": "intensive"})),
        (CKD, "Condition", json!({"icd10_code": "N18.3", "label": "CKD stage 3"})),
        (RETINOPATHY, "Condition", json!({"icd10_code": "E11.3", "label": "Diabetic retinopathy"})),
        (METFORMIN, "Medication", json!({"atc_code": "A10BA02", "label": "Metformin"})),
    ] {
        writer.put_node(&mut batch, node, ts(100), &NodeValue::new(kind, props));
    }

    writer.put_edge(&mut batch, P1, DX, CKD, ts(110), &EdgeValue::new(json!({"status": "active"})));
    writer.put_edge(&mut batch, P2, DX, CKD, ts(120), &EdgeValue::new(json!({"status": "active"})));
    writer.put_edge(&mut batch, P1, RX, METFORMIN, ts(130), &EdgeValue::new(json!({"dose": "500mg"})));
    writer.put_edge(&mut batch, P3, DX, RETINOPATHY, ts(140), &EdgeValue::new(json!({"status": "active"})));
    writer.remove_edge(&mut batch, P1, DX, CKD, ts(200));

    store.write(batch).expect("commit fixture");
    (dir, store)
}

/// A star: one hub node with `spokes` patients attached, for fan-out testing.
pub fn hub_fixture(spokes: u64) -> (TempDir, RocksKv) {
    let (dir, store) = open();
    let writer = TemporalWriter::new(&store).expect("writer");
    let mut batch = WriteBatch::default();

    writer.put_node(&mut batch, CKD, ts(100), &NodeValue::new("Condition", json!({})));
    for i in 1..=spokes {
        let patient = NodeId(i);
        writer.put_node(&mut batch, patient, ts(100), &NodeValue::new("Patient", json!({})));
        writer.put_edge(&mut batch, patient, DX, CKD, ts(110 + i), &EdgeValue::new(json!({})));
    }

    store.write(batch).expect("commit hub fixture");
    (dir, store)
}
