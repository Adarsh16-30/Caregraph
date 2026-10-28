//! Repair markers (production hardening P2) are staged into the same batch as
//! the structural change and persisted through the real RocksDB write path.

use caregraph::embedding::repair::{stage_repairs, RepairRecord};
use caregraph::embedding::state::{GraphMutation, MutationContext};
use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::keys::encode_repair_key;
use caregraph::{EdgeType, ModelKind, NodeId, Timestamp};
use rocksdb::WriteBatch;
use tempfile::TempDir;

fn mutation(ts: u64) -> GraphMutation {
    GraphMutation::AddEdge {
        src: NodeId(1),
        dst: NodeId(2),
        edge_type: EdgeType::DiagnosedWith,
        ts: Timestamp(ts),
    }
}

#[test]
fn stale_nodes_get_a_durable_marker_in_the_same_batch() {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path()).unwrap();

    let mut ctx = MutationContext::new(mutation(1_000), ModelKind::GraphSAGE);
    ctx.fallback = true;
    ctx.stale_nodes = vec![NodeId(1), NodeId(2), NodeId(7)];

    let mut batch = WriteBatch::default();
    stage_repairs(&mut batch, &store, &ctx, "diabetes130_graphsage").unwrap();
    // Nothing is visible until the batch is written: staging is not writing.
    assert!(store
        .get(
            cf::CF_REPAIR,
            &encode_repair_key(NodeId(7), Timestamp(1_000))
        )
        .unwrap()
        .is_none());
    store.write(batch).unwrap();

    for node in [1u64, 2, 7] {
        let raw = store
            .get(
                cf::CF_REPAIR,
                &encode_repair_key(NodeId(node), Timestamp(1_000)),
            )
            .unwrap()
            .unwrap_or_else(|| panic!("node {node} has no repair marker"));
        let rec: RepairRecord = serde_json::from_slice(&raw).unwrap();
        assert_eq!(rec.model_id, "diabetes130_graphsage");
        assert!(rec.reason.contains("not refreshed"));
    }
    // A node that was not stale gets no marker.
    assert!(store
        .get(
            cf::CF_REPAIR,
            &encode_repair_key(NodeId(3), Timestamp(1_000))
        )
        .unwrap()
        .is_none());
}

#[test]
fn a_mutation_with_no_stale_nodes_stages_nothing() {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path()).unwrap();
    let ctx = MutationContext::new(mutation(5), ModelKind::GAT);
    let mut batch = WriteBatch::default();
    stage_repairs(&mut batch, &store, &ctx, "m").unwrap();
    assert!(
        batch.is_empty(),
        "a successful embedding leaves no repair marker"
    );
}
