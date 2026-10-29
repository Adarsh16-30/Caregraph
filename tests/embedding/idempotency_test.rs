//! A retried request must not apply a mutation twice (production-hardening P3).
//!
//! The client of a mutation that timed out cannot know whether it landed. With a
//! `request_id` the first execution's outcome is stored in the same batch as the
//! commit, and a retry returns it instead of re-applying the mutation. Runs the
//! real pipeline against the real model and a real RocksDB.

use std::collections::BTreeMap;
use std::sync::Mutex;

use caregraph::config::TsPolicy;
use caregraph::embedding::caps::CapController;
use caregraph::embedding::metrics::EmbeddingMetrics;
use caregraph::embedding::pipeline::{admit_mutation, run_mutation_pipeline};
use caregraph::embedding::sequencer::Sequencer;
use caregraph::embedding::state::GraphMutation;
use caregraph::embedding::worker::pool::PoolConfig;
use caregraph::embedding::EmbeddingModel;
use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::TemporalWriter;
use caregraph::types::{EdgeType, ModelKind, NodeId, Timestamp};
use caregraph::CareGraphError;
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

fn fixture() -> (TempDir, RocksKv) {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path().join("db")).unwrap();
    let writer = TemporalWriter::new(&store).unwrap();
    let mut batch = WriteBatch::default();
    writer.put_node(
        &mut batch,
        NodeId(1),
        Timestamp(0),
        &NodeValue::new("Patient", json!({})),
    );
    writer.put_node(
        &mut batch,
        NodeId(2),
        Timestamp(0),
        &NodeValue::new("Condition", json!({})),
    );
    drop(writer);
    store.write(batch).unwrap();
    (dir, store)
}

fn dump(store: &RocksKv, name: &'static str) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    store
        .scan_from_narrow_prefix(name, &[], &[], &mut |k, v| {
            out.insert(k.to_vec(), v.to_vec());
            std::ops::ControlFlow::Continue(())
        })
        .unwrap();
    out
}

#[test]
fn a_retry_with_the_same_request_id_replays_and_writes_nothing() {
    let model = EmbeddingModel::spawn_with(
        "diabetes130_graphsage",
        PoolConfig {
            workers: 1,
            torch_threads: 1,
            ..PoolConfig::default()
        },
    )
    .unwrap();
    let (_dir, store) = fixture();
    let registry = prometheus::Registry::new();
    let metrics = EmbeddingMetrics::new(&registry).unwrap();
    let caps = Mutex::new(CapController::pinned(512, 1_500));
    let sequencer = Sequencer::open(&store, TsPolicy::Strict).unwrap();

    let mutation = GraphMutation::AddEdge {
        src: NodeId(1),
        dst: NodeId(2),
        edge_type: EdgeType::DiagnosedWith,
        ts: Timestamp(0), // server-assigned
    };
    let run = |id: Option<&str>| {
        run_mutation_pipeline(
            mutation,
            &EdgeValue::new(json!({})),
            ModelKind::GraphSAGE,
            &store,
            &model,
            &metrics,
            &caps,
            &sequencer,
            id,
            false,
            None,
        )
        .unwrap()
    };

    let first = run(Some("req-1"));
    assert!(!first.replayed);
    assert!(
        first.timestamp.as_u64() > 0,
        "the server assigned a timestamp"
    );
    let snapshot: Vec<_> = [cf::CF_EDGES, cf::CF_EMBEDDINGS, cf::CF_COMMIT_META]
        .iter()
        .map(|c| dump(&store, c))
        .collect();

    // The retry returns the original outcome and changes nothing.
    let retry = run(Some("req-1"));
    assert!(retry.replayed);
    assert_eq!(
        retry.timestamp, first.timestamp,
        "same timestamp as the original"
    );
    assert_eq!(retry.commit_seq, first.commit_seq);
    assert_eq!(retry.ctx.affected, first.ctx.affected);
    let after: Vec<_> = [cf::CF_EDGES, cf::CF_EMBEDDINGS, cf::CF_COMMIT_META]
        .iter()
        .map(|c| dump(&store, c))
        .collect();
    assert!(
        snapshot == after,
        "a replayed request must not write anything"
    );
    assert_eq!(
        metrics.mutations_total.get(),
        1,
        "only the first execution ran"
    );

    // A different request id is a different request: it executes.
    let other = run(Some("req-2"));
    assert!(!other.replayed);
    assert!(other.timestamp > first.timestamp);

    // Without a request id nothing is deduplicated.
    assert!(!run(None).replayed);
}

#[test]
fn a_concurrent_duplicate_request_id_is_refused_not_applied_twice() {
    let (_dir, store) = fixture();
    let sequencer = Sequencer::open(&store, TsPolicy::Strict).unwrap();
    let mutation = GraphMutation::AddEdge {
        src: NodeId(1),
        dst: NodeId(2),
        edge_type: EdgeType::DiagnosedWith,
        ts: Timestamp(0),
    };
    let first = admit_mutation(mutation, &store, &sequencer, Some("same")).unwrap();
    let second = admit_mutation(mutation, &store, &sequencer, Some("same"));
    assert!(
        matches!(second, Err(CareGraphError::Aborted(_))),
        "a duplicate that arrives while the first is still running must be refused"
    );
    drop(first);
    assert!(
        admit_mutation(mutation, &store, &sequencer, Some("same")).is_ok(),
        "once the first finished or was dropped, the id is free again"
    );
}
