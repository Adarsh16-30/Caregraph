//! Hub deferral plus the repair worker, with a real trained model.
//!
//! The claim under test: moving a hub's embedding update off the write path does
//! not change what ends up stored. For the same mutations,
//!
//! * the nodes still updated synchronously get exactly the embeddings a fully
//!   synchronous update computes;
//! * the nodes that were deferred have no embedding at the mutation's timestamp
//!   until the repair worker runs, are listed by `pending_repairs` meanwhile,
//!   and afterwards hold exactly the embeddings the synchronous update computed;
//! * a burst of mutations on one hub is repaired once, not once per mutation.
//!
//! "Exactly" is the same tolerance `associative_correctness_test.rs` uses:
//! float32 noise from summing the same neighbours in a different tensor shape,
//! three orders of magnitude below a structural error. The fixture is small
//! enough that no cap binds, so the two paths see identical receptive fields.

use std::collections::{BTreeSet, HashMap};

use caregraph::embedding::meta::CommitMeta;
use caregraph::embedding::metrics::EmbeddingMetrics;
use caregraph::embedding::repair::pending_repairs;
use caregraph::embedding::repair_worker::{run_pass, RepairModel, RepairParams, RepairQueue};
use caregraph::embedding::state::GraphMutation;
use caregraph::embedding::{AtomicCommitter, EmbeddingModel};
use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::keys::encode_commit_meta_key;
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::{TemporalIndex, TemporalWriter};
use caregraph::types::{EdgeType, ModelKind, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

const HUB: NodeId = NodeId(100_000);
const MED_A: NodeId = NodeId(200_001);
const MED_B: NodeId = NodeId(200_002);
const HUB_PATIENTS: u64 = 40;
const NEW_1: NodeId = NodeId(500);
const NEW_2: NodeId = NodeId(501);
const HUB_DEGREE: usize = 10;
const GRAPHSAGE: &str = "diabetes130_graphsage";
const GAT: &str = "diabetes130_gat";

/// A deployed model under test, with the kind the server would label its output with.
struct Subject {
    model: EmbeddingModel,
    id: &'static str,
    kind: ModelKind,
}

impl Subject {
    fn spawn(id: &'static str, kind: ModelKind) -> Self {
        let model = EmbeddingModel::spawn(id)
            .unwrap_or_else(|e| panic!("ml/deployed/{id} must exist and spawn: {e}"));
        Subject { model, id, kind }
    }
}

const FANOUT: usize = 512;
const EXPANDED: usize = 1_500;
const ABS_TOLERANCE: f32 = 1e-4;
const REL_TOLERANCE: f32 = 1e-3;

struct Fixture {
    _dir: TempDir,
    store: RocksKv,
}

fn node(writer: &TemporalWriter<'_>, batch: &mut WriteBatch, id: NodeId, kind: &str) {
    writer.put_node(batch, id, Timestamp(0), &NodeValue::new(kind, json!({})));
}

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

/// A hub condition shared by `HUB_PATIENTS` patients, two small medications, and
/// two new patients each on a medication but not yet on the hub.
fn build() -> Fixture {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    let writer = TemporalWriter::new(&store).expect("writer");
    let mut batch = WriteBatch::default();

    node(&writer, &mut batch, HUB, "Condition");
    node(&writer, &mut batch, MED_A, "Medication");
    node(&writer, &mut batch, MED_B, "Medication");
    for i in 1..=HUB_PATIENTS {
        node(&writer, &mut batch, NodeId(i), "Patient");
        edge(
            &writer,
            &mut batch,
            NodeId(i),
            EdgeType::DiagnosedWith,
            HUB,
            i,
        );
    }
    node(&writer, &mut batch, NEW_1, "Patient");
    node(&writer, &mut batch, NEW_2, "Patient");
    edge(
        &writer,
        &mut batch,
        NEW_1,
        EdgeType::PrescribedMedication,
        MED_A,
        100,
    );
    edge(
        &writer,
        &mut batch,
        NEW_1,
        EdgeType::PrescribedMedication,
        MED_B,
        101,
    );
    edge(
        &writer,
        &mut batch,
        NEW_2,
        EdgeType::PrescribedMedication,
        MED_A,
        102,
    );
    edge(
        &writer,
        &mut batch,
        NodeId(1),
        EdgeType::PrescribedMedication,
        MED_A,
        103,
    );
    edge(
        &writer,
        &mut batch,
        NodeId(2),
        EdgeType::PrescribedMedication,
        MED_B,
        104,
    );
    drop(writer);
    store.write(batch).expect("commit fixture");
    Fixture { _dir: dir, store }
}

fn join_hub(patient: NodeId, at: u64) -> GraphMutation {
    GraphMutation::AddEdge {
        src: patient,
        dst: HUB,
        edge_type: EdgeType::DiagnosedWith,
        ts: Timestamp(at),
    }
}

fn commit(
    fx: &Fixture,
    subject: &Subject,
    mutation: GraphMutation,
    hub_degree: Option<usize>,
) -> caregraph::embedding::state::MutationContext {
    AtomicCommitter::new(&fx.store)
        .expect("committer")
        .with_hub_degree(hub_degree)
        .commit(
            mutation,
            &EdgeValue::new(json!({})),
            subject.kind,
            &subject.model,
            FANOUT,
            EXPANDED,
            None,
            false,
        )
        .expect("commit")
}

fn embedding_at(fx: &Fixture, node: NodeId, ts: u64) -> Option<Vec<f32>> {
    TemporalIndex::new(&fx.store)
        .embedding_as_of(node, Timestamp(ts))
        .expect("read embedding")
        .map(|e| e.vector)
}

/// Number of stored embedding versions for `node`.
fn versions(fx: &Fixture, node: NodeId) -> usize {
    let prefix = caregraph::temporal::keys::node_prefix(node);
    let mut n = 0;
    fx.store
        .scan_from(cf::CF_EMBEDDINGS, &prefix, &prefix, &mut |_, _| {
            n += 1;
            std::ops::ControlFlow::Continue(())
        })
        .expect("scan");
    n
}

fn assert_close(what: &str, got: &[f32], want: &[f32]) {
    assert_eq!(got.len(), want.len(), "{what}: dimension");
    for (i, (&a, &b)) in got.iter().zip(want).enumerate() {
        let diff = (a - b).abs();
        assert!(
            diff <= ABS_TOLERANCE || diff <= b.abs() * REL_TOLERANCE,
            "{what} dim {i}: {a} vs {b} (diff {diff})"
        );
    }
}

fn models(subject: &Subject) -> Vec<RepairModel<'_>> {
    vec![RepairModel {
        model: &subject.model,
        kind: subject.kind,
    }]
}

/// Run repair passes until the queue is empty. Returns the total repaired.
fn repair_all(fx: &Fixture, subject: &Subject, queue: &RepairQueue) -> usize {
    let metrics = EmbeddingMetrics::new(&prometheus::Registry::new()).expect("metrics");
    let params = RepairParams {
        fanout_cap: FANOUT,
        max_expanded_nodes: EXPANDED,
        ..RepairParams::default()
    };
    let mut repaired = 0;
    for _ in 0..50 {
        if queue.is_empty() {
            return repaired;
        }
        let stats = run_pass(&fx.store, queue, &metrics, &models(subject), &params);
        assert_eq!(stats.failed, 0, "a repair pass failed: {stats:?}");
        repaired += stats.repaired;
    }
    panic!("the repair queue did not drain");
}

fn node_set(v: &[NodeId]) -> BTreeSet<NodeId> {
    v.iter().copied().collect()
}

#[test]
fn graphsage_a_deferred_hub_update_equals_the_synchronous_one_once_repaired() {
    deferred_equals_synchronous_once_repaired(GRAPHSAGE, ModelKind::GraphSAGE);
}

#[test]
fn gat_a_deferred_hub_update_equals_the_synchronous_one_once_repaired() {
    deferred_equals_synchronous_once_repaired(GAT, ModelKind::GAT);
}

#[test]
fn graphsage_a_burst_of_mutations_on_one_hub_is_repaired_once() {
    burst_is_repaired_once(GRAPHSAGE, ModelKind::GraphSAGE);
}

#[test]
fn gat_a_burst_of_mutations_on_one_hub_is_repaired_once() {
    burst_is_repaired_once(GAT, ModelKind::GAT);
}

fn deferred_equals_synchronous_once_repaired(id: &'static str, kind: ModelKind) {
    let subject = Subject::spawn(id, kind);
    let reference = build();
    let deferred = build();
    let at = 1_001;

    let ref_ctx = commit(&reference, &subject, join_hub(NEW_1, at), None);
    let def_ctx = commit(&deferred, &subject, join_hub(NEW_1, at), Some(HUB_DEGREE));
    assert!(!ref_ctx.fallback && !def_ctx.fallback);

    // Conservation: what the synchronous update refreshed is now either
    // refreshed or deferred, and the two sets do not overlap.
    let sync_now = node_set(&def_ctx.affected);
    let deferred_now = node_set(&def_ctx.deferred_nodes);
    assert!(sync_now.is_disjoint(&deferred_now));
    assert_eq!(
        node_set(&ref_ctx.affected),
        sync_now
            .union(&deferred_now)
            .copied()
            .collect::<BTreeSet<_>>()
    );
    assert!(
        deferred_now.contains(&HUB),
        "the hub itself must be deferred"
    );
    assert!(
        sync_now.contains(&NEW_1),
        "the patient who joined stays synchronous"
    );

    // The synchronous part is already exact.
    for n in &sync_now {
        assert_close(
            &format!("synchronous node {n:?}"),
            &embedding_at(&deferred, *n, at).expect("synchronous embedding"),
            &embedding_at(&reference, *n, at).expect("reference embedding"),
        );
    }

    // Deferred nodes have no embedding yet, and the markers say so.
    for n in &deferred_now {
        assert!(
            embedding_at(&deferred, *n, at).is_none(),
            "{n:?} was written early"
        );
    }
    let pending = pending_repairs(&deferred.store).expect("pending");
    assert_eq!(
        pending.iter().map(|p| p.node).collect::<BTreeSet<_>>(),
        deferred_now
    );
    assert!(pending.iter().all(|p| p.model_id == subject.id));
    assert!(pending_repairs(&reference.store)
        .expect("pending")
        .is_empty());

    // Provenance records the policy in force.
    let meta = |fx: &Fixture, n: NodeId| {
        let bytes = fx
            .store
            .get(
                cf::CF_COMMIT_META,
                &encode_commit_meta_key(n, Timestamp(at)),
            )
            .expect("read meta")
            .expect("meta present");
        CommitMeta::deserialize(&bytes).expect("decode meta")
    };
    let m = meta(&deferred, NEW_1);
    assert_eq!(m.hub_defer_degree, Some(HUB_DEGREE));
    assert_eq!(m.deferred_count, deferred_now.len());
    assert!(!m.repair);
    assert_eq!(meta(&reference, NEW_1).hub_defer_degree, None);

    // The repair worker refreshes exactly the deferred nodes.
    let queue = RepairQueue::new();
    assert_eq!(
        queue.rebuild(&deferred.store).expect("rebuild"),
        deferred_now.len()
    );
    assert_eq!(repair_all(&deferred, &subject, &queue), deferred_now.len());
    assert!(pending_repairs(&deferred.store)
        .expect("pending")
        .is_empty());

    for n in &deferred_now {
        assert_close(
            &format!("repaired node {n:?}"),
            &embedding_at(&deferred, *n, at).expect("repaired embedding"),
            &embedding_at(&reference, *n, at).expect("reference embedding"),
        );
        assert!(
            meta(&deferred, *n).repair,
            "{n:?} should be marked as repaired"
        );
    }
}

fn burst_is_repaired_once(id: &'static str, kind: ModelKind) {
    let subject = Subject::spawn(id, kind);
    let reference = build();
    let deferred = build();
    let (t1, t2) = (1_001, 1_002);

    for fx_degree in [(&reference, None), (&deferred, Some(HUB_DEGREE))] {
        commit(fx_degree.0, &subject, join_hub(NEW_1, t1), fx_degree.1);
        commit(fx_degree.0, &subject, join_hub(NEW_2, t2), fx_degree.1);
    }

    // Both mutations marked the hub's neighbours; the queue keeps the newest.
    let queue = RepairQueue::new();
    let queued = queue.rebuild(&deferred.store).expect("rebuild");
    assert!(queued > HUB_PATIENTS as usize);
    let repaired = repair_all(&deferred, &subject, &queue);
    assert_eq!(
        repaired, queued,
        "one repair per node, not one per mutation"
    );
    assert!(pending_repairs(&deferred.store)
        .expect("pending")
        .is_empty());

    // After the burst, every node the second mutation affected matches the
    // fully synchronous result at its timestamp.
    let mut compared = 0;
    let mut by_node: HashMap<NodeId, Vec<f32>> = HashMap::new();
    for n in std::iter::once(HUB)
        .chain((1..=HUB_PATIENTS).map(NodeId))
        .chain([NEW_1, NEW_2, MED_A, MED_B])
    {
        if let Some(want) = embedding_at(&reference, n, t2) {
            let got = embedding_at(&deferred, n, t2)
                .unwrap_or_else(|| panic!("{n:?} has no embedding after repair"));
            assert_close(&format!("node {n:?} after the burst"), &got, &want);
            by_node.insert(n, got);
            compared += 1;
        }
    }
    assert!(compared > HUB_PATIENTS as usize);

    // The coalescing, observed: an ordinary hub patient was refreshed by both
    // synchronous mutations, but only once by the repair.
    let patient = NodeId(5);
    assert_eq!(versions(&reference, patient), 2);
    assert_eq!(versions(&deferred, patient), 1);
}
