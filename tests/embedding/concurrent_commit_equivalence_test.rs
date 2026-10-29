//! Concurrent execution must equal sequential execution (production-hardening P3).
//!
//! With several embedding workers, mutations are *computed* in parallel. The
//! ordered-commit sequencer (`embedding/sequencer.rs`) is what keeps that safe,
//! and this is the test that proves it, against the real model, real RocksDB,
//! and the real pipeline:
//!
//! For each of 50 seeds a plan of 200 random add/remove mutations with strictly
//! increasing timestamps is applied to two identical databases —
//!
//! * **sequential:** one worker, one thread, in timestamp order;
//! * **concurrent:** four workers, sixteen threads in flight at once;
//!
//! and the results must match:
//!
//! 1. every structural column family (edges, reverse index, nodes) is
//!    byte-identical — the concurrent run did not lose, duplicate or reorder a
//!    write;
//! 2. the stored embeddings have the same keys, labels and (within float noise)
//!    values, and `CF_COMMIT_META`/`CF_META` are byte-identical;
//! 3. every node's latest embedding equals a from-scratch full recompute on the
//!    final graph — the property that a lost update would break;
//! 4. the run was not vacuous: across the seeds, the sequencer detected and
//!    repaired conflicts (`mutation_conflict_retries_total > 0`), and the
//!    sequential run had none.
//!
//! Half the seeds are hub-heavy (most mutations touch the shared hub node), which
//! maximises read/write overlap between in-flight mutations.
//!
//! Threads dequeue in timestamp order. A shuffled order would deadlock a bounded
//! thread pool (all threads holding high timestamps, waiting for a low one
//! nobody picked up); in the server each request's own thread admits itself, so
//! that cannot happen there either.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Mutex;

use caregraph::config::TsPolicy;
use caregraph::embedding::caps::CapController;
use caregraph::embedding::metrics::EmbeddingMetrics;
use caregraph::embedding::pipeline::{admit_mutation, execute_admitted};
use caregraph::embedding::sequencer::Sequencer;
use caregraph::embedding::state::GraphMutation;
use caregraph::embedding::worker::pool::PoolConfig;
use caregraph::embedding::{associative, EmbeddingModel};
use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::{TemporalIndex, TemporalWriter};
use caregraph::types::{EdgeType, Embedding, ModelKind, NodeId, Timestamp};
use caregraph::CareGraphError;
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

const SEEDS: u64 = 50;
const MUTATIONS: usize = 200;
const IN_FLIGHT: usize = 16;
const WORKERS: usize = 4;
const BASE_TS: u64 = 1_000;

const PATIENTS: u64 = 40;
const HUB: NodeId = NodeId(100_000);
const MEDS: [NodeId; 3] = [NodeId(200_000), NodeId(200_001), NodeId(200_002)];

/// Between concurrent and sequential runs of the *same* computation on the same
/// inputs: both pools pin torch to one thread, so these are expected to be
/// identical; the bound only absorbs float non-associativity if they are not.
const EQUIV_ABS_TOL: f32 = 1e-5;
/// Latest embedding vs from-scratch recompute: the existing exactness suite's
/// tolerance (see associative_correctness_test.rs for its derivation).
const RECOMPUTE_ABS_TOL: f32 = 1e-4;
const RECOMPUTE_REL_TOL: f32 = 1e-3;

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

struct Base {
    _dir: TempDir,
    store: RocksKv,
    node_ids: Vec<NodeId>,
}

/// The hub-shaped fixture from the exactness suite: one condition many patients
/// share, plus a few medications.
fn build_base() -> Base {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    let writer = TemporalWriter::new(&store).expect("writer");
    let mut batch = WriteBatch::default();
    let mut node_ids = vec![HUB];
    node_ids.extend(MEDS);
    writer.put_node(
        &mut batch,
        HUB,
        Timestamp(0),
        &NodeValue::new("Condition", json!({})),
    );
    for m in MEDS {
        writer.put_node(
            &mut batch,
            m,
            Timestamp(0),
            &NodeValue::new("Medication", json!({})),
        );
    }
    for i in 0..PATIENTS {
        let p = NodeId(1 + i);
        node_ids.push(p);
        writer.put_node(
            &mut batch,
            p,
            Timestamp(0),
            &NodeValue::new("Patient", json!({})),
        );
        writer.put_edge(
            &mut batch,
            p,
            EdgeType::DiagnosedWith,
            HUB,
            Timestamp(1 + i),
            &EdgeValue::new(json!({})),
        );
    }
    drop(writer);
    store.write(batch).expect("commit fixture");
    Base {
        _dir: dir,
        store,
        node_ids,
    }
}

/// A deterministic plan of valid add/remove mutations (a removal only targets a
/// live edge) with strictly increasing timestamps, plus the final live edge set.
fn plan(seed: u64, hub_heavy: bool) -> (Vec<GraphMutation>, Vec<(NodeId, NodeId)>) {
    let mut rng = Rng::new(0xC0FFEE ^ seed);
    let mut live: HashSet<(NodeId, NodeId, EdgeType)> = (0..PATIENTS)
        .map(|i| (NodeId(1 + i), HUB, EdgeType::DiagnosedWith))
        .collect();
    let mut muts = Vec::with_capacity(MUTATIONS);
    for i in 0..MUTATIONS {
        let ts = Timestamp(BASE_TS + 1 + i as u64);
        let hub = if hub_heavy {
            rng.below(4) != 0
        } else {
            rng.below(3) == 0
        };
        let patient = NodeId(1 + rng.below(PATIENTS));
        let (dst, et) = if hub {
            (HUB, EdgeType::DiagnosedWith)
        } else {
            (MEDS[rng.below(3) as usize], EdgeType::PrescribedMedication)
        };
        let key = (patient, dst, et);
        let m = if live.remove(&key) {
            GraphMutation::RemoveEdge {
                src: patient,
                dst,
                edge_type: et,
                ts,
            }
        } else {
            live.insert(key);
            GraphMutation::AddEdge {
                src: patient,
                dst,
                edge_type: et,
                ts,
            }
        };
        muts.push(m);
    }
    let edges = live.iter().map(|&(s, d, _)| (s, d)).collect();
    (muts, edges)
}

/// Apply `muts` through the real pipeline: admit all in timestamp order, then
/// execute with `threads` threads dequeuing in that order.
fn apply(
    store: &RocksKv,
    model: &EmbeddingModel,
    muts: &[GraphMutation],
    threads: usize,
    metrics: &EmbeddingMetrics,
) {
    let sequencer = Sequencer::open(store, TsPolicy::Strict).expect("sequencer");
    let caps = Mutex::new(CapController::pinned(512, 1_500));

    let queue: Mutex<VecDeque<_>> = Mutex::new(
        muts.iter()
            .map(|m| {
                (
                    admit_mutation(*m, store, &sequencer, None).expect("admit"),
                    EdgeValue::new(json!({})),
                )
            })
            .collect(),
    );

    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                let job = queue.lock().unwrap().pop_front();
                let Some((admitted, value)) = job else { return };
                let out = execute_admitted(
                    admitted,
                    &value,
                    ModelKind::GraphSAGE,
                    store,
                    model,
                    metrics,
                    &caps,
                    &sequencer,
                    false,
                    None,
                )
                .expect("a well-formed mutation must commit");
                assert!(!out.ctx.fallback, "unexpected fallback on a small fixture");
            });
        }
    });
}

fn dump(store: &RocksKv, name: &'static str) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    store
        .scan_from_narrow_prefix(name, &[], &[], &mut |k, v| {
            out.insert(k.to_vec(), v.to_vec());
            std::ops::ControlFlow::Continue(())
        })
        .expect("scan");
    out
}

fn pool(workers: usize) -> PoolConfig {
    PoolConfig {
        workers,
        torch_threads: 1,
        ..PoolConfig::default()
    }
}

#[test]
fn concurrent_commit_equals_sequential_across_fifty_seeds() {
    let one = EmbeddingModel::spawn_with("diabetes130_graphsage", pool(1)).expect("1-worker pool");
    let many =
        EmbeddingModel::spawn_with("diabetes130_graphsage", pool(WORKERS)).expect("4-worker pool");

    let seq_registry = prometheus::Registry::new();
    let con_registry = prometheus::Registry::new();
    let seq_metrics = EmbeddingMetrics::new(&seq_registry).unwrap();
    let con_metrics = EmbeddingMetrics::new(&con_registry).unwrap();

    let mut worst_equiv = 0.0f32;
    let mut worst_recompute = 0.0f32;
    let mut embeddings_compared = 0usize;

    for seed in 0..SEEDS {
        let hub_heavy = seed % 2 == 1;
        let (muts, final_edges) = plan(seed, hub_heavy);

        let seq = build_base();
        let con = build_base();
        apply(&seq.store, &one, &muts, 1, &seq_metrics);
        apply(&con.store, &many, &muts, IN_FLIGHT, &con_metrics);

        // (1) structure and provenance: byte-identical.
        for name in [
            cf::CF_EDGES,
            cf::CF_REVERSE,
            cf::CF_NODES,
            cf::CF_COMMIT_META,
            cf::CF_META,
        ] {
            assert!(
                dump(&seq.store, name) == dump(&con.store, name),
                "seed {seed} (hub_heavy={hub_heavy}): {name} differs between sequential and \
                 concurrent execution"
            );
        }
        assert!(
            dump(&con.store, cf::CF_REPAIR).is_empty(),
            "seed {seed}: unexpected repair markers"
        );

        // (2) embeddings: same keys, same labels, same values.
        let seq_emb = dump(&seq.store, cf::CF_EMBEDDINGS);
        let con_emb = dump(&con.store, cf::CF_EMBEDDINGS);
        assert_eq!(
            seq_emb.keys().collect::<Vec<_>>(),
            con_emb.keys().collect::<Vec<_>>(),
            "seed {seed}: the set of (node, timestamp) embeddings differs"
        );
        for (key, sv) in &seq_emb {
            let a = Embedding::deserialize(sv).unwrap();
            let b = Embedding::deserialize(&con_emb[key]).unwrap();
            assert_eq!(a.model_id, b.model_id);
            assert_eq!(a.computation_path, b.computation_path);
            assert_eq!(a.vector.len(), b.vector.len());
            for (x, y) in a.vector.iter().zip(&b.vector) {
                let d = (x - y).abs();
                worst_equiv = worst_equiv.max(d);
                assert!(
                    d <= EQUIV_ABS_TOL,
                    "seed {seed}: embedding differs by {d} between sequential and concurrent runs"
                );
            }
            embeddings_compared += 1;
        }

        // (3) every node's latest embedding equals a from-scratch recompute on
        // the final graph: the property a lost update would violate.
        let last_ts = Timestamp(BASE_TS + MUTATIONS as u64);
        let reference = associative::full_recompute(
            &con.store,
            &one,
            &con.node_ids,
            &final_edges,
            &con.node_ids,
            ModelKind::GraphSAGE,
            last_ts,
        )
        .expect("full recompute");
        let index = TemporalIndex::new(&con.store);
        for (node, expected) in reference {
            let stored = index
                .embedding_as_of(node, Timestamp(u64::MAX))
                .unwrap()
                .unwrap_or_else(|| panic!("seed {seed}: node {node:?} has no embedding"));
            for (got, want) in stored.vector.iter().zip(&expected.vector) {
                let d = (got - want).abs();
                worst_recompute = worst_recompute.max(d);
                let tol = RECOMPUTE_ABS_TOL.max(RECOMPUTE_REL_TOL * want.abs());
                assert!(
                    d <= tol,
                    "seed {seed}: node {node:?} latest embedding is off by {d} from a full \
                     recompute on the final graph (a lost or misordered update)"
                );
            }
        }
    }

    // (4) not vacuous.
    let retries = con_metrics.mutation_conflict_retries_total.get();
    eprintln!(
        "concurrent-commit equivalence: {SEEDS} seeds x {MUTATIONS} mutations, {WORKERS} workers, \
         {IN_FLIGHT} in flight; {embeddings_compared} embeddings compared; \
         conflict retries: {retries}; worst seq-vs-concurrent diff {worst_equiv:e}; \
         worst vs full recompute {worst_recompute:e}"
    );
    assert_eq!(
        seq_metrics.mutation_conflict_retries_total.get(),
        0,
        "a single in-order thread can never conflict with itself"
    );
    assert!(
        retries > 0,
        "no conflict was ever detected across {SEEDS} seeds; the test would pass even without \
         the sequencer's conflict repair, so it proves nothing"
    );
}

#[test]
fn strict_policy_rejects_a_backdated_mutation_and_writes_nothing() {
    let model = EmbeddingModel::spawn_with("diabetes130_graphsage", pool(1)).expect("pool");
    let base = build_base();
    let registry = prometheus::Registry::new();
    let metrics = EmbeddingMetrics::new(&registry).unwrap();

    let first = GraphMutation::AddEdge {
        src: NodeId(1),
        dst: MEDS[0],
        edge_type: EdgeType::PrescribedMedication,
        ts: Timestamp(BASE_TS + 500),
    };
    apply(&base.store, &model, &[first], 1, &metrics);

    let before: Vec<_> = [cf::CF_EDGES, cf::CF_REVERSE, cf::CF_EMBEDDINGS, cf::CF_META]
        .iter()
        .map(|c| dump(&base.store, c))
        .collect();

    // The high-water mark is now BASE_TS + 500 (recovered from storage, so this
    // also proves it is persisted, not just held in memory).
    let sequencer = Sequencer::open(&base.store, TsPolicy::Strict).unwrap();
    assert_eq!(sequencer.high_water_mark(), BASE_TS + 500);
    let backdated = GraphMutation::AddEdge {
        src: NodeId(2),
        dst: MEDS[1],
        edge_type: EdgeType::PrescribedMedication,
        ts: Timestamp(BASE_TS + 100),
    };
    let err = admit_mutation(backdated, &base.store, &sequencer, None)
        .err()
        .expect("a timestamp below the high-water mark must be refused");
    assert!(matches!(err, CareGraphError::Aborted(_)), "got {err:?}");

    let after: Vec<_> = [cf::CF_EDGES, cf::CF_REVERSE, cf::CF_EMBEDDINGS, cf::CF_META]
        .iter()
        .map(|c| dump(&base.store, c))
        .collect();
    assert!(
        before == after,
        "a rejected mutation must leave no trace in storage"
    );
}
