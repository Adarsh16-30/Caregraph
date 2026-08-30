//! GAT counterpart to `associative_correctness_test.rs`'s Phase 4 success
//! criterion, extended to Phase 5's GAT path: "incremental result must
//! exactly match a full-graph recompute for the same mutation, on 50
//! randomised mutation sequences" — this time through `gat_incremental.rs`
//! rather than `associative.rs`.
//!
//! # Why this test calls resolve + patch + gat_incremental_update directly,
//! not `AtomicCommitter::commit`
//!
//! `AtomicCommitter::commit` does exactly this sequence internally, but it
//! also stages and writes the structural mutation — which would mean this
//! test's "incremental" and "full recompute" halves are comparing embeddings
//! computed against two different committed database states (the mutation
//! landed for one, not the other), not the same instant's graph computed two
//! ways. Calling the same three functions `AtomicCommitter` calls, without
//! its final `store.write`, keeps both halves reading the identical
//! pre-mutation-plus-one-patched-edge state `resolver.rs::patch_subgraph_for_mutation`
//! produces — which is the actual claim under test.
//!
//! See `associative_correctness_test.rs`'s module doc for why "exactly"
//! means float32-tolerance-exact, not bit-identical, and why that tolerance
//! is chosen where it is; the same reasoning and the same tolerance apply
//! here unchanged — nothing about GAT's attention aggregation changes the
//! argument that a 2-hop-bounded subgraph recompute is mathematically exact
//! for a 2-layer message-passing model (see `gat_incremental.rs`'s module
//! doc).

use std::collections::{HashMap, HashSet};

use caregraph::embedding::resolver::{patch_subgraph_for_mutation, AffectedSubgraphResolver};
use caregraph::embedding::state::{GraphMutation, MutationContext};
use caregraph::embedding::{associative, gat_incremental, EmbeddingModel};
use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::{TemporalIndex, TemporalWriter};
use caregraph::types::{EdgeType, ModelKind, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

const SEQUENCES: u32 = 50;
const FANOUT_CAP: usize = 512;
const MAX_EXPANDED_NODES: usize = 1_500;

const ABS_TOLERANCE: f32 = 1e-4;
const REL_TOLERANCE: f32 = 1e-3;

/// xorshift64* — same generator `associative_correctness_test.rs` uses, with
/// a distinct seed base so the two suites' mutation sequences never
/// coincidentally match (they don't need to; they're independent proofs).
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
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
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
}

/// Same hub-shaped fixture shape as `associative_correctness_test.rs` — see
/// that file's doc for why a hub is what makes the exactness claim
/// non-trivial.
struct Fixture {
    _dir: TempDir,
    store: RocksKv,
    live_edges: HashSet<(NodeId, NodeId, EdgeType)>,
    node_ids: Vec<NodeId>,
    clock: u64,
}

const PATIENTS: u64 = 40;
const HUB: NodeId = NodeId(100_000);
const MEDS: [NodeId; 3] = [NodeId(200_000), NodeId(200_001), NodeId(200_002)];

fn build_fixture() -> Fixture {
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

    let mut live_edges = HashSet::new();
    for i in 0..PATIENTS {
        let patient = NodeId(1 + i);
        node_ids.push(patient);
        writer.put_node(
            &mut batch,
            patient,
            Timestamp(0),
            &NodeValue::new("Patient", json!({})),
        );
        writer.put_edge(
            &mut batch,
            patient,
            EdgeType::DiagnosedWith,
            HUB,
            Timestamp(1 + i),
            &EdgeValue::new(json!({})),
        );
        live_edges.insert((patient, HUB, EdgeType::DiagnosedWith));
    }

    drop(writer);
    store.write(batch).expect("commit fixture");

    Fixture {
        _dir: dir,
        store,
        live_edges,
        node_ids,
        clock: 1000,
    }
}

impl Fixture {
    fn random_mutation(&mut self, rng: &mut Rng) -> GraphMutation {
        self.clock += 1;
        let ts = Timestamp(self.clock);

        let touch_hub = rng.below(3) == 0;
        let (src, dst, edge_type) = if touch_hub {
            let patient = NodeId(1 + rng.below(PATIENTS));
            (patient, HUB, EdgeType::DiagnosedWith)
        } else {
            let patient = NodeId(1 + rng.below(PATIENTS));
            let med = MEDS[rng.below(MEDS.len() as u64) as usize];
            (patient, med, EdgeType::PrescribedMedication)
        };

        let writer = TemporalWriter::new(&self.store).expect("writer");
        let mut batch = WriteBatch::default();
        let key = (src, dst, edge_type);

        let mutation = if self.live_edges.contains(&key) {
            writer.remove_edge(&mut batch, src, edge_type, dst, ts);
            self.live_edges.remove(&key);
            GraphMutation::RemoveEdge {
                src,
                dst,
                edge_type,
                ts,
            }
        } else {
            writer.put_edge(
                &mut batch,
                src,
                edge_type,
                dst,
                ts,
                &EdgeValue::new(json!({})),
            );
            self.live_edges.insert(key);
            GraphMutation::AddEdge {
                src,
                dst,
                edge_type,
                ts,
            }
        };
        drop(writer);
        self.store.write(batch).expect("commit mutation");
        mutation
    }

    fn all_edges(&self) -> Vec<(NodeId, NodeId)> {
        self.live_edges.iter().map(|&(s, d, _)| (s, d)).collect()
    }
}

#[test]
fn gat_incremental_matches_full_recompute_across_fifty_random_sequences() {
    let model = EmbeddingModel::spawn("diabetes130_gat")
        .expect("ml/deployed/diabetes130_gat must exist — run ml/train_gat.py first");

    let mut mismatches: Vec<String> = Vec::new();
    let mut hub_touching_checked = 0usize;
    let mut total_checked = 0usize;

    for seq in 0..SEQUENCES {
        let mut fx = build_fixture();
        let mut rng = Rng::new(0xDEC0DE ^ (seq as u64).wrapping_mul(0x9E37_79B9));

        for _ in 0..8 {
            let mutation = fx.random_mutation(&mut rng);
            let as_of = mutation.timestamp();

            // The same three steps AtomicCommitter::commit runs, minus the
            // final write — see the module doc for why that matters here.
            let index = TemporalIndex::new(&fx.store);
            let resolver = AffectedSubgraphResolver::new(&fx.store, FANOUT_CAP, MAX_EXPANDED_NODES);
            let mut subgraph = resolver
                .resolve(mutation)
                .expect("resolve must not error on a well-formed fixture");
            patch_subgraph_for_mutation(&mut subgraph, &index, mutation)
                .expect("patch must not error on a well-formed fixture");

            let mut ctx = MutationContext::new(mutation, ModelKind::GAT);
            gat_incremental::gat_incremental_update(&mut ctx, &fx.store, &model, subgraph, as_of)
                .expect("gat_incremental_update must not error on a well-formed fixture");
            assert!(
                !ctx.fallback,
                "seq {seq}: unexpected fallback on a small, well-formed fixture"
            );

            // full_recompute is model-architecture-agnostic — it just calls
            // model.forward() over the whole graph — so the same function
            // associative_correctness_test.rs uses works unchanged here,
            // just tagged ModelKind::GAT.
            let reference = associative::full_recompute(
                &fx.store,
                &model,
                &fx.node_ids,
                &fx.all_edges(),
                &ctx.affected,
                ModelKind::GAT,
                as_of,
            )
            .expect("full_recompute must not error on a well-formed fixture");

            let incremental_by_node: HashMap<NodeId, &Vec<f32>> = ctx
                .embeddings_after
                .iter()
                .map(|(n, e)| (*n, &e.vector))
                .collect();
            let reference_by_node: HashMap<NodeId, &Vec<f32>> =
                reference.iter().map(|(n, e)| (*n, &e.vector)).collect();

            total_checked += 1;
            if fx.live_edges.iter().any(|&(_, d, _)| d == HUB) && ctx.affected.contains(&HUB) {
                hub_touching_checked += 1;
            }

            for &node in &ctx.affected {
                let (Some(inc), Some(full)) =
                    (incremental_by_node.get(&node), reference_by_node.get(&node))
                else {
                    mismatches.push(format!("seq {seq} node {node:?}: missing from one side"));
                    continue;
                };
                for (i, (&a, &b)) in inc.iter().zip(full.iter()).enumerate() {
                    let diff = (a - b).abs();
                    if diff > ABS_TOLERANCE && diff > (b.abs() * REL_TOLERANCE) {
                        mismatches.push(format!(
                            "seq {seq} node {node:?} dim {i}: incremental {a} vs full-recompute \
                             {b} (diff {diff}, exceeds {ABS_TOLERANCE} abs / {REL_TOLERANCE} rel)"
                        ));
                    }
                }
            }
        }
    }

    assert!(
        hub_touching_checked > 0,
        "the random sequences never exercised a hub-touching mutation; the exactness \
         claim needs at least one to be meaningful"
    );
    assert!(
        mismatches.is_empty(),
        "{}/{} affected-node checks failed:\n{}",
        mismatches.len(),
        total_checked,
        mismatches.join("\n")
    );
}
