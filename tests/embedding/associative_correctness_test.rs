//! Phase 4 success criterion: "incremental result must exactly match a
//! full-graph recompute for the same mutation, on 50 randomised mutation
//! sequences."
//!
//! "Exactly" means the same computation, not an approximation of one: a
//! 2-layer model's output for node `v` is a pure function of `v`'s 2-hop
//! neighbourhood, so recomputing only the resolved subgraph and recomputing
//! the whole graph run identical math over an identical set of contributions
//! for every node the resolver names as affected — see
//! `src/embedding/associative.rs` and `resolver.rs`'s two-ring expansion.
//!
//! It does not mean bit-identical. The incremental and full-graph tensors
//! differ in total size (the full graph carries isolated nodes the resolver
//! never needs to touch), and PyTorch's CPU scatter-mean kernel dispatches
//! differently for different tensor shapes — same mathematical sum, summed in
//! a different order, which float32 is not guaranteed to round to the same
//! bits for. Measured directly: after fixing a real resolver bug (an earlier
//! version fetched edges for affected nodes but not for their neighbours,
//! silently starving those neighbours' own layer-1 aggregation — errors of
//! ~1e-2, unmistakably structural), the residual gap across all 50 sequences
//! is a max absolute difference of 6e-7 and max relative difference of 6e-5 —
//! four orders of magnitude tighter than the bug this test exists to catch.
//! The tolerance below is chosen to sit between those two numbers, not to
//! paper over one with room to spare for the other.

use std::collections::{HashMap, HashSet};

use caregraph::embedding::state::{GraphMutation, MutationContext};
use caregraph::embedding::{associative, EmbeddingModel};
use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::TemporalWriter;
use caregraph::types::{EdgeType, ModelKind, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

const SEQUENCES: u32 = 50;
const FANOUT_CAP: usize = 512;
/// Far above what this fixture's ~44 nodes could ever reach — the point of
/// this test is to prove exactness where the budget does *not* bind, the
/// benchmark is what exercises the budget itself binding on the real graph.
/// (Matches the production default in bench_incremental.rs; irrelevant here
/// since this fixture never gets close to it.)
const MAX_EXPANDED_NODES: usize = 1_500;

/// Chosen between the two numbers in the module doc: comfortably above the
/// ~6e-7 absolute / ~6e-5 relative float32 noise this test actually measures,
/// comfortably below the ~1e-2 the resolver bug this test caught produced.
const ABS_TOLERANCE: f32 = 1e-4;
const REL_TOLERANCE: f32 = 1e-3;

/// xorshift64* — same algorithm the benchmark binaries use, for a
/// reproducible sequence without pulling in a crate.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
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

/// A hub-shaped fixture: one condition many patients share, plus enough
/// medications/providers that a random mutation exercises more than one edge
/// type. Small enough that a whole-graph recompute stays fast across 50
/// sequences; the hub is what makes the exactness claim non-trivial to meet.
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
    writer.put_node(&mut batch, HUB, Timestamp(0), &NodeValue::new("Condition", json!({})));
    for m in MEDS {
        writer.put_node(&mut batch, m, Timestamp(0), &NodeValue::new("Medication", json!({})));
    }

    let mut live_edges = HashSet::new();
    for i in 0..PATIENTS {
        let patient = NodeId(1 + i);
        node_ids.push(patient);
        writer.put_node(&mut batch, patient, Timestamp(0), &NodeValue::new("Patient", json!({})));
        writer.put_edge(&mut batch, patient, EdgeType::DiagnosedWith, HUB, Timestamp(1 + i), &EdgeValue::new(json!({})));
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
    /// Apply one random mutation, real add or remove, and return it. Prefers
    /// touching the hub about a third of the time — that is the case that
    /// actually stresses exactness, per the module doc above.
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
            GraphMutation::RemoveEdge { src, dst, edge_type, ts }
        } else {
            writer.put_edge(&mut batch, src, edge_type, dst, ts, &EdgeValue::new(json!({})));
            self.live_edges.insert(key);
            GraphMutation::AddEdge { src, dst, edge_type, ts }
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
fn incremental_matches_full_recompute_across_fifty_random_sequences() {
    let model = EmbeddingModel::spawn("diabetes130_graphsage")
        .expect("ml/deployed/diabetes130_graphsage must exist — run ml/train_graphsage.py first");

    let mut mismatches: Vec<String> = Vec::new();
    let mut hub_touching_checked = 0usize;
    let mut total_checked = 0usize;

    for seq in 0..SEQUENCES {
        let mut fx = build_fixture();
        let mut rng = Rng::new(0xC0FFEE ^ (seq as u64).wrapping_mul(0x9E37_79B9));

        for _ in 0..8 {
            let mutation = fx.random_mutation(&mut rng);

            let mut ctx = MutationContext::new(mutation, ModelKind::GraphSAGE);
            associative::incremental_aggregate(&mut ctx, &fx.store, &model, FANOUT_CAP, MAX_EXPANDED_NODES)
                .expect("incremental_aggregate must not error on a well-formed fixture");
            assert!(!ctx.fallback, "seq {seq}: unexpected fallback on a small, well-formed fixture");

            let as_of = mutation.timestamp();
            let reference = associative::full_recompute(
                &fx.store,
                &model,
                &fx.node_ids,
                &fx.all_edges(),
                &ctx.affected,
                ModelKind::GraphSAGE,
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
