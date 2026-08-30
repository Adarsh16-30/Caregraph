//! Property-based point-in-time correctness (PRD Phase 2).
//!
//! Phase 2's success criterion: for any sequence of mutations, `as_of(T)` must
//! match an independently reconstructed reference state, at 20 or more distinct
//! historical timestamps.
//!
//! The reference model here is deliberately naive — a sorted map walked
//! linearly. It shares no code with `TemporalIndex`, so agreement between them
//! is evidence rather than tautology. If both were built on the same key
//! encoding, this test would only prove the encoding is self-consistent.
//!
//! Mutation *sequences* are synthetic; the storage engine is a real on-disk
//! RocksDB, as everywhere else.

use std::collections::BTreeMap;

use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::record::EdgeValue;
use caregraph::temporal::{TemporalIndex, TemporalWriter};
use caregraph::types::{EdgeType, NodeId, Timestamp};
use proptest::prelude::*;
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;

const SRC: NodeId = NodeId(1);
const REL: EdgeType = EdgeType::DiagnosedWith;

/// One generated mutation against a single adjacency list.
#[derive(Debug, Clone, Copy)]
struct Mutation {
    dst: u64,
    ts: u64,
    /// `None` is a removal.
    payload: Option<u32>,
}

/// Independent reference model: `(dst, ts) -> payload`, where `None` is a
/// tombstone. A later write to the same `(dst, ts)` overwrites the earlier one,
/// matching RocksDB's put semantics for an identical key.
type Reference = BTreeMap<(u64, u64), Option<u32>>;

fn build_reference(mutations: &[Mutation]) -> Reference {
    let mut model = Reference::new();
    for m in mutations {
        model.insert((m.dst, m.ts), m.payload);
    }
    model
}

/// Reconstruct the live adjacency list at `as_of` by brute force: for each dst,
/// take the newest version at or before `as_of`, and keep it only if that
/// version is not a tombstone.
fn reference_as_of(model: &Reference, as_of: u64) -> Vec<(u64, u32)> {
    let mut newest: BTreeMap<u64, (u64, Option<u32>)> = BTreeMap::new();

    for (&(dst, ts), &payload) in model {
        if ts > as_of {
            continue;
        }
        match newest.get(&dst) {
            Some(&(seen_ts, _)) if seen_ts >= ts => {}
            _ => {
                newest.insert(dst, (ts, payload));
            }
        }
    }

    newest
        .into_iter()
        .filter_map(|(dst, (_, payload))| payload.map(|p| (dst, p)))
        .collect()
}

/// Apply the same mutations to a real RocksDB and read them back at `as_of`.
fn actual_as_of(store: &RocksKv, as_of: u64) -> Vec<(u64, u32)> {
    let index = TemporalIndex::new(store);
    index
        .edges_as_of(SRC, REL, Timestamp(as_of))
        .expect("point-in-time read")
        .into_iter()
        .map(|edge| {
            let payload = edge.properties["p"]
                .as_u64()
                .expect("edge payload should round-trip as an integer");
            (edge.dst.as_u64(), payload as u32)
        })
        .collect()
}

fn apply(store: &RocksKv, mutations: &[Mutation]) {
    let writer = TemporalWriter::new(store).expect("writer");
    let mut batch = WriteBatch::default();

    for m in mutations {
        let ts = Timestamp(m.ts);
        match m.payload {
            Some(p) => writer.put_edge(
                &mut batch,
                SRC,
                REL,
                NodeId(m.dst),
                ts,
                &EdgeValue::new(json!({ "p": p })),
            ),
            None => writer.remove_edge(&mut batch, SRC, REL, NodeId(m.dst), ts),
        }
    }

    store.write(batch).expect("batch commit");
}

fn open() -> (TempDir, RocksKv) {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    (dir, store)
}

// ---------------------------------------------------------------------------
// Deterministic scenarios — the cases most likely to be wrong
// ---------------------------------------------------------------------------

#[test]
fn a_removed_edge_is_still_visible_before_its_removal() {
    let (_dir, store) = open();
    apply(
        &store,
        &[
            Mutation {
                dst: 10,
                ts: 100,
                payload: Some(1),
            },
            Mutation {
                dst: 10,
                ts: 200,
                payload: None,
            },
        ],
    );

    // This is the case a hard delete would get wrong: the edge genuinely
    // existed at t=150, and erasing the key would rewrite that history.
    assert_eq!(actual_as_of(&store, 150), vec![(10, 1)]);
    assert_eq!(
        actual_as_of(&store, 200),
        vec![],
        "removed at exactly t=200"
    );
    assert_eq!(actual_as_of(&store, 250), vec![]);
    assert_eq!(actual_as_of(&store, 99), vec![], "did not exist yet");
}

#[test]
fn an_edge_can_be_removed_and_re_added() {
    let (_dir, store) = open();
    apply(
        &store,
        &[
            Mutation {
                dst: 10,
                ts: 100,
                payload: Some(1),
            },
            Mutation {
                dst: 10,
                ts: 200,
                payload: None,
            },
            Mutation {
                dst: 10,
                ts: 300,
                payload: Some(2),
            },
        ],
    );

    assert_eq!(actual_as_of(&store, 150), vec![(10, 1)]);
    assert_eq!(actual_as_of(&store, 250), vec![]);
    assert_eq!(actual_as_of(&store, 350), vec![(10, 2)]);
}

#[test]
fn one_edges_removal_does_not_hide_its_neighbours() {
    let (_dir, store) = open();
    apply(
        &store,
        &[
            Mutation {
                dst: 10,
                ts: 100,
                payload: Some(1),
            },
            Mutation {
                dst: 20,
                ts: 110,
                payload: Some(2),
            },
            Mutation {
                dst: 30,
                ts: 120,
                payload: Some(3),
            },
            Mutation {
                dst: 20,
                ts: 200,
                payload: None,
            },
        ],
    );

    // dst=20's tombstone sorts between dst=10 and dst=30's live versions,
    // because the key orders by timestamp before dst.
    assert_eq!(actual_as_of(&store, 250), vec![(10, 1), (30, 3)]);
    assert_eq!(actual_as_of(&store, 150), vec![(10, 1), (20, 2), (30, 3)]);
}

#[test]
fn the_newest_version_wins_when_several_share_a_timestamp_region() {
    let (_dir, store) = open();
    apply(
        &store,
        &[
            Mutation {
                dst: 10,
                ts: 100,
                payload: Some(1),
            },
            Mutation {
                dst: 10,
                ts: 101,
                payload: Some(2),
            },
            Mutation {
                dst: 10,
                ts: 102,
                payload: Some(3),
            },
        ],
    );

    assert_eq!(actual_as_of(&store, 100), vec![(10, 1)]);
    assert_eq!(actual_as_of(&store, 101), vec![(10, 2)]);
    assert_eq!(actual_as_of(&store, 1000), vec![(10, 3)]);
}

#[test]
fn a_windowed_scan_reports_removals_as_well_as_additions() {
    let (_dir, store) = open();
    apply(
        &store,
        &[
            Mutation {
                dst: 10,
                ts: 100,
                payload: Some(1),
            },
            Mutation {
                dst: 20,
                ts: 200,
                payload: Some(2),
            },
            Mutation {
                dst: 10,
                ts: 300,
                payload: None,
            },
            Mutation {
                dst: 30,
                ts: 400,
                payload: Some(3),
            },
        ],
    );

    let index = TemporalIndex::new(&store);
    let window = index
        .edge_versions_in_window(SRC, REL, Timestamp(200), Timestamp(300))
        .unwrap();

    // Newest first, and the retraction at t=300 must be included — a diagnosis
    // being withdrawn is a change in the window.
    let seen: Vec<(u64, u64, bool)> = window
        .iter()
        .map(|v| (v.timestamp.as_u64(), v.dst.as_u64(), v.deleted))
        .collect();
    assert_eq!(seen, vec![(300, 10, true), (200, 20, false)]);
}

// ---------------------------------------------------------------------------
// Property: as_of always agrees with the reference reconstruction
// ---------------------------------------------------------------------------

/// Mutations over a small dst space and a short timeline, so that generated
/// sequences actually collide — overwrites, removals, and re-adds on the same
/// edge are the interesting cases, and a wide space would rarely produce them.
fn mutation_strategy() -> impl Strategy<Value = Vec<Mutation>> {
    prop::collection::vec((1u64..=6, 1u64..=40, prop::option::of(0u32..1000)), 1..40).prop_map(
        |raw| {
            raw.into_iter()
                .map(|(dst, ts, payload)| Mutation { dst, ts, payload })
                .collect()
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Each case opens a real RocksDB instance, so cases are expensive.
        cases: 48,
        ..ProptestConfig::default()
    })]

    /// Phase 2 success criterion: `as_of(T)` matches a reference reconstruction
    /// at every timestamp across the timeline — well beyond the 20 required.
    #[test]
    fn as_of_matches_the_reference_reconstruction_at_every_timestamp(
        mutations in mutation_strategy()
    ) {
        let (_dir, store) = open();
        apply(&store, &mutations);
        let model = build_reference(&mutations);

        // 0 and 41 sit outside the generated range, covering "before anything
        // existed" and "after everything settled".
        for as_of in 0u64..=41 {
            let expected = reference_as_of(&model, as_of);
            let actual = actual_as_of(&store, as_of);
            prop_assert_eq!(
                actual,
                expected,
                "divergence at as_of={} for mutations {:?}",
                as_of,
                mutations
            );
        }
    }

    /// Reading a single edge must agree with reconstructing the whole
    /// adjacency list and picking that edge out of it. These take different
    /// code paths through the index.
    #[test]
    fn single_edge_reads_agree_with_full_adjacency_reads(
        mutations in mutation_strategy()
    ) {
        let (_dir, store) = open();
        apply(&store, &mutations);
        let index = TemporalIndex::new(&store);

        for as_of in [0u64, 5, 12, 20, 33, 41] {
            let adjacency = index.edges_as_of(SRC, REL, Timestamp(as_of)).unwrap();

            for dst in 1u64..=6 {
                let via_adjacency = adjacency.iter().find(|e| e.dst.as_u64() == dst);
                let via_single = index
                    .edge_as_of(SRC, REL, NodeId(dst), Timestamp(as_of))
                    .unwrap();

                prop_assert_eq!(
                    via_single.as_ref(),
                    via_adjacency,
                    "edge_as_of and edges_as_of disagree on dst={} at as_of={}",
                    dst,
                    as_of
                );
            }
        }
    }

    /// A windowed scan must return exactly the versions whose timestamps fall
    /// inside the window, newest first.
    #[test]
    fn windowed_scans_return_exactly_the_versions_in_range(
        mutations in mutation_strategy(),
        from in 0u64..=40,
        span in 0u64..=20,
    ) {
        let (_dir, store) = open();
        apply(&store, &mutations);
        let to = from + span;

        let model = build_reference(&mutations);
        let mut expected: Vec<u64> = model
            .keys()
            .map(|&(_, ts)| ts)
            .filter(|ts| *ts >= from && *ts <= to)
            .collect();
        expected.sort_unstable();
        expected.reverse();

        let index = TemporalIndex::new(&store);
        let actual: Vec<u64> = index
            .edge_versions_in_window(SRC, REL, Timestamp(from), Timestamp(to))
            .unwrap()
            .iter()
            .map(|v| v.timestamp.as_u64())
            .collect();

        prop_assert_eq!(actual, expected);
    }
}
