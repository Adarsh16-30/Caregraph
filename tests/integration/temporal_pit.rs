//! Point-in-time retrieval over a real RocksDB instance.
//!
//! Verifies Contribution 2 end to end: with the inverted-timestamp encoding, a
//! single forward seek lands on the newest version at or before `as_of`. No
//! reverse iterator, no secondary index, no scan-and-filter.
//!
//! Phase 2 builds the `as_of()` read API on top of what is asserted here.

use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::keys::{as_of_prefix, decode_edge_key, edge_prefix, encode_edge_key};
use caregraph::types::{EdgeType, NodeId, Timestamp};
use tempfile::TempDir;

const PATIENT: NodeId = NodeId(1);
const CONDITION: NodeId = NodeId(900);
const REL: EdgeType = EdgeType::DiagnosedWith;

/// A database holding five versions of one edge, at t=100..500.
fn db_with_version_history() -> (TempDir, RocksKv) {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");

    for ts in [100u64, 200, 300, 400, 500] {
        let key = encode_edge_key(PATIENT, REL, Timestamp(ts), CONDITION);
        store
            .put(cf::CF_EDGES, &key, format!("v{ts}").as_bytes())
            .unwrap();
    }
    (dir, store)
}

/// The timestamp of whatever a point-in-time seek at `as_of` returns.
fn version_visible_at(store: &RocksKv, as_of: u64) -> Option<u64> {
    let prefix = edge_prefix(PATIENT, REL);
    let seek = as_of_prefix(&prefix, Timestamp(as_of));
    let (key, _) = store.seek(cf::CF_EDGES, &prefix, &seek).unwrap()?;
    let (_, _, ts, _) = decode_edge_key(&key).expect("well-formed edge key");
    Some(ts.as_u64())
}

#[test]
fn seek_returns_the_newest_version_at_or_before_as_of() {
    let (_dir, store) = db_with_version_history();

    // Between versions: must see the older one, never the future one.
    assert_eq!(version_visible_at(&store, 350), Some(300));
    assert_eq!(version_visible_at(&store, 499), Some(400));
    // Exactly on a version boundary: that version is visible.
    assert_eq!(version_visible_at(&store, 300), Some(300));
    // Past the newest write.
    assert_eq!(version_visible_at(&store, 10_000), Some(500));
}

#[test]
fn timestamp_max_resolves_to_current_state() {
    let (_dir, store) = db_with_version_history();
    assert_eq!(
        version_visible_at(&store, Timestamp::MAX.as_u64()),
        Some(500)
    );
}

#[test]
fn a_time_before_the_first_write_sees_nothing() {
    let (_dir, store) = db_with_version_history();
    assert_eq!(
        version_visible_at(&store, 99),
        None,
        "the edge did not exist yet at t=99"
    );
}

#[test]
fn prefix_scan_yields_versions_newest_first() {
    let (_dir, store) = db_with_version_history();
    let prefix = edge_prefix(PATIENT, REL);

    let timestamps: Vec<u64> = store
        .scan_prefix(cf::CF_EDGES, &prefix)
        .unwrap()
        .iter()
        .map(|(k, _)| decode_edge_key(k).unwrap().2.as_u64())
        .collect();

    assert_eq!(timestamps, vec![500, 400, 300, 200, 100]);
}

#[test]
fn a_seek_never_crosses_into_another_nodes_adjacency_list() {
    let (_dir, store) = db_with_version_history();

    // A different patient with an edge older than anything we will ask for.
    let other = NodeId(2);
    store
        .put(
            cf::CF_EDGES,
            &encode_edge_key(other, REL, Timestamp(1), CONDITION),
            b"other-patient",
        )
        .unwrap();

    // Ask patient 2 for a time before its only edge existed. A leaking iterator
    // would hand back patient 1's data instead of None.
    let prefix = edge_prefix(other, REL);
    let seek = as_of_prefix(&prefix, Timestamp(0));
    assert_eq!(store.seek(cf::CF_EDGES, &prefix, &seek).unwrap(), None);
}

#[test]
fn edge_types_on_the_same_node_do_not_collide() {
    let (_dir, store) = db_with_version_history();

    store
        .put(
            cf::CF_EDGES,
            &encode_edge_key(
                PATIENT,
                EdgeType::PrescribedMedication,
                Timestamp(250),
                NodeId(77),
            ),
            b"metformin",
        )
        .unwrap();

    // The DIAGNOSED_WITH history is unchanged by a PRESCRIBED edge landing
    // between two of its versions.
    assert_eq!(version_visible_at(&store, 350), Some(300));

    let meds = store
        .scan_prefix(
            cf::CF_EDGES,
            &edge_prefix(PATIENT, EdgeType::PrescribedMedication),
        )
        .unwrap();
    assert_eq!(meds.len(), 1);
}
