//! Phase 1 — KV abstraction against a real, running RocksDB instance.

use caregraph::storage::{cf, KvStore, RocksKv};
use rocksdb::WriteBatch;
use tempfile::TempDir;

/// Open a real RocksDB in a fresh temp directory. Returned `TempDir` must be
/// held for the lifetime of the test — dropping it deletes the database.
fn open() -> (TempDir, RocksKv) {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    (dir, store)
}

#[test]
fn all_four_column_families_are_open() {
    let (_dir, store) = open();
    for name in cf::ALL {
        store
            .cf_handle(name)
            .unwrap_or_else(|e| panic!("column family {name} should be open: {e}"));
    }
    assert_eq!(cf::ALL.len(), 4, "PRD 3.3 defines exactly four families");
}

#[test]
fn put_then_get_round_trips_through_rocksdb() {
    let (_dir, store) = open();
    store.put(cf::CF_NODES, b"patient:P00001", b"stage-3-ckd").unwrap();

    assert_eq!(
        store.get(cf::CF_NODES, b"patient:P00001").unwrap().as_deref(),
        Some(&b"stage-3-ckd"[..])
    );
}

#[test]
fn get_of_an_absent_key_is_none_not_an_error() {
    let (_dir, store) = open();
    assert_eq!(store.get(cf::CF_NODES, b"never-written").unwrap(), None);
}

#[test]
fn column_families_are_isolated_from_one_another() {
    let (_dir, store) = open();
    // Same key bytes, four families, four distinct values.
    for (i, name) in cf::ALL.iter().enumerate() {
        store.put(name, b"shared-key", &[i as u8]).unwrap();
    }
    for (i, name) in cf::ALL.iter().enumerate() {
        assert_eq!(
            store.get(name, b"shared-key").unwrap(),
            Some(vec![i as u8]),
            "{name} leaked or was overwritten by another family"
        );
    }
}

#[test]
fn delete_removes_the_key() {
    let (_dir, store) = open();
    store.put(cf::CF_NODES, b"k", b"v").unwrap();
    store.delete(cf::CF_NODES, b"k").unwrap();
    assert_eq!(store.get(cf::CF_NODES, b"k").unwrap(), None);
}

#[test]
fn a_write_batch_commits_every_entry_or_none() {
    let (_dir, store) = open();

    let mut batch = WriteBatch::default();
    let nodes = store.cf_handle(cf::CF_NODES).unwrap();
    let embeddings = store.cf_handle(cf::CF_EMBEDDINGS).unwrap();
    batch.put_cf(&nodes, b"n1", b"node-state");
    batch.put_cf(&embeddings, b"n1", b"embedding-state");
    store.write(batch).unwrap();

    // Cross-family batch write is the mechanism Rule 5 depends on; if either
    // half were missing here, the atomic-commit guarantee could not hold.
    assert_eq!(store.get(cf::CF_NODES, b"n1").unwrap().as_deref(), Some(&b"node-state"[..]));
    assert_eq!(
        store.get(cf::CF_EMBEDDINGS, b"n1").unwrap().as_deref(),
        Some(&b"embedding-state"[..])
    );
}

#[test]
fn data_survives_closing_and_reopening_the_database() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("caregraph");

    {
        let store = RocksKv::open(&path).unwrap();
        store.put(cf::CF_NODES, b"durable", b"written-before-close").unwrap();
        store.flush(cf::CF_NODES).unwrap();
    } // store dropped — RocksDB closed

    let reopened = RocksKv::open(&path).unwrap();
    assert_eq!(
        reopened.get(cf::CF_NODES, b"durable").unwrap().as_deref(),
        Some(&b"written-before-close"[..]),
        "WAL-backed durability is the premise of the whole storage layer"
    );
}
