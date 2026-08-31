//! Rule 8 — real encryption at rest, proven by reading the raw on-disk files
//! RocksDB writes, not just by calling the API and trusting it worked.
//!
//! Every assertion here is against actual bytes on disk after a real
//! `flush()` (SST files aren't written until then — data sitting in the
//! memtable is in-process RAM, and finding plaintext there would prove
//! nothing about encryption at rest). The unencrypted control case is what
//! makes the encrypted case's "not found" meaningful: it proves the marker
//! *would* be visible in the raw files if nothing encrypted it, ruling out
//! "the search was wrong" as an explanation for the encrypted case passing.

use caregraph::storage::{cf, KvStore, RocksKv};
use tempfile::TempDir;

/// A key length away from any real block/file boundary and unlikely to
/// collide with anything RocksDB's own format bytes produce by chance.
const MARKER: &[u8] = b"CAREGRAPH-RULE8-PLAINTEXT-MARKER-4f9a1c";

const KEY_A: [u8; 32] = [0x11; 32];
const KEY_B: [u8; 32] = [0x22; 32]; // deliberately wrong key for the negative test

/// Concatenates every regular file under `dir` (recursively) into one buffer
/// — good enough to grep for a marker across however many SST/WAL/MANIFEST
/// files RocksDB decided to create.
fn read_all_files(dir: &std::path::Path) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read_dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(bytes) = std::fs::read(&path) {
                buf.extend_from_slice(&bytes);
            }
        }
    }
    buf
}

#[test]
fn unencrypted_database_leaks_plaintext_to_disk_control_case() {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    store.put(cf::CF_NODES, b"marker-key", MARKER).unwrap();
    store.flush(cf::CF_NODES).unwrap();

    let raw = read_all_files(dir.path());
    assert!(
        raw.windows(MARKER.len()).any(|w| w == MARKER),
        "control case: an unencrypted database must show the plaintext marker \
         in its raw on-disk files, or this test's search methodology can't be \
         trusted to prove anything about the encrypted case below"
    );
}

#[test]
fn encrypted_database_hides_plaintext_from_the_raw_files() {
    let dir = TempDir::new().expect("temp dir");
    let store =
        RocksKv::open_encrypted(dir.path().join("caregraph"), &KEY_A).expect("open encrypted");
    store.put(cf::CF_NODES, b"marker-key", MARKER).unwrap();
    store.flush(cf::CF_NODES).unwrap();
    drop(store);

    let raw = read_all_files(dir.path());
    assert!(
        !raw.windows(MARKER.len()).any(|w| w == MARKER),
        "Rule 8 violation: the plaintext marker is readable in the raw on-disk \
         files of an \"encrypted\" database"
    );
}

#[test]
fn encrypted_database_round_trips_correctly_with_the_right_key() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store =
            RocksKv::open_encrypted(dir.path().join("caregraph"), &KEY_A).expect("open encrypted");
        store.put(cf::CF_NODES, b"marker-key", MARKER).unwrap();
        store.flush(cf::CF_NODES).unwrap();
    } // drop closes the DB

    let reopened = RocksKv::open_encrypted(dir.path().join("caregraph"), &KEY_A)
        .expect("reopen with the same key must succeed");
    assert_eq!(
        reopened
            .get(cf::CF_NODES, b"marker-key")
            .unwrap()
            .as_deref(),
        Some(MARKER),
        "data written under encryption must read back byte-identical with the \
         correct key"
    );
}

#[test]
fn encrypted_database_refuses_the_wrong_key() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store =
            RocksKv::open_encrypted(dir.path().join("caregraph"), &KEY_A).expect("open encrypted");
        store.put(cf::CF_NODES, b"marker-key", MARKER).unwrap();
        store.flush(cf::CF_NODES).unwrap();
    }

    // Opening with the wrong key must not silently succeed and return the
    // correct plaintext — that would mean the data was never really
    // encrypted under the key at all. RocksDB's own block checksums are
    // computed over the plaintext before CTR-mode encryption ever touches
    // it, so decrypting with the wrong key produces garbage that fails
    // checksum validation — the expected, honest failure mode, not silent
    // wrong-data corruption.
    match RocksKv::open_encrypted(dir.path().join("caregraph"), &KEY_B) {
        Err(_) => {} // opening itself failed — acceptable, and what we expect
        Ok(store) => {
            // If open succeeded (e.g. only column-family metadata was read
            // before any checksum-guarded block), the actual read must still
            // not return the correct plaintext.
            let result = store.get(cf::CF_NODES, b"marker-key");
            match result {
                Err(_) => {} // read-time checksum failure — expected
                Ok(value) => assert_ne!(
                    value.as_deref(),
                    Some(MARKER),
                    "Rule 8 violation: the wrong key decrypted the correct plaintext — \
                     encryption is not actually keyed on CAREGRAPH_ENCRYPTION_KEY"
                ),
            }
        }
    }
}
