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

// ---------------------------------------------------------------------------
// Cipher-implementation compatibility
//
// The block cipher behind encryption at rest was replaced (a hand-written AES
// became the RustCrypto implementation). AES-256 is a fixed function, so a
// database written under the old code must open and read under the new. The
// fixture below was produced by the *old* implementation and is committed; the
// test opens a copy of it with the same key. It contains one value that lives
// in an SST file and one that lives only in the write-ahead log, so both file
// types are decrypted.
// ---------------------------------------------------------------------------

const LEGACY_KEY: [u8; 32] = [0x5a; 32];
const FIXTURE: &str = "tests/fixtures/legacy_aes_db";

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Writes the committed fixture. Ignored: run once, with the cipher whose
/// output the fixture is meant to pin, via
/// `cargo test --test integration generate_legacy_fixture -- --ignored`.
#[test]
#[ignore]
fn generate_legacy_fixture() {
    let dest = std::path::Path::new(FIXTURE);
    let _ = std::fs::remove_dir_all(dest);
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    {
        let store = RocksKv::open_encrypted(dest, &LEGACY_KEY).unwrap();
        store
            .put(cf::CF_NODES, b"flushed-key", b"flushed-value-in-an-sst")
            .unwrap();
        store.flush(cf::CF_NODES).unwrap();
        store
            .put(cf::CF_META, b"wal-key", b"wal-value-only-in-the-log")
            .unwrap();
    }
    let has_log = std::fs::read_dir(dest)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.path().extension().is_some_and(|x| x == "log")
                && e.metadata().map(|m| m.len() > 0).unwrap_or(false)
        });
    assert!(has_log, "fixture needs a non-empty WAL file");
}

#[test]
fn a_database_written_by_the_previous_cipher_opens_and_reads_under_the_current_one() {
    let work = TempDir::new().unwrap();
    let copy = work.path().join("db");
    copy_dir(std::path::Path::new(FIXTURE), &copy);

    let store = RocksKv::open_encrypted(&copy, &LEGACY_KEY).expect("open the legacy fixture");
    assert_eq!(
        store.get(cf::CF_NODES, b"flushed-key").unwrap().as_deref(),
        Some(&b"flushed-value-in-an-sst"[..]),
        "value stored in an encrypted SST"
    );
    assert_eq!(
        store.get(cf::CF_META, b"wal-key").unwrap().as_deref(),
        Some(&b"wal-value-only-in-the-log"[..]),
        "value recovered from the encrypted WAL"
    );
}

#[test]
fn the_legacy_fixture_is_actually_encrypted_and_rejects_another_key() {
    let raw = read_all_files(std::path::Path::new(FIXTURE));
    assert!(!raw.is_empty());
    for needle in [
        &b"flushed-value-in-an-sst"[..],
        &b"wal-value-only-in-the-log"[..],
    ] {
        assert!(
            !raw.windows(needle.len()).any(|w| w == needle),
            "plaintext leaked into the committed fixture"
        );
    }
    let work = TempDir::new().unwrap();
    let copy = work.path().join("db");
    copy_dir(std::path::Path::new(FIXTURE), &copy);
    let wrong = RocksKv::open_encrypted(&copy, &KEY_B);
    let readable = wrong
        .ok()
        .and_then(|s| s.get(cf::CF_NODES, b"flushed-key").ok().flatten());
    assert_ne!(
        readable.as_deref(),
        Some(&b"flushed-value-in-an-sst"[..]),
        "a wrong key must not read the data"
    );
}

/// Throughput of encrypted vs plain storage, for comparing cipher
/// implementations on the same machine. Ignored (it takes a while and prints
/// instead of asserting): run with
/// `cargo test --release --test integration encrypted_storage_throughput -- --ignored --nocapture`.
#[test]
#[ignore]
fn encrypted_storage_throughput() {
    use std::time::Instant;

    fn run(label: &str, key: Option<&[u8; 32]>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let store = match key {
            Some(k) => RocksKv::open_encrypted(&path, k).unwrap(),
            None => RocksKv::open(&path).unwrap(),
        };
        const N: u64 = 300_000;
        let value = vec![0xabu8; 200];

        let t = Instant::now();
        for chunk in 0..(N / 1000) {
            let mut batch = rocksdb::WriteBatch::default();
            let cf = store.cf_handle(cf::CF_NODES).unwrap();
            for i in 0..1000u64 {
                batch.put_cf(&cf, (chunk * 1000 + i).to_be_bytes(), &value);
            }
            drop(cf);
            store.write(batch).unwrap();
        }
        store.flush(cf::CF_NODES).unwrap();
        let ingest = t.elapsed();

        // 100k pseudo-random point reads.
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let t = Instant::now();
        let mut hits = 0u64;
        for _ in 0..100_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if store
                .get(cf::CF_NODES, &(x % N).to_be_bytes())
                .unwrap()
                .is_some()
            {
                hits += 1;
            }
        }
        let reads = t.elapsed();
        assert_eq!(hits, 100_000);

        println!(
            "{label:>10}: ingest {:>7.0} ms ({:>6.0} MB/s)   100k point reads {:>6.0} ms",
            ingest.as_secs_f64() * 1e3,
            (N * 200) as f64 / 1e6 / ingest.as_secs_f64(),
            reads.as_secs_f64() * 1e3
        );
    }

    let key = [0x77u8; 32];
    for rep in 1..=3 {
        println!("-- repetition {rep}");
        run("plain", None);
        run("encrypted", Some(&key));
    }
}
