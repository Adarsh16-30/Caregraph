//! Backup, restore, checkpoint and re-encryption against real databases.
//!
//! Every comparison is a byte-for-byte dump of every column family, not a spot
//! check, because "restored correctly" means *all* of the state, including the
//! audit chain and the high-water mark that live beside the graph data.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use caregraph::security::audit::{verify_store, Fields, MutationAudit};
use caregraph::storage::backup::{create_checkpoint, BackupManager};
use caregraph::storage::maintenance::{check_database, reencrypt};
use caregraph::storage::{cf, KvStore, RocksKv};
use rocksdb::WriteBatch;
use tempfile::TempDir;

const MARKER: &[u8] = b"CAREGRAPH-BACKUP-PLAINTEXT-MARKER-7c21e0";
const KEY_A: [u8; 32] = [0xa1; 32];
const KEY_B: [u8; 32] = [0xb2; 32];

type Dump = BTreeMap<(&'static str, Vec<u8>), Vec<u8>>;

/// Every key and value of every column family.
fn dump(store: &RocksKv) -> Dump {
    let mut out = Dump::new();
    for name in cf::ALL {
        store
            .scan_from_narrow_prefix(name, &[], &[], &mut |k, v| {
                out.insert((name, k.to_vec()), v.to_vec());
                ControlFlow::Continue(())
            })
            .unwrap();
    }
    out
}

/// Commits records `range`, each as one atomic batch touching several column
/// families and carrying one audit record, like a real mutation does.
fn commit(store: &RocksKv, audit: &MutationAudit, range: std::ops::RangeInclusive<u64>) {
    let edges = store.cf_handle(cf::CF_EDGES).unwrap();
    let nodes = store.cf_handle(cf::CF_NODES).unwrap();
    let embeddings = store.cf_handle(cf::CF_EMBEDDINGS).unwrap();
    for i in range {
        let mut batch = WriteBatch::default();
        batch.put_cf(&edges, i.to_be_bytes(), format!("edge-{i}"));
        batch.put_cf(&nodes, i.to_be_bytes(), MARKER);
        batch.put_cf(&embeddings, i.to_be_bytes(), vec![i as u8; 64]);
        let pending = audit
            .stage(
                &mut batch,
                store,
                Fields {
                    ts_us: 1_000 + i,
                    principal: "spiffe://caregraph/ingest".into(),
                    rpc: "AddEdge".into(),
                    request_id: format!("req-{i}"),
                    src: i,
                    dst: i + 1,
                    affected_count: 3,
                    outcome: "committed".into(),
                    commit_seq: i,
                },
            )
            .unwrap();
        store.write(batch).unwrap();
        audit.advance(pending);
    }
}

fn open(path: &Path, key: Option<&[u8]>) -> RocksKv {
    match key {
        Some(k) => RocksKv::open_encrypted(path, k).unwrap(),
        None => RocksKv::open(path).unwrap(),
    }
}

fn all_bytes(dir: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(bytes) = std::fs::read(&p) {
                out.extend(bytes);
            }
        }
    }
    out
}

fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                total += std::fs::metadata(&p).unwrap().len();
            }
        }
    }
    total
}

struct Fixture {
    root: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Fixture {
            root: TempDir::new().unwrap(),
        }
    }
    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }
}

#[test]
fn a_restore_reproduces_exactly_the_state_at_the_backup_point() {
    for key in [None, Some(&KEY_A[..])] {
        let fx = Fixture::new();
        let store = open(&fx.path("db"), key);
        let audit = MutationAudit::open(&store).unwrap();
        commit(&store, &audit, 1..=50);
        let at_backup = dump(&store);

        let manager = BackupManager::open(fx.path("backups"), key).unwrap();
        let info = manager.create(&store).unwrap();
        assert_eq!(info.backup_id, 1);
        assert!(info.sequence > 0 && info.num_files > 0);

        // More commits after the backup must not appear in it.
        commit(&store, &audit, 51..=80);
        assert_ne!(dump(&store), at_backup);

        manager.restore(None, &fx.path("restored")).unwrap();
        let restored = open(&fx.path("restored"), key);
        assert_eq!(
            dump(&restored),
            at_backup,
            "encrypted={}: every column family must equal the backup point",
            key.is_some()
        );
        let summary = verify_store(&restored).unwrap().unwrap();
        assert_eq!(
            (summary.records, summary.head_seq),
            (50, 50),
            "the audit chain is restored whole and still verifies"
        );
    }
}

#[test]
fn backups_are_incremental_and_each_one_restores_its_own_point() {
    let fx = Fixture::new();
    let store = open(&fx.path("db"), Some(&KEY_A));
    let audit = MutationAudit::open(&store).unwrap();
    let manager = BackupManager::open(fx.path("backups"), Some(&KEY_A)).unwrap();

    commit(&store, &audit, 1..=400);
    let first = manager.create(&store).unwrap();
    let after_first = dir_size(&fx.path("backups"));
    let at_first = dump(&store);

    commit(&store, &audit, 401..=440);
    let second = manager.create(&store).unwrap();
    let after_second = dir_size(&fx.path("backups"));

    assert!(second.sequence > first.sequence);
    assert_eq!(
        manager
            .list()
            .iter()
            .map(|b| b.backup_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(
        after_second - after_first < after_first,
        "adding 10% more data grew the backup directory by {} bytes on top of {after_first}; \
         a full second copy would have roughly doubled it",
        after_second - after_first
    );

    manager.restore(Some(1), &fx.path("r1")).unwrap();
    assert_eq!(dump(&open(&fx.path("r1"), Some(&KEY_A))), at_first);
    manager.restore(Some(2), &fx.path("r2")).unwrap();
    assert_eq!(dump(&open(&fx.path("r2"), Some(&KEY_A))), dump(&store));

    manager.purge(1).unwrap();
    assert_eq!(manager.list().len(), 1);
    assert_eq!(manager.list()[0].backup_id, 2);
    manager.restore(None, &fx.path("r3")).unwrap();
    assert_eq!(dump(&open(&fx.path("r3"), Some(&KEY_A))), dump(&store));
}

#[test]
fn a_healthy_backup_verifies_and_reports_what_it_contains() {
    let fx = Fixture::new();
    let store = open(&fx.path("db"), Some(&KEY_A));
    let audit = MutationAudit::open(&store).unwrap();
    commit(&store, &audit, 1..=25);
    let manager = BackupManager::open(fx.path("backups"), Some(&KEY_A)).unwrap();
    let id = manager.create(&store).unwrap().backup_id;

    let report = manager.verify(id, &fx.path("scratch")).unwrap();
    assert_eq!(report.database.audit_records, 25);
    assert_eq!(report.database.total_keys(), 25 * 3 + 25 + 1);
    assert!(
        !fx.path("scratch").exists(),
        "the scratch restore is removed afterwards"
    );
}

/// The engine's own check passes a backup whose bytes were flipped (it only
/// compares file sizes); ours must not.
#[test]
fn a_backup_with_a_flipped_byte_fails_verification() {
    let fx = Fixture::new();
    let store = open(&fx.path("db"), Some(&KEY_A));
    let audit = MutationAudit::open(&store).unwrap();
    commit(&store, &audit, 1..=200);
    let manager = BackupManager::open(fx.path("backups"), Some(&KEY_A)).unwrap();
    let id = manager.create(&store).unwrap().backup_id;
    assert!(manager.verify(id, &fx.path("scratch-ok")).is_ok());

    // Corrupt the largest backed-up table file in place, keeping its length.
    let mut biggest: Option<(u64, PathBuf)> = None;
    let mut stack = vec![fx.path("backups")];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "sst")
                || p.to_string_lossy().contains(".sst")
            {
                let len = std::fs::metadata(&p).unwrap().len();
                if biggest.as_ref().map_or(true, |(l, _)| len > *l) {
                    biggest = Some((len, p));
                }
            }
        }
    }
    let (len, victim) = biggest.expect("the backup contains table files");
    let mut bytes = std::fs::read(&victim).unwrap();
    let at = (len / 2) as usize;
    bytes[at] ^= 0xff;
    std::fs::write(&victim, bytes).unwrap();

    let verdict = manager.verify(id, &fx.path("scratch-bad"));
    assert!(verdict.is_err(), "a corrupted backup must not verify");
    assert!(!fx.path("scratch-bad").exists());
}

#[test]
fn encrypted_backups_hide_the_data_and_need_the_same_key() {
    // Control: the same data in a plaintext backup is visible, so the encrypted
    // case's "not found" means something.
    let plain = Fixture::new();
    let store = open(&plain.path("db"), None);
    let audit = MutationAudit::open(&store).unwrap();
    commit(&store, &audit, 1..=20);
    BackupManager::open(plain.path("backups"), None)
        .unwrap()
        .create(&store)
        .unwrap();
    let raw = all_bytes(&plain.path("backups"));
    assert!(
        raw.windows(MARKER.len()).any(|w| w == MARKER),
        "control: a plaintext backup shows the marker"
    );

    let fx = Fixture::new();
    let store = open(&fx.path("db"), Some(&KEY_A));
    let audit = MutationAudit::open(&store).unwrap();
    commit(&store, &audit, 1..=20);
    let manager = BackupManager::open(fx.path("backups"), Some(&KEY_A)).unwrap();
    manager.create(&store).unwrap();
    let raw = all_bytes(&fx.path("backups"));
    assert!(
        !raw.windows(MARKER.len()).any(|w| w == MARKER),
        "an encrypted database must produce an encrypted backup"
    );
    assert!(
        !raw.windows(b"AddEdge".len()).any(|w| w == b"AddEdge"),
        "audit records in the backup are ciphertext too"
    );
    drop(manager);

    for (wrong, why) in [
        (Some(&KEY_B[..]), "another key"),
        (
            None,
            "no key (would write plaintext into an encrypted directory)",
        ),
    ] {
        let err = BackupManager::open(fx.path("backups"), wrong)
            .err()
            .unwrap_or_else(|| panic!("opening with {why} must fail"));
        assert!(err.to_string().contains("key"), "{why}: {err}");
    }

    let manager = BackupManager::open(fx.path("backups"), Some(&KEY_A)).unwrap();
    manager.restore(None, &fx.path("restored")).unwrap();
    let restored = open(&fx.path("restored"), Some(&KEY_A));
    assert_eq!(
        restored.get(cf::CF_NODES, &7u64.to_be_bytes()).unwrap(),
        Some(MARKER.to_vec())
    );
}

#[test]
fn restore_never_overwrites_an_existing_database() {
    let fx = Fixture::new();
    let store = open(&fx.path("db"), None);
    let audit = MutationAudit::open(&store).unwrap();
    commit(&store, &audit, 1..=5);
    let manager = BackupManager::open(fx.path("backups"), None).unwrap();
    manager.create(&store).unwrap();
    drop(store);

    let err = manager.restore(None, &fx.path("db")).unwrap_err();
    assert!(err.to_string().contains("not empty"), "{err}");
    // An empty existing directory is fine.
    std::fs::create_dir_all(fx.path("empty")).unwrap();
    manager.restore(None, &fx.path("empty")).unwrap();
}

#[test]
fn an_empty_backup_directory_has_nothing_to_restore() {
    let fx = Fixture::new();
    let manager = BackupManager::open(fx.path("backups"), None).unwrap();
    assert!(manager.list().is_empty());
    assert!(manager.restore(None, &fx.path("out")).is_err());
}

#[test]
fn a_checkpoint_is_a_consistent_openable_copy_and_is_not_overwritten() {
    let fx = Fixture::new();
    let store = open(&fx.path("db"), Some(&KEY_A));
    let audit = MutationAudit::open(&store).unwrap();
    commit(&store, &audit, 1..=60);
    let before = dump(&store);

    let seq = create_checkpoint(&store, &fx.path("ckpt")).unwrap();
    assert_eq!(seq, store.raw().latest_sequence_number());
    commit(&store, &audit, 61..=70);

    let copy = open(&fx.path("ckpt"), Some(&KEY_A));
    assert_eq!(
        dump(&copy),
        before,
        "the checkpoint is the state at its sequence"
    );
    assert!(create_checkpoint(&store, &fx.path("ckpt")).is_err());
}

#[test]
fn re_encrypting_yields_an_equal_database_that_only_the_new_key_opens() {
    let fx = Fixture::new();
    {
        let store = open(&fx.path("old"), Some(&KEY_A));
        let audit = MutationAudit::open(&store).unwrap();
        commit(&store, &audit, 1..=120);
    }
    let report = reencrypt(&fx.path("old"), Some(&KEY_A), &fx.path("new"), Some(&KEY_B)).unwrap();
    assert_eq!(report.audit_records, 120);

    let old = open(&fx.path("old"), Some(&KEY_A));
    let new = open(&fx.path("new"), Some(&KEY_B));
    assert_eq!(dump(&old), dump(&new));
    drop((old, new));

    let raw = all_bytes(&fx.path("new"));
    assert!(!raw.windows(MARKER.len()).any(|w| w == MARKER));
    let opens_with_old_key = RocksKv::open_encrypted(fx.path("new"), &KEY_A)
        .ok()
        .is_some_and(|s| check_database(&s).is_ok());
    assert!(
        !opens_with_old_key,
        "the old key must not read the new database"
    );

    assert!(
        reencrypt(&fx.path("old"), Some(&KEY_A), &fx.path("new"), Some(&KEY_B)).is_err(),
        "never into an existing directory"
    );
}
