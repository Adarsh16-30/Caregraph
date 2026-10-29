//! Backup and restore.
//!
//! RocksDB's `BackupEngine` takes incremental backups (unchanged SST files are
//! shared between backups), and a restore reproduces the database exactly as it
//! was when the backup was taken: every column family, including the audit
//! chain, comes back together because they were captured as one consistent
//! point.
//!
//! # Encryption
//!
//! The engine is opened with the same encrypted environment the database uses,
//! so backup files are ciphertext under the same data key. Opening it with a
//! plain environment would write a *plaintext* copy of an encrypted database, so
//! [`BackupManager::open`] takes the key (or its absence) as a required argument
//! and records a fingerprint of it beside the backups: a directory created under
//! one key cannot be opened with another, and an encrypted directory cannot be
//! opened as plaintext. Restoring needs the same data key. The key itself is not
//! in the backup; keep the wrapped key and the key-encryption key somewhere the
//! backup storage cannot reach.
//!
//! # What "verified" means
//!
//! The engine's own `verify_backup` only checks that each file exists with the
//! expected size, which a flipped byte passes. [`BackupManager::verify`] goes
//! further: it restores the backup to a scratch directory, opens it, reads every
//! key of every column family (RocksDB checks block checksums on read) and
//! verifies the audit chain.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use rocksdb::checkpoint::Checkpoint;
use rocksdb::Env;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{CareGraphError, Result};
use crate::storage::maintenance::{self, DatabaseCheck};
use crate::storage::{open_encrypted_env, RocksKv};

const META_FILE: &str = "caregraph_backup_meta.json";
const META_FORMAT: u32 = 1;

/// Fingerprint of a data key: the first eight bytes of a domain-separated
/// SHA-256. Stored in the clear beside backups; reveals nothing usable about
/// the key.
pub fn key_fingerprint(key: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(b"caregraph-backup-key-id");
    h.update(key);
    h.finalize()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupInfo {
    pub backup_id: u32,
    pub created_unix: i64,
    pub size_bytes: u64,
    pub num_files: u32,
    /// RocksDB sequence number captured by the backup (0 if unknown, e.g. a
    /// backup made by another tool).
    pub sequence: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Meta {
    format: u32,
    /// `None`: the directory holds plaintext backups.
    key_id: Option<String>,
    /// backup id → sequence number at the time it was taken.
    sequences: std::collections::BTreeMap<u32, u64>,
}

/// What [`BackupManager::verify`] found.
#[derive(Debug)]
pub struct VerifyReport {
    pub backup_id: u32,
    pub database: DatabaseCheck,
}

pub struct BackupManager {
    dir: PathBuf,
    /// Keeps the encrypted environment alive for as long as the engine.
    _env: Env,
    key: Option<crate::security::keys::SecretKey>,
    engine: Mutex<BackupEngine>,
    meta: Mutex<Meta>,
}

impl BackupManager {
    /// Opens (creating if absent) the backup directory `dir`. `key` is the data
    /// key of the database being backed up, or `None` for an unencrypted one.
    pub fn open(dir: impl AsRef<Path>, key: Option<&[u8]>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let wanted = key.map(key_fingerprint);
        let meta_path = dir.join(META_FILE);
        let mut meta = match std::fs::read(&meta_path) {
            Ok(bytes) => serde_json::from_slice::<Meta>(&bytes)
                .map_err(|e| backup_err(format!("reading {}: {e}", meta_path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Meta {
                format: META_FORMAT,
                key_id: wanted.clone(),
                ..Meta::default()
            },
            Err(e) => return Err(e.into()),
        };
        if meta.key_id != wanted {
            return Err(backup_err(match (&meta.key_id, &wanted) {
                (Some(_), None) => {
                    "this backup directory is encrypted; a data key is required".to_string()
                }
                (None, Some(_)) => {
                    "this backup directory holds plaintext backups; refusing to mix in an \
                         encrypted database"
                        .to_string()
                }
                _ => "this backup directory was created under a different data key".to_string(),
            }));
        }
        meta.format = META_FORMAT;

        let env = match key {
            Some(k) => open_encrypted_env(k)?,
            None => Env::new().map_err(|e| backup_err(e.to_string()))?,
        };
        let opts = BackupEngineOptions::new(&dir).map_err(|e| backup_err(e.to_string()))?;
        let engine = BackupEngine::open(&opts, &env).map_err(|e| backup_err(e.to_string()))?;

        let mgr = BackupManager {
            dir,
            _env: env,
            key: key.map(|k| {
                let mut copy = zeroize::Zeroizing::new([0u8; crate::storage::KEY_LEN]);
                copy.copy_from_slice(k);
                copy
            }),
            engine: Mutex::new(engine),
            meta: Mutex::new(meta),
        };
        mgr.save_meta()?;
        Ok(mgr)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn save_meta(&self) -> Result<()> {
        let meta = self.meta.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = self.dir.join(format!("{META_FILE}.tmp"));
        std::fs::write(&tmp, serde_json::to_vec_pretty(&*meta)?)?;
        std::fs::rename(&tmp, self.dir.join(META_FILE))?;
        Ok(())
    }

    /// Takes a new backup of `store`. Memtables are flushed first so the backup
    /// does not depend on write-ahead-log files.
    pub fn create(&self, store: &RocksKv) -> Result<BackupInfo> {
        let sequence = store.raw().latest_sequence_number();
        let mut engine = self.engine.lock().unwrap_or_else(|e| e.into_inner());
        engine
            .create_new_backup_flush(store.raw(), true)
            .map_err(|e| backup_err(e.to_string()))?;
        let newest = engine
            .get_backup_info()
            .into_iter()
            .max_by_key(|i| i.backup_id)
            .ok_or_else(|| backup_err("the engine reports no backup after creating one".into()))?;
        drop(engine);

        self.meta
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sequences
            .insert(newest.backup_id, sequence);
        self.save_meta()?;
        Ok(BackupInfo {
            backup_id: newest.backup_id,
            created_unix: newest.timestamp,
            size_bytes: newest.size,
            num_files: newest.num_files,
            sequence,
        })
    }

    pub fn list(&self) -> Vec<BackupInfo> {
        let engine = self.engine.lock().unwrap_or_else(|e| e.into_inner());
        let meta = self.meta.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<BackupInfo> = engine
            .get_backup_info()
            .into_iter()
            .map(|i| BackupInfo {
                backup_id: i.backup_id,
                created_unix: i.timestamp,
                size_bytes: i.size,
                num_files: i.num_files,
                sequence: meta.sequences.get(&i.backup_id).copied().unwrap_or(0),
            })
            .collect();
        out.sort_by_key(|i| i.backup_id);
        out
    }

    /// Keeps the newest `keep` backups, deleting the rest and any file only they
    /// referenced.
    pub fn purge(&self, keep: usize) -> Result<()> {
        let mut engine = self.engine.lock().unwrap_or_else(|e| e.into_inner());
        engine
            .purge_old_backups(keep)
            .map_err(|e| backup_err(e.to_string()))?;
        let live: std::collections::BTreeSet<u32> = engine
            .get_backup_info()
            .into_iter()
            .map(|i| i.backup_id)
            .collect();
        drop(engine);
        self.meta
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sequences
            .retain(|id, _| live.contains(id));
        self.save_meta()
    }

    /// Restores backup `id` (the newest if `None`) into `target`, which must not
    /// exist or must be empty: a restore never overwrites a database.
    pub fn restore(&self, id: Option<u32>, target: &Path) -> Result<u32> {
        if target.exists() && std::fs::read_dir(target)?.next().is_some() {
            return Err(backup_err(format!(
                "{} is not empty; restore into a new directory",
                target.display()
            )));
        }
        std::fs::create_dir_all(target)?;
        let id = match id {
            Some(id) => id,
            None => self
                .list()
                .last()
                .map(|i| i.backup_id)
                .ok_or_else(|| backup_err("there are no backups to restore".into()))?,
        };
        let mut engine = self.engine.lock().unwrap_or_else(|e| e.into_inner());
        engine
            .restore_from_backup(target, target, &RestoreOptions::default(), id)
            .map_err(|e| backup_err(format!("restoring backup {id}: {e}")))?;
        Ok(id)
    }

    /// Restores backup `id` into `scratch`, opens it, reads every key of every
    /// column family and verifies the audit chain. `scratch` is removed
    /// afterwards, whether or not the checks pass.
    pub fn verify(&self, id: u32, scratch: &Path) -> Result<VerifyReport> {
        {
            let engine = self.engine.lock().unwrap_or_else(|e| e.into_inner());
            engine
                .verify_backup(id)
                .map_err(|e| backup_err(format!("backup {id} is missing files: {e}")))?;
        }
        let result = (|| {
            self.restore(Some(id), scratch)?;
            let store = match &self.key {
                Some(k) => RocksKv::open_encrypted(scratch, k.as_ref())?,
                None => RocksKv::open(scratch)?,
            };
            let database = maintenance::check_database(&store)?;
            Ok(VerifyReport {
                backup_id: id,
                database,
            })
        })();
        remove_dir_fully(scratch);
        result
    }
}

/// Removes `dir`, retrying: after a failed restore the engine's copy threads can
/// still be finishing a file when the first removal runs, and Windows will not
/// delete a file that is open.
fn remove_dir_fully(dir: &Path) {
    for _ in 0..40 {
        let _ = std::fs::remove_dir_all(dir);
        if !dir.exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    tracing::warn!(dir = %dir.display(), "could not remove the verification scratch directory");
}

/// A point-in-time copy of the database made of hard links. Cheap (no data is
/// copied), consistent, and on the same filesystem as `store`. The copy is
/// encrypted under the same key as the database it came from.
pub fn create_checkpoint(store: &RocksKv, target: &Path) -> Result<u64> {
    if target.exists() {
        return Err(backup_err(format!("{} already exists", target.display())));
    }
    let sequence = store.raw().latest_sequence_number();
    Checkpoint::new(store.raw())
        .and_then(|c| c.create_checkpoint(target))
        .map_err(|e| backup_err(e.to_string()))?;
    Ok(sequence)
}

/// Whether `name` is acceptable as a checkpoint name: a plain file name, no
/// separators, so a caller can never choose where the server writes.
pub fn valid_checkpoint_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && !name.starts_with('.')
}

fn backup_err(msg: String) -> CareGraphError {
    CareGraphError::Io(std::io::Error::other(format!("backup: {msg}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_names_cannot_escape_their_directory() {
        for ok in ["nightly", "before-upgrade_2", "a.b"] {
            assert!(valid_checkpoint_name(ok), "{ok}");
        }
        for bad in [
            "",
            ".",
            "..",
            ".hidden",
            "a/b",
            "a\\b",
            "../x",
            "a b",
            &"x".repeat(65),
        ] {
            assert!(!valid_checkpoint_name(bad), "{bad}");
        }
    }

    #[test]
    fn fingerprints_differ_per_key_and_are_stable() {
        assert_eq!(key_fingerprint(&[1; 32]), key_fingerprint(&[1; 32]));
        assert_ne!(key_fingerprint(&[1; 32]), key_fingerprint(&[2; 32]));
        assert_eq!(key_fingerprint(&[1; 32]).len(), 16);
    }
}
