//! Offline database maintenance: a full read-everything check and a logical
//! copy under a different data key.
//!
//! Both work through [`KvStore`] only, so they exercise the same read path the
//! server does.

use std::ops::ControlFlow;
use std::path::Path;

use rocksdb::WriteBatch;

use crate::error::{CareGraphError, Result};
use crate::security::audit;
use crate::storage::{cf, KvStore, RocksKv};

/// Keys per write batch when copying. Large enough to amortise the write, small
/// enough to bound memory (values are small: edges, vectors).
const COPY_BATCH_KEYS: usize = 10_000;

/// The outcome of [`check_database`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseCheck {
    /// Key count per column family, in [`cf::ALL`] order.
    pub keys: Vec<(&'static str, u64)>,
    /// Records in the mutation audit chain (verified).
    pub audit_records: u64,
    /// Hex head hash of the audit chain (empty if there are no records).
    pub audit_head: String,
}

impl DatabaseCheck {
    pub fn total_keys(&self) -> u64 {
        self.keys.iter().map(|(_, n)| n).sum()
    }
}

/// Reads every key of every column family, so any corrupt block surfaces as an
/// error, then verifies the audit chain against its recorded head.
pub fn check_database(store: &RocksKv) -> Result<DatabaseCheck> {
    let mut keys = Vec::new();
    for name in cf::ALL {
        let mut n = 0u64;
        store.scan_from_narrow_prefix(name, &[], &[], &mut |_, _| {
            n += 1;
            ControlFlow::Continue(())
        })?;
        keys.push((name, n));
    }
    match audit::verify_store(store)? {
        Ok(summary) => Ok(DatabaseCheck {
            keys,
            audit_records: summary.records,
            audit_head: summary.head_hash,
        }),
        Err(e) => Err(CareGraphError::Io(std::io::Error::other(format!(
            "audit chain broken at record {}: {}",
            e.at_seq, e.reason
        )))),
    }
}

/// Copies every key of every column family from `src` into a new database at
/// `dst` encrypted under `dst_key` (`None` for plaintext), then reads `dst` back
/// and compares key counts. `dst` must not exist. This is how a data key is
/// rotated: the copy is the only place the new key is ever used, and the old
/// database is left untouched until the operator swaps them.
///
/// The copy is logical, so RocksDB sequence numbers in `dst` differ from `src`;
/// a database produced this way cannot continue as a replica of the original.
pub fn reencrypt(
    src: &Path,
    src_key: Option<&[u8]>,
    dst: &Path,
    dst_key: Option<&[u8]>,
) -> Result<DatabaseCheck> {
    if dst.exists() {
        return Err(CareGraphError::Io(std::io::Error::other(format!(
            "{} already exists",
            dst.display()
        ))));
    }
    let source = open(src, src_key)?;
    let target = open(dst, dst_key)?;

    for name in cf::ALL {
        let handle = target.cf_handle(name)?;
        let mut batch = WriteBatch::default();
        let mut pending = 0usize;
        let mut failure = None;
        source.scan_from_narrow_prefix(name, &[], &[], &mut |k, v| {
            batch.put_cf(&handle, k, v);
            pending += 1;
            if pending >= COPY_BATCH_KEYS {
                if let Err(e) = target.write(std::mem::take(&mut batch)) {
                    failure = Some(e);
                    return ControlFlow::Break(());
                }
                pending = 0;
            }
            ControlFlow::Continue(())
        })?;
        if let Some(e) = failure {
            return Err(e);
        }
        if pending > 0 {
            target.write(batch)?;
        }
        target.flush(name)?;
    }

    let from = check_database(&source)?;
    let to = check_database(&target)?;
    if from != to {
        return Err(CareGraphError::Io(std::io::Error::other(
            "the copy does not match the source (key counts or audit chain differ)",
        )));
    }
    Ok(to)
}

fn open(path: &Path, key: Option<&[u8]>) -> Result<RocksKv> {
    match key {
        Some(k) => RocksKv::open_encrypted(path, k),
        None => RocksKv::open(path),
    }
}
