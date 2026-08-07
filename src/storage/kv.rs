//! Layer 1 — the key-value abstraction over RocksDB (PRD 3.1, Phase 1).
//!
//! # Rule 1 boundary
//!
//! [`KvStore`] exists so the layers above it can be written against a narrow,
//! documented surface — *not* so that the storage engine can be swapped for a
//! test double. [`RocksKv`] is the only permissible implementation. An
//! in-memory `HashMap` implementation of this trait would satisfy the compiler
//! and violate Rule 1; `scripts/check_rules.sh` greps for exactly that.
//!
//! Synthetic data is confined to `*_synthetic_test.rs`; every other code path,
//! including integration tests, runs against a real RocksDB instance on disk.

use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions,
    WriteBatch,
};

use crate::error::{CareGraphError, Result};
use crate::storage::cf;

/// The concrete RocksDB handle. Multi-threaded column-family mode lets several
/// tokio worker threads hold CF handles concurrently without external locking.
pub type Db = DBWithThreadMode<MultiThreaded>;

/// One key-value pair as returned by a scan.
pub type Entry = (Vec<u8>, Vec<u8>);

/// The storage surface every higher layer is written against.
pub trait KvStore: Send + Sync {
    fn put(&self, cf: &'static str, key: &[u8], value: &[u8]) -> Result<()>;

    fn get(&self, cf: &'static str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn delete(&self, cf: &'static str, key: &[u8]) -> Result<()>;

    /// Commit a batch atomically. This is the single write path for
    /// mutation-plus-embedding commits (Rule 5).
    fn write(&self, batch: WriteBatch) -> Result<()>;

    /// Walk entries from `seek_key` forward, stopping at the end of `prefix` or
    /// when `visit` breaks.
    ///
    /// This is the primitive every temporal read is built from. It hands
    /// borrowed slices to the callback rather than returning a `Vec` so that a
    /// caller which only needs the first few versions of a long history does
    /// not pay to materialize the whole thing.
    ///
    /// Because timestamps are stored inverted, walking *forward* from
    /// `as_of_prefix(prefix, t)` walks *backward* through time, newest first.
    fn scan_from(
        &self,
        cf: &'static str,
        prefix: &[u8],
        seek_key: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> ControlFlow<()>,
    ) -> Result<()>;

    /// Seek to the first entry at or after `seek_key`, stopping if the entry
    /// falls outside `prefix`.
    ///
    /// With CareGraph's inverted-timestamp encoding this single call *is* the
    /// point-in-time read: seek to `as_of_prefix(prefix, as_of)` and the first
    /// hit is the newest version at or before `as_of` (Contribution 2).
    fn seek(&self, cf: &'static str, prefix: &[u8], seek_key: &[u8]) -> Result<Option<Entry>> {
        let mut found = None;
        self.scan_from(cf, prefix, seek_key, &mut |key, value| {
            found = Some((key.to_vec(), value.to_vec()));
            ControlFlow::Break(())
        })?;
        Ok(found)
    }

    /// Every entry sharing `prefix`, in key order — newest version first.
    fn scan_prefix(&self, cf: &'static str, prefix: &[u8]) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        self.scan_from(cf, prefix, prefix, &mut |key, value| {
            out.push((key.to_vec(), value.to_vec()));
            ControlFlow::Continue(())
        })?;
        Ok(out)
    }

    /// Flush memtables to SST files. Used by the encryption-at-rest
    /// verification test, which reads SST files directly (Rule 8).
    fn flush(&self, cf: &'static str) -> Result<()>;
}

/// A real, on-disk RocksDB instance with CareGraph's four column families open.
pub struct RocksKv {
    db: Db,
}

impl RocksKv {
    /// Open (creating if absent) a CareGraph database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Db::open_cf_descriptors(&cf::db_options(), path, cf::descriptors())?;
        Ok(RocksKv { db })
    }

    /// Borrow the raw handle. Needed by `atomic_commit`, which builds a
    /// `WriteBatch` against live CF handles (PRD Section 9.2).
    pub fn raw(&self) -> &Db {
        &self.db
    }

    /// Resolve a column family handle, turning the absence of one into a typed
    /// error rather than a panic — a missing CF means the database was opened
    /// by something other than CareGraph.
    pub fn cf_handle(&self, name: &'static str) -> Result<Arc<BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or(CareGraphError::MissingColumnFamily(name))
    }

    /// Read options scoped to a single prefix, so an iterator stops at the end
    /// of one adjacency list instead of running into the next node's.
    fn prefix_read_opts() -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        opts
    }

    /// Permanently remove the database files at `path`.
    pub fn destroy(path: impl AsRef<Path>) -> Result<()> {
        Db::destroy(&cf::db_options(), path)?;
        Ok(())
    }
}

impl KvStore for RocksKv {
    fn put(&self, cf: &'static str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        self.db.put_cf(&handle, key, value)?;
        Ok(())
    }

    fn get(&self, cf: &'static str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = self.cf_handle(cf)?;
        Ok(self.db.get_cf(&handle, key)?)
    }

    fn delete(&self, cf: &'static str, key: &[u8]) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        self.db.delete_cf(&handle, key)?;
        Ok(())
    }

    fn write(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch)?;
        Ok(())
    }

    fn scan_from(
        &self,
        cf: &'static str,
        prefix: &[u8],
        seek_key: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> ControlFlow<()>,
    ) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        let iter = self.db.iterator_cf_opt(
            &handle,
            Self::prefix_read_opts(),
            IteratorMode::From(seek_key, Direction::Forward),
        );

        for item in iter {
            let (key, value) = item?;
            // `prefix_same_as_start` bounds the iterator to the seek key's
            // extractor output, but the guard is re-checked here: the seek key
            // may be shorter than the extractor width, and a caller may pass a
            // prefix narrower than the one RocksDB derived.
            if !key.starts_with(prefix) {
                break;
            }
            if visit(&key, &value).is_break() {
                break;
            }
        }
        Ok(())
    }

    fn flush(&self, cf: &'static str) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        self.db.flush_cf(&handle)?;
        Ok(())
    }
}
