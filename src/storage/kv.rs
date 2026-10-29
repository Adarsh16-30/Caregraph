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

/// Callback signature shared by [`KvStore::scan_from`] and its variants —
/// named so the signatures below read as one screen-width line rather than
/// tripping clippy's `type_complexity` lint on the inlined closure type.
pub type ScanVisitor<'a> = dyn FnMut(&[u8], &[u8]) -> ControlFlow<()> + 'a;

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
    ///
    /// `prefix` and `seek_key` must be at least as wide as the column family's
    /// configured fixed-prefix extractor (`storage::cf::prefix_len`) — `CF_EDGES`
    /// and `CF_REVERSE` are tuned to `EDGE_PREFIX_LEN` (`[src_id | edge_type]`).
    /// A narrower prefix needs [`scan_from_narrow_prefix`](Self::scan_from_narrow_prefix).
    fn scan_from(
        &self,
        cf: &'static str,
        prefix: &[u8],
        seek_key: &[u8],
        visit: &mut ScanVisitor<'_>,
    ) -> Result<()>;

    /// Like [`scan_from`](Self::scan_from), for a `prefix` *narrower* than the
    /// column family's configured fixed-prefix extractor width — e.g. seeking
    /// by `[src_id]` alone (8 bytes) against `CF_EDGES`, whose extractor is
    /// fixed at `EDGE_PREFIX_LEN` (10 bytes, `[src_id | edge_type]`).
    ///
    /// `scan_from`'s `prefix_same_as_start` read option asks RocksDB to bound
    /// the iterator using the extractor's output on the seek key. Handing it a
    /// key shorter than the extractor's width leaves RocksDB unable to derive
    /// that bound, and the iterator silently returns nothing — found the hard
    /// way by [`TemporalIndex::all_edges_as_of_limited`](crate::temporal::TemporalIndex::all_edges_as_of_limited),
    /// which needs exactly this narrower scan to fetch a node's adjacency
    /// across every edge type in one call instead of one call per type. This
    /// bypasses the extractor-based bound and relies solely on the manual
    /// `key.starts_with(prefix)` check every `scan_from` implementation
    /// already performs as a safety net.
    ///
    /// Default: forwards to `scan_from`, correct for any implementation whose
    /// column families are not tuned with a wider fixed-prefix extractor.
    fn scan_from_narrow_prefix(
        &self,
        cf: &'static str,
        prefix: &[u8],
        seek_key: &[u8],
        visit: &mut ScanVisitor<'_>,
    ) -> Result<()> {
        self.scan_from(cf, prefix, seek_key, visit)
    }

    /// Forces the write-ahead log to stable storage.
    fn flush_wal(&self) -> Result<()> {
        Ok(())
    }

    /// The store's node-type cache, if it keeps one (see
    /// [`NodeTypeCache`](crate::storage::node_types::NodeTypeCache)).
    fn node_type_cache(&self) -> Option<&crate::storage::node_types::NodeTypeCache> {
        None
    }

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
    sync_writes: std::sync::atomic::AtomicBool,
    node_types: crate::storage::node_types::NodeTypeCache,
    /// Latest RocksDB sequence number, published after every write so a
    /// replication stream can wait for new data instead of polling.
    commits: tokio::sync::watch::Sender<u64>,
    /// The most recent batches, as written, for replication streams that are
    /// keeping up. Reading them back from the write-ahead log costs a directory
    /// scan and a file seek per request (tens of milliseconds on the real
    /// database); a follower that is current should not pay that on every write.
    /// The log remains the source for catching up.
    recent: std::sync::Mutex<RecentBatches>,
    /// Serialises writes so the tail records exact sequence ranges.
    write_order: std::sync::Mutex<()>,
    cf_ids: std::sync::OnceLock<std::collections::BTreeMap<String, u32>>,
}

/// Upper bounds on the in-memory tail of recent batches.
const RECENT_MAX_BYTES: usize = 32 << 20;
const RECENT_MAX_BATCHES: usize = 8192;

/// A batch as written: the sequence number of its first operation, its operation
/// count and its serialized bytes.
#[derive(Clone)]
pub struct RecentBatch {
    pub first_seq: u64,
    pub count: u32,
    pub data: Arc<Vec<u8>>,
}

#[derive(Default)]
struct RecentBatches {
    entries: std::collections::VecDeque<RecentBatch>,
    bytes: usize,
}

/// How long and how much write-ahead log to keep after it has been flushed to
/// table files. A follower can resume from any sequence still in the log; one
/// that falls further behind must be re-bootstrapped. Zero keeps nothing (the
/// default, and what a standalone server wants).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalRetention {
    pub ttl_seconds: u64,
    pub size_limit_mb: u64,
}

impl RocksKv {
    /// Open (creating if absent) a CareGraph database at `path`, unencrypted.
    ///
    /// This is what every test, benchmark, and dev-mode invocation uses —
    /// forcing an encryption key onto every `RocksKv::open` call in the
    /// codebase would be a much bigger, unrelated change. Production callers
    /// that want Rule 8's real encryption at rest use
    /// [`RocksKv::open_encrypted`] instead; see `main.rs`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, None, WalRetention::default())
    }

    /// Open (creating if absent) a CareGraph database at `path` with every
    /// file RocksDB writes — SST files, WAL, MANIFEST — encrypted on disk
    /// under AES-256 in CTR mode (PRD Section 0, Rule 8; see
    /// `src/storage/encryption.rs`).
    pub fn open_encrypted(path: impl AsRef<Path>, key: &[u8]) -> Result<Self> {
        Self::open_with(path, Some(key), WalRetention::default())
    }

    /// The general constructor: optional encryption key and WAL retention.
    pub fn open_with(
        path: impl AsRef<Path>,
        key: Option<&[u8]>,
        retention: WalRetention,
    ) -> Result<Self> {
        let mut opts = cf::db_options();
        // Keep the Env alive until the database is open: it must outlive the
        // options that reference it.
        let env = key
            .map(crate::storage::encryption::open_encrypted_env)
            .transpose()?;
        if let Some(env) = &env {
            opts.set_env(env);
        }
        if retention.ttl_seconds > 0 {
            opts.set_wal_ttl_seconds(retention.ttl_seconds);
        }
        if retention.size_limit_mb > 0 {
            opts.set_wal_size_limit_mb(retention.size_limit_mb);
        }
        let db = Db::open_cf_descriptors(&opts, path, cf::descriptors())?;
        let latest = db.latest_sequence_number();
        Ok(RocksKv {
            db,
            sync_writes: std::sync::atomic::AtomicBool::new(false),
            node_types: Default::default(),
            commits: tokio::sync::watch::channel(latest).0,
            recent: std::sync::Mutex::new(RecentBatches::default()),
            write_order: std::sync::Mutex::new(()),
            cf_ids: std::sync::OnceLock::new(),
        })
    }

    /// The newest RocksDB sequence number: every write batch consumes one
    /// number per operation, so this is also a count of operations ever applied.
    pub fn latest_sequence(&self) -> u64 {
        self.db.latest_sequence_number()
    }

    /// Notifies on every write with the new latest sequence number.
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.commits.subscribe()
    }

    /// Column-family name → numeric id, as this database numbers them. A
    /// replicated batch refers to families by id, so two databases can exchange
    /// batches only if these maps are equal.
    pub fn cf_ids(&self) -> Result<&std::collections::BTreeMap<String, u32>> {
        if let Some(map) = self.cf_ids.get() {
            return Ok(map);
        }
        let map = crate::replication::batch_decode::cf_id_map(self)?;
        Ok(self.cf_ids.get_or_init(|| map))
    }

    /// Batches from the in-memory tail starting exactly at sequence `from`, in
    /// order and contiguous, up to the limits. `None` if the tail does not hold
    /// `from` (it is older than the tail, or newer than anything written), in
    /// which case the caller reads the write-ahead log instead.
    pub fn recent_since(
        &self,
        from: u64,
        max_batches: usize,
        max_bytes: usize,
    ) -> Option<Vec<RecentBatch>> {
        let recent = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        let start = recent.entries.partition_point(|b| b.first_seq < from);
        if recent.entries.get(start)?.first_seq != from {
            return None;
        }
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut expected = from;
        for b in recent.entries.iter().skip(start) {
            if b.first_seq != expected {
                break;
            }
            bytes += b.data.len();
            expected = b.first_seq + u64::from(b.count);
            out.push(b.clone());
            if out.len() >= max_batches || bytes >= max_bytes {
                break;
            }
        }
        Some(out)
    }

    /// The batches written after sequence `after`, oldest first. An error
    /// means the log no longer reaches back that far.
    pub fn updates_since(&self, after: u64) -> Result<rocksdb::DBWALIterator> {
        Ok(self.db.get_updates_since(after)?)
    }

    /// Applies one batch received from a leader. The batch must be the very next
    /// one: it must start at `latest + 1`, and afterwards the sequence number
    /// must have advanced by exactly its operation count, as it did on the
    /// leader. Anything else means this database has diverged from the leader's
    /// history and is reported as [`CareGraphError::Diverged`] without applying
    /// (or, if it cannot be detected beforehand, after).
    pub fn apply_replicated(&self, first_seq: u64, count: u32, data: &[u8]) -> Result<()> {
        let before = self.latest_sequence();
        if before + 1 != first_seq {
            return Err(CareGraphError::Diverged(format!(
                "this database is at sequence {before} but the leader sent a batch starting at {first_seq}"
            )));
        }
        let batch = WriteBatch::from_data(data);
        if batch.len() != count as usize {
            return Err(CareGraphError::Diverged(format!(
                "a batch announced {count} operations but holds {}",
                batch.len()
            )));
        }
        let touches_nodes = crate::replication::batch_decode::touched_cfs(data)
            .contains(&self.cf_ids()?[cf::CF_NODES]);
        self.write(batch)?;
        let after = self.latest_sequence();
        if after != first_seq + u64::from(count) - 1 {
            return Err(CareGraphError::Diverged(format!(
                "applying {count} operations at {first_seq} left the sequence at {after}"
            )));
        }
        if touches_nodes {
            self.node_types.clear();
        }
        Ok(())
    }

    /// fsync the WAL on every `write` (durable against OS crash / power loss,
    /// not just a process kill). Off by default for dev/tests; production
    /// config requires it.
    pub fn set_sync_writes(&self, on: bool) {
        self.sync_writes
            .store(on, std::sync::atomic::Ordering::Relaxed);
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

    /// Read options for [`KvStore::scan_from_narrow_prefix`]. Bypasses the
    /// column family's fixed-prefix bloom filter entirely — it cannot be used
    /// correctly on a shorter-than-configured prefix, so there is nothing to
    /// opt into — and relies purely on the manual `starts_with` boundary check
    /// in `scan_from_with_opts`'s loop body, same as `prefix_read_opts` does.
    fn narrow_prefix_read_opts() -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_total_order_seek(true);
        opts
    }

    /// Shared loop for [`KvStore::scan_from`] and
    /// [`KvStore::scan_from_narrow_prefix`] — they differ only in which read
    /// options make the RocksDB-side bound (or lack of one) correct for the
    /// width of `prefix`; the manual `starts_with` boundary check is identical
    /// either way and is what actually enforces correctness.
    fn scan_from_with_opts(
        &self,
        cf: &'static str,
        prefix: &[u8],
        seek_key: &[u8],
        read_opts: ReadOptions,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        let iter = self.db.iterator_cf_opt(
            &handle,
            read_opts,
            IteratorMode::From(seek_key, Direction::Forward),
        );

        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            if visit(&key, &value).is_break() {
                break;
            }
        }
        Ok(())
    }

    /// Permanently remove the database files at `path`.
    pub fn destroy(path: impl AsRef<Path>) -> Result<()> {
        Db::destroy(&cf::db_options(), path)?;
        Ok(())
    }
}

impl KvStore for RocksKv {
    fn flush_wal(&self) -> Result<()> {
        self.db.flush_wal(true)?;
        Ok(())
    }

    fn node_type_cache(&self) -> Option<&crate::storage::node_types::NodeTypeCache> {
        Some(&self.node_types)
    }

    fn put(&self, cf: &'static str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&handle, key, value);
        self.write(batch)
    }

    fn get(&self, cf: &'static str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = self.cf_handle(cf)?;
        Ok(self.db.get_cf(&handle, key)?)
    }

    fn delete(&self, cf: &'static str, key: &[u8]) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(&handle, key);
        self.write(batch)
    }

    fn write(&self, batch: WriteBatch) -> Result<()> {
        // One writer at a time through here, so the sequence numbers a batch
        // occupied can be read off exactly (after - count + 1 ..= after): the
        // in-memory tail is only correct if no other write lands in between.
        let _ordered = self.write_order.lock().unwrap_or_else(|e| e.into_inner());
        let before = self.db.latest_sequence_number();
        let count = batch.len();
        let data = (count > 0).then(|| Arc::new(batch.data().to_vec()));
        if self.sync_writes.load(std::sync::atomic::Ordering::Relaxed) {
            let mut wo = rocksdb::WriteOptions::default();
            wo.set_sync(true);
            self.db.write_opt(batch, &wo)?;
        } else {
            self.db.write(batch)?;
        }
        let after = self.db.latest_sequence_number();
        if let Some(data) = data {
            let mut recent = self.recent.lock().unwrap_or_else(|e| e.into_inner());
            if after == before + count as u64 {
                // RocksDB stamps the batch with its sequence number as it writes
                // it to the log (the first eight bytes, little-endian); do the
                // same so the tail is byte-identical to what the log holds.
                let mut stamped = data.as_ref().clone();
                stamped[..8].copy_from_slice(&(before + 1).to_le_bytes());
                let data = Arc::new(stamped);
                recent.bytes += data.len();
                recent.entries.push_back(RecentBatch {
                    first_seq: before + 1,
                    count: count as u32,
                    data,
                });
                while recent.bytes > RECENT_MAX_BYTES || recent.entries.len() > RECENT_MAX_BATCHES {
                    match recent.entries.pop_front() {
                        Some(old) => recent.bytes -= old.data.len(),
                        None => break,
                    }
                }
            } else {
                // Something else wrote in between (it should not happen: every
                // write comes through here); the tail would have a hole.
                recent.entries.clear();
                recent.bytes = 0;
            }
        }
        self.commits.send_replace(after);
        Ok(())
    }

    fn scan_from(
        &self,
        cf: &'static str,
        prefix: &[u8],
        seek_key: &[u8],
        visit: &mut ScanVisitor<'_>,
    ) -> Result<()> {
        self.scan_from_with_opts(cf, prefix, seek_key, Self::prefix_read_opts(), visit)
    }

    fn scan_from_narrow_prefix(
        &self,
        cf: &'static str,
        prefix: &[u8],
        seek_key: &[u8],
        visit: &mut ScanVisitor<'_>,
    ) -> Result<()> {
        self.scan_from_with_opts(cf, prefix, seek_key, Self::narrow_prefix_read_opts(), visit)
    }

    fn flush(&self, cf: &'static str) -> Result<()> {
        let handle = self.cf_handle(cf)?;
        self.db.flush_cf(&handle)?;
        Ok(())
    }
}
