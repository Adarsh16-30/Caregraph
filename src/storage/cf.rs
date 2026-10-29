//! Column family definitions — the versioned schema itself (PRD 3.3).
//!
//! | CF                | Key                                              | Value |
//! |-------------------|--------------------------------------------------|-------|
//! | `CF_EDGES`        | `[src_id \| edge_type \| timestamp_desc \| dst_id]` | edge properties |
//! | `CF_REVERSE`      | same encoding, src/dst swapped                    | edge properties |
//! | `CF_NODES`        | `[node_id \| timestamp_desc]`                     | node properties |
//! | `CF_EMBEDDINGS`   | `[node_id \| timestamp_desc]`                     | vector + model_id + computation_path |
//! | `CF_COMMIT_META`  | `[node_id \| timestamp_desc]`                     | dispatch decision + effective caps + attribution |
//! | `CF_REPAIR`       | `[node_id \| timestamp_desc]`                     | stale-embedding marker (why, which model) |
//! | `CF_META`         | byte-string key (`hwm`, `req:<id>`)               | commit high-water mark, idempotency records |
//! | `CF_AUDIT`        | chain sequence number (u64 BE)                    | hash-chained audit record of a committed mutation |
//!
//! `CF_COMMIT_META` is a Phase 9 addition beyond PRD 3.3's four families. It
//! carries the per-mutation provenance that makes an embedding *explainable*
//! rather than merely retrievable: which aggregation path ran and why, which
//! receptive-field caps were in force at that instant, and which edges drove the
//! change. It deliberately reuses the node-key layout so that reading a node,
//! its embedding, and the reason its embedding changed are all the same seek.
//!
//! Each family gets a fixed-length prefix extractor matching the *non-temporal*
//! portion of its key. That lets RocksDB use prefix bloom filters to skip whole
//! SST files during a point-in-time seek, which is what keeps Contribution 2's
//! retrieval at O(log n) rather than degrading as version history accumulates.

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Options, SliceTransform,
};

use crate::temporal::keys::{EDGE_PREFIX_LEN, NODE_PREFIX_LEN};

pub const CF_EDGES: &str = "cf_edges";
pub const CF_REVERSE: &str = "cf_reverse";
pub const CF_NODES: &str = "cf_nodes";
pub const CF_EMBEDDINGS: &str = "cf_embeddings";
pub const CF_COMMIT_META: &str = "cf_commit_meta";
/// Durable "this node's embedding is stale" markers (production hardening P2).
pub const CF_REPAIR: &str = "cf_repair";
/// Small server-state records that are not node-keyed: the persisted commit
/// high-water mark, request-id idempotency records. Plain byte-string keys, so
/// no fixed-prefix extractor.
pub const CF_META: &str = "cf_meta";
/// Hash-chained audit records for committed mutations, keyed by chain sequence
/// number (8 bytes, big endian). Written in the same batch as the mutation.
pub const CF_AUDIT: &str = "cf_audit";

/// Every column family CareGraph opens. Used both at open time and by the
/// integration tests that assert all five exist. `create_missing_column_families`
/// means an existing database on disk gains a newly-added family silently on
/// its next open — nothing here needs a migration step.
pub const ALL: [&str; 8] = [
    CF_EDGES,
    CF_REVERSE,
    CF_NODES,
    CF_EMBEDDINGS,
    CF_COMMIT_META,
    CF_REPAIR,
    CF_META,
    CF_AUDIT,
];

/// Options every family shares: one block cache for the whole process (see
/// `crate::memory`), with index and filter blocks held in it so they are neither
/// re-read from disk nor allocated outside the budget, and the memtable size.
fn base_cf_options(cache: &Cache, write_buffer_bytes: usize) -> (Options, BlockBasedOptions) {
    let mut opts = Options::default();
    opts.set_write_buffer_size(write_buffer_bytes);
    opts.set_compression_type(DBCompressionType::Lz4);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    (opts, table)
}

/// Options for a family whose keys carry a fixed-width prefix before the
/// inverted timestamp.
fn versioned_cf_options(prefix_len: usize, cache: &Cache, write_buffer_bytes: usize) -> Options {
    let (mut opts, mut table) = base_cf_options(cache, write_buffer_bytes);
    // A Bloom filter so point-in-time seeks skip SST files that cannot contain
    // the key's prefix.
    table.set_bloom_filter(10.0, false);
    opts.set_block_based_table_factory(&table);
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
    // Version history is append-only and read by seek, so level compaction with
    // a bloom filter on the prefix serves reads better than universal.
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts
}

/// The prefix width used by a given column family, in bytes.
pub fn prefix_len(cf: &str) -> Option<usize> {
    match cf {
        CF_EDGES | CF_REVERSE => Some(EDGE_PREFIX_LEN),
        CF_NODES | CF_EMBEDDINGS | CF_COMMIT_META | CF_REPAIR => Some(NODE_PREFIX_LEN),
        _ => None,
    }
}

/// Descriptors for every family, in [`ALL`] order, sized by the memory plan.
pub fn descriptors() -> Vec<ColumnFamilyDescriptor> {
    descriptors_with(crate::memory::current().write_buffer_mb as usize)
}

/// Memtable size for a database that only ever holds a trickle of small records
/// (the per-pod audit log).
pub const SMALL_WRITE_BUFFER_MB: usize = 4;

/// As [`descriptors`], with an explicit memtable size per family. Every family
/// shares the process-wide cache.
pub fn descriptors_with(write_buffer_mb: usize) -> Vec<ColumnFamilyDescriptor> {
    let cache = crate::memory::rocksdb_cache();
    let write_buffer = write_buffer_mb << 20;
    ALL.iter()
        .map(|name| {
            if *name == CF_META || *name == CF_AUDIT {
                let (mut opts, table) = base_cf_options(cache, write_buffer);
                opts.set_block_based_table_factory(&table);
                return ColumnFamilyDescriptor::new(*name, opts);
            }
            let prefix =
                prefix_len(name).expect("every versioned CF in ALL has a known prefix width");
            ColumnFamilyDescriptor::new(*name, versioned_cf_options(prefix, cache, write_buffer))
        })
        .collect()
}

/// Database-wide options.
pub fn db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    // Durability is the point of the system (Rule 5); never disable the WAL.
    opts.set_use_fsync(false);
    let plan = crate::memory::current();
    opts.increase_parallelism((plan.background_jobs as i32).min(num_cpus()));
    // One limit on every memtable in the process, charged to the shared cache.
    if let Some(manager) = crate::memory::memtable_manager() {
        opts.set_write_buffer_manager(manager);
    }
    opts
}

fn num_cpus() -> i32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(2)
}
