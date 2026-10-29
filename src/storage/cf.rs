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

/// Every column family CareGraph opens. Used both at open time and by the
/// integration tests that assert all five exist. `create_missing_column_families`
/// means an existing database on disk gains a newly-added family silently on
/// its next open — nothing here needs a migration step.
pub const ALL: [&str; 7] = [
    CF_EDGES,
    CF_REVERSE,
    CF_NODES,
    CF_EMBEDDINGS,
    CF_COMMIT_META,
    CF_REPAIR,
    CF_META,
];

/// Options for a family whose keys carry a fixed-width prefix before the
/// inverted timestamp.
fn versioned_cf_options(prefix_len: usize, cache: &Cache, write_buffer_bytes: usize) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(write_buffer_bytes);
    // One block cache shared by every column family (RocksDB's default is a
    // small per-process cache that this graph's working set exceeds), with
    // index and filter blocks held in it so they are not re-read from disk, and
    // a Bloom filter so point-in-time seeks skip SST files that cannot contain
    // the key's prefix.
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    table.set_bloom_filter(10.0, false);
    opts.set_block_based_table_factory(&table);
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
    opts.set_compression_type(DBCompressionType::Lz4);
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

/// Descriptors for all four families, in [`ALL`] order.
pub fn descriptors() -> Vec<ColumnFamilyDescriptor> {
    let cache = Cache::new_lru_cache(env_mb("CAREGRAPH_BLOCK_CACHE_MB", 256) << 20);
    let write_buffer = env_mb("CAREGRAPH_WRITE_BUFFER_MB", 64) << 20;
    ALL.iter()
        .map(|name| {
            if *name == CF_META {
                let mut opts = Options::default();
                opts.set_compression_type(DBCompressionType::Lz4);
                return ColumnFamilyDescriptor::new(*name, opts);
            }
            let prefix =
                prefix_len(name).expect("every versioned CF in ALL has a known prefix width");
            ColumnFamilyDescriptor::new(*name, versioned_cf_options(prefix, &cache, write_buffer))
        })
        .collect()
}

/// Megabytes from an environment variable, or `default`. Block cache and write
/// buffer sizing are deployment-dependent (container memory limit), so they are
/// configurable rather than baked in.
fn env_mb(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Database-wide options.
pub fn db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    // Durability is the point of the system (Rule 5); never disable the WAL.
    opts.set_use_fsync(false);
    opts.increase_parallelism(num_cpus());
    opts
}

fn num_cpus() -> i32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(2)
}
