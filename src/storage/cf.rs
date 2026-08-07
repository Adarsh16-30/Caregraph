//! Column family definitions — the versioned schema itself (PRD 3.3).
//!
//! | CF                | Key                                              | Value |
//! |-------------------|--------------------------------------------------|-------|
//! | `CF_EDGES`        | `[src_id \| edge_type \| timestamp_desc \| dst_id]` | edge properties |
//! | `CF_REVERSE`      | same encoding, src/dst swapped                    | edge properties |
//! | `CF_NODES`        | `[node_id \| timestamp_desc]`                     | node properties |
//! | `CF_EMBEDDINGS`   | `[node_id \| timestamp_desc]`                     | vector + model_id + computation_path |
//!
//! Each family gets a fixed-length prefix extractor matching the *non-temporal*
//! portion of its key. That lets RocksDB use prefix bloom filters to skip whole
//! SST files during a point-in-time seek, which is what keeps Contribution 2's
//! retrieval at O(log n) rather than degrading as version history accumulates.

use rocksdb::{ColumnFamilyDescriptor, DBCompressionType, Options, SliceTransform};

use crate::temporal::keys::{EDGE_PREFIX_LEN, NODE_PREFIX_LEN};

pub const CF_EDGES: &str = "cf_edges";
pub const CF_REVERSE: &str = "cf_reverse";
pub const CF_NODES: &str = "cf_nodes";
pub const CF_EMBEDDINGS: &str = "cf_embeddings";

/// Every column family CareGraph opens. Used both at open time and by the
/// integration tests that assert all four exist.
pub const ALL: [&str; 4] = [CF_EDGES, CF_REVERSE, CF_NODES, CF_EMBEDDINGS];

/// Options for a family whose keys carry a fixed-width prefix before the
/// inverted timestamp.
fn versioned_cf_options(prefix_len: usize) -> Options {
    let mut opts = Options::default();
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
        CF_NODES | CF_EMBEDDINGS => Some(NODE_PREFIX_LEN),
        _ => None,
    }
}

/// Descriptors for all four families, in [`ALL`] order.
pub fn descriptors() -> Vec<ColumnFamilyDescriptor> {
    ALL.iter()
        .map(|name| {
            let prefix = prefix_len(name).expect("every CF in ALL has a known prefix width");
            ColumnFamilyDescriptor::new(*name, versioned_cf_options(prefix))
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
    opts.increase_parallelism(num_cpus());
    opts
}

fn num_cpus() -> i32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(2)
}
