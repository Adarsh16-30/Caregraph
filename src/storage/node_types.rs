//! Process-local cache of each node's type, keyed by [`NodeId`].
//!
//! `build_model_input` needs every node's type to fill the one-hot feature
//! row, and reading it from `CF_NODES` costs one RocksDB seek per node — a
//! quarter of an incremental mutation's latency on the real clinical graph.
//!
//! The cache is exact under one invariant: a node's type never changes once
//! the node exists, and nodes are created only by the bulk loader, which
//! cannot run while a server holds the database open. Each entry records the
//! timestamp of the version it was read from, so it answers only for reads at
//! or after that instant; an earlier `as_of` falls through to RocksDB. A
//! follower applying replicated batches must call [`clear`](NodeTypeCache::clear)
//! when a batch touches `CF_NODES`.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::{NodeId, Timestamp};

/// Entries beyond the memory plan's cap are not inserted; lookups keep working
/// and the overflow simply reads from RocksDB, so memory stays bounded.
fn max_entries() -> usize {
    crate::memory::current().node_type_entries as usize
}

#[derive(Default)]
pub struct NodeTypeCache {
    entries: RwLock<HashMap<NodeId, (Timestamp, u8)>>,
}

impl NodeTypeCache {
    /// Feature dimension of `node`'s type as of `as_of`, if cached and valid
    /// for that instant.
    pub fn get(&self, node: NodeId, as_of: Timestamp) -> Option<u8> {
        let map = self.entries.read().ok()?;
        match map.get(&node) {
            Some(&(version_ts, dim)) if as_of >= version_ts => Some(dim),
            _ => None,
        }
    }

    pub fn insert(&self, node: NodeId, version_ts: Timestamp, dim: u8) {
        if let Ok(mut map) = self.entries.write() {
            if map.len() < max_entries() {
                map.insert(node, (version_ts, dim));
            }
        }
    }

    pub fn clear(&self) {
        if let Ok(mut map) = self.entries.write() {
            map.clear();
        }
    }

    pub fn len(&self) -> usize {
        self.entries.read().map(|m| m.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
