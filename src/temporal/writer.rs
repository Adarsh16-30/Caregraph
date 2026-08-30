//! Staging versioned writes into a `WriteBatch`.
//!
//! Every write goes through a batch rather than a direct put, because from
//! Phase 5 the structural mutation and its embedding update must land in the
//! *same* batch (Rule 5). Building the write path that way from the start means
//! atomic commit is a matter of adding entries to a batch that already exists,
//! rather than restructuring the write path later.
//!
//! `write_mutation` (PRD Section 9.2) is implemented on top of these primitives
//! at Phase 5.

use std::sync::Arc;

use rocksdb::{BoundColumnFamily, WriteBatch};

use crate::error::Result;
use crate::storage::{cf, RocksKv};
use crate::temporal::keys::{encode_edge_key, encode_node_key};
use crate::temporal::record::{EdgeValue, NodeValue};
use crate::types::{EdgeType, NodeId, Timestamp};

/// Holds the column-family handles needed to stage a versioned write.
pub struct TemporalWriter<'a> {
    nodes: Arc<BoundColumnFamily<'a>>,
    edges: Arc<BoundColumnFamily<'a>>,
    reverse: Arc<BoundColumnFamily<'a>>,
}

impl<'a> TemporalWriter<'a> {
    pub fn new(store: &'a RocksKv) -> Result<Self> {
        Ok(TemporalWriter {
            nodes: store.cf_handle(cf::CF_NODES)?,
            edges: store.cf_handle(cf::CF_EDGES)?,
            reverse: store.cf_handle(cf::CF_REVERSE)?,
        })
    }

    /// Append a node version at `ts`.
    pub fn put_node(&self, batch: &mut WriteBatch, node: NodeId, ts: Timestamp, value: &NodeValue) {
        batch.put_cf(&self.nodes, encode_node_key(node, ts), value.encode());
    }

    /// Retract a node at `ts` by appending a tombstone version.
    ///
    /// Deliberately *not* a `delete`: removing the key would erase the history
    /// that point-in-time reconstruction reads, making `as_of(T)` for a `T`
    /// before the retraction wrongly report the node as never having existed.
    pub fn remove_node(
        &self,
        batch: &mut WriteBatch,
        node: NodeId,
        ts: Timestamp,
        node_type: &str,
    ) {
        self.put_node(batch, node, ts, &NodeValue::tombstone(node_type));
    }

    /// Append an edge version at `ts`, mirrored into `CF_REVERSE`.
    ///
    /// Both families are written in the same batch, so forward and reverse
    /// adjacency can never disagree about what the graph looked like at a
    /// timestamp.
    pub fn put_edge(
        &self,
        batch: &mut WriteBatch,
        src: NodeId,
        edge_type: EdgeType,
        dst: NodeId,
        ts: Timestamp,
        value: &EdgeValue,
    ) {
        let encoded = value.encode();
        batch.put_cf(
            &self.edges,
            encode_edge_key(src, edge_type, ts, dst),
            &encoded,
        );
        batch.put_cf(
            &self.reverse,
            encode_edge_key(dst, edge_type, ts, src),
            &encoded,
        );
    }

    /// Remove an edge at `ts` by appending a tombstone version to both families.
    pub fn remove_edge(
        &self,
        batch: &mut WriteBatch,
        src: NodeId,
        edge_type: EdgeType,
        dst: NodeId,
        ts: Timestamp,
    ) {
        self.put_edge(batch, src, edge_type, dst, ts, &EdgeValue::tombstone());
    }
}
