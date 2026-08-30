//! Temporal key encoding — implements Contribution 2: O(log n) point-in-time retrieval.
//!
//! # Why inverted timestamps
//!
//! RocksDB iterates keys in ascending lexicographic byte order. By storing the
//! bitwise complement of the timestamp, a *newer* version produces a *smaller*
//! byte sequence and therefore sorts first within its prefix. A point-in-time
//! read becomes a single forward seek to `as_of_prefix(..)`, landing directly on
//! the first version at or before `as_of` — no reverse iterator, no secondary
//! index. That is the whole of Contribution 2.
//!
//! # Provenance
//!
//! `encode_edge_key` and `as_of_prefix` are reproduced verbatim from PRD
//! Section 9.1 ("Critical Code — Provided, Do Not Modify"). Do not substitute,
//! simplify, or reorder their field layout: the on-disk format and the
//! O(log n) seek property both depend on it.

use crate::types::{EdgeType, NodeId, Timestamp};

/// Fixed-width prefix of an edge key: `[src_id | edge_type]`.
pub const EDGE_PREFIX_LEN: usize = NodeId::WIDTH + EdgeType::WIDTH;
/// Full edge key length: `[src_id | edge_type | timestamp_desc | dst_id]`.
pub const EDGE_KEY_LEN: usize = EDGE_PREFIX_LEN + Timestamp::WIDTH + NodeId::WIDTH;
/// Fixed-width prefix of a node or embedding key: `[node_id]`.
pub const NODE_PREFIX_LEN: usize = NodeId::WIDTH;
/// Full node/embedding key length: `[node_id | timestamp_desc]`.
pub const NODE_KEY_LEN: usize = NODE_PREFIX_LEN + Timestamp::WIDTH;

// ---------------------------------------------------------------------------
// PRD Section 9.1 — provided verbatim. Do not modify.
// ---------------------------------------------------------------------------

pub fn encode_edge_key(src: NodeId, edge_type: EdgeType, ts: Timestamp, dst: NodeId) -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&src.to_be_bytes());
    key.extend_from_slice(&edge_type.to_be_bytes());
    // Descending timestamp: invert bits so the newest version sorts first
    // in a forward RocksDB prefix scan — avoids a reverse-iterator on every read.
    key.extend_from_slice(&(!ts.as_u64()).to_be_bytes());
    key.extend_from_slice(&dst.to_be_bytes());
    key
}

pub fn as_of_prefix(node_or_edge_prefix: &[u8], as_of: Timestamp) -> Vec<u8> {
    let mut prefix = node_or_edge_prefix.to_vec();
    prefix.extend_from_slice(&(!as_of.as_u64()).to_be_bytes());
    prefix // seek_to(prefix) lands exactly at the first version <= as_of
}

// ---------------------------------------------------------------------------
// Supporting encoders, consistent with the layout above.
// ---------------------------------------------------------------------------

/// Node state key for `CF_NODES`: `[node_id | timestamp_desc]` (PRD 3.3).
pub fn encode_node_key(node: NodeId, ts: Timestamp) -> Vec<u8> {
    let mut key = Vec::with_capacity(NODE_KEY_LEN);
    key.extend_from_slice(&node.to_be_bytes());
    key.extend_from_slice(&(!ts.as_u64()).to_be_bytes());
    key
}

/// Versioned embedding key for `CF_EMBEDDINGS`: `[node_id | timestamp_desc]`.
///
/// Called by `atomic_commit` (PRD Section 9.2). Shares the node-key layout so
/// that a snapshot read of a node and of its embedding are the same seek.
pub fn encode_embedding_key(node: NodeId, ts: Timestamp) -> Vec<u8> {
    encode_node_key(node, ts)
}

/// The `[src_id | edge_type]` prefix that bounds a single adjacency list.
pub fn edge_prefix(src: NodeId, edge_type: EdgeType) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(EDGE_PREFIX_LEN);
    prefix.extend_from_slice(&src.to_be_bytes());
    prefix.extend_from_slice(&edge_type.to_be_bytes());
    prefix
}

/// The `[node_id]` prefix that bounds every version of one node or embedding.
pub fn node_prefix(node: NodeId) -> Vec<u8> {
    node.to_be_bytes().to_vec()
}

/// Recover the timestamp from the inverted field at `offset`.
fn decode_ts_at(key: &[u8], offset: usize) -> Option<Timestamp> {
    let raw: [u8; Timestamp::WIDTH] = key
        .get(offset..offset + Timestamp::WIDTH)?
        .try_into()
        .ok()?;
    Some(Timestamp(!u64::from_be_bytes(raw)))
}

/// Decode a full edge key back into its components.
pub fn decode_edge_key(key: &[u8]) -> Option<(NodeId, EdgeType, Timestamp, NodeId)> {
    if key.len() != EDGE_KEY_LEN {
        return None;
    }
    let src = NodeId::from_be_bytes(key[0..8].try_into().ok()?);
    let edge_type = EdgeType::from_u16(u16::from_be_bytes(key[8..10].try_into().ok()?))?;
    let ts = decode_ts_at(key, EDGE_PREFIX_LEN)?;
    let dst = NodeId::from_be_bytes(key[18..26].try_into().ok()?);
    Some((src, edge_type, ts, dst))
}

/// Decode a node or embedding key back into its components.
pub fn decode_node_key(key: &[u8]) -> Option<(NodeId, Timestamp)> {
    if key.len() != NODE_KEY_LEN {
        return None;
    }
    let node = NodeId::from_be_bytes(key[0..8].try_into().ok()?);
    let ts = decode_ts_at(key, NODE_PREFIX_LEN)?;
    Some((node, ts))
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: EdgeType = EdgeType::DiagnosedWith;

    #[test]
    fn edge_key_has_the_documented_layout() {
        let key = encode_edge_key(NodeId(1), T, Timestamp(42), NodeId(2));
        assert_eq!(key.len(), EDGE_KEY_LEN);
        assert_eq!(&key[..EDGE_PREFIX_LEN], &edge_prefix(NodeId(1), T)[..]);
    }

    #[test]
    fn newer_versions_sort_before_older_ones() {
        let older = encode_edge_key(NodeId(1), T, Timestamp(100), NodeId(2));
        let newer = encode_edge_key(NodeId(1), T, Timestamp(200), NodeId(2));
        assert!(
            newer < older,
            "inverted timestamp must put the newest version first in a forward scan"
        );
    }

    #[test]
    fn as_of_seek_target_precedes_every_version_at_or_before_it() {
        let prefix = edge_prefix(NodeId(1), T);
        let seek = as_of_prefix(&prefix, Timestamp(150));

        // Versions at or before as_of must sort at/after the seek target...
        for ts in [150u64, 100, 1] {
            let key = encode_edge_key(NodeId(1), T, Timestamp(ts), NodeId(2));
            assert!(
                key >= seek,
                "ts={ts} should be reachable from a forward seek"
            );
        }
        // ...and versions after it must sort strictly before, so the seek skips them.
        for ts in [151u64, 200, u64::MAX] {
            let key = encode_edge_key(NodeId(1), T, Timestamp(ts), NodeId(2));
            assert!(
                key < seek,
                "ts={ts} is in the future of as_of and must be skipped"
            );
        }
    }

    #[test]
    fn different_adjacency_lists_never_share_a_prefix() {
        let a = edge_prefix(NodeId(1), EdgeType::DiagnosedWith);
        let b = edge_prefix(NodeId(1), EdgeType::PrescribedMedication);
        let c = edge_prefix(NodeId(2), EdgeType::DiagnosedWith);
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn edge_keys_round_trip() {
        let key = encode_edge_key(NodeId(7), EdgeType::HasLabResult, Timestamp(99), NodeId(8));
        assert_eq!(
            decode_edge_key(&key),
            Some((NodeId(7), EdgeType::HasLabResult, Timestamp(99), NodeId(8)))
        );
    }

    #[test]
    fn node_and_embedding_keys_round_trip() {
        let key = encode_embedding_key(NodeId(7), Timestamp(99));
        assert_eq!(key, encode_node_key(NodeId(7), Timestamp(99)));
        assert_eq!(decode_node_key(&key), Some((NodeId(7), Timestamp(99))));
    }

    #[test]
    fn timestamp_max_seeks_to_the_newest_version() {
        let seek = as_of_prefix(&node_prefix(NodeId(1)), Timestamp::MAX);
        assert_eq!(&seek[NODE_PREFIX_LEN..], &[0u8; 8]);
    }
}
