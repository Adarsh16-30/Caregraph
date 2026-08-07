//! Layer 2 — Temporal Indexing (PRD 3.1).
//!
//! Versioned key encoding and point-in-time range scans. Phase 2 adds the
//! `as_of(timestamp)` read API on top of the encoders in [`keys`].

pub mod keys;

pub use keys::{
    as_of_prefix, decode_edge_key, decode_node_key, edge_prefix, encode_edge_key,
    encode_embedding_key, encode_node_key, node_prefix,
};
