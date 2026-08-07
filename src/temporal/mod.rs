//! Layer 2 — Temporal Indexing (PRD 3.1).
//!
//! Versioned key encoding ([`keys`]), tombstone-aware value envelopes
//! ([`record`]), point-in-time and windowed reads ([`index`]), and batch-staged
//! versioned writes ([`writer`]).

pub mod index;
pub mod keys;
pub mod record;
pub mod writer;

pub use index::{Edge, EdgeVersion, Node, TemporalIndex};
pub use keys::{
    as_of_prefix, decode_edge_key, decode_node_key, edge_prefix, encode_edge_key,
    encode_embedding_key, encode_node_key, node_prefix,
};
pub use record::{EdgeValue, NodeValue};
pub use writer::TemporalWriter;
