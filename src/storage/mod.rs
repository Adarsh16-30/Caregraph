//! Layer 1 — Storage (PRD 3.1): RocksDB, column families, WAL-backed durability.

pub mod backup;
pub mod cf;
pub mod encryption;
pub mod kv;
pub mod maintenance;
pub mod node_types;

pub use cf::{
    ALL, CF_AUDIT, CF_COMMIT_META, CF_EDGES, CF_EMBEDDINGS, CF_META, CF_NODES, CF_REPAIR,
    CF_REVERSE,
};
pub use encryption::{decode_hex_key, open_encrypted_env, ENCRYPTION_KEY_ENV, KEY_LEN};
pub use kv::{Db, Entry, KvStore, RocksKv};
