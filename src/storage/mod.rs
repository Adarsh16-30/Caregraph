//! Layer 1 — Storage (PRD 3.1): RocksDB, column families, WAL-backed durability.

pub mod cf;
pub mod encryption;
pub mod kv;

pub use cf::{ALL, CF_EDGES, CF_EMBEDDINGS, CF_NODES, CF_REVERSE};
pub use encryption::{decode_hex_key, open_encrypted_env, ENCRYPTION_KEY_ENV, KEY_LEN};
pub use kv::{Db, Entry, KvStore, RocksKv};
