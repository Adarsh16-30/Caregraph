//! Error type shared across the storage, temporal, and embedding layers.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, CareGraphError>;

#[derive(Debug, Error)]
pub enum CareGraphError {
    #[error("rocksdb: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("column family {0} is not open on this database handle")]
    MissingColumnFamily(&'static str),

    #[error("malformed key in {cf}: expected {expected} bytes, found {found}")]
    MalformedKey {
        cf: &'static str,
        expected: usize,
        found: usize,
    },

    #[error("malformed value: {0}")]
    MalformedValue(#[from] serde_json::Error),

    #[error("unknown edge type discriminant {0}")]
    UnknownEdgeType(u16),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("encryption: {0}")]
    Encryption(String),
}
