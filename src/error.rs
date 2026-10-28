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

    /// A dependency is temporarily unable to serve (embedding workers down,
    /// circuit breaker open, queue full). Retryable by the caller; maps to
    /// gRPC `UNAVAILABLE`, never `INTERNAL`.
    #[error("unavailable: {0}")]
    Unavailable(String),

    /// A bounded wait elapsed (worker request deadline). Maps to
    /// `DEADLINE_EXCEEDED`.
    #[error("timed out: {0}")]
    Timeout(String),
}
