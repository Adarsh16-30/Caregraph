//! CareGraph server entry point.
//!
//! Phase 1 scope: open a real RocksDB instance with all four column families and
//! report readiness. The tonic gRPC service (PRD Layer 5) is wired in at Phase 6;
//! until then this binary deliberately serves nothing rather than serving a
//! placeholder that would violate Rule 2.

use anyhow::Context;
use caregraph::storage::{cf, RocksKv};
use caregraph::{db_path_from_env, Timestamp};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "caregraph=info".into()),
        )
        .init();

    let path = db_path_from_env();
    if let Some(parent) = std::path::Path::new(&path).parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating database directory {}", parent.display()))?;
    }

    let store = RocksKv::open(&path).with_context(|| format!("opening RocksDB at {path}"))?;

    // Prove the four column families are actually open rather than assuming it.
    for name in cf::ALL {
        store
            .cf_handle(name)
            .with_context(|| format!("column family {name} is missing"))?;
    }

    tracing::info!(
        path = %path,
        column_families = ?cf::ALL,
        started_at = ?Timestamp::now(),
        "caregraph storage layer ready"
    );

    tracing::warn!(
        "no query service is running: the gRPC API lands in Phase 6 (PRD Section 7). \
         This binary currently opens and validates storage only."
    );

    Ok(())
}
