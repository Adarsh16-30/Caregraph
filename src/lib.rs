//! CareGraph — a temporally-versioned graph database with incrementally-maintained
//! GNN embeddings.
//!
//! # Layering (PRD 3.1)
//!
//! | Layer | Module | Status |
//! |-------|--------|--------|
//! | 1. Storage | [`storage`] | Phase 1 — implemented |
//! | 2. Temporal indexing | [`temporal`] | Phase 2 — implemented |
//! | 3. Graph semantics | [`graph`] | Phase 3 — implemented |
//! | 4. Incremental embedding | `embedding` | Phases 4–5 |
//! | 5. Query & API | `api` | Phase 6 |
//! | 6. Observability & compliance | `observability` | Phase 7 |
//!
//! Modules for later phases are deliberately absent rather than stubbed. A stub
//! that compiles is indistinguishable from an implementation to anything except
//! a reader, which is precisely the failure mode Section 0 exists to prevent.

pub mod embedding;
pub mod error;
pub mod graph;
pub mod storage;
pub mod temporal;
pub mod types;

pub use error::{CareGraphError, Result};
pub use types::{ComputationPath, EdgeType, Embedding, ModelKind, NodeId, Timestamp};

/// Default on-disk location for the database, overridable via `CAREGRAPH_DB_PATH`.
pub const DEFAULT_DB_PATH: &str = "data/db/caregraph";

/// Resolve the database path from the environment, falling back to the default.
pub fn db_path_from_env() -> String {
    std::env::var("CAREGRAPH_DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_string())
}
