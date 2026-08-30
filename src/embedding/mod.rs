//! Layer 4 — Incremental Embedding (PRD 3.1). Phase 4 shipped the associative
//! (GraphSAGE/GCN) path. Phase 5 adds `atomic_commit.rs`, wired into
//! `pipeline.rs` below, and `gat_incremental.rs` — the non-associative GAT
//! path `atomic_commit.rs` dispatches to when `ModelKind::GAT` is active.

pub mod associative;
pub mod atomic_commit;
pub mod gat_incremental;
pub mod metrics;
pub mod model_bridge;
pub mod pipeline;
pub mod resolver;
pub mod state;

pub use atomic_commit::AtomicCommitter;
pub use gat_incremental::gat_incremental_update;
pub use model_bridge::EmbeddingModel;
pub use pipeline::run_mutation_pipeline;
pub use resolver::{AffectedSubgraphResolver, NODE_TYPES};
pub use state::{GraphMutation, MutationContext, ResolutionTruncation};
