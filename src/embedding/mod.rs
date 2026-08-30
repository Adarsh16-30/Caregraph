//! Layer 4 — Incremental Embedding (PRD 3.1). Phase 4 shipped the associative
//! (GraphSAGE/GCN) path. Phase 5 adds `atomic_commit.rs`, wired into
//! `pipeline.rs` below; `gat_incremental.rs` (the non-associative GAT path)
//! is still outstanding.

pub mod associative;
pub mod atomic_commit;
pub mod metrics;
pub mod model_bridge;
pub mod pipeline;
pub mod resolver;
pub mod state;

pub use atomic_commit::AtomicCommitter;
pub use model_bridge::EmbeddingModel;
pub use pipeline::run_mutation_pipeline;
pub use resolver::{AffectedSubgraphResolver, NODE_TYPES};
pub use state::{GraphMutation, MutationContext, ResolutionTruncation};
