//! Layer 4 — Incremental Embedding (PRD 3.1). Phase 4: the associative
//! (GraphSAGE/GCN) path. Phase 5 adds `gat_incremental.rs` and
//! `atomic_commit.rs`; this module's shape is written not to need changing
//! when they land.

pub mod associative;
pub mod metrics;
pub mod model_bridge;
pub mod pipeline;
pub mod resolver;
pub mod state;

pub use model_bridge::EmbeddingModel;
pub use pipeline::run_mutation_pipeline;
pub use resolver::{AffectedSubgraphResolver, NODE_TYPES};
pub use state::{GraphMutation, MutationContext, ResolutionTruncation};
