//! Layer 4 — Incremental Embedding (PRD 3.1). Phase 4 shipped the associative
//! (GraphSAGE/GCN) path. Phase 5 added `atomic_commit.rs`, wired into
//! `pipeline.rs` below, and the non-associative GAT path (originally
//! `gat_incremental.rs`). Phase 9 renamed that module to `staged_incremental.rs`
//! and generalized it: `atomic_commit.rs` now dispatches to it whenever the
//! active model's manifest declares `is_associative: false`, not only for GAT
//! specifically (see [`manifest::ModelManifest`]).

pub mod associative;
pub mod atomic_commit;
pub mod attribution;
pub mod caps;
pub mod manifest;
pub mod meta;
pub mod metrics;
pub mod model_bridge;
pub mod pipeline;
pub mod resolver;
pub mod staged_incremental;
pub mod state;

pub use atomic_commit::AtomicCommitter;
pub use attribution::attribute_targets;
pub use caps::{CapAdjustment, CapController, CapRung};
pub use manifest::ModelManifest;
pub use meta::{Attribution, CommitMeta, DispatchDecision, EdgeAttribution, EffectiveCaps};
pub use model_bridge::EmbeddingModel;
pub use pipeline::run_mutation_pipeline;
pub use resolver::{AffectedSubgraphResolver, NODE_TYPES};
pub use staged_incremental::staged_incremental_update;
pub use state::{GraphMutation, MutationContext, ResolutionTruncation};
