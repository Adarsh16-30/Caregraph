//! Layer 3 — Graph Semantics.
//!
//! Bounded traversal, snapshot reconstruction, and the server-side limits that
//! keep both bounded regardless of what a client requests.

pub mod limits;
pub mod snapshot;
pub mod traversal;

pub use limits::TraversalLimits;
pub use snapshot::{CareGraphSnapshot, RelatedEntity, SnapshotReader};
pub use traversal::{
    Direction, TraversalRequest, TraversalResult, Traverser, Truncation, VisitedNode,
};
