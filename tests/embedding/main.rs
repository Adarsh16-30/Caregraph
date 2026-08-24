//! Phase 4 embedding-layer correctness suite.
//!
//! Needs a real trained model on disk (`ml/deployed/diabetes130_graphsage/`)
//! and a real Python interpreter with torch + torch_geometric importable —
//! spawns the actual `ml/embedding_server.py` worker and runs actual forward
//! passes through it, per Rule 3. Not run by `cargo test --test unit` or
//! `--test integration`; not yet wired into CI, the same way the Rule 4
//! baseline benchmarks are not — until a CI image carries Python + torch.
//! Run explicitly: `cargo test --test embedding`.

mod associative_correctness_test;
