//! Rule 5 fault injection suite.
//!
//! Needs a real trained model (`ml/deployed/diabetes130_graphsage/`) and a
//! real Python + torch interpreter, same preconditions as
//! `tests/embedding/associative_correctness_test.rs`. Not run by
//! `cargo test --test unit` or `--test integration`.
//! Run explicitly: `cargo test --test fault_injection -- --test-threads=1`.

mod atomic_commit_survives_a_kill;
