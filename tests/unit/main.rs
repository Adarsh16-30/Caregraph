//! Unit test suite.
//!
//! This is the one place where synthetic data is permitted (PRD Rule 1, Rule 6),
//! and only in files named `*_synthetic_test.rs`. The storage engine underneath
//! is still a real RocksDB instance — synthetic here refers to the *mutation
//! sequences and graph shapes* being generated, not to the database being faked.

mod graph_fixture;
mod snapshot_synthetic_test;
mod temporal_pit_synthetic_test;
mod traversal_synthetic_test;
