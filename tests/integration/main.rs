//! CareGraph integration test suite.
//!
//! Every test here opens a real RocksDB instance in a temporary directory. There
//! is no in-memory substitute anywhere in this suite — that is the Phase 1
//! success criterion and the Rule 1 boundary.

mod api_endpoint_test;
mod storage_kv;
mod temporal_pit;
