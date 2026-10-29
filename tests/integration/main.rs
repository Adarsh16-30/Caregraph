//! CareGraph integration test suite.
//!
//! Every test here opens a real RocksDB instance in a temporary directory. There
//! is no in-memory substitute anywhere in this suite — that is the Phase 1
//! success criterion and the Rule 1 boundary.

mod api_endpoint_test;
mod audit_test;
mod encryption_at_rest;
mod limits_test;
mod mtls_test;
mod repair_marker_test;
mod security_test;
mod shutdown_health_test;
mod storage_kv;
mod temporal_pit;
