//! CareGraph integration test suite.
//!
//! Every test here opens a real RocksDB instance in a temporary directory. There
//! is no in-memory substitute anywhere in this suite — that is the Phase 1
//! success criterion and the Rule 1 boundary.

mod admin_endpoint_test;
mod api_endpoint_test;
mod audit_test;
mod backup_restore_test;
mod encryption_at_rest;
mod kube_label_test;
mod limits_test;
mod repair_marker_test;
mod replication_test;
mod security_test;
mod shutdown_health_test;
mod storage_kv;
mod temporal_pit;
