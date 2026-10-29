//! The audit chains against real databases: the mutation chain staged into
//! commit batches, the per-pod chain for reads and denials, tamper detection,
//! recovery across restarts, and the fail-closed behaviour of read auditing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::{SnapshotRequest, TraverseRequest};
use caregraph::api::{AuthInterceptor, CareGraphApi, ServiceOptions};
use caregraph::security::audit::{
    verify_chain, verify_store, AuditLog, AuditUnavailable, Fields, MutationAudit,
};
use caregraph::security::{Policy, PolicyHandle};
use caregraph::storage::{cf, KvStore, RocksKv};
use rocksdb::WriteBatch;
use tempfile::TempDir;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request};

fn fields(n: u64) -> Fields {
    Fields {
        ts_us: 1_000 + n,
        principal: "spiffe://caregraph/ingest".into(),
        rpc: "AddEdge".into(),
        request_id: format!("req-{n}"),
        src: n,
        dst: n + 1,
        affected_count: 7,
        outcome: "committed".into(),
        commit_seq: n,
    }
}

/// Stages and durably writes `n` mutation audit records, one batch each.
fn commit_records(store: &RocksKv, audit: &MutationAudit, range: std::ops::RangeInclusive<u64>) {
    for i in range {
        let mut batch = WriteBatch::default();
        let pending = audit.stage(&mut batch, store, fields(i)).unwrap();
        store.write(batch).unwrap();
        audit.advance(pending);
    }
}

#[test]
fn a_mutation_chain_verifies_survives_a_restart_and_keeps_extending() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    {
        let store = RocksKv::open(&path).unwrap();
        let audit = MutationAudit::open(&store).unwrap();
        commit_records(&store, &audit, 1..=5);
        let summary = verify_store(&store).unwrap().unwrap();
        assert_eq!((summary.records, summary.head_seq), (5, 5));
        assert_eq!(audit.head(), (5, summary.head_hash));
    }
    let store = RocksKv::open(&path).unwrap();
    let audit = MutationAudit::open(&store).unwrap();
    assert_eq!(audit.head().0, 5, "the head is recovered from CF_META");
    commit_records(&store, &audit, 6..=8);
    let summary = verify_store(&store).unwrap().unwrap();
    assert_eq!((summary.records, summary.head_seq), (8, 8));
}

#[test]
fn a_staged_record_whose_batch_never_lands_does_not_move_the_head() {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path().join("db")).unwrap();
    let audit = MutationAudit::open(&store).unwrap();
    commit_records(&store, &audit, 1..=2);

    // Staged but never written (the commit failed or the process died).
    let mut abandoned = WriteBatch::default();
    let _pending = audit.stage(&mut abandoned, &store, fields(3)).unwrap();
    drop(abandoned);

    assert_eq!(audit.head().0, 2);
    commit_records(&store, &audit, 3..=3);
    assert_eq!(verify_store(&store).unwrap().unwrap().head_seq, 3);
}

#[test]
fn editing_or_removing_stored_records_is_detected() {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path().join("db")).unwrap();
    let audit = MutationAudit::open(&store).unwrap();
    commit_records(&store, &audit, 1..=6);
    assert!(verify_store(&store).unwrap().is_ok());

    // Change one byte inside record 3's stored value.
    let key = 3u64.to_be_bytes();
    let original = store.get(cf::CF_AUDIT, &key).unwrap().unwrap();
    let mut edited = original.clone();
    let at = edited
        .windows(6)
        .position(|w| w == b"ingest")
        .expect("principal text present");
    edited[at] = b'X';
    store.put(cf::CF_AUDIT, &key, &edited).unwrap();
    let err = verify_store(&store).unwrap().unwrap_err();
    assert_eq!(err.at_seq, 3);
    store.put(cf::CF_AUDIT, &key, &original).unwrap();
    assert!(
        verify_store(&store).unwrap().is_ok(),
        "restoring it heals the chain"
    );

    // Delete a record in the middle.
    store.delete(cf::CF_AUDIT, &4u64.to_be_bytes()).unwrap();
    assert!(verify_store(&store).unwrap().is_err());
}

#[test]
fn removing_the_tail_is_caught_by_the_recorded_head() {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path().join("db")).unwrap();
    let audit = MutationAudit::open(&store).unwrap();
    commit_records(&store, &audit, 1..=4);
    // The remaining records still form a valid chain on their own; only the
    // head recorded in CF_META betrays that two were dropped.
    store.delete(cf::CF_AUDIT, &4u64.to_be_bytes()).unwrap();
    store.delete(cf::CF_AUDIT, &3u64.to_be_bytes()).unwrap();
    let err = verify_store(&store).unwrap().unwrap_err();
    assert!(err.reason.contains("tail"), "{}", err.reason);
}

fn pod_fields(principal: &str, rpc: &str, subject: u64) -> Fields {
    Fields {
        principal: principal.into(),
        rpc: rpc.into(),
        src: subject,
        outcome: "requested".into(),
        ..Fields::default()
    }
}

#[tokio::test]
async fn durable_events_are_chained_in_order_and_survive_reopening() {
    let dir = TempDir::new().unwrap();
    let log = AuditLog::open_dir(&dir.path().join("audit"), "pod-a", 256).unwrap();

    let mut tasks = Vec::new();
    for i in 0..200u64 {
        let log = log.clone();
        tasks.push(tokio::spawn(async move {
            log.record_durable(pod_fields("analyst", "Snapshot", i))
                .await
                .unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let records = log.records().unwrap();
    assert_eq!(records.len(), 200);
    let subjects: std::collections::BTreeSet<u64> = records.iter().map(|r| r.src).collect();
    assert_eq!(subjects.len(), 200, "every read is recorded exactly once");
    assert!(records.iter().all(|r| r.chain == "pod-a" && r.ts_us > 0));
    let summary = verify_chain(records).unwrap();
    assert_eq!(log.head(), (summary.head_seq, summary.head_hash.clone()));

    drop(log);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let reopened = AuditLog::open_dir(&dir.path().join("audit"), "pod-a", 256).unwrap();
    assert_eq!(reopened.head().0, 200, "the chain resumes where it stopped");
    reopened
        .record_durable(pod_fields("analyst", "Traverse", 1))
        .await
        .unwrap();
    assert!(verify_chain(reopened.records().unwrap()).is_ok());
}

#[tokio::test]
async fn a_full_queue_refuses_new_events_instead_of_dropping_them() {
    let dir = TempDir::new().unwrap();
    let log = AuditLog::open_dir(&dir.path().join("audit"), "pod-b", 1).unwrap();
    let mut tasks = Vec::new();
    for i in 0..2000u64 {
        let log = log.clone();
        tasks.push(tokio::spawn(async move {
            log.record_durable(pod_fields("p", "Snapshot", i)).await
        }));
    }
    let (mut ok, mut full) = (0u64, 0u64);
    for t in tasks {
        match t.await.unwrap() {
            Ok(()) => ok += 1,
            Err(AuditUnavailable::QueueFull) => full += 1,
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
    assert!(
        full > 0,
        "a one-slot queue under 2000 concurrent events must overflow"
    );
    assert_eq!(
        log.records().unwrap().len() as u64,
        ok,
        "exactly the accepted events are on disk; refused ones are not silently recorded"
    );
    assert_eq!(log.queue_full_count(), full);
}

// ---------------------------------------------------------------------------
// Through the real gRPC service
// ---------------------------------------------------------------------------

struct Served {
    client: CareGraphServiceClient<Channel>,
    log: Arc<AuditLog>,
    _dir: TempDir,
    _server: tokio::task::JoinHandle<()>,
}

const KEY: &str = "audit-test-key";

async fn serve(policy_text: Option<&str>, queue: usize) -> Served {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path().join("db")).unwrap();
    let log = AuditLog::open_dir(&dir.path().join("audit"), "pod-test", queue).unwrap();
    let policy = policy_text.map(|t| PolicyHandle::fixed(Policy::parse(t).unwrap()));
    let api = CareGraphApi::with_options(
        store,
        None,
        None,
        &prometheus::Registry::new(),
        ServiceOptions {
            policy,
            audit_log: Some(log.clone()),
            ..ServiceOptions::default()
        },
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener);
    let service =
        InterceptedService::new(CareGraphServiceServer::new(api), AuthInterceptor::new(KEY));
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    Served {
        client: CareGraphServiceClient::new(channel),
        log,
        _dir: dir,
        _server: server,
    }
}

fn authed<T>(m: T) -> Request<T> {
    let mut r = Request::new(m);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    r
}

#[tokio::test]
async fn every_read_is_recorded_with_who_and_which_subject_before_it_is_served() {
    let mut s = serve(None, 1024).await;
    for subject in [11u64, 22, 33] {
        s.client
            .snapshot(authed(SnapshotRequest {
                min_sequence: 0,
                subject,
                as_of_us: u64::MAX,
                edge_types: vec![],
            }))
            .await
            .unwrap();
    }
    s.client
        .traverse(authed(TraverseRequest {
            min_sequence: 0,
            start: 44,
            as_of_us: u64::MAX,
            max_hops: 1,
            ..TraverseRequest::default()
        }))
        .await
        .unwrap();

    let records = s.log.records().unwrap();
    let seen: Vec<(String, u64)> = records.iter().map(|r| (r.rpc.clone(), r.src)).collect();
    assert_eq!(
        seen,
        vec![
            ("Snapshot".to_string(), 11),
            ("Snapshot".to_string(), 22),
            ("Snapshot".to_string(), 33),
            ("Traverse".to_string(), 44),
        ]
    );
    assert!(records.iter().all(|r| r.principal == "api-key"));
    assert!(verify_chain(records).is_ok());
}

#[tokio::test]
async fn a_denied_request_is_audited_and_returns_nothing() {
    let mut s = serve(
        Some("[principals]\n\"someone-else\" = [\"reader\"]\n"),
        1024,
    )
    .await;
    let err = s
        .client
        .snapshot(authed(SnapshotRequest {
            min_sequence: 0,
            subject: 99,
            as_of_us: u64::MAX,
            edge_types: vec![],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    // Denials are queued without waiting; give the writer a moment.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let records = s.log.records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, "denied");
    assert_eq!(records[0].rpc, "Snapshot");
    assert_eq!(records[0].principal, "api-key");
}

#[tokio::test]
async fn reads_are_refused_not_served_unaudited_when_the_log_cannot_keep_up() {
    let s = serve(None, 1).await;
    let mut tasks = Vec::new();
    for i in 0..400u64 {
        let mut client = s.client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .snapshot(authed(SnapshotRequest {
                    min_sequence: 0,
                    subject: i,
                    as_of_us: u64::MAX,
                    edge_types: vec![],
                }))
                .await
        }));
    }
    let (mut served, mut refused) = (0u64, 0u64);
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => served += 1,
            Err(e) => {
                assert_eq!(e.code(), Code::Unavailable, "{}", e.message());
                refused += 1;
            }
        }
    }
    assert!(
        refused > 0,
        "with a one-slot audit queue some reads must be refused"
    );
    assert_eq!(
        s.log.records().unwrap().len() as u64,
        served,
        "a read was served if and only if its audit record is on disk"
    );
}
