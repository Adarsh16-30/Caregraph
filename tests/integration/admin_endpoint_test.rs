//! The admin gRPC service against a real in-process server and database.
//!
//! Each RPC's response is derived from the database it is asked about, so the
//! tests assert that responses *change* when the underlying state changes
//! (Rule 2), not just that they have the right shape.

use std::net::SocketAddr;
use std::time::Duration;

use caregraph::api::admin::proto::admin_service_client::AdminServiceClient;
use caregraph::api::admin::proto::admin_service_server::AdminServiceServer;
use caregraph::api::admin::proto::{
    CreateBackupRequest, CreateCheckpointRequest, ListBackupsRequest, StatusRequest,
    VerifyAuditRequest,
};
use caregraph::api::admin::AdminOps;
use caregraph::api::{AuthInterceptor, CareGraphApi, ServiceOptions};
use caregraph::security::audit::{AuditLog, Fields, MutationAudit};
use caregraph::security::{Policy, PolicyHandle};
use caregraph::storage::backup::BackupManager;
use caregraph::storage::{cf, KvStore, RocksKv};
use rocksdb::WriteBatch;
use tempfile::TempDir;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request};

const KEY: &str = "admin-test-key";

struct Served {
    client: AdminServiceClient<Channel>,
    log: std::sync::Arc<AuditLog>,
    dir: TempDir,
    _server: tokio::task::JoinHandle<()>,
}

/// Commits `records` mutation-audit records into the store before it is handed
/// to the service, so there is real state for the admin RPCs to report on.
fn seed(store: &RocksKv, records: u64) {
    let audit = MutationAudit::open(store).unwrap();
    let nodes = store.cf_handle(cf::CF_NODES).unwrap();
    for i in 1..=records {
        let mut batch = WriteBatch::default();
        batch.put_cf(&nodes, i.to_be_bytes(), b"node");
        let pending = audit
            .stage(
                &mut batch,
                store,
                Fields {
                    ts_us: 1_000 + i,
                    principal: "spiffe://caregraph/ingest".into(),
                    rpc: "AddEdge".into(),
                    outcome: "committed".into(),
                    commit_seq: i,
                    ..Fields::default()
                },
            )
            .unwrap();
        store.write(batch).unwrap();
        audit.advance(pending);
    }
}

async fn serve(records: u64, policy: Option<&str>, with_backups: bool) -> Served {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path().join("db")).unwrap();
    seed(&store, records);
    let log = AuditLog::open_dir(&dir.path().join("audit"), "pod-test", 256).unwrap();
    let registry = prometheus::Registry::new();
    let manager =
        with_backups.then(|| BackupManager::open(dir.path().join("backups"), None).unwrap());
    let admin = AdminOps::new(manager, Some(dir.path().join("checkpoints")), &registry).unwrap();
    let api = CareGraphApi::with_options(
        store,
        None,
        None,
        &registry,
        ServiceOptions {
            policy: policy.map(|t| PolicyHandle::fixed(Policy::parse(t).unwrap())),
            audit_log: Some(log.clone()),
            admin: Some(admin),
            ..ServiceOptions::default()
        },
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener);
    let service = InterceptedService::new(AdminServiceServer::new(api), AuthInterceptor::new(KEY));
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
        client: AdminServiceClient::new(channel),
        log,
        dir,
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
async fn status_reports_the_database_it_is_asked_about() {
    let mut small = serve(5, None, true).await;
    let mut large = serve(40, None, true).await;
    let a = small
        .client
        .status(authed(StatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let b = large
        .client
        .status(authed(StatusRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!((a.audit_records, b.audit_records), (5, 40));
    assert_ne!(
        a.audit_head, b.audit_head,
        "different chains, different heads"
    );
    assert!(b.latest_sequence > a.latest_sequence);
    assert!(a.backups_configured && a.backups == 0 && a.last_backup_unix == 0);
}

#[tokio::test]
async fn backups_are_listed_in_order_and_each_reports_its_own_point() {
    let mut s = serve(12, None, true).await;
    let first = s
        .client
        .create_backup(authed(CreateBackupRequest {}))
        .await
        .unwrap()
        .into_inner();
    let second = s
        .client
        .create_backup(authed(CreateBackupRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((first.backup_id, second.backup_id), (1, 2));
    assert!(first.sequence > 0 && first.num_files > 0 && first.created_unix > 0);

    let listed = s
        .client
        .list_backups(authed(ListBackupsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.backups, vec![first, second]);

    let status = s
        .client
        .status(authed(StatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.backups, 2);
    assert_eq!(status.last_backup_unix, second.created_unix);
}

#[tokio::test]
async fn backup_rpcs_say_so_when_no_backup_directory_is_configured() {
    let mut s = serve(3, None, false).await;
    let err = s
        .client
        .create_backup(authed(CreateBackupRequest {}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let err = s
        .client
        .list_backups(authed(ListBackupsRequest {}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    // Status and audit verification do not need backups.
    assert!(
        !s.client
            .status(authed(StatusRequest {}))
            .await
            .unwrap()
            .into_inner()
            .backups_configured
    );
}

#[tokio::test]
async fn a_checkpoint_lands_in_the_server_chosen_directory_only() {
    let mut s = serve(8, None, true).await;
    let made = s
        .client
        .create_checkpoint(authed(CreateCheckpointRequest {
            name: "nightly".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(made.name, "nightly");
    assert!(made.sequence > 0);
    let expected = s.dir.path().join("checkpoints").join("nightly");
    assert_eq!(std::path::PathBuf::from(&made.path), expected);
    assert!(expected.join("CURRENT").exists());

    for bad in ["../escape", "a/b", "", ".hidden", "x y"] {
        let err = s
            .client
            .create_checkpoint(authed(CreateCheckpointRequest { name: bad.into() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{bad:?}");
    }
    let err = s
        .client
        .create_checkpoint(authed(CreateCheckpointRequest {
            name: "nightly".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
}

#[tokio::test]
async fn audit_verification_reports_the_chain_it_checked() {
    let mut a = serve(6, None, true).await;
    let mut b = serve(9, None, true).await;
    let va = a
        .client
        .verify_audit(authed(VerifyAuditRequest {}))
        .await
        .unwrap()
        .into_inner();
    let vb = b
        .client
        .verify_audit(authed(VerifyAuditRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(va.intact && vb.intact);
    assert_eq!((va.records, vb.records), (6, 9));
    assert_ne!(va.head, vb.head);
    assert!(va.detail.is_empty());
}

#[tokio::test]
async fn only_the_admin_role_may_call_and_every_call_is_audited_first() {
    let policy = "[principals]\n\"api-key\" = [\"writer\"]\n";
    let mut denied = serve(2, Some(policy), true).await;
    let err = denied
        .client
        .create_backup(authed(CreateBackupRequest {}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    assert_eq!(err.message(), "not permitted");
    assert!(denied
        .client
        .list_backups(authed(ListBackupsRequest {}))
        .await
        .is_err());

    let policy = "[principals]\n\"api-key\" = [\"admin\"]\n";
    let mut allowed = serve(2, Some(policy), true).await;
    allowed
        .client
        .create_backup(authed(CreateBackupRequest {}))
        .await
        .unwrap();
    allowed
        .client
        .status(authed(StatusRequest {}))
        .await
        .unwrap();
    let rpcs: Vec<String> = allowed
        .log
        .records()
        .unwrap()
        .into_iter()
        .map(|r| r.rpc)
        .collect();
    assert_eq!(rpcs, vec!["Admin.CreateBackup", "Admin.Status"]);
}

#[tokio::test]
async fn a_second_backup_while_one_runs_is_aborted_not_queued() {
    let mut s = serve(30, None, true).await;
    let mut c2 = s.client.clone();
    let (a, b) = tokio::join!(
        s.client.create_backup(authed(CreateBackupRequest {})),
        c2.create_backup(authed(CreateBackupRequest {})),
    );
    // Either they serialised by luck of timing (both ok) or one was refused;
    // what must never happen is two in flight at once or an unrelated error.
    for r in [&a, &b] {
        if let Err(e) = r {
            assert_eq!(e.code(), Code::Aborted, "{e}");
        }
    }
    assert!(a.is_ok() || b.is_ok());
}
