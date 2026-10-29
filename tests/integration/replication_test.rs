//! Replication against real databases and real gRPC streams, in-process and
//! without the embedding workers (so it runs in the fast suite): a leader's
//! write batches are shipped to a follower, which must end up byte-for-byte the
//! same. Failover, fencing and the synchronous-acknowledgement gate are checked
//! on the same footing.

// tonic fixes `Status` as the error type of a stream item; the scripted-leader helpers below
// return stream items.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use caregraph::api::admin::proto::admin_service_client::AdminServiceClient;
use caregraph::api::admin::proto::admin_service_server::AdminServiceServer;
use caregraph::api::admin::proto::{DemoteRequest, PromoteRequest, StatusRequest};
use caregraph::api::admin::AdminOps;
use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::{AddEdgeRequest, EdgeType, ModelKind, SnapshotRequest};
use caregraph::api::{CareGraphApi, ServiceOptions};
use caregraph::replication::bootstrap::{fetch, fetch_with_retry};
use caregraph::replication::epoch::EpochHistory;
use caregraph::replication::follower::{self, FollowerOptions};
use caregraph::replication::proto::checkpoint_chunk::Payload;
use caregraph::replication::proto::replication_service_client::ReplicationServiceClient;
use caregraph::replication::proto::replication_service_server::{
    ReplicationService, ReplicationServiceServer,
};
use caregraph::replication::proto::{
    AckRequest, AckResponse, CheckpointChunk, CheckpointFile, CheckpointFooter, CheckpointHeader,
    FetchCheckpointRequest, FileChunk, ReplicateRequest, ReplicatedBatch,
};
use caregraph::replication::{ensure_epoch, AckOutcome, ReplicationConfig, ReplicationState, Role};
use caregraph::security::audit::{Fields, MutationAudit};
use caregraph::storage::{cf, KvStore, RocksKv, WalRetention};
use rocksdb::WriteBatch;
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Server};
use tonic::Code;

type Dump = BTreeMap<(&'static str, Vec<u8>), Vec<u8>>;

fn dump(store: &RocksKv) -> Dump {
    let mut out = Dump::new();
    for name in cf::ALL {
        store
            .scan_from_narrow_prefix(name, &[], &[], &mut |k, v| {
                out.insert((name, k.to_vec()), v.to_vec());
                ControlFlow::Continue(())
            })
            .unwrap();
    }
    out
}

/// One atomic batch per record, touching several column families and carrying
/// an audit record, like a real mutation.
fn commit(store: &RocksKv, audit: &MutationAudit, range: std::ops::RangeInclusive<u64>) {
    let edges = store.cf_handle(cf::CF_EDGES).unwrap();
    let nodes = store.cf_handle(cf::CF_NODES).unwrap();
    let embeddings = store.cf_handle(cf::CF_EMBEDDINGS).unwrap();
    for i in range {
        let mut batch = WriteBatch::default();
        batch.put_cf(&edges, i.to_be_bytes(), format!("edge-{i}"));
        batch.put_cf(&nodes, i.to_be_bytes(), format!("node-{i}"));
        batch.put_cf(&embeddings, i.to_be_bytes(), vec![i as u8; 32]);
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

async fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

struct Node {
    api: CareGraphApi,
    addr: SocketAddr,
    server: JoinHandle<()>,
    follower: Option<(watch::Sender<bool>, JoinHandle<()>)>,
}

struct Spec<'a> {
    dir: &'a Path,
    role: Role,
    leader: Option<SocketAddr>,
    min_acks: usize,
    ack_timeout: Duration,
    /// Data key, when the node's database is encrypted at rest.
    key: Option<[u8; 32]>,
}

impl<'a> Spec<'a> {
    fn leader(dir: &'a Path) -> Self {
        Spec {
            dir,
            role: Role::Leader,
            leader: None,
            min_acks: 0,
            ack_timeout: Duration::from_secs(5),
            key: None,
        }
    }
    fn follower(dir: &'a Path, leader: SocketAddr) -> Self {
        Spec {
            dir,
            role: Role::Follower,
            leader: Some(leader),
            min_acks: 0,
            ack_timeout: Duration::from_secs(5),
            key: None,
        }
    }
}

async fn start(spec: Spec<'_>) -> Node {
    std::fs::create_dir_all(spec.dir).unwrap();
    let store = RocksKv::open_with(
        spec.dir.join("db"),
        spec.key.as_ref().map(|k| &k[..]),
        WalRetention {
            ttl_seconds: 3600,
            size_limit_mb: 0,
        },
    )
    .unwrap();
    let epoch = EpochHistory::read(&store).unwrap().current();
    let repl = ReplicationState::open(
        ReplicationConfig {
            role: spec.role,
            replica_id: spec.dir.file_name().unwrap().to_string_lossy().into_owned(),
            min_acks: spec.min_acks,
            ack_timeout: spec.ack_timeout,
            sync_lag_threshold: 1_000_000,
            max_lag_for_ready: 1_000_000,
            leader_addr: spec.leader.map(|a| format!("http://{a}")),
            state_file: Some(spec.dir.join("role.json")),
            bootstrap_dir: Some(spec.dir.join("bootstrap")),
        },
        epoch,
    )
    .unwrap();
    ensure_epoch(&store, &repl).unwrap();
    let registry = prometheus::Registry::new();
    let admin = AdminOps::new(None, None, &registry).unwrap();
    let api = CareGraphApi::with_options(
        store,
        None,
        None,
        &registry,
        ServiceOptions {
            admin: Some(admin),
            replication: Some(repl.clone()),
            ..ServiceOptions::default()
        },
    )
    .unwrap();

    let addr = free_addr();
    let server = {
        let api = api.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(ReplicationServiceServer::new(api.clone()))
                .add_service(AdminServiceServer::new(api.clone()))
                .add_service(CareGraphServiceServer::new(api))
                .serve(addr)
                .await
                .unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let follower = (repl.role() == Role::Follower).then(|| {
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(follower::run(
            api.clone(),
            FollowerOptions {
                leader_addr: format!("http://{}", spec.leader.unwrap()),
                ..FollowerOptions::default()
            },
            stop_rx,
        ));
        (stop_tx, task)
    });
    Node {
        api,
        addr,
        server,
        follower,
    }
}

impl Node {
    fn store(&self) -> &RocksKv {
        self.api.store()
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://{}", self.addr))
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    async fn admin(&self) -> AdminServiceClient<Channel> {
        AdminServiceClient::new(self.channel().await)
    }

    /// Stops the node and releases its database so it can be reopened.
    async fn stop(self) {
        if let Some((stop, task)) = self.follower {
            let _ = stop.send(true);
            let _ = task.await;
        }
        self.server.abort();
        let _ = self.server.await;
        drop(self.api);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

fn dirs() -> (TempDir, PathBuf, PathBuf) {
    let root = TempDir::new().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    (root, a, b)
}

async fn converged(leader: &Node, follower: &Node) {
    wait_until("the follower to catch up", Duration::from_secs(20), || {
        follower.store().latest_sequence() == leader.store().latest_sequence()
            && follower.api.replication().applied() == leader.store().latest_sequence()
    })
    .await;
}

#[tokio::test]
async fn a_follower_converges_to_the_leaders_exact_state_and_sequence() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    // History that exists before the follower connects: replayed from the log.
    commit(leader.store(), &audit, 1..=100);

    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;
    assert_eq!(dump(follower.store()), dump(leader.store()));

    // Then live writes.
    commit(leader.store(), &audit, 101..=160);
    converged(&leader, &follower).await;
    assert_eq!(
        follower.store().latest_sequence(),
        leader.store().latest_sequence(),
        "sequence numbers are identical, not just the data"
    );
    assert_eq!(dump(follower.store()), dump(leader.store()));
    assert_eq!(
        caregraph::security::audit::verify_store(follower.store())
            .unwrap()
            .unwrap()
            .records,
        160,
        "the audit chain replicates with the data and verifies on the follower"
    );
    assert!(follower.api.replication().ready_for_reads());
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn a_follower_resumes_after_a_restart_with_no_gaps_or_duplicates() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=40);

    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;
    follower.stop().await;

    // The leader keeps writing while the follower is down.
    commit(leader.store(), &audit, 41..=90);

    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;
    assert_eq!(dump(follower.store()), dump(leader.store()));
    assert_eq!(
        caregraph::security::audit::verify_store(follower.store())
            .unwrap()
            .unwrap()
            .records,
        90,
        "every record exactly once"
    );
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn a_follower_that_asks_for_history_the_log_no_longer_has_is_told_to_bootstrap() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=30);
    // Flush every family so the log is rolled and the old file is archived,
    // then remove the archive: the log no longer reaches back to sequence 1.
    for name in cf::ALL {
        leader.store().flush(name).unwrap();
    }
    commit(leader.store(), &audit, 31..=35);
    let archive = a.join("db").join("archive");
    let mut removed = 0;
    if archive.exists() {
        for entry in std::fs::read_dir(&archive).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
            removed += 1;
        }
    }
    assert!(removed > 0, "the test needs an archived log file to remove");

    // While the leader process is up, its in-memory tail still holds these
    // batches and would serve them, correctly. Restart it so only the log is
    // left to read.
    leader.stop().await;
    let leader = start(Spec::leader(&a)).await;

    let follower = start(Spec::follower(&b, leader.addr)).await;
    wait_until(
        "the follower to be told it needs a bootstrap",
        Duration::from_secs(10),
        || follower.api.replication().needs_bootstrap(),
    )
    .await;
    assert!(!follower.api.replication().ready_for_reads());
    assert_eq!(
        follower.store().latest_sequence(),
        0,
        "nothing was applied past a gap"
    );
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn a_follower_with_a_different_schema_is_refused() {
    let (_root, a, _b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let mut client = ReplicationServiceClient::new(leader.channel().await);

    let mut cfs: std::collections::HashMap<String, u32> = leader
        .store()
        .cf_ids()
        .unwrap()
        .clone()
        .into_iter()
        .collect();
    let status = client
        .replicate(ReplicateRequest {
            follower_id: "x".into(),
            from_seq: 1,
            follower_epoch: 0,
            column_families: cfs.clone(),
        })
        .await;
    assert!(status.is_ok(), "the matching map is accepted");

    *cfs.get_mut(cf::CF_EDGES).unwrap() += 1;
    let err = client
        .replicate(ReplicateRequest {
            follower_id: "x".into(),
            from_seq: 1,
            follower_epoch: 0,
            column_families: cfs.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().starts_with("schema_mismatch"), "{err}");

    cfs.remove(cf::CF_AUDIT);
    assert!(client
        .replicate(ReplicateRequest {
            follower_id: "x".into(),
            from_seq: 1,
            follower_epoch: 0,
            column_families: cfs,
        })
        .await
        .is_err());
    leader.stop().await;
}

#[tokio::test]
async fn a_planned_failover_loses_nothing_and_the_old_leader_is_fenced() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=50);
    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;

    let before = leader
        .admin()
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!((before.role.as_str(), before.epoch), ("leader", 1));
    assert_eq!(before.followers.len(), 1, "the leader sees its standby");
    let standby = follower
        .admin()
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!((standby.role.as_str(), standby.epoch), ("follower", 1));
    assert!(standby.ready_for_reads && !standby.needs_bootstrap);

    // Demote the old leader: writes stop, and the final sequence is reported.
    let demoted = leader
        .admin()
        .await
        .demote(DemoteRequest { timeout_ms: 5_000 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(demoted.final_sequence, leader.store().latest_sequence());
    assert!(demoted.follower_caught_up);
    assert_eq!(leader.api.replication().role(), Role::Demoted);

    // Promote the standby, naming the sequence it must have reached.
    let promoted = follower
        .admin()
        .await
        .promote(PromoteRequest {
            wait_for_sequence: demoted.final_sequence,
            wait_timeout_ms: 5_000,
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(promoted.epoch, 2);
    assert!(
        promoted.sequence > demoted.final_sequence,
        "the epoch record is written"
    );
    assert_eq!(follower.api.replication().role(), Role::Leader);

    // Nothing was lost: everything the old leader committed is on the new one.
    let (old, new) = (dump(leader.store()), dump(follower.store()));
    for (key, value) in &old {
        assert_eq!(
            new.get(key),
            Some(value),
            "{:?} survived the failover",
            key.0
        );
    }
    let history = EpochHistory::read(follower.store()).unwrap();
    assert_eq!(history.current(), 2);
    assert_eq!(history.start_of(2), Some(demoted.final_sequence + 1));

    // The old leader refuses writes, the new one accepts them.
    let mut old_client = CareGraphServiceClient::new(leader.channel().await);
    let err = old_client.add_edge(add_edge()).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("replaced as leader"), "{err}");
    assert!(follower.api.replication().writable().is_ok());

    // The new leader can be written to directly and keeps a valid audit chain.
    let new_audit = MutationAudit::open(follower.store()).unwrap();
    commit(follower.store(), &new_audit, 51..=60);
    assert_eq!(
        caregraph::security::audit::verify_store(follower.store())
            .unwrap()
            .unwrap()
            .records,
        60
    );
    follower.stop().await;
    leader.stop().await;
}

fn add_edge() -> AddEdgeRequest {
    AddEdgeRequest {
        src: 1,
        dst: 2,
        edge_type: EdgeType::DiagnosedWith as i32,
        timestamp_us: 1_000_000,
        properties_json: "{}".into(),
        model: ModelKind::Graphsage as i32,
        ..AddEdgeRequest::default()
    }
}

#[tokio::test]
async fn a_follower_refuses_client_writes_and_says_where_to_send_them() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let follower = start(Spec::follower(&b, leader.addr)).await;
    let mut client = CareGraphServiceClient::new(follower.channel().await);
    let err = client.add_edge(add_edge()).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("read replica"), "{err}");
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn a_leader_that_learns_of_a_newer_epoch_fences_itself() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=20);
    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;

    // The standby is promoted while the old leader is still up and unaware
    // (the operator could not reach it).
    follower
        .admin()
        .await
        .promote(PromoteRequest {
            wait_for_sequence: 0,
            wait_timeout_ms: 0,
            force: true,
        })
        .await
        .unwrap();
    assert!(
        leader.api.replication().writable().is_ok(),
        "it does not know yet"
    );

    // Any node that is in the newer epoch and contacts it proves it stale.
    let mut client = ReplicationServiceClient::new(leader.channel().await);
    let err = client
        .replicate(ReplicateRequest {
            follower_id: "new-follower".into(),
            from_seq: 2,
            follower_epoch: 2,
            column_families: leader
                .store()
                .cf_ids()
                .unwrap()
                .clone()
                .into_iter()
                .collect(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().starts_with("stale_leader"), "{err}");
    assert_eq!(leader.api.replication().role(), Role::Fenced);
    assert!(leader.api.replication().writable().is_err());

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn an_ack_from_a_newer_epoch_fences_the_leader() {
    let (_root, a, _b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let mut client = ReplicationServiceClient::new(leader.channel().await);
    let ok = client
        .ack(AckRequest {
            follower_id: "f".into(),
            applied_seq: 1,
            epoch: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ok.leader_epoch, 1);
    assert!(leader.api.replication().writable().is_ok());

    let err = client
        .ack(AckRequest {
            follower_id: "f".into(),
            applied_seq: 1,
            epoch: 5,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(leader.api.replication().role(), Role::Fenced);
    leader.stop().await;
}

#[tokio::test]
async fn a_deposed_leader_that_kept_writing_cannot_rejoin_without_a_bootstrap() {
    let (_root, a, b) = dirs();
    let old = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(old.store()).unwrap();
    commit(old.store(), &audit, 1..=20);
    let standby = start(Spec::follower(&b, old.addr)).await;
    converged(&old, &standby).await;

    // Promote without demoting (force), then the old leader keeps writing:
    // its history now forks from the new leader's.
    standby
        .admin()
        .await
        .promote(PromoteRequest {
            wait_for_sequence: 0,
            wait_timeout_ms: 0,
            force: true,
        })
        .await
        .unwrap();
    commit(old.store(), &audit, 21..=30);

    // Asking the new leader to stream from the old leader's position (epoch 1,
    // past the point where epoch 2 began) is a divergence.
    let mut client = ReplicationServiceClient::new(standby.channel().await);
    let err = client
        .replicate(ReplicateRequest {
            follower_id: "old-leader".into(),
            from_seq: old.store().latest_sequence() + 1,
            follower_epoch: 1,
            column_families: old.store().cf_ids().unwrap().clone().into_iter().collect(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().starts_with("needs_bootstrap"), "{err}");
    assert!(err.message().contains("diverged"), "{err}");
    standby.stop().await;
    old.stop().await;
}

#[tokio::test]
async fn promotion_waits_for_the_sequence_it_was_told_to_reach_unless_forced() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=10);
    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;

    let mut admin = follower.admin().await;
    let ahead = follower.store().latest_sequence() + 1_000;
    let err = admin
        .promote(PromoteRequest {
            wait_for_sequence: ahead,
            wait_timeout_ms: 150,
            force: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded);
    assert_eq!(
        follower.api.replication().role(),
        Role::Follower,
        "still a follower"
    );

    let forced = admin
        .promote(PromoteRequest {
            wait_for_sequence: ahead,
            wait_timeout_ms: 150,
            force: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(forced.epoch, 2);

    // Only a follower can be promoted, only a leader demoted.
    let err = admin.promote(PromoteRequest::default()).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let mut leader_admin = leader.admin().await;
    leader_admin
        .demote(DemoteRequest { timeout_ms: 1_000 })
        .await
        .unwrap();
    // Repeating a demotion is allowed: that is how an interrupted one finishes.
    leader_admin
        .demote(DemoteRequest { timeout_ms: 1_000 })
        .await
        .unwrap();
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn a_synchronous_leader_holds_the_ack_until_the_standby_has_the_write() {
    let (_root, a, b) = dirs();
    let leader = start(Spec {
        min_acks: 1,
        ..Spec::leader(&a)
    })
    .await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    let repl = leader.api.replication().clone();

    // No standby yet: degraded to asynchronous, and counted.
    commit(leader.store(), &audit, 1..=3);
    let seq = leader.store().latest_sequence();
    assert_eq!(
        repl.wait_acked(seq, || leader.store().latest_sequence())
            .await,
        AckOutcome::Degraded
    );

    let follower = start(Spec::follower(&b, leader.addr)).await;
    wait_until("the standby to register", Duration::from_secs(10), || {
        repl.follower_count() == 1
    })
    .await;
    converged(&leader, &follower).await;

    // Each write is acknowledged only once the standby has it.
    let mut waits = Vec::new();
    for i in 4..=33u64 {
        commit(leader.store(), &audit, i..=i);
        let seq = leader.store().latest_sequence();
        let t = Instant::now();
        let outcome = repl
            .wait_acked(seq, || leader.store().latest_sequence())
            .await;
        waits.push(t.elapsed());
        assert_eq!(outcome, AckOutcome::Acked, "write {i}");
        assert!(
            follower.store().latest_sequence() >= seq,
            "acknowledged, so the standby must already hold sequence {seq}"
        );
    }
    waits.sort();
    eprintln!(
        "sync ack wait over 30 writes: p50 {:?} max {:?}",
        waits[waits.len() / 2],
        waits[waits.len() - 1]
    );

    // The standby goes away: writes degrade again rather than failing.
    follower.stop().await;
    wait_until("the standby to deregister", Duration::from_secs(10), || {
        repl.follower_count() == 0
    })
    .await;
    commit(leader.store(), &audit, 34..=34);
    assert_eq!(
        repl.wait_acked(leader.store().latest_sequence(), || leader
            .store()
            .latest_sequence())
            .await,
        AckOutcome::Degraded
    );
    leader.stop().await;
}

#[tokio::test]
async fn a_replica_read_can_wait_for_a_sequence_and_refuses_one_it_will_not_reach() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=5);
    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;
    let mut client = CareGraphServiceClient::new(follower.channel().await);

    let snapshot = |min_sequence: u64| SnapshotRequest {
        subject: 3,
        as_of_us: u64::MAX,
        min_sequence,
        ..SnapshotRequest::default()
    };
    let reachable = follower.store().latest_sequence();
    let ok = client.snapshot(snapshot(reachable)).await;
    let none = client.snapshot(snapshot(0)).await;
    assert_eq!(
        ok.as_ref().err().map(|e| e.code()),
        none.as_ref().err().map(|e| e.code()),
        "a satisfied min_sequence behaves like none"
    );

    let t = Instant::now();
    let err = client
        .snapshot(snapshot(reachable + 10_000))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert!(
        t.elapsed() >= Duration::from_secs(1),
        "it waited before giving up"
    );

    // A write that arrives while the read waits unblocks it.
    let wait = {
        let target = reachable + 1;
        let mut c = client.clone();
        tokio::spawn(async move { c.snapshot(snapshot(target)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    commit(leader.store(), &audit, 6..=6);
    let result = wait.await.unwrap();
    assert_ne!(result.err().map(|e| e.code()), Some(Code::Unavailable));
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn replicate_streams_the_leaders_own_batches_and_acks_report_its_state() {
    // Two leaders with different histories produce different streams.
    let (_r1, a, _) = dirs();
    let (_r2, c, _) = dirs();
    let one = start(Spec::leader(&a)).await;
    let two = start(Spec::leader(&c)).await;
    commit(
        one.store(),
        &MutationAudit::open(one.store()).unwrap(),
        1..=5,
    );
    commit(
        two.store(),
        &MutationAudit::open(two.store()).unwrap(),
        1..=9,
    );

    /// Every batch from sequence 1 up to the first heartbeat (the leader has
    /// nothing more to send).
    async fn all_batches(n: &Node) -> Vec<(u64, u32, Vec<u8>)> {
        let mut client = ReplicationServiceClient::new(n.channel().await);
        let mut stream = client
            .replicate(ReplicateRequest {
                follower_id: "probe".into(),
                from_seq: 1,
                follower_epoch: 0,
                column_families: n.store().cf_ids().unwrap().clone().into_iter().collect(),
            })
            .await
            .unwrap()
            .into_inner();
        let mut out = Vec::new();
        loop {
            let m = stream.message().await.unwrap().unwrap();
            if m.count == 0 {
                assert_eq!(
                    m.leader_seq,
                    n.store().latest_sequence(),
                    "heartbeat reports the tail"
                );
                return out;
            }
            assert_eq!(m.epoch, 1);
            out.push((m.seq, m.count, m.data));
        }
    }
    let (s1, s2) = (all_batches(&one).await, all_batches(&two).await);
    assert_eq!(
        (s1.len(), s2.len()),
        (6, 10),
        "the epoch record plus one batch per commit"
    );
    assert_eq!(s1[0].0, 1, "the stream starts exactly where asked");
    for w in s1.windows(2) {
        assert_eq!(w[1].0, w[0].0 + u64::from(w[0].1), "contiguous");
    }
    assert!(s1 != s2, "different leaders, different streams");
    // The streamed bytes are the leader's own write batches, byte for byte.
    let replayed = RocksKv::open(_r1.path().join("replayed")).unwrap();
    for (seq, count, data) in &s1 {
        replayed.apply_replicated(*seq, *count, data).unwrap();
    }
    assert_eq!(dump(&replayed), dump(one.store()));

    let mut c1 = ReplicationServiceClient::new(one.channel().await);
    let mut c2 = ReplicationServiceClient::new(two.channel().await);
    let ack = |id: &str| AckRequest {
        follower_id: id.into(),
        applied_seq: 1,
        epoch: 1,
    };
    let r1 = c1.ack(ack("f")).await.unwrap().into_inner();
    let r2 = c2.ack(ack("f")).await.unwrap().into_inner();
    assert_eq!(r1.leader_seq, one.store().latest_sequence());
    assert_eq!(r2.leader_seq, two.store().latest_sequence());
    assert_ne!(r1.leader_seq, r2.leader_seq);

    // A standalone node, or a follower, does not serve the stream.
    let (_r3, d, e) = dirs();
    let follower = start(Spec::follower(&d, one.addr)).await;
    let mut cf = ReplicationServiceClient::new(follower.channel().await);
    let err = cf
        .replicate(ReplicateRequest {
            follower_id: "x".into(),
            from_seq: 1,
            follower_epoch: 0,
            column_families: follower
                .store()
                .cf_ids()
                .unwrap()
                .clone()
                .into_iter()
                .collect(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let _ = e;
    follower.stop().await;
    one.stop().await;
    two.stop().await;
}

#[test]
fn the_apply_path_refuses_a_batch_out_of_order_and_leaves_the_database_alone() {
    let dir = TempDir::new().unwrap();
    let leader = RocksKv::open(dir.path().join("l")).unwrap();
    let follower = RocksKv::open(dir.path().join("f")).unwrap();
    let nodes = leader.cf_handle(cf::CF_NODES).unwrap();
    let mut batch = WriteBatch::default();
    batch.put_cf(&nodes, b"n1", b"v");
    batch.put_cf(&nodes, b"n2", b"v");
    let count = batch.len() as u32;
    let data = batch.data().to_vec();

    let err = follower.apply_replicated(5, count, &data).unwrap_err();
    assert!(
        matches!(err, caregraph::CareGraphError::Diverged(_)),
        "{err}"
    );
    let err = follower.apply_replicated(1, count + 1, &data).unwrap_err();
    assert!(
        matches!(err, caregraph::CareGraphError::Diverged(_)),
        "{err}"
    );
    assert_eq!(follower.latest_sequence(), 0, "nothing was applied");
    assert!(follower.get(cf::CF_NODES, b"n1").unwrap().is_none());

    follower.apply_replicated(1, count, &data).unwrap();
    assert_eq!(follower.latest_sequence(), u64::from(count));
    assert_eq!(follower.get(cf::CF_NODES, b"n2").unwrap().unwrap(), b"v");
    // Applying it again (a duplicate delivery) is a divergence, not a no-op.
    assert!(follower.apply_replicated(1, count, &data).is_err());
}

#[test]
fn a_replicated_batch_that_touches_nodes_clears_the_node_type_cache_and_others_do_not() {
    use caregraph::types::{NodeId, Timestamp};
    let dir = TempDir::new().unwrap();
    let leader = RocksKv::open(dir.path().join("l")).unwrap();
    let follower = RocksKv::open(dir.path().join("f")).unwrap();
    let cache = follower.node_type_cache().unwrap();
    cache.insert(NodeId(7), Timestamp(10), 2);
    assert!(cache.get(NodeId(7), Timestamp(20)).is_some());

    let edges = leader.cf_handle(cf::CF_EDGES).unwrap();
    let nodes = leader.cf_handle(cf::CF_NODES).unwrap();
    let mut first = WriteBatch::default();
    first.put_cf(&edges, b"e", b"v");
    let (c1, d1) = (first.len() as u32, first.data().to_vec());
    follower.apply_replicated(1, c1, &d1).unwrap();
    assert!(
        cache.get(NodeId(7), Timestamp(20)).is_some(),
        "an edge-only batch leaves the cache alone"
    );

    let mut second = WriteBatch::default();
    second.put_cf(&nodes, b"n", b"v");
    follower
        .apply_replicated(1 + u64::from(c1), second.len() as u32, second.data())
        .unwrap();
    assert!(
        cache.get(NodeId(7), Timestamp(20)).is_none(),
        "a batch that writes nodes invalidates it"
    );
}

#[test]
fn the_in_memory_tail_holds_exactly_the_batches_written_and_is_bounded() {
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path()).unwrap();
    let nodes = store.cf_handle(cf::CF_NODES).unwrap();

    // Single-operation writes (put, delete) go through the same path as batches.
    store.put(cf::CF_NODES, b"a", b"1").unwrap();
    store.delete(cf::CF_NODES, b"a").unwrap();
    let mut batch = WriteBatch::default();
    batch.put_cf(&nodes, b"b", b"2");
    batch.put_cf(&nodes, b"c", b"3");
    batch.put_cf(&nodes, b"d", b"4");
    store.write(batch).unwrap();
    store.put(cf::CF_NODES, b"e", b"5").unwrap();

    let tail = store.recent_since(1, 100, usize::MAX).unwrap();
    let shape: Vec<(u64, u32)> = tail.iter().map(|b| (b.first_seq, b.count)).collect();
    assert_eq!(
        shape,
        vec![(1, 1), (2, 1), (3, 3), (6, 1)],
        "exact sequence ranges"
    );
    assert_eq!(store.latest_sequence(), 6);

    // Byte-identical to what the write-ahead log holds.
    let logged: Vec<(u64, Vec<u8>)> = store
        .updates_since(0)
        .unwrap()
        .map(|r| {
            let (seq, b) = r.unwrap();
            (seq, b.data().to_vec())
        })
        .collect();
    assert_eq!(logged.len(), tail.len());
    for (b, (seq, data)) in tail.iter().zip(&logged) {
        assert_eq!((b.first_seq, b.data.as_ref()), (*seq, data));
    }

    // Starting mid-tail, and asking for a position that is not a batch start.
    assert_eq!(store.recent_since(3, 100, usize::MAX).unwrap().len(), 2);
    assert!(
        store.recent_since(4, 100, usize::MAX).is_none(),
        "4 is inside a batch"
    );
    assert!(
        store.recent_since(7, 100, usize::MAX).is_none(),
        "nothing written yet"
    );
    assert_eq!(
        store.recent_since(1, 2, usize::MAX).unwrap().len(),
        2,
        "batch limit"
    );

    // The tail is bounded: the oldest batches fall out, the newest stay.
    for i in 0..9_000u32 {
        store.put(cf::CF_NODES, &i.to_be_bytes(), b"v").unwrap();
    }
    let newest = store.latest_sequence();
    assert!(store.recent_since(1, 100, usize::MAX).is_none(), "evicted");
    let last = store.recent_since(newest, 100, usize::MAX).unwrap();
    assert_eq!((last.len(), last[0].first_seq), (1, newest));
}

// ---------------------------------------------------------------------------
// Building a replica over the network: FetchCheckpoint
// ---------------------------------------------------------------------------

fn options_for(addr: SocketAddr) -> FollowerOptions {
    FollowerOptions {
        leader_addr: format!("http://{addr}"),
        ..FollowerOptions::default()
    }
}

#[tokio::test]
async fn a_replica_built_from_the_leaders_checkpoint_has_its_exact_state_and_then_follows() {
    let (_root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=200);

    let dest = b.join("db");
    let done = fetch_with_retry(
        &options_for(leader.addr),
        "replica-1",
        &dest,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(done.sequence, leader.store().latest_sequence());
    assert!(done.files > 0 && done.bytes > 0);
    assert!(
        !b.join("db.partial").exists(),
        "the staging directory is gone after a successful fetch"
    );

    // The copy, opened on its own, is the leader's database as of the checkpoint.
    {
        let copy = RocksKv::open(&dest).unwrap();
        assert_eq!(copy.latest_sequence(), done.sequence);
        assert_eq!(dump(&copy), dump(leader.store()));
    }

    // And it can become a follower and catch up on what happened after.
    commit(leader.store(), &audit, 201..=260);
    let follower = start(Spec::follower(&b, leader.addr)).await;
    converged(&leader, &follower).await;
    assert_eq!(dump(follower.store()), dump(leader.store()));

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn a_later_checkpoint_reports_a_later_sequence() {
    let (root, a, _b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=10);
    let first = fetch(&options_for(leader.addr), "r", &root.path().join("first"))
        .await
        .unwrap();
    commit(leader.store(), &audit, 11..=40);
    let second = fetch(&options_for(leader.addr), "r", &root.path().join("second"))
        .await
        .unwrap();
    assert!(second.sequence > first.sequence, "{first:?} vs {second:?}");
    assert_eq!(second.sequence, leader.store().latest_sequence());
    leader.stop().await;
}

#[tokio::test]
async fn a_follower_does_not_serve_checkpoints() {
    let (root, a, b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let follower = start(Spec::follower(&b, leader.addr)).await;
    let error = fetch(&options_for(follower.addr), "r", &root.path().join("out"))
        .await
        .unwrap_err();
    let status = error
        .chain()
        .find_map(|c| c.downcast_ref::<tonic::Status>())
        .expect("a gRPC status");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(!root.path().join("out").exists());
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn a_database_is_never_overwritten_by_a_bootstrap() {
    let (root, a, _b) = dirs();
    let leader = start(Spec::leader(&a)).await;
    let dest = root.path().join("existing");
    std::fs::create_dir_all(&dest).unwrap();
    std::fs::write(dest.join("CURRENT"), b"precious").unwrap();
    let error = fetch_with_retry(
        &options_for(leader.addr),
        "r",
        &dest,
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("already holds data"),
        "{error:#}"
    );
    assert_eq!(std::fs::read(dest.join("CURRENT")).unwrap(), b"precious");
    leader.stop().await;
}

/// A stand-in leader that sends whatever checkpoint it is told to, to show the
/// client refuses every way a transfer can be wrong.
struct Scripted(Vec<Result<CheckpointChunk, tonic::Status>>);

#[tonic::async_trait]
impl ReplicationService for Scripted {
    type ReplicateStream =
        tokio_stream::wrappers::ReceiverStream<Result<ReplicatedBatch, tonic::Status>>;
    type FetchCheckpointStream =
        tokio_stream::wrappers::ReceiverStream<Result<CheckpointChunk, tonic::Status>>;

    async fn replicate(
        &self,
        _: tonic::Request<ReplicateRequest>,
    ) -> Result<tonic::Response<Self::ReplicateStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("not part of this test"))
    }

    async fn ack(
        &self,
        _: tonic::Request<AckRequest>,
    ) -> Result<tonic::Response<AckResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not part of this test"))
    }

    async fn fetch_checkpoint(
        &self,
        _: tonic::Request<FetchCheckpointRequest>,
    ) -> Result<tonic::Response<Self::FetchCheckpointStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        for m in &self.0 {
            let m = match m {
                Ok(c) => Ok(c.clone()),
                Err(s) => Err(tonic::Status::new(s.code(), s.message())),
            };
            tx.send(m).await.unwrap();
        }
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }
}

async fn scripted(chunks: Vec<Result<CheckpointChunk, tonic::Status>>) -> SocketAddr {
    let addr = free_addr();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ReplicationServiceServer::new(Scripted(chunks)))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

fn header(files: &[(&str, u64)]) -> Result<CheckpointChunk, tonic::Status> {
    Ok(CheckpointChunk {
        payload: Some(Payload::Header(CheckpointHeader {
            sequence: 7,
            epoch: 1,
            files: files
                .iter()
                .map(|(n, s)| CheckpointFile {
                    name: n.to_string(),
                    size: *s,
                })
                .collect(),
        })),
    })
}

fn file(name: &str, offset: u64, data: &[u8]) -> Result<CheckpointChunk, tonic::Status> {
    Ok(CheckpointChunk {
        payload: Some(Payload::File(FileChunk {
            name: name.to_string(),
            offset,
            data: data.to_vec(),
        })),
    })
}

fn footer(files: u64, bytes: u64) -> Result<CheckpointChunk, tonic::Status> {
    Ok(CheckpointChunk {
        payload: Some(Payload::Footer(CheckpointFooter { files, bytes })),
    })
}

async fn assert_refused(chunks: Vec<Result<CheckpointChunk, tonic::Status>>, why: &str) {
    let root = TempDir::new().unwrap();
    let addr = scripted(chunks).await;
    let dest = root.path().join("db");
    let error = fetch(&options_for(addr), "r", &dest).await.unwrap_err();
    assert!(
        !dest.exists(),
        "{why}: nothing may appear at the destination"
    );
    assert!(
        !root.path().join("db.partial").exists(),
        "{why}: the staging directory must be cleaned up"
    );
    eprintln!("{why}: refused with: {error:#}");
}

#[tokio::test]
async fn a_checkpoint_that_does_not_arrive_whole_is_refused_and_leaves_nothing() {
    // Ends without the footer.
    assert_refused(
        vec![header(&[("CURRENT", 4)]), file("CURRENT", 0, b"abcd")],
        "no footer",
    )
    .await;
    // Stops in the middle of a file.
    assert_refused(
        vec![
            header(&[("000001.sst", 10)]),
            file("000001.sst", 0, b"abc"),
            footer(1, 3),
        ],
        "short file",
    )
    .await;
    // More bytes than announced.
    assert_refused(
        vec![
            header(&[("000001.sst", 2)]),
            file("000001.sst", 0, b"abcd"),
            footer(1, 4),
        ],
        "long file",
    )
    .await;
    // A chunk at the wrong offset.
    assert_refused(
        vec![
            header(&[("000001.sst", 4)]),
            file("000001.sst", 2, b"ab"),
            footer(1, 2),
        ],
        "wrong offset",
    )
    .await;
    // A footer that disagrees with what arrived.
    assert_refused(
        vec![
            header(&[("CURRENT", 4)]),
            file("CURRENT", 0, b"abcd"),
            footer(1, 99),
        ],
        "footer mismatch",
    )
    .await;
    // The leader fails midway.
    assert_refused(
        vec![
            header(&[("CURRENT", 4)]),
            file("CURRENT", 0, b"ab"),
            Err(tonic::Status::internal("disk error on the leader")),
        ],
        "leader error",
    )
    .await;
}

#[tokio::test]
async fn a_leader_cannot_make_the_replica_write_outside_its_directory() {
    let root = TempDir::new().unwrap();
    let outside = root.path().join("evil");
    for name in ["../evil", "a/b", "..", "x\\y"] {
        let addr = scripted(vec![
            header(&[(name, 4)]),
            file(name, 0, b"evil"),
            footer(1, 4),
        ])
        .await;
        let dest = root.path().join("inner").join("db");
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        let error = fetch(&options_for(addr), "r", &dest).await.unwrap_err();
        assert!(
            error.to_string().contains("not a plain file name"),
            "{name:?}: {error:#}"
        );
        assert!(!outside.exists() && !dest.exists());
    }
}

#[tokio::test]
async fn a_well_formed_checkpoint_with_empty_files_is_assembled_exactly() {
    let root = TempDir::new().unwrap();
    let addr = scripted(vec![
        header(&[("CURRENT", 4), ("IDENTITY", 0), ("LOCK", 0)]),
        file("CURRENT", 0, b"ab"),
        file("CURRENT", 2, b"cd"),
        footer(3, 4),
    ])
    .await;
    let dest = root.path().join("db");
    let done = fetch(&options_for(addr), "r", &dest).await.unwrap();
    assert_eq!(
        (done.sequence, done.epoch, done.files, done.bytes),
        (7, 1, 3, 4)
    );
    assert_eq!(std::fs::read(dest.join("CURRENT")).unwrap(), b"abcd");
    assert_eq!(std::fs::read(dest.join("IDENTITY")).unwrap(), b"");
    assert_eq!(std::fs::read(dest.join("LOCK")).unwrap(), b"");
}

#[tokio::test]
async fn an_encrypted_leader_streams_files_that_are_still_encrypted_and_open_only_with_its_key() {
    let (_root, a, b) = dirs();
    let key = [7u8; 32];
    let leader = start(Spec {
        key: Some(key),
        ..Spec::leader(&a)
    })
    .await;
    let audit = MutationAudit::open(leader.store()).unwrap();
    commit(leader.store(), &audit, 1..=50);

    let dest = b.join("db");
    fetch(&options_for(leader.addr), "r", &dest).await.unwrap();

    // Nothing on the replica's disk is readable without the key: the plaintext
    // value the commits wrote ("edge-1", ...) appears in no file.
    for entry in std::fs::read_dir(&dest).unwrap().flatten() {
        let bytes = std::fs::read(entry.path()).unwrap();
        assert!(
            !bytes.windows(5).any(|w| w == b"edge-"),
            "{} holds plaintext",
            entry.path().display()
        );
    }
    assert!(
        RocksKv::open(&dest).is_err(),
        "opening without the key must fail"
    );
    let copy = RocksKv::open_encrypted(&dest, &key).unwrap();
    assert_eq!(dump(&copy), dump(leader.store()));
    drop(copy);
    leader.stop().await;
}
