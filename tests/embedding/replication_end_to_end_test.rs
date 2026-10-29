//! Replication with the real mutation pipeline: real GraphSAGE forward passes
//! on the leader, a synchronous standby, and a failover that keeps writing.
//!
//! The in-process integration suite proves the stream, fencing and the
//! acknowledgement gate on synthetic batches; this proves they hold for what the
//! server actually writes (edges, embeddings, provenance, idempotency records,
//! the audit chain and the high-water mark all in one batch).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::path::Path;
use std::time::{Duration, Instant};

use caregraph::api::admin::proto::admin_service_client::AdminServiceClient;
use caregraph::api::admin::proto::admin_service_server::AdminServiceServer;
use caregraph::api::admin::proto::{DemoteRequest, PromoteRequest};
use caregraph::api::admin::AdminOps;
use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::{
    AddEdgeRequest, EdgeType as ProtoEdgeType, ModelKind as ProtoModelKind, SnapshotRequest,
};
use caregraph::api::{CareGraphApi, ServiceOptions};
use caregraph::config::TsPolicy;
use caregraph::replication::epoch::EpochHistory;
use caregraph::replication::follower::{self, FollowerOptions};
use caregraph::replication::proto::replication_service_server::ReplicationServiceServer;
use caregraph::replication::{ensure_epoch, ReplicationConfig, ReplicationState, Role};
use caregraph::security::audit::verify_store;
use caregraph::storage::backup::create_checkpoint;
use caregraph::storage::{cf, KvStore, RocksKv, WalRetention};
use caregraph::temporal::record::NodeValue;
use caregraph::temporal::TemporalWriter;
use caregraph::types::{NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::watch;
use tonic::transport::{Channel, Server};
use tonic::Code;

const MODEL: &str = "diabetes130_graphsage";

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

fn seed_nodes(store: &RocksKv) {
    let writer = TemporalWriter::new(store).unwrap();
    let mut batch = WriteBatch::default();
    for p in 1..=6u64 {
        writer.put_node(
            &mut batch,
            NodeId(p),
            Timestamp(0),
            &NodeValue::new("Patient", json!({})),
        );
    }
    for c in 101..=103u64 {
        writer.put_node(
            &mut batch,
            NodeId(c),
            Timestamp(0),
            &NodeValue::new("Condition", json!({})),
        );
    }
    drop(writer);
    store.write(batch).unwrap();
}

struct Node {
    api: CareGraphApi,
    addr: SocketAddr,
    server: tokio::task::JoinHandle<()>,
    follower: Option<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
}

async fn start(
    dir: &Path,
    role: Role,
    leader: Option<SocketAddr>,
    min_acks: usize,
    seed: bool,
) -> Node {
    let store = RocksKv::open_with(
        dir.join("db"),
        None,
        WalRetention {
            ttl_seconds: 3600,
            size_limit_mb: 0,
        },
    )
    .unwrap();
    store.set_sync_writes(true);
    if seed {
        seed_nodes(&store);
    }
    let epoch = EpochHistory::read(&store).unwrap().current();
    let repl = ReplicationState::open(
        ReplicationConfig {
            role,
            replica_id: dir.file_name().unwrap().to_string_lossy().into_owned(),
            min_acks,
            ack_timeout: Duration::from_secs(10),
            leader_addr: leader.map(|a| format!("http://{a}")),
            state_file: Some(dir.join("role.json")),
            ..ReplicationConfig::default()
        },
        epoch,
    )
    .unwrap();
    ensure_epoch(&store, &repl).unwrap();
    let registry = prometheus::Registry::new();
    let api = CareGraphApi::with_options(
        store,
        Some(MODEL),
        None,
        &registry,
        ServiceOptions {
            ts_policy: TsPolicy::Strict,
            admin: Some(AdminOps::new(None, None, &registry).unwrap()),
            replication: Some(repl.clone()),
            ..ServiceOptions::default()
        },
    )
    .unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
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
    let follower = (role == Role::Follower).then(|| {
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(follower::run(
            api.clone(),
            FollowerOptions {
                leader_addr: format!("http://{}", leader.unwrap()),
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
    async fn client(&self) -> CareGraphServiceClient<Channel> {
        CareGraphServiceClient::new(self.channel().await)
    }
    async fn admin(&self) -> AdminServiceClient<Channel> {
        AdminServiceClient::new(self.channel().await)
    }
    async fn stop(self) {
        if let Some((stop, task)) = self.follower {
            let _ = stop.send(true);
            let _ = task.await;
        }
        self.server.abort();
        let _ = self.server.await;
    }
}

fn edge(patient: u64, condition: u64, request_id: &str) -> AddEdgeRequest {
    AddEdgeRequest {
        src: patient,
        dst: condition,
        edge_type: ProtoEdgeType::DiagnosedWith as i32,
        timestamp_us: 0, // server-assigned
        properties_json: "{}".into(),
        model: ProtoModelKind::Graphsage as i32,
        request_id: request_id.into(),
        ..AddEdgeRequest::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_mutations_replicate_exactly_and_a_failover_keeps_accepting_writes() {
    let root = TempDir::new().unwrap();
    let (a, b) = (root.path().join("a"), root.path().join("b"));
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();

    // The leader, with its starting nodes; the standby is bootstrapped from a
    // checkpoint of it, the way an operator would.
    let leader = start(&a, Role::Leader, None, 1, true).await;
    create_checkpoint(leader.store(), &b.join("db")).unwrap();
    let standby = start(&b, Role::Follower, Some(leader.addr), 0, false).await;
    let repl = leader.api.replication().clone();
    let deadline = Instant::now() + Duration::from_secs(20);
    while repl.follower_count() == 0 {
        assert!(Instant::now() < deadline, "the standby never connected");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 12 real mutations, each acknowledged only once the standby has it.
    let mut client = leader.client().await;
    let mut last_sequence = 0;
    for (n, (patient, condition)) in (1..=6u64)
        .flat_map(|p| [(p, 101u64), (p, 102u64)])
        .enumerate()
    {
        let resp = client
            .add_edge(edge(patient, condition, &format!("req-{n}")))
            .await
            .unwrap_or_else(|e| panic!("mutation {n} failed: {e}"))
            .into_inner();
        assert!(resp.db_sequence > last_sequence, "sequence advances");
        last_sequence = resp.db_sequence;
        assert!(
            standby.store().latest_sequence() >= resp.db_sequence,
            "mutation {n} was acknowledged, so the standby must already hold it"
        );
    }

    // The standby is byte-for-byte the leader: edges, embeddings, provenance,
    // idempotency records, audit chain, high-water mark.
    assert_eq!(
        standby.store().latest_sequence(),
        leader.store().latest_sequence()
    );
    assert_eq!(dump(standby.store()), dump(leader.store()));
    assert_eq!(verify_store(standby.store()).unwrap().unwrap().records, 12);

    // A retry of an acknowledged request is idempotent on the replicated state.
    let retry = client
        .add_edge(edge(1, 101, "req-0"))
        .await
        .unwrap()
        .into_inner();
    assert!(retry.replayed);

    // Read-your-writes on the standby.
    let mut reader = standby.client().await;
    let snapshot = |min_sequence| SnapshotRequest {
        subject: 1,
        as_of_us: u64::MAX,
        min_sequence,
        ..SnapshotRequest::default()
    };
    let from_standby = reader
        .snapshot(snapshot(last_sequence))
        .await
        .unwrap()
        .into_inner();
    let from_leader = client.snapshot(snapshot(0)).await.unwrap().into_inner();
    assert_eq!(from_standby, from_leader, "same state, same answer");
    assert!(
        !from_leader.related.is_empty(),
        "the snapshot has real edges"
    );

    // The standby will not take writes.
    let err = reader.add_edge(edge(2, 103, "nope")).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Planned failover.
    let demoted = leader
        .admin()
        .await
        .demote(DemoteRequest { timeout_ms: 10_000 })
        .await
        .unwrap()
        .into_inner();
    assert!(demoted.follower_caught_up);
    let promoted = standby
        .admin()
        .await
        .promote(PromoteRequest {
            wait_for_sequence: demoted.final_sequence,
            wait_timeout_ms: 10_000,
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(promoted.epoch, 2);

    // The old leader is fenced; the new one writes real mutations under the
    // strict timestamp policy, which only works if its high-water mark and
    // audit chain were rebuilt from the replicated state.
    let err = client.add_edge(edge(3, 103, "late")).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let resp = reader
        .add_edge(edge(3, 103, "after-failover"))
        .await
        .expect("the promoted node accepts a real mutation")
        .into_inner();
    assert!(resp.timestamp_us > 0 && !resp.replayed);
    assert_eq!(
        verify_store(standby.store()).unwrap().unwrap().records,
        13,
        "the audit chain continues unbroken across the failover"
    );
    let history = EpochHistory::read(standby.store()).unwrap();
    assert_eq!(history.current(), 2);

    standby.stop().await;
    leader.stop().await;
}
