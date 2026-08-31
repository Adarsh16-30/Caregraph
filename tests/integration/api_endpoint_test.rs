//! Rule 2 ("No Fake Query Endpoints") gate: every RPC must read or write
//! through a real layer underneath, and its response must vary with its
//! input — never a fixed shape regardless of what was asked. This suite
//! proves that against a real tonic server bound to a real, ephemeral local
//! TCP port and a real generated gRPC client, not an in-process function
//! call that skips serialization, auth, or the network stack entirely.
//!
//! Deliberately does not exercise the mutation RPCs' success path — that
//! needs a real spawned embedding model (Python + torch), the same
//! precondition `tests/embedding` and `tests/fault_injection` already carry,
//! and this suite runs in `cargo test --test integration`'s default,
//! always-on sweep, which does not set up Python. It does exercise the
//! mutation RPCs' *input validation*, which needs no model at all — an
//! invalid request being rejected while a well-formed one of the same shape
//! is not is itself a real "varies with input" proof.

use std::net::SocketAddr;
use std::time::Duration;

use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::{
    AddEdgeRequest, Direction as ProtoDirection, EdgeType as ProtoEdgeType,
    ModelKind as ProtoModelKind, SimilarCarePathwaysRequest, SnapshotRequest, TraverseRequest,
};
use caregraph::api::{AuthInterceptor, CareGraphApi};
use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::keys::encode_embedding_key;
use caregraph::temporal::record::NodeValue;
use caregraph::temporal::TemporalWriter;
use caregraph::types::{EdgeType, Embedding, ModelKind as DomainModelKind, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;
use tonic::transport::{Channel, Server};
use tonic::Request;

const API_KEY: &str = "test-only-shared-secret";

const HUB: NodeId = NodeId(1);
const PATIENT_A: NodeId = NodeId(2);
const PATIENT_B: NodeId = NodeId(3);
const ISOLATED: NodeId = NodeId(4);

/// Seeds a small fixture: `HUB` (a Condition) shared by two patients, plus
/// an isolated node with no edges — enough shape for hop-count and
/// existence to produce genuinely different responses. Also writes one
/// directly-constructed embedding for `PATIENT_A`, clearly labelled as
/// fixture data (`model_id: "test-fixture-model"`), so the similarity RPC
/// has something real to read without needing a spawned model — the same
/// role a hand-picked vector plays in `src/api/similarity.rs`'s own unit
/// tests, just written through the real storage layer instead of called as
/// a bare function.
fn seed_fixture() -> (TempDir, RocksKv) {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    let writer = TemporalWriter::new(&store).expect("writer");
    let mut batch = WriteBatch::default();

    writer.put_node(
        &mut batch,
        HUB,
        Timestamp(0),
        &NodeValue::new("Condition", json!({})),
    );
    writer.put_node(
        &mut batch,
        PATIENT_A,
        Timestamp(0),
        &NodeValue::new("Patient", json!({})),
    );
    writer.put_node(
        &mut batch,
        PATIENT_B,
        Timestamp(0),
        &NodeValue::new("Patient", json!({})),
    );
    writer.put_node(
        &mut batch,
        ISOLATED,
        Timestamp(0),
        &NodeValue::new("Patient", json!({})),
    );

    writer.put_edge(
        &mut batch,
        PATIENT_A,
        EdgeType::DiagnosedWith,
        HUB,
        Timestamp(10),
        &caregraph::temporal::record::EdgeValue::new(json!({})),
    );
    writer.put_edge(
        &mut batch,
        PATIENT_B,
        EdgeType::DiagnosedWith,
        HUB,
        Timestamp(11),
        &caregraph::temporal::record::EdgeValue::new(json!({})),
    );

    let embedding = Embedding::new(
        vec![1.0, 0.0, 0.0],
        "test-fixture-model",
        DomainModelKind::GraphSAGE,
        caregraph::types::ComputationPath::Associative,
    );
    batch.put_cf(
        &store.cf_handle(cf::CF_EMBEDDINGS).expect("cf handle"),
        encode_embedding_key(PATIENT_A, Timestamp(20)),
        embedding.serialize(),
    );

    drop(writer);
    store.write(batch).expect("commit fixture");
    (dir, store)
}

/// Binds a real ephemeral TCP port, serves the real `CareGraphApi` behind
/// the real auth interceptor on it, and returns a connected real client —
/// no in-process shortcut past serialization or the network stack.
async fn spawn_test_server(
    store: RocksKv,
) -> (CareGraphServiceClient<Channel>, tokio::task::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    drop(listener);

    let registry = prometheus::Registry::new();
    // No model deployed for this suite (see module doc) — mutation RPCs'
    // input-validation path still runs; their success path is out of scope.
    let api = CareGraphApi::new(store, None, None, &registry).expect("build service");
    let interceptor = AuthInterceptor::new(API_KEY);
    let service = CareGraphServiceServer::with_interceptor(api, interceptor);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .expect("server");
    });

    // Real startup latency, not simulated: give the listener a moment to
    // actually be ready before the client's first connection attempt.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("valid uri")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("connect to test server");

    (CareGraphServiceClient::new(channel), handle)
}

fn authed<T>(msg: T) -> Request<T> {
    let mut req = Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {API_KEY}").parse().unwrap(),
    );
    req
}

#[tokio::test]
async fn traverse_response_varies_with_max_hops() {
    let (_dir, store) = seed_fixture();
    let (mut client, _server) = spawn_test_server(store).await;

    let one_hop = client
        .traverse(authed(TraverseRequest {
            start: PATIENT_A.as_u64(),
            as_of_us: Timestamp::now().as_u64(),
            max_hops: 1,
            direction: ProtoDirection::Both as i32,
            edge_types: vec![],
            window_from_us: 0,
            window_to_us: 0,
            max_nodes: 0,
            max_edges: 0,
        }))
        .await
        .expect("traverse 1 hop")
        .into_inner();

    let two_hops = client
        .traverse(authed(TraverseRequest {
            start: PATIENT_A.as_u64(),
            as_of_us: Timestamp::now().as_u64(),
            max_hops: 2,
            direction: ProtoDirection::Both as i32,
            edge_types: vec![],
            window_from_us: 0,
            window_to_us: 0,
            max_nodes: 0,
            max_edges: 0,
        }))
        .await
        .expect("traverse 2 hops")
        .into_inner();

    // 1 hop from PATIENT_A reaches only HUB; 2 hops also reaches PATIENT_B
    // through it. A fixed-response endpoint would return the same node set
    // for both — this is the real variation Rule 2 requires.
    assert!(
        two_hops.nodes.len() > one_hop.nodes.len(),
        "1-hop nodes={:?} 2-hop nodes={:?} — response did not vary with max_hops",
        one_hop.nodes,
        two_hops.nodes
    );
    assert!(two_hops
        .nodes
        .iter()
        .any(|n| n.node_id == PATIENT_B.as_u64()));
    assert!(!one_hop
        .nodes
        .iter()
        .any(|n| n.node_id == PATIENT_B.as_u64()));
}

#[tokio::test]
async fn snapshot_response_varies_with_subject_existence() {
    let (_dir, store) = seed_fixture();
    let (mut client, _server) = spawn_test_server(store).await;
    let now = Timestamp::now().as_u64();

    let existing = client
        .snapshot(authed(SnapshotRequest {
            subject: PATIENT_A.as_u64(),
            as_of_us: now,
            edge_types: vec![],
        }))
        .await
        .expect("snapshot existing")
        .into_inner();

    let missing = client
        .snapshot(authed(SnapshotRequest {
            subject: 999_999,
            as_of_us: now,
            edge_types: vec![],
        }))
        .await
        .expect("snapshot missing")
        .into_inner();

    assert!(existing.found, "expected PATIENT_A to be found");
    assert!(
        !missing.found,
        "expected an unwritten node id to be not found"
    );
    assert!(existing.subject.is_some());
    assert!(missing.subject.is_none());
}

#[tokio::test]
async fn similar_care_pathways_varies_with_embedding_presence() {
    let (_dir, store) = seed_fixture();
    let (mut client, _server) = spawn_test_server(store).await;
    let now = Timestamp::now().as_u64();

    let has_embedding = client
        .similar_care_pathways(authed(SimilarCarePathwaysRequest {
            node_id: PATIENT_A.as_u64(),
            as_of_us: now,
            top_k: 5,
        }))
        .await
        .expect("similarity for a node with an embedding")
        .into_inner();

    let no_embedding = client
        .similar_care_pathways(authed(SimilarCarePathwaysRequest {
            node_id: ISOLATED.as_u64(),
            as_of_us: now,
            top_k: 5,
        }))
        .await
        .expect("similarity for a node without one")
        .into_inner();

    assert!(!has_embedding.query_node_has_no_embedding);
    assert!(no_embedding.query_node_has_no_embedding);
    assert!(no_embedding.matches.is_empty());
}

#[tokio::test]
async fn requests_without_a_valid_bearer_token_are_rejected() {
    let (_dir, store) = seed_fixture();
    let (mut client, _server) = spawn_test_server(store).await;
    let now = Timestamp::now().as_u64();

    // No authorization metadata at all.
    let unauthenticated = client
        .snapshot(Request::new(SnapshotRequest {
            subject: PATIENT_A.as_u64(),
            as_of_us: now,
            edge_types: vec![],
        }))
        .await;
    assert_eq!(
        unauthenticated.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );

    // Wrong token.
    let mut wrong = Request::new(SnapshotRequest {
        subject: PATIENT_A.as_u64(),
        as_of_us: now,
        edge_types: vec![],
    });
    wrong
        .metadata_mut()
        .insert("authorization", "Bearer not-the-real-key".parse().unwrap());
    assert_eq!(
        client.snapshot(wrong).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );

    // Correct token succeeds — same RPC, same request body, only the auth
    // metadata differs, and the outcome differs with it.
    let ok = client
        .snapshot(authed(SnapshotRequest {
            subject: PATIENT_A.as_u64(),
            as_of_us: now,
            edge_types: vec![],
        }))
        .await;
    assert!(ok.is_ok());
}

#[tokio::test]
async fn add_edge_rejects_an_invalid_request_before_touching_any_model() {
    let (_dir, store) = seed_fixture();
    let (mut client, _server) = spawn_test_server(store).await;

    // edge_type left unspecified (0) — rejected by input validation
    // (proto_edge_type) before model dispatch ever runs, so this exercises
    // real validation logic without needing a spawned model.
    let missing_edge_type = client
        .add_edge(authed(AddEdgeRequest {
            src: PATIENT_A.as_u64(),
            dst: PATIENT_B.as_u64(),
            edge_type: ProtoEdgeType::Unspecified as i32,
            timestamp_us: 100,
            properties_json: String::new(),
            model: ProtoModelKind::Graphsage as i32,
        }))
        .await;
    assert_eq!(
        missing_edge_type.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );

    // A well-formed request reaches model dispatch and fails there instead
    // — a different error for a different reason, because no model is
    // deployed in this suite (see module doc). Different input, different
    // failure mode: still real variation, not one fixed rejection.
    let no_model_deployed = client
        .add_edge(authed(AddEdgeRequest {
            src: PATIENT_A.as_u64(),
            dst: PATIENT_B.as_u64(),
            edge_type: ProtoEdgeType::PrescribedMedication as i32,
            timestamp_us: 100,
            properties_json: String::new(),
            model: ProtoModelKind::Graphsage as i32,
        }))
        .await;
    assert_eq!(
        no_model_deployed.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}
