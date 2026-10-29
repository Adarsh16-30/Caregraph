//! Input limits, rate limiting and message-size caps, against a real tonic
//! server on a real ephemeral port and a real generated client.
//!
//! No embedding model is deployed here, so a mutation that passes validation
//! reaches the model check and fails `FAILED_PRECONDITION`; that code is how
//! these tests tell "accepted by the limits" from "rejected by the limits".

use std::net::SocketAddr;
use std::time::Duration;

use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::{
    AddEdgeRequest, EdgeType as ProtoEdgeType, ModelKind as ProtoModelKind, SnapshotRequest,
};
use caregraph::api::{AuthInterceptor, CareGraphApi, ServiceOptions};
use caregraph::server::{InputLimits, RateLimiter};
use caregraph::storage::RocksKv;
use caregraph::types::Timestamp;
use tempfile::TempDir;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request};

const API_KEY: &str = "limits-test-secret";
const MAX_REQUEST_BYTES: usize = 256 * 1024;

struct Harness {
    client: CareGraphServiceClient<Channel>,
    _dir: TempDir,
    _server: tokio::task::JoinHandle<()>,
}

async fn start(options: ServiceOptions) -> Harness {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener);

    let registry = prometheus::Registry::new();
    let api = CareGraphApi::with_options(store, None, None, &registry, options).expect("service");
    let service = InterceptedService::new(
        CareGraphServiceServer::new(api).max_decoding_message_size(MAX_REQUEST_BYTES),
        AuthInterceptor::new(API_KEY),
    );
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .expect("server");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("connect");
    Harness {
        client: CareGraphServiceClient::new(channel),
        _dir: dir,
        _server: server,
    }
}

fn authed<T>(msg: T) -> Request<T> {
    let mut req = Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {API_KEY}").parse().unwrap(),
    );
    req
}

fn add_edge(properties_json: String, timestamp_us: u64, explain: bool) -> AddEdgeRequest {
    AddEdgeRequest {
        src: 1,
        dst: 2,
        edge_type: ProtoEdgeType::DiagnosedWith as i32,
        timestamp_us,
        properties_json,
        model: ProtoModelKind::Graphsage as i32,
        explain,
        request_id: String::new(),
    }
}

fn limits() -> InputLimits {
    InputLimits {
        max_properties_bytes: 1024,
        max_properties_depth: 8,
        ..InputLimits::default()
    }
}

#[tokio::test]
async fn properties_json_is_bounded_in_size_depth_and_shape() {
    let mut h = start(ServiceOptions {
        input: limits(),
        ..ServiceOptions::default()
    })
    .await;
    let now = Timestamp::now().as_u64();

    // Within limits: passes validation and reaches the (absent) model.
    let ok = h
        .client
        .add_edge(authed(add_edge(r#"{"a":1}"#.into(), now, false)))
        .await
        .expect_err("no model is deployed");
    assert_eq!(ok.code(), Code::FailedPrecondition);

    let oversize = format!(r#"{{"a":"{}"}}"#, "x".repeat(2048));
    let deep = format!("{}1{}", r#"{"a":"#.repeat(20), "}".repeat(20));
    for (label, body) in [
        ("oversize", oversize),
        ("too deep", deep),
        ("not an object", "[1,2,3]".to_string()),
        ("malformed", "{nope".to_string()),
    ] {
        let err = h
            .client
            .add_edge(authed(add_edge(body, now, false)))
            .await
            .expect_err(label);
        assert_eq!(err.code(), Code::InvalidArgument, "{label}");
    }
}

#[tokio::test]
async fn a_far_future_timestamp_is_refused_and_a_server_assigned_one_is_not() {
    let mut h = start(ServiceOptions::default()).await;
    let now = Timestamp::now().as_u64();

    let far = h
        .client
        .add_edge(authed(add_edge(String::new(), now + 3_600_000_000, false)))
        .await
        .expect_err("an hour ahead of the server clock");
    assert_eq!(far.code(), Code::InvalidArgument);

    let assigned = h
        .client
        .add_edge(authed(add_edge(String::new(), 0, false)))
        .await
        .expect_err("no model is deployed");
    assert_eq!(assigned.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn explain_is_refused_when_disabled_and_allowed_when_enabled() {
    let now = Timestamp::now().as_u64();

    let mut off = start(ServiceOptions {
        input: InputLimits {
            explain_enabled: false,
            ..InputLimits::default()
        },
        ..ServiceOptions::default()
    })
    .await;
    let denied = off
        .client
        .add_edge(authed(add_edge(String::new(), now, true)))
        .await
        .expect_err("explain disabled");
    assert_eq!(denied.code(), Code::PermissionDenied);

    let mut on = start(ServiceOptions::default()).await;
    let reached_model = on
        .client
        .add_edge(authed(add_edge(String::new(), now, true)))
        .await
        .expect_err("no model is deployed");
    assert_eq!(reached_model.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn requests_beyond_the_rate_limit_are_refused_with_a_retry_hint() {
    let mut h = start(ServiceOptions {
        rate_limiter: RateLimiter::new(2, 3),
        ..ServiceOptions::default()
    })
    .await;

    let mut accepted = 0;
    let mut refused = Vec::new();
    for _ in 0..12 {
        match h
            .client
            .snapshot(authed(SnapshotRequest {
                subject: 1,
                as_of_us: u64::MAX,
                edge_types: vec![],
            }))
            .await
        {
            Ok(_) => accepted += 1,
            Err(status) => refused.push(status),
        }
    }
    assert!(
        (3..=5).contains(&accepted),
        "burst of 3 plus whatever refilled during the loop, got {accepted}"
    );
    assert!(!refused.is_empty(), "the limit never engaged");
    for status in &refused {
        assert_eq!(status.code(), Code::ResourceExhausted);
        let hint = status
            .metadata()
            .get("retry-after-ms")
            .expect("a refusal says when to retry");
        assert!(hint.to_str().unwrap().parse::<u64>().unwrap() > 0);
    }

    // Reads and writes are budgeted separately: this write is validated, not
    // refused for the reads just spent.
    let write = h
        .client
        .add_edge(authed(add_edge(String::new(), 0, false)))
        .await
        .expect_err("no model is deployed");
    assert_eq!(write.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn a_message_over_the_decode_limit_is_refused_by_the_transport() {
    let mut h = start(ServiceOptions {
        // Large enough that the *property limit* would accept it; only the
        // transport cap can refuse a body this big.
        input: InputLimits {
            max_properties_bytes: 4 * 1024 * 1024,
            ..InputLimits::default()
        },
        ..ServiceOptions::default()
    })
    .await;
    let body = format!(r#"{{"a":"{}"}}"#, "x".repeat(MAX_REQUEST_BYTES + 1024));
    let err = h
        .client
        .add_edge(authed(add_edge(body, 0, false)))
        .await
        .expect_err("over the decode limit");
    assert!(
        matches!(err.code(), Code::OutOfRange | Code::ResourceExhausted),
        "got {:?}: {}",
        err.code(),
        err.message()
    );
}
