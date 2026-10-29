//! Traces leave the server in the OTLP wire format and carry the caller's trace.
//!
//! A stand-in OTLP/gRPC collector (a real tonic server speaking the real protocol)
//! receives what the server exports. A read goes through the real request layer with a
//! W3C `traceparent` header from a pretend caller; the exported `rpc` span must be a
//! child of the caller's span in the caller's trace, must name only the method, and
//! the resource must name the service and pod.
//!
//! One test per process: the global tracing subscriber can be installed once.

use std::collections::HashMap;
use std::time::Duration;

use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::SnapshotRequest;
use caregraph::api::{CareGraphApi, ServiceOptions};
use caregraph::config::{Config, Source};
use caregraph::server::observe::{ObserveLayer, RpcMetrics};
use caregraph::storage::RocksKv;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

struct Collector(mpsc::UnboundedSender<ExportTraceServiceRequest>);

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let _ = self.0.send(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

struct Env(HashMap<&'static str, String>);
impl Source for Env {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
}

fn free_addr() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_servers_span_joins_the_callers_trace_and_arrives_over_otlp() {
    // The collector.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let collector_addr = free_addr();
    tokio::spawn(async move {
        Server::builder()
            .add_service(TraceServiceServer::new(Collector(tx)))
            .serve(collector_addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The server's telemetry, pointed at it, sampling everything.
    let cfg = Config::load(&Env(HashMap::from([
        (
            "CAREGRAPH_OTEL_ENDPOINT",
            format!("http://{collector_addr}"),
        ),
        ("CAREGRAPH_OTEL_SAMPLE_RATIO", "1".to_string()),
        ("CAREGRAPH_POD_NAME", "writer-test-0".to_string()),
        ("CAREGRAPH_API_KEY", "k".to_string()),
    ])))
    .unwrap();
    let guard = caregraph::telemetry::init(&cfg);

    // The service, behind the request layer.
    let dir = TempDir::new().unwrap();
    let registry = prometheus::Registry::new();
    let api = CareGraphApi::with_options(
        RocksKv::open(dir.path().join("db")).unwrap(),
        None,
        None,
        &registry,
        ServiceOptions::default(),
    )
    .unwrap();
    let metrics = RpcMetrics::register(&registry).unwrap();
    let addr = free_addr();
    tokio::spawn(async move {
        Server::builder()
            .layer(ObserveLayer::new(metrics))
            .add_service(CareGraphServiceServer::new(api))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A caller that is already inside a sampled trace.
    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const CALLER_SPAN: &str = "00f067aa0ba902b7";
    const SUBJECT: u64 = 424_242_424;
    let mut client = CareGraphServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let mut request = Request::new(SnapshotRequest {
        subject: SUBJECT,
        ..SnapshotRequest::default()
    });
    request.metadata_mut().insert(
        "traceparent",
        format!("00-{TRACE_ID}-{CALLER_SPAN}-01").parse().unwrap(),
    );
    // The node does not exist; the status does not matter, the span does.
    let _ = client.snapshot(request).await;

    // Flush the exporter (this blocks until the batch is sent).
    tokio::task::spawn_blocking(move || drop(guard))
        .await
        .unwrap();

    let mut exported = Vec::new();
    while let Ok(Some(batch)) = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
        exported.push(batch);
        if exported.iter().flat_map(|b| &b.resource_spans).count() > 0 {
            break;
        }
    }
    assert!(!exported.is_empty(), "the collector received nothing");

    let resource_spans: Vec<_> = exported
        .iter()
        .flat_map(|b| b.resource_spans.iter())
        .collect();
    let resource_attrs: HashMap<String, String> = resource_spans
        .iter()
        .filter_map(|r| r.resource.as_ref())
        .flat_map(|r| r.attributes.iter())
        .filter_map(|kv| match kv.value.as_ref()?.value.as_ref()? {
            Value::StringValue(s) => Some((kv.key.clone(), s.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        resource_attrs.get("service.name").map(String::as_str),
        Some("caregraph")
    );
    assert_eq!(
        resource_attrs
            .get("service.instance.id")
            .map(String::as_str),
        Some("writer-test-0")
    );

    let spans: Vec<_> = resource_spans
        .iter()
        .flat_map(|r| r.scope_spans.iter())
        .flat_map(|s| s.spans.iter())
        .collect();
    let rpc = spans.iter().find(|s| s.name == "rpc").unwrap_or_else(|| {
        panic!(
            "no `rpc` span among {:?}",
            spans.iter().map(|s| &s.name).collect::<Vec<_>>()
        )
    });
    assert_eq!(
        hex(&rpc.trace_id),
        TRACE_ID,
        "the span belongs to the caller's trace"
    );
    assert_eq!(
        hex(&rpc.parent_span_id),
        CALLER_SPAN,
        "and is a child of the caller's span"
    );

    // Only the method is recorded: nothing about the subject, and no principal.
    let attrs: Vec<(String, String)> = rpc
        .attributes
        .iter()
        .map(|kv| (kv.key.clone(), format!("{:?}", kv.value)))
        .collect();
    assert!(
        attrs
            .iter()
            .any(|(k, v)| k == "rpc.method" && v.contains("Snapshot")),
        "{attrs:?}"
    );
    let everything = format!("{spans:?}");
    assert!(
        !everything.contains(&SUBJECT.to_string()),
        "the subject id leaked into a span: {everything}"
    );
}
