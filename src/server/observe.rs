//! Per-RPC request metrics and the root tracing span, as one tower layer around
//! every gRPC service.
//!
//! * `rpc_requests_total{rpc,code}` — one increment per request, by gRPC status.
//! * `rpc_duration_seconds{rpc}` — time from receiving the request to the handler
//!   returning its response. For server-streaming RPCs (`Replicate`) that is the time
//!   to the first response headers, not the life of the stream.
//!
//! The status comes from the response's `grpc-status` header. tonic puts it there
//! for every error a handler or a layer returns (a "trailers-only" response); a
//! successful response carries it in trailers after the body, so a response without
//! the header is counted as `OK`. An error raised after the body has started (a
//! stream that fails midway) is not visible here.
//!
//! The span is named `rpc` and carries only the method name (never a principal, id
//! or payload), and adopts the caller's W3C `traceparent` when there is one.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};
use tonic::codegen::http::{Request, Response};
use tracing::Instrument;

pub struct RpcMetrics {
    requests: IntCounterVec,
    duration: HistogramVec,
}

impl RpcMetrics {
    pub fn register(registry: &Registry) -> prometheus::Result<Arc<Self>> {
        let requests = IntCounterVec::new(
            Opts::new("rpc_requests_total", "gRPC requests by method and status"),
            &["rpc", "code"],
        )?;
        // 0.5 ms to about 16 s: point reads are sub-millisecond, a write that runs a
        // forward pass and waits for the standby is tens to hundreds of milliseconds.
        let buckets = prometheus::exponential_buckets(0.0005, 2.0, 16)?;
        let duration = HistogramVec::new(
            HistogramOpts::new(
                "rpc_duration_seconds",
                "Time from receiving a gRPC request to the handler returning its response",
            )
            .buckets(buckets),
            &["rpc"],
        )?;
        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(duration.clone()))?;
        Ok(Arc::new(RpcMetrics { requests, duration }))
    }

    fn observe(&self, rpc: &str, code: &str, seconds: f64) {
        self.requests.with_label_values(&[rpc, code]).inc();
        self.duration.with_label_values(&[rpc]).observe(seconds);
    }
}

/// `/caregraph.v1.CareGraphService/AddEdge` becomes `AddEdge`. The method set is
/// fixed by the protos, so the label's cardinality is bounded; anything that does
/// not look like a gRPC path is folded into `other`.
fn method_of(path: &str) -> &str {
    let mut parts = path.trim_start_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(_service), Some(method), None) if !method.is_empty() => method,
        _ => "other",
    }
}

fn code_name(headers: &tonic::codegen::http::HeaderMap) -> &'static str {
    let Some(raw) = headers.get("grpc-status").and_then(|v| v.to_str().ok()) else {
        return "OK";
    };
    match raw.trim().parse::<i32>().map(tonic::Code::from_i32) {
        Ok(tonic::Code::Ok) => "OK",
        Ok(tonic::Code::Cancelled) => "CANCELLED",
        Ok(tonic::Code::InvalidArgument) => "INVALID_ARGUMENT",
        Ok(tonic::Code::DeadlineExceeded) => "DEADLINE_EXCEEDED",
        Ok(tonic::Code::NotFound) => "NOT_FOUND",
        Ok(tonic::Code::AlreadyExists) => "ALREADY_EXISTS",
        Ok(tonic::Code::PermissionDenied) => "PERMISSION_DENIED",
        Ok(tonic::Code::ResourceExhausted) => "RESOURCE_EXHAUSTED",
        Ok(tonic::Code::FailedPrecondition) => "FAILED_PRECONDITION",
        Ok(tonic::Code::Aborted) => "ABORTED",
        Ok(tonic::Code::OutOfRange) => "OUT_OF_RANGE",
        Ok(tonic::Code::Unimplemented) => "UNIMPLEMENTED",
        Ok(tonic::Code::Internal) => "INTERNAL",
        Ok(tonic::Code::Unavailable) => "UNAVAILABLE",
        Ok(tonic::Code::DataLoss) => "DATA_LOSS",
        Ok(tonic::Code::Unauthenticated) => "UNAUTHENTICATED",
        _ => "UNKNOWN",
    }
}

#[derive(Clone)]
pub struct ObserveLayer {
    metrics: Arc<RpcMetrics>,
}

impl ObserveLayer {
    pub fn new(metrics: Arc<RpcMetrics>) -> Self {
        ObserveLayer { metrics }
    }
}

impl<S> tower::Layer<S> for ObserveLayer {
    type Service = Observe<S>;
    fn layer(&self, inner: S) -> Observe<S> {
        Observe {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Observe<S> {
    inner: S,
    metrics: Arc<RpcMetrics>,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl<S, B, ResB> tower::Service<Request<B>> for Observe<S>
where
    S: tower::Service<Request<B>, Response = Response<ResB>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
    ResB: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        // `call` must run on the instance `poll_ready` was called on.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let metrics = self.metrics.clone();
        let rpc = method_of(req.uri().path()).to_string();

        let span = tracing::info_span!("rpc", rpc.method = %rpc);
        crate::telemetry::adopt_remote_parent(&span, req.headers());

        Box::pin(
            async move {
                let started = Instant::now();
                let result = inner.call(req).await;
                let code = match &result {
                    Ok(response) => code_name(response.headers()),
                    Err(_) => "UNKNOWN",
                };
                metrics.observe(&rpc, code, started.elapsed().as_secs_f64());
                result
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use tower::{Layer, Service, ServiceExt};

    #[test]
    fn the_method_is_the_last_path_segment() {
        assert_eq!(
            method_of("/caregraph.v1.CareGraphService/AddEdge"),
            "AddEdge"
        );
        assert_eq!(method_of("/grpc.health.v1.Health/Check"), "Check");
        assert_eq!(method_of("/"), "other");
        assert_eq!(method_of("/a/b/c"), "other");
        assert_eq!(method_of("garbage"), "other");
    }

    #[test]
    fn a_missing_status_is_ok_and_a_present_one_is_named() {
        let mut h = tonic::codegen::http::HeaderMap::new();
        assert_eq!(code_name(&h), "OK");
        h.insert("grpc-status", "14".parse().unwrap());
        assert_eq!(code_name(&h), "UNAVAILABLE");
        h.insert("grpc-status", "8".parse().unwrap());
        assert_eq!(code_name(&h), "RESOURCE_EXHAUSTED");
        h.insert("grpc-status", "not-a-number".parse().unwrap());
        assert_eq!(code_name(&h), "UNKNOWN");
    }

    #[tokio::test]
    async fn requests_are_counted_by_method_and_status() {
        let registry = Registry::new();
        let metrics = RpcMetrics::register(&registry).unwrap();
        let svc = tower::service_fn(|req: Request<()>| async move {
            let mut res = Response::new(());
            if req.uri().path().ends_with("Fail") {
                res.headers_mut()
                    .insert("grpc-status", "14".parse().unwrap());
            }
            Ok::<_, Infallible>(res)
        });
        let mut svc = ObserveLayer::new(metrics.clone()).layer(svc);
        for path in ["/p.S/Ok", "/p.S/Ok", "/p.S/Fail"] {
            let req = Request::builder().uri(path).body(()).unwrap();
            svc.ready().await.unwrap().call(req).await.unwrap();
        }
        assert_eq!(metrics.requests.with_label_values(&["Ok", "OK"]).get(), 2);
        assert_eq!(
            metrics
                .requests
                .with_label_values(&["Fail", "UNAVAILABLE"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .duration
                .with_label_values(&["Ok"])
                .get_sample_count(),
            2
        );
    }
}
