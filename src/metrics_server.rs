//! `GET /metrics` (Phase 7) — the Prometheus scrape endpoint
//! `observability/prometheus/prometheus.yml` has pointed `caregraph:9100` at
//! since Phase 1, and `infrastructure/docker-compose/dev-stack.yml` has
//! exposed port 9100 for since Phase 1 too. Both were dead weight until this
//! module existed: a `prometheus::Registry` was built in `main.rs` from
//! Phase 4 onward and metrics were recorded into it, but nothing ever served
//! it over HTTP — every dashboard panel and recording rule referencing these
//! series had zero real data behind it until now.

use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use prometheus::{Encoder, Registry, TextEncoder};

use crate::server::Health;

async fn metrics_handler(State(registry): State<Registry>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buf = Vec::new();
    // TextEncoder::encode only fails on a write error into `buf`, which a
    // Vec<u8> never produces — but real (not fabricated) content on the
    // encode failure path all the same, not a fake "always empty" body.
    if let Err(e) = encoder.encode(&metric_families, &mut buf) {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {e}"),
        )
            .into_response();
    }
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, encoder.format_type())],
        buf,
    )
        .into_response()
}

async fn healthz(State(state): State<Probes>) -> impl IntoResponse {
    if state.health.live() {
        (
            axum::http::StatusCode::OK,
            "ok
",
        )
            .into_response()
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "database is not answering reads
",
        )
            .into_response()
    }
}

async fn readyz(State(state): State<Probes>) -> impl IntoResponse {
    let r = state.health.readiness();
    if r.ready {
        (
            axum::http::StatusCode::OK,
            "ready
"
            .to_string(),
        )
            .into_response()
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "not ready: {}
",
                r.reasons.join("; ")
            ),
        )
            .into_response()
    }
}

/// The node's replication role as one word. The chart's label sidecar reads this to
/// keep the pod's `caregraph.io/role` label, and with it the write Service, correct.
async fn role(State(state): State<Probes>) -> impl IntoResponse {
    state.health.role().to_string()
}

async fn startupz(State(state): State<Probes>) -> impl IntoResponse {
    if state.health.is_started() {
        (
            axum::http::StatusCode::OK,
            "started
",
        )
            .into_response()
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "starting
",
        )
            .into_response()
    }
}

#[derive(Clone)]
struct Probes {
    health: Arc<Health>,
}

/// The HTTP surface: `/metrics` plus the three probes.
pub fn router(registry: Registry, health: Arc<Health>) -> Router {
    let probes = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/startupz", get(startupz))
        .route("/role", get(role))
        .with_state(Probes { health });
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(registry)
        .merge(probes)
}

/// Serves `/metrics` and the probes on `addr` until the process exits. Spawn
/// as its own tokio task alongside the gRPC server — it is a separate listener
/// on a separate port (Prometheus's own convention, not gRPC's), not a route on
/// the gRPC server.
pub async fn serve(
    addr: std::net::SocketAddr,
    registry: Registry,
    health: Arc<Health>,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(registry, health)).await
}
