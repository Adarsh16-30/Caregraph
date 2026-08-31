//! `GET /metrics` (Phase 7) — the Prometheus scrape endpoint
//! `observability/prometheus/prometheus.yml` has pointed `caregraph:9100` at
//! since Phase 1, and `infrastructure/docker-compose/dev-stack.yml` has
//! exposed port 9100 for since Phase 1 too. Both were dead weight until this
//! module existed: a `prometheus::Registry` was built in `main.rs` from
//! Phase 4 onward and metrics were recorded into it, but nothing ever served
//! it over HTTP — every dashboard panel and recording rule referencing these
//! series had zero real data behind it until now.

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use prometheus::{Encoder, Registry, TextEncoder};

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

/// Serves `GET /metrics` on `addr` until the process exits. Spawn as its own
/// tokio task alongside the gRPC server — it is a separate listener on a
/// separate port (Prometheus's own convention, not gRPC's), not a route on
/// the gRPC server.
pub async fn serve(addr: std::net::SocketAddr, registry: Registry) -> std::io::Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(registry);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}
