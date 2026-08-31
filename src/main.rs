//! CareGraph server entry point.
//!
//! Opens a real RocksDB instance, spawns whichever trained embedding models
//! are actually deployed under `ml/deployed/`, and serves the tonic gRPC API
//! (PRD Layer 5, Phase 6) — mutation, traversal, snapshot, and similarity,
//! all wired to real reads and writes (`src/api/mod.rs`).

use std::path::Path;

use anyhow::{bail, Context};
use caregraph::api::{AuthInterceptor, CareGraphApi};
use caregraph::storage::{cf, RocksKv};
use caregraph::{db_path_from_env, Timestamp};
use tonic::transport::Server;

/// Standard model directory names this build knows how to spawn. Neither is
/// required to exist — a server with only one deployed still serves every
/// RPC except mutations requesting the missing kind (see
/// `CareGraphApi::model_for`).
const DEFAULT_GRAPHSAGE_MODEL: &str = "diabetes130_graphsage";
const DEFAULT_GAT_MODEL: &str = "diabetes130_gat";

fn deployed(model_id: &str) -> bool {
    Path::new("ml/deployed")
        .join(model_id)
        .join("model.pt")
        .exists()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "caregraph=info".into()),
        )
        .init();

    let path = db_path_from_env();
    if let Some(parent) = std::path::Path::new(&path).parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating database directory {}", parent.display()))?;
    }

    let store = RocksKv::open(&path).with_context(|| format!("opening RocksDB at {path}"))?;
    for name in cf::ALL {
        store
            .cf_handle(name)
            .with_context(|| format!("column family {name} is missing"))?;
    }

    // Rule 2's own honesty standard applied to auth, not just RPC bodies: a
    // server that "has auth" but defaults it to open when unconfigured is a
    // fake auth endpoint. See src/api/mod.rs's module doc.
    let api_key = std::env::var("CAREGRAPH_API_KEY").ok();
    let Some(api_key) = api_key.filter(|k| !k.is_empty()) else {
        bail!(
            "CAREGRAPH_API_KEY is not set. The gRPC API layer (Phase 6) requires an \
             explicit bearer token before it will serve any RPC — an unset key is refused \
             rather than defaulted to \"auth disabled\". Set it to a real secret:\n\n    \
             export CAREGRAPH_API_KEY=\"$(openssl rand -hex 32)\"\n"
        );
    };

    let graphsage_model = std::env::var("CAREGRAPH_GRAPHSAGE_MODEL")
        .unwrap_or_else(|_| DEFAULT_GRAPHSAGE_MODEL.to_string());
    let gat_model =
        std::env::var("CAREGRAPH_GAT_MODEL").unwrap_or_else(|_| DEFAULT_GAT_MODEL.to_string());

    let graphsage_deployed = deployed(&graphsage_model);
    let gat_deployed = deployed(&gat_model);
    if !graphsage_deployed && !gat_deployed {
        tracing::warn!(
            "no embedding model is deployed under ml/deployed/ — mutation RPCs will fail \
             with failed_precondition until one is trained (ml/train_graphsage.py or ml/train_gat.py)"
        );
    }

    let registry = prometheus::Registry::new();
    let api = CareGraphApi::new(
        store,
        graphsage_deployed.then_some(graphsage_model.as_str()),
        gat_deployed.then_some(gat_model.as_str()),
        &registry,
    )
    .context("building the gRPC service")?;

    let addr = std::env::var("CAREGRAPH_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()
        .context("CAREGRAPH_GRPC_ADDR is not a valid socket address")?;

    tracing::info!(
        path = %path,
        %addr,
        graphsage_deployed,
        gat_deployed,
        started_at = ?Timestamp::now(),
        "caregraph gRPC service starting"
    );

    let interceptor = AuthInterceptor::new(api_key);
    let service =
        caregraph::api::proto::care_graph_service_server::CareGraphServiceServer::with_interceptor(
            api,
            interceptor,
        );

    Server::builder()
        .add_service(service)
        .serve(addr)
        .await
        .context("gRPC server exited")?;

    Ok(())
}
