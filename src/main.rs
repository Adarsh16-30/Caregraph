//! CareGraph server entry point.
//!
//! Opens a real RocksDB instance, spawns whichever trained embedding models
//! are actually deployed under `ml/deployed/`, and serves the tonic gRPC API
//! (PRD Layer 5, Phase 6) — mutation, traversal, snapshot, and similarity,
//! all wired to real reads and writes (`src/api/mod.rs`).

use std::path::Path;

use anyhow::{bail, Context};
use caregraph::api::{AuthInterceptor, CareGraphApi};
use caregraph::storage::{cf, decode_hex_key, RocksKv, ENCRYPTION_KEY_ENV};
use caregraph::{db_path_from_env, Timestamp};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

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

/// Environment variables that configure mTLS (Phase 7). All three are
/// required together — a server cert without requiring a client cert back
/// is one-way TLS, not the mutual TLS Phase 7 calls for, so partial config
/// is refused rather than silently downgraded to something weaker than what
/// was asked for.
const TLS_CERT_ENV: &str = "CAREGRAPH_TLS_CERT";
const TLS_KEY_ENV: &str = "CAREGRAPH_TLS_KEY";
const TLS_CLIENT_CA_ENV: &str = "CAREGRAPH_TLS_CLIENT_CA";

/// Builds mTLS config from `CAREGRAPH_TLS_CERT`/`_KEY`/`_CLIENT_CA` (PEM file
/// paths) if `CAREGRAPH_TLS_CERT` is set, requiring the other two. Returns
/// `None` if TLS is not configured at all — the server then serves plaintext
/// gRPC, which is what every test and local dev invocation still uses.
fn tls_config_from_env() -> anyhow::Result<Option<ServerTlsConfig>> {
    let Some(cert_path) = std::env::var(TLS_CERT_ENV).ok().filter(|v| !v.is_empty()) else {
        tracing::warn!(
            "{TLS_CERT_ENV} is not set — the gRPC listener is plaintext, not mTLS. Set \
             {TLS_CERT_ENV}, {TLS_KEY_ENV}, and {TLS_CLIENT_CA_ENV} (PEM file paths) to \
             enable Phase 7's mutual TLS."
        );
        return Ok(None);
    };

    let key_path = std::env::var(TLS_KEY_ENV).with_context(|| {
        format!(
            "{TLS_CERT_ENV} is set, so {TLS_KEY_ENV} (the matching private key) is required too"
        )
    })?;
    let client_ca_path = std::env::var(TLS_CLIENT_CA_ENV).with_context(|| {
        format!(
            "{TLS_CERT_ENV} is set, so {TLS_CLIENT_CA_ENV} is required too — mTLS means client \
             certificates are verified, not just the server presenting one"
        )
    })?;

    let cert = std::fs::read(&cert_path)
        .with_context(|| format!("reading {TLS_CERT_ENV} at {cert_path}"))?;
    let key =
        std::fs::read(&key_path).with_context(|| format!("reading {TLS_KEY_ENV} at {key_path}"))?;
    let client_ca = std::fs::read(&client_ca_path)
        .with_context(|| format!("reading {TLS_CLIENT_CA_ENV} at {client_ca_path}"))?;

    tracing::info!("mTLS enabled (Phase 7): client certificates will be required and verified");

    Ok(Some(
        ServerTlsConfig::new()
            .identity(Identity::from_pem(cert, key))
            .client_ca_root(Certificate::from_pem(client_ca)),
    ))
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

    // Rule 8: real encryption at rest, opt-in via CAREGRAPH_ENCRYPTION_KEY —
    // same "explicit and fail loud, never silently insecure" shape as
    // CAREGRAPH_API_KEY just below. Unset means unencrypted, which is what
    // every test, benchmark, and local dev invocation still uses (see
    // RocksKv::open_encrypted's doc comment); set-but-malformed is refused
    // outright rather than silently falling back to unencrypted.
    let store = match std::env::var(ENCRYPTION_KEY_ENV) {
        Ok(hex) => {
            let key = decode_hex_key(&hex).map_err(anyhow::Error::msg)?;
            tracing::info!("encryption at rest enabled (AES-256-CTR, Rule 8)");
            RocksKv::open_encrypted(&path, &key)
                .with_context(|| format!("opening encrypted RocksDB at {path}"))?
        }
        Err(std::env::VarError::NotPresent) => {
            tracing::warn!(
                "{ENCRYPTION_KEY_ENV} is not set — database at {path} is unencrypted. \
                 Set it to a real AES-256 key to enable Rule 8's encryption at rest:\n\n    \
                 export {ENCRYPTION_KEY_ENV}=\"$(openssl rand -hex 32)\"\n"
            );
            RocksKv::open(&path).with_context(|| format!("opening RocksDB at {path}"))?
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("{ENCRYPTION_KEY_ENV} is set but is not valid UTF-8");
        }
    };
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

    let mut server_builder = Server::builder();
    if let Some(tls_config) = tls_config_from_env()? {
        server_builder = server_builder
            .tls_config(tls_config)
            .context("configuring mTLS")?;
    }

    server_builder
        .add_service(service)
        .serve(addr)
        .await
        .context("gRPC server exited")?;

    Ok(())
}
