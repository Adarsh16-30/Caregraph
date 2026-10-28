//! CareGraph server entry point.
//!
//! Opens a real RocksDB instance, spawns whichever trained embedding models
//! are actually deployed under the ML directory, and serves the tonic gRPC API
//! (PRD Layer 5) — mutation, traversal, snapshot, and similarity, all wired to
//! real reads and writes (`src/api/mod.rs`). All settings come from the typed,
//! validated [`caregraph::config::Config`].

use std::path::Path;

use anyhow::{bail, Context};
use caregraph::api::{AuthInterceptor, CareGraphApi};
use caregraph::config::{Config, Layered, ProcessEnv};
use caregraph::storage::{cf, decode_hex_key, RocksKv};
use caregraph::Timestamp;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

fn deployed(cfg: &Config, model_id: &str) -> bool {
    cfg.model_dir(model_id).join("model.pt").exists()
}

/// Builds mTLS config when all three PEM paths are configured (`Config`
/// already refused partial configuration). Plaintext gRPC is only reachable in
/// development; production validation rejects it before we get here.
fn tls_config(cfg: &Config) -> anyhow::Result<Option<ServerTlsConfig>> {
    let (Some(cert_path), Some(key_path), Some(ca_path)) =
        (&cfg.tls_cert, &cfg.tls_key, &cfg.tls_client_ca)
    else {
        tracing::warn!(
            "mTLS is not configured — the gRPC listener is plaintext. Set CAREGRAPH_TLS_CERT, \
             CAREGRAPH_TLS_KEY and CAREGRAPH_TLS_CLIENT_CA (PEM paths) to enable it; production \
             mode requires it."
        );
        return Ok(None);
    };
    let cert = std::fs::read(cert_path)
        .with_context(|| format!("reading TLS cert at {}", cert_path.display()))?;
    let key = std::fs::read(key_path)
        .with_context(|| format!("reading TLS key at {}", key_path.display()))?;
    let ca = std::fs::read(ca_path)
        .with_context(|| format!("reading client CA at {}", ca_path.display()))?;
    tracing::info!("mTLS enabled: client certificates will be required and verified");
    Ok(Some(
        ServerTlsConfig::new()
            .identity(Identity::from_pem(cert, key))
            .client_ca_root(Certificate::from_pem(ca)),
    ))
}

/// Parses `--config <path>` (or `CAREGRAPH_CONFIG`) and `--print-config`.
fn load_config() -> anyhow::Result<(Config, bool)> {
    let mut args = std::env::args().skip(1);
    let mut config_path = std::env::var("CAREGRAPH_CONFIG").ok();
    let mut print = false;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => config_path = Some(args.next().context("--config needs a path")?),
            "--print-config" => print = true,
            "-h" | "--help" => {
                println!("usage: caregraph [--config <file.toml>] [--print-config]");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    let file = match &config_path {
        Some(p) => {
            Some(std::fs::read_to_string(p).with_context(|| format!("reading config file {p}"))?)
        }
        None => None,
    };
    let layered = Layered::new(file.as_deref(), ProcessEnv)?;
    Ok((Config::load(&layered)?, print))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load caregraph/.env into the process environment, if one exists, before
    // anything reads configuration. A missing .env (CI, Docker, production) is
    // normal; real env vars always win because dotenvy never overwrites.
    dotenvy::dotenv().ok();

    let (cfg, print_config) = load_config()?;
    if print_config {
        print!("{}", cfg.redacted());
        return Ok(());
    }

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "caregraph=info".into());
    if cfg.log_json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    // Library code (model spawn, manifest load) resolves ML assets through
    // these variables so a TOML-supplied value applies everywhere.
    std::env::set_var("CAREGRAPH_ML_DIR", &cfg.ml_dir);
    std::env::set_var("CAREGRAPH_PYTHON", &cfg.python);

    let path = cfg.db_path.clone();
    if let Some(parent) = Path::new(&path).parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating database directory {}", parent.display()))?;
    }

    // Rule 8: real encryption at rest. Production refuses to start without a
    // key (Config::validate); in development an unset key warns and runs
    // unencrypted, while a malformed key is always a hard error.
    let store = match &cfg.encryption_key_hex {
        Some(hex) => {
            let key = decode_hex_key(hex).map_err(anyhow::Error::msg)?;
            tracing::info!("encryption at rest enabled (AES-256-CTR, Rule 8)");
            RocksKv::open_encrypted(&path, &key)
                .with_context(|| format!("opening encrypted RocksDB at {path}"))?
        }
        None => {
            tracing::warn!(
                "CAREGRAPH_ENCRYPTION_KEY is not set — database at {path} is unencrypted. \
                 Set it to a real AES-256 key to enable Rule 8's encryption at rest:\n\n    \
                 export CAREGRAPH_ENCRYPTION_KEY=\"$(openssl rand -hex 32)\"\n"
            );
            RocksKv::open(&path).with_context(|| format!("opening RocksDB at {path}"))?
        }
    };
    store.set_sync_writes(cfg.sync_writes);
    for name in cf::ALL {
        store
            .cf_handle(name)
            .with_context(|| format!("column family {name} is missing"))?;
    }

    // Rule 2's own honesty standard applied to auth: a server that "has auth"
    // but defaults it to open when unconfigured is a fake auth endpoint.
    let Some(api_key) = cfg.api_key.clone() else {
        bail!(
            "CAREGRAPH_API_KEY is not set. The gRPC API layer requires an explicit bearer \
             token before it will serve any RPC — an unset key is refused rather than \
             defaulted to \"auth disabled\". Set it to a real secret:\n\n    \
             export CAREGRAPH_API_KEY=\"$(openssl rand -hex 32)\"\n"
        );
    };

    let graphsage_deployed = deployed(&cfg, &cfg.graphsage_model);
    let gat_deployed = deployed(&cfg, &cfg.gat_model);
    if !graphsage_deployed && !gat_deployed {
        tracing::warn!(
            "no embedding model is deployed under {} — mutation RPCs will fail \
             with failed_precondition until one is trained",
            cfg.ml_dir.join("deployed").display()
        );
    }

    let registry = prometheus::Registry::new();
    let api = CareGraphApi::new(
        store,
        graphsage_deployed.then_some(cfg.graphsage_model.as_str()),
        gat_deployed.then_some(cfg.gat_model.as_str()),
        &registry,
    )
    .context("building the gRPC service")?;

    let addr = cfg.grpc_addr;
    tracing::info!(
        path = %path,
        %addr,
        environment = ?cfg.environment,
        graphsage_deployed,
        gat_deployed,
        sync_writes = cfg.sync_writes,
        started_at = ?Timestamp::now(),
        "caregraph gRPC service starting"
    );

    let metrics_addr = cfg.metrics_addr;
    let metrics_registry = registry.clone();
    tokio::spawn(async move {
        if let Err(e) = caregraph::metrics_server::serve(metrics_addr, metrics_registry).await {
            tracing::error!(%metrics_addr, error = %e, "metrics server exited");
        }
    });
    tracing::info!(%metrics_addr, "serving GET /metrics");

    let interceptor = AuthInterceptor::new(api_key);
    let service =
        caregraph::api::proto::care_graph_service_server::CareGraphServiceServer::with_interceptor(
            api,
            interceptor,
        );

    let mut server_builder = Server::builder();
    if let Some(tls) = tls_config(&cfg)? {
        server_builder = server_builder.tls_config(tls).context("configuring mTLS")?;
    }

    server_builder
        .add_service(service)
        .serve(addr)
        .await
        .context("gRPC server exited")?;

    Ok(())
}
