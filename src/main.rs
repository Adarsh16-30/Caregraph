//! CareGraph server entry point.
//!
//! Opens a real RocksDB instance, spawns whichever trained embedding models
//! are actually deployed under the ML directory, and serves the tonic gRPC API
//! (PRD Layer 5) — mutation, traversal, snapshot, and similarity, all wired to
//! real reads and writes (`src/api/mod.rs`). All settings come from the typed,
//! validated [`caregraph::config::Config`].

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context};
use caregraph::api::admin::AdminOps;
use caregraph::api::{AuthInterceptor, CareGraphApi, MutualTlsInterceptor, ServiceOptions};
use caregraph::config::{Config, Layered, ProcessEnv};
use caregraph::replication::epoch::EpochHistory;
use caregraph::replication::follower::{self, FollowerOptions};
use caregraph::replication::{ReplicationConfig, ReplicationState};
use caregraph::security::audit::AuditLog;
use caregraph::security::{tls, PolicyHandle, TlsPaths, TlsState, TokenHandle};
use caregraph::server::shutdown::{run_until_shutdown, wait_for_signal};
use caregraph::server::{Health, PoolCollector, RateLimiter};
use caregraph::storage::backup::BackupManager;
use caregraph::storage::{cf, decode_hex_key, RocksKv, WalRetention};
use caregraph::Timestamp;
use tonic::transport::Server;

fn deployed(cfg: &Config, model_id: &str) -> bool {
    cfg.model_dir(model_id).join("model.pt").exists()
}

/// Loads mutual-TLS state when all three PEM paths are configured (`Config`
/// already refused partial configuration). Plaintext gRPC is only reachable in
/// development; production validation rejects it before we get here.
fn tls_state(cfg: &Config) -> anyhow::Result<Option<Arc<TlsState>>> {
    let (Some(cert), Some(key), Some(client_ca)) =
        (&cfg.tls_cert, &cfg.tls_key, &cfg.tls_client_ca)
    else {
        tracing::warn!(
            "mTLS is not configured: the gRPC listener is plaintext and callers authenticate \
             with the shared CAREGRAPH_API_KEY. Set CAREGRAPH_TLS_CERT, CAREGRAPH_TLS_KEY and \
             CAREGRAPH_TLS_CLIENT_CA (PEM paths) to enable it; production mode requires it."
        );
        return Ok(None);
    };
    let state = TlsState::load(TlsPaths {
        cert: cert.clone(),
        key: key.clone(),
        client_ca: client_ca.clone(),
        crl: cfg.tls_crl.clone(),
    })?;
    tracing::info!("mTLS enabled: client certificates are required and verified (TLS 1.3)");
    Ok(Some(state))
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
        match cfg.memory_plan() {
            Ok(plan) => print!("\n{}", plan.describe()),
            Err(e) => println!("\nmemory plan: {e}"),
        }
        return Ok(());
    }

    // Held to the end of main so the trace exporter flushes on shutdown.
    let _telemetry = caregraph::telemetry::init(&cfg);

    // The memory plan must be fixed before the first database is opened: the
    // shared cache and the memtable limit are created from it.
    let plan = caregraph::memory::install(
        cfg.memory_plan()
            .context("the memory budget cannot be met")?,
    );
    tracing::info!("memory plan:\n{}", plan.describe());

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
    // The key is either given directly or unwrapped from a stored, wrapped data
    // key with a key-encryption key; either way it is held only for the open.
    let dek: Option<caregraph::security::keys::SecretKey> = match (&cfg.kek_file, &cfg.wrapped_dek)
    {
        (Some(kek), Some(wrapped)) => {
            let provider = caregraph::security::keys::FileKekProvider::new(kek);
            Some(
                caregraph::security::keys::load_dek(&provider, wrapped)
                    .context("unwrapping the data-encryption key")?,
            )
        }
        _ => cfg
            .encryption_key_hex
            .as_deref()
            .map(|hex| decode_hex_key(hex).map(zeroize::Zeroizing::new))
            .transpose()
            .map_err(anyhow::Error::msg)?,
    };
    // How much write-ahead log a leader keeps for followers to resume from.
    let retention = WalRetention {
        ttl_seconds: cfg.replication.wal_ttl_secs,
        size_limit_mb: cfg.replication.wal_size_mb,
    };
    let store = match &dek {
        Some(key) => {
            tracing::info!("encryption at rest enabled (AES-256-CTR, Rule 8)");
            RocksKv::open_with(&path, Some(&key[..]), retention)
                .with_context(|| format!("opening encrypted RocksDB at {path}"))?
        }
        None => {
            tracing::warn!(
                "no encryption key is configured — database at {path} is unencrypted.                  Set CAREGRAPH_ENCRYPTION_KEY to a real AES-256 key to enable Rule 8's                  encryption at rest:

                     export CAREGRAPH_ENCRYPTION_KEY=\"$(openssl rand -hex 32)\"
"
            );
            RocksKv::open_with(&path, None, retention)
                .with_context(|| format!("opening RocksDB at {path}"))?
        }
    };
    store.set_sync_writes(cfg.sync_writes);
    for name in cf::ALL {
        store
            .cf_handle(name)
            .with_context(|| format!("column family {name} is missing"))?;
    }

    // Rule 2's own honesty standard applied to auth: a server that "has auth"
    // but defaults it to open when unconfigured is a fake auth endpoint. With
    // mTLS the client certificate is the credential; without it the shared key
    // is, and it must be set.
    let tls = tls_state(&cfg)?;
    let api_key = cfg.api_key.clone();
    if tls.is_none() && api_key.is_none() {
        bail!(
            "CAREGRAPH_API_KEY is not set. Without mTLS the gRPC API layer requires an explicit \
             bearer token before it will serve any RPC; an unset key is refused rather than \
             defaulted to \"auth disabled\". Set it to a real secret:\n\n    \
             export CAREGRAPH_API_KEY=\"$(openssl rand -hex 32)\"\n"
        );
    }

    let policy = match &cfg.rbac_policy {
        Some(path) => {
            let handle = PolicyHandle::from_file(path)?;
            tokio::spawn(handle.clone().watch(std::time::Duration::from_secs(5)));
            tracing::info!(path = %path.display(), "RBAC policy loaded (watching for changes)");
            Some(handle)
        }
        None => {
            tracing::warn!(
                "no RBAC policy configured: every authenticated caller may call every RPC"
            );
            None
        }
    };
    let tokens = match &cfg.tokens_file {
        Some(path) => {
            let handle = TokenHandle::from_file(path)?;
            tokio::spawn(handle.clone().watch(std::time::Duration::from_secs(5)));
            Some(handle)
        }
        None => None,
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
    let audit_log = match &cfg.audit_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating audit directory {}", dir.display()))?;
            let audit_store = match &dek {
                Some(key) => RocksKv::open_audit(dir, Some(&key[..])),
                None => RocksKv::open_audit(dir, None),
            }
            .with_context(|| format!("opening audit database at {}", dir.display()))?;
            let log = AuditLog::start(audit_store, &cfg.pod_name, 4096)
                .context("starting the audit log")?;
            tracing::info!(dir = %dir.display(), "read auditing enabled (fail-closed)");
            Some(log)
        }
        None => {
            tracing::warn!("CAREGRAPH_AUDIT_DIR is not set: reads and denials are NOT audited");
            None
        }
    };
    let repl = ReplicationState::open(
        ReplicationConfig {
            role: cfg.replication.role,
            replica_id: cfg.pod_name.clone(),
            min_acks: cfg.replication.min_acks,
            ack_timeout: std::time::Duration::from_millis(cfg.replication.ack_timeout_ms),
            sync_lag_threshold: cfg.replication.sync_lag,
            max_lag_for_ready: cfg.replication.ready_lag,
            leader_addr: cfg.replication.leader_addr.clone(),
            state_file: cfg.replication.state_file.clone(),
            bootstrap_dir: cfg.replication.bootstrap_dir.clone(),
        },
        EpochHistory::read(&store)
            .context("reading the leadership epoch history")?
            .current(),
    )
    .context("opening the replication state")?;
    caregraph::replication::ensure_epoch(&store, &repl).context("recording the leader epoch")?;
    repl.register_metrics(&registry)
        .context("registering replication metrics")?;
    tracing::info!(
        role = repl.role().name(),
        epoch = repl.epoch(),
        min_acks = cfg.replication.min_acks,
        "replication"
    );

    let admin = {
        let manager = match &cfg.backup_dir {
            Some(dir) => Some(
                BackupManager::open(dir, dek.as_ref().map(|k| &k[..]))
                    .with_context(|| format!("opening backup directory {}", dir.display()))?,
            ),
            None => {
                tracing::warn!("CAREGRAPH_BACKUP_DIR is not set: backups cannot be taken");
                None
            }
        };
        let checkpoints = cfg.backup_dir.as_ref().map(|d| d.join("checkpoints"));
        Some(AdminOps::new(manager, checkpoints, &registry).context("registering backup metrics")?)
    };
    let api = CareGraphApi::with_options(
        store,
        graphsage_deployed.then_some(cfg.graphsage_model.as_str()),
        gat_deployed.then_some(cfg.gat_model.as_str()),
        &registry,
        ServiceOptions {
            ts_policy: cfg.ts_policy,
            input: cfg.input.clone(),
            rate_limiter: RateLimiter::new(cfg.server.rate_limit_rps, cfg.server.rate_limit_burst),
            policy,
            audit_log: audit_log.clone(),
            admin,
            replication: Some(repl.clone()),
        },
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

    let health = Health::with_replica(
        {
            let api = api.clone();
            Arc::new(move || api.model_statuses())
        },
        {
            let api = api.clone();
            Arc::new(move || api.store_responds())
        },
        {
            let repl = repl.clone();
            Arc::new(move || repl.not_ready_reason())
        },
    );
    {
        let repl = repl.clone();
        health.set_role_probe(Arc::new(move || repl.role().name()));
    }
    PoolCollector::register(health.clone(), &registry).context("registering pool metrics")?;
    let rpc_metrics = caregraph::server::observe::RpcMetrics::register(&registry)
        .context("registering rpc metrics")?;
    if let (Some(log), Some(dir)) = (&audit_log, &cfg.audit_dir) {
        let audit_metrics = caregraph::security::anchor::AuditMetrics::register(&registry)
            .context("registering audit metrics")?;
        let head_source = api.clone();
        caregraph::security::anchor::spawn(
            log.clone(),
            Arc::new(move || head_source.mutation_audit_head()),
            dir.clone(),
            audit_metrics,
            std::time::Duration::from_secs(60),
        );
    }
    {
        let api = api.clone();
        caregraph::memory::MemoryCollector::register(
            plan.clone(),
            Arc::new(move || api.worker_pids()),
            &registry,
        )
        .context("registering memory metrics")?;
    }

    let metrics_addr = cfg.metrics_addr;
    let metrics_task = {
        let (registry, health) = (registry.clone(), health.clone());
        tokio::spawn(async move {
            if let Err(e) = caregraph::metrics_server::serve(metrics_addr, registry, health).await {
                tracing::error!(%metrics_addr, error = %e, "metrics server exited");
            }
        })
    };
    tracing::info!(%metrics_addr, "serving /metrics, /healthz, /readyz, /startupz");

    let limits = cfg.server.clone();
    let service_server = || {
        caregraph::api::proto::care_graph_service_server::CareGraphServiceServer::new(api.clone())
            .max_decoding_message_size(limits.max_decoding_bytes)
            .max_encoding_message_size(limits.max_encoding_bytes)
    };

    let admin_server = || {
        caregraph::api::admin::proto::admin_service_server::AdminServiceServer::new(api.clone())
            .max_decoding_message_size(limits.max_decoding_bytes)
            .max_encoding_message_size(limits.max_encoding_bytes)
    };

    let replication_server = || {
        caregraph::replication::proto::replication_service_server::ReplicationServiceServer::new(
            api.clone(),
        )
        // A write batch is one message, and a bulk load can make a large one;
        // this service is authenticated and internal, so it is not held to the
        // client-facing message limits.
        .max_decoding_message_size(limits.max_decoding_bytes)
        .max_encoding_message_size(caregraph::replication::MAX_BATCH_MESSAGE_BYTES)
    };

    // A follower applies the leader's stream for as long as it is one.
    let (follower_stop, follower_stop_rx) = tokio::sync::watch::channel(false);
    let follower_task = (repl.role() == caregraph::replication::Role::Follower).then(|| {
        let r = &cfg.replication;
        tokio::spawn(follower::run(
            api.clone(),
            FollowerOptions {
                leader_addr: r.leader_addr.clone().unwrap_or_default(),
                ca: r.ca.clone(),
                cert: r.cert.clone(),
                key: r.key.clone(),
                server_name: r.server_name.clone(),
                token: r.token.clone(),
            },
            follower_stop_rx,
        ))
    });

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status(
            "caregraph.v1.CareGraphService",
            tonic_health::ServingStatus::Serving,
        )
        .await;
    {
        // Mirror readiness into the gRPC health service so gRPC-aware load
        // balancers see the same verdict the HTTP probe gives.
        let (health, mut reporter) = (health.clone(), health_reporter.clone());
        tokio::spawn(async move {
            loop {
                let status = if health.readiness().ready {
                    tonic_health::ServingStatus::Serving
                } else {
                    tonic_health::ServingStatus::NotServing
                };
                reporter
                    .set_service_status("caregraph.v1.CareGraphService", status)
                    .await;
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        });
    }

    let mut server_builder = Server::builder()
        .timeout(limits.request_timeout)
        .concurrency_limit_per_connection(limits.concurrency_limit_per_connection)
        .max_concurrent_streams(Some(limits.max_concurrent_streams))
        .http2_keepalive_interval(Some(limits.http2_keepalive_interval))
        .http2_keepalive_timeout(Some(limits.http2_keepalive_timeout))
        .tcp_nodelay(true)
        .layer(caregraph::server::observe::ObserveLayer::new(rpc_metrics));

    health.mark_started();
    let outcome = match (tls, api_key) {
        (Some(tls), _) => {
            tls.register_metrics(&registry)
                .context("registering TLS metrics")?;
            tokio::spawn(tls.clone().watch(std::time::Duration::from_secs(10)));
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding {addr}"))?;
            let incoming = tls::incoming(listener, tls, std::time::Duration::from_secs(10));
            let service = tonic::service::interceptor::InterceptedService::new(
                service_server(),
                MutualTlsInterceptor::new(tokens.clone()),
            );
            let admin_service = tonic::service::interceptor::InterceptedService::new(
                admin_server(),
                MutualTlsInterceptor::new(tokens.clone()),
            );
            let replication_service = tonic::service::interceptor::InterceptedService::new(
                replication_server(),
                MutualTlsInterceptor::new(tokens),
            );
            run_until_shutdown(
                move |stop| {
                    server_builder
                        .add_service(health_service)
                        .add_service(service)
                        .add_service(admin_service)
                        .add_service(replication_service)
                        .serve_with_incoming_shutdown(incoming, stop)
                },
                wait_for_signal(),
                &health,
                &limits,
            )
            .await?
        }
        (None, Some(key)) => {
            let service = tonic::service::interceptor::InterceptedService::new(
                service_server(),
                AuthInterceptor::new(key.clone()),
            );
            let admin_service = tonic::service::interceptor::InterceptedService::new(
                admin_server(),
                AuthInterceptor::new(key.clone()),
            );
            let replication_service = tonic::service::interceptor::InterceptedService::new(
                replication_server(),
                AuthInterceptor::new(key),
            );
            run_until_shutdown(
                move |stop| {
                    server_builder
                        .add_service(health_service)
                        .add_service(service)
                        .add_service(admin_service)
                        .add_service(replication_service)
                        .serve_with_shutdown(addr, stop)
                },
                wait_for_signal(),
                &health,
                &limits,
            )
            .await?
        }
        (None, None) => unreachable!("checked above: plaintext requires CAREGRAPH_API_KEY"),
    };
    tracing::info!(?outcome, "gRPC server stopped");
    let _ = follower_stop.send(true);
    if let Some(task) = follower_task {
        let _ = task.await;
    }

    // Requests whose clients went away keep running on the blocking pool;
    // wait for them, then force the WAL to disk before the store closes.
    if !api.quiesce(limits.drain_deadline).await {
        tracing::warn!("in-flight database work did not finish before the drain deadline");
    }
    if let Err(e) = api.flush() {
        tracing::error!(error = %e, "flushing the WAL at shutdown failed");
    }
    metrics_task.abort();
    drop(health);
    drop(api);
    tracing::info!("shutdown complete");
    Ok(())
}
