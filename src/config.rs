//! Typed, validated server configuration.
//!
//! Layering, lowest to highest precedence: built-in defaults, an optional TOML
//! file (`--config <path>` or `CAREGRAPH_CONFIG`), then `CAREGRAPH_*`
//! environment variables. Any secret may instead be supplied as `<NAME>_FILE`
//! (a path whose trimmed contents are the value), which is how Kubernetes
//! Secret mounts are consumed.
//!
//! `CAREGRAPH_ENV=production` turns every "insecure but convenient" default
//! into a startup refusal: the server will not run plaintext, unencrypted,
//! un-fsynced, or with legacy timestamp semantics in production.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};

pub const DEFAULT_GRAPHSAGE_MODEL: &str = "diabetes130_graphsage";
pub const DEFAULT_GAT_MODEL: &str = "diabetes130_gat";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Development,
    Production,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsPolicy {
    /// Reject timestamps at or below the high-water mark.
    Strict,
    /// The server assigns timestamps.
    ServerAssigned,
    /// Accept any client timestamp (pre-hardening behaviour).
    Legacy,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub environment: Environment,
    pub db_path: String,
    pub grpc_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub api_key: Option<String>,
    pub encryption_key_hex: Option<String>,
    /// Key-encryption key file and wrapped data key: the alternative to
    /// `encryption_key_hex` that allows rotating the KEK without re-encrypting.
    pub kek_file: Option<PathBuf>,
    pub wrapped_dek: Option<PathBuf>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_client_ca: Option<PathBuf>,
    pub tls_crl: Option<PathBuf>,
    pub tokens_file: Option<PathBuf>,
    /// Directory of this pod's own audit database (reads, denials, admin).
    pub audit_dir: Option<PathBuf>,
    /// Where the admin service writes backups and checkpoints. Unset disables them.
    pub backup_dir: Option<PathBuf>,
    pub pod_name: String,
    pub graphsage_model: String,
    pub gat_model: String,
    pub ml_dir: PathBuf,
    pub python: String,
    pub ts_policy: TsPolicy,
    pub sync_writes: bool,
    pub explain_enabled: bool,
    pub rbac_policy: Option<PathBuf>,
    pub log_json: bool,
    /// OTLP/gRPC collector for traces (`CAREGRAPH_OTEL_ENDPOINT`, or the standard
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`). Unset: no traces are exported.
    pub otel_endpoint: Option<String>,
    /// Fraction of root traces sampled (`CAREGRAPH_OTEL_SAMPLE_RATIO`). A caller that
    /// already sampled a trace (W3C `traceparent`) is always followed.
    pub otel_sample_ratio: f64,
    pub server: crate::server::ServerLimits,
    pub input: crate::server::InputLimits,
    pub replication: ReplicationSettings,
    /// Memory budget for the whole process tree, in MB (`CAREGRAPH_MEMORY_MB`).
    /// `None` keeps the historical defaults. See `crate::memory`.
    pub memory_mb: Option<u64>,
    /// Expected resident memory of one embedding worker, in MB.
    pub worker_memory_mb: u64,
}

/// Replication role, how a follower reaches its leader, and how the leader
/// waits for standbys.
#[derive(Debug, Clone)]
pub struct ReplicationSettings {
    pub role: crate::replication::Role,
    pub leader_addr: Option<String>,
    /// CA, certificate and key a follower uses to authenticate to the leader.
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub server_name: Option<String>,
    pub token: Option<String>,
    /// Standby confirmations a write waits for (0 = asynchronous).
    pub min_acks: usize,
    pub ack_timeout_ms: u64,
    pub sync_lag: u64,
    pub ready_lag: u64,
    pub wal_ttl_secs: u64,
    pub wal_size_mb: u64,
    pub state_file: Option<PathBuf>,
    /// Scratch space, on the database volume, for checkpoints streamed to new replicas.
    pub bootstrap_dir: Option<PathBuf>,
}

impl ReplicationSettings {
    fn load(src: &dyn Source, db_path: &str) -> Result<Self> {
        use crate::replication::Role;
        let role = match src.get("CAREGRAPH_ROLE") {
            None => Role::Standalone,
            Some(v) => match Role::parse(&v) {
                Some(r @ (Role::Standalone | Role::Leader | Role::Follower)) => r,
                _ => bail!("CAREGRAPH_ROLE must be standalone|leader|follower, got `{v}`"),
            },
        };
        let number = |name: &str, default: u64| -> Result<u64> {
            match src.get(name) {
                Some(v) => v
                    .parse()
                    .with_context(|| format!("{name} must be a non-negative integer")),
                None => Ok(default),
            }
        };
        let replicated = role != Role::Standalone;
        Ok(ReplicationSettings {
            role,
            leader_addr: src.get("CAREGRAPH_LEADER_ADDR"),
            ca: src.get("CAREGRAPH_REPLICATION_CA").map(PathBuf::from),
            cert: src.get("CAREGRAPH_REPLICATION_CERT").map(PathBuf::from),
            key: src.get("CAREGRAPH_REPLICATION_KEY").map(PathBuf::from),
            server_name: src.get("CAREGRAPH_REPLICATION_SERVER_NAME"),
            token: secret(src, "CAREGRAPH_REPLICATION_TOKEN")?,
            min_acks: number(
                "CAREGRAPH_REPLICATION_MIN_ACKS",
                u64::from(role == Role::Leader),
            )? as usize,
            ack_timeout_ms: number("CAREGRAPH_REPLICATION_ACK_TIMEOUT_MS", 5_000)?,
            sync_lag: number("CAREGRAPH_REPLICATION_SYNC_LAG", 10_000)?,
            ready_lag: number("CAREGRAPH_REPLICATION_READY_LAG", 10_000)?,
            wal_ttl_secs: number(
                "CAREGRAPH_WAL_TTL_SECS",
                if replicated { 6 * 3600 } else { 0 },
            )?,
            wal_size_mb: number("CAREGRAPH_WAL_SIZE_MB", 0)?,
            state_file: match src.get("CAREGRAPH_STATE_FILE") {
                Some(p) => Some(PathBuf::from(p)),
                None if replicated => Some(PathBuf::from(format!("{db_path}.role.json"))),
                None => None,
            },
            bootstrap_dir: match src.get("CAREGRAPH_BOOTSTRAP_DIR") {
                Some(p) => Some(PathBuf::from(p)),
                None if replicated => Some(PathBuf::from(format!("{db_path}.bootstrap"))),
                None => None,
            },
        })
    }

    pub fn summary(&self) -> String {
        format!(
            "{} (leader {}, min_acks {}, wal_ttl {}s)",
            self.role.name(),
            self.leader_addr.as_deref().unwrap_or("-"),
            self.min_acks,
            self.wal_ttl_secs
        )
    }
}

/// Source of raw string settings, keyed by the env var name
/// (e.g. `CAREGRAPH_DB_PATH`). Abstracted so validation is unit-testable
/// without mutating the process environment.
pub trait Source {
    fn get(&self, key: &str) -> Option<String>;
}

pub struct ProcessEnv;

impl Source for ProcessEnv {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok().filter(|v| !v.is_empty())
    }
}

/// Layers an optional TOML file beneath another source. Keys in the file are
/// the env var name lower-cased without the `CAREGRAPH_` prefix
/// (`db_path = "..."`).
pub struct Layered<S: Source> {
    file: HashMap<String, String>,
    top: S,
}

impl<S: Source> Layered<S> {
    pub fn new(file_toml: Option<&str>, top: S) -> Result<Self> {
        let mut file = HashMap::new();
        if let Some(text) = file_toml {
            let table: toml::Table = text.parse().context("parsing config file as TOML")?;
            for (k, v) in table {
                let s = match v {
                    toml::Value::String(s) => s,
                    toml::Value::Boolean(b) => b.to_string(),
                    toml::Value::Integer(i) => i.to_string(),
                    other => bail!(
                        "config key `{k}`: unsupported value type {}",
                        other.type_str()
                    ),
                };
                file.insert(k.to_ascii_lowercase(), s);
            }
        }
        Ok(Self { file, top })
    }
}

impl<S: Source> Source for Layered<S> {
    fn get(&self, key: &str) -> Option<String> {
        if let Some(v) = self.top.get(key) {
            return Some(v);
        }
        let short = key
            .strip_prefix("CAREGRAPH_")
            .unwrap_or(key)
            .to_ascii_lowercase();
        self.file.get(&short).cloned()
    }
}

/// Looks up `NAME`, then `NAME_FILE` (reading and trimming the file).
fn secret(src: &dyn Source, name: &str) -> Result<Option<String>> {
    if let Some(v) = src.get(name) {
        return Ok(Some(v));
    }
    let file_key = format!("{name}_FILE");
    match src.get(&file_key) {
        Some(path) => {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {file_key} at {path}"))?;
            let v = raw.trim().to_string();
            if v.is_empty() {
                bail!("{file_key} at {path} is empty");
            }
            Ok(Some(v))
        }
        None => Ok(None),
    }
}

fn parse_bool(name: &str, v: &str) -> Result<bool> {
    match v.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(anyhow!("{name} must be a boolean (true/false), got `{v}`")),
    }
}

impl Config {
    pub fn load(src: &dyn Source) -> Result<Self> {
        let environment = match src.get("CAREGRAPH_ENV").as_deref() {
            None | Some("development") | Some("dev") => Environment::Development,
            Some("production") | Some("prod") => Environment::Production,
            Some(other) => {
                bail!("CAREGRAPH_ENV must be `development` or `production`, got `{other}`")
            }
        };
        let prod = environment == Environment::Production;

        let addr = |name: &str, default: &str| -> Result<SocketAddr> {
            src.get(name)
                .unwrap_or_else(|| default.to_string())
                .parse()
                .with_context(|| format!("{name} is not a valid socket address"))
        };
        let boolean = |name: &str, default: bool| -> Result<bool> {
            match src.get(name) {
                Some(v) => parse_bool(name, &v),
                None => Ok(default),
            }
        };

        let ts_policy = match src.get("CAREGRAPH_TS_POLICY").as_deref() {
            None => {
                if prod {
                    TsPolicy::Strict
                } else {
                    TsPolicy::Legacy
                }
            }
            Some("strict") => TsPolicy::Strict,
            Some("server_assigned") => TsPolicy::ServerAssigned,
            Some("legacy") => TsPolicy::Legacy,
            Some(o) => {
                bail!("CAREGRAPH_TS_POLICY must be strict|server_assigned|legacy, got `{o}`")
            }
        };

        let explain_enabled = boolean("CAREGRAPH_EXPLAIN_ENABLED", !prod)?;
        let cfg = Config {
            environment,
            db_path: src
                .get("CAREGRAPH_DB_PATH")
                .unwrap_or_else(|| crate::DEFAULT_DB_PATH.to_string()),
            grpc_addr: addr("CAREGRAPH_GRPC_ADDR", "0.0.0.0:50051")?,
            metrics_addr: addr("CAREGRAPH_METRICS_ADDR", "0.0.0.0:9100")?,
            api_key: secret(src, "CAREGRAPH_API_KEY")?,
            encryption_key_hex: secret(src, "CAREGRAPH_ENCRYPTION_KEY")?,
            kek_file: src.get("CAREGRAPH_KEK_FILE").map(PathBuf::from),
            wrapped_dek: src.get("CAREGRAPH_WRAPPED_DEK").map(PathBuf::from),
            tls_cert: src.get("CAREGRAPH_TLS_CERT").map(PathBuf::from),
            tls_key: src.get("CAREGRAPH_TLS_KEY").map(PathBuf::from),
            tls_client_ca: src.get("CAREGRAPH_TLS_CLIENT_CA").map(PathBuf::from),
            tls_crl: src.get("CAREGRAPH_TLS_CRL").map(PathBuf::from),
            tokens_file: src.get("CAREGRAPH_TOKENS_FILE").map(PathBuf::from),
            audit_dir: src.get("CAREGRAPH_AUDIT_DIR").map(PathBuf::from),
            backup_dir: src.get("CAREGRAPH_BACKUP_DIR").map(PathBuf::from),
            pod_name: src
                .get("CAREGRAPH_POD_NAME")
                .or_else(|| src.get("HOSTNAME"))
                .unwrap_or_else(|| "caregraph".to_string()),
            graphsage_model: src
                .get("CAREGRAPH_GRAPHSAGE_MODEL")
                .unwrap_or_else(|| DEFAULT_GRAPHSAGE_MODEL.to_string()),
            gat_model: src
                .get("CAREGRAPH_GAT_MODEL")
                .unwrap_or_else(|| DEFAULT_GAT_MODEL.to_string()),
            ml_dir: src
                .get("CAREGRAPH_ML_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("ml")),
            python: src
                .get("CAREGRAPH_PYTHON")
                .unwrap_or_else(|| "python".to_string()),
            ts_policy,
            sync_writes: boolean("CAREGRAPH_SYNC_WRITES", prod)?,
            explain_enabled,
            rbac_policy: src.get("CAREGRAPH_RBAC_POLICY").map(PathBuf::from),
            log_json: boolean("CAREGRAPH_LOG_JSON", prod)?,
            otel_endpoint: src
                .get("CAREGRAPH_OTEL_ENDPOINT")
                .or_else(|| src.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
                .filter(|v| !v.trim().is_empty()),
            otel_sample_ratio: match src.get("CAREGRAPH_OTEL_SAMPLE_RATIO") {
                None => 0.05,
                Some(v) => v
                    .trim()
                    .parse::<f64>()
                    .ok()
                    .filter(|r| (0.0..=1.0).contains(r))
                    .ok_or_else(|| {
                        anyhow!("CAREGRAPH_OTEL_SAMPLE_RATIO must be between 0 and 1, got `{v}`")
                    })?,
            },
            server: crate::server::ServerLimits::load(src)?,
            input: crate::server::InputLimits::load(src, explain_enabled)?,
            memory_mb: match src
                .get("CAREGRAPH_MEMORY_MB")
                .filter(|v| !v.trim().is_empty())
            {
                Some(v) => Some(
                    v.trim()
                        .parse::<u64>()
                        .ok()
                        .filter(|&n| n > 0)
                        .with_context(|| {
                            format!("CAREGRAPH_MEMORY_MB must be a positive number of megabytes, got `{v}`")
                        })?,
                ),
                None => None,
            },
            worker_memory_mb: match src.get("CAREGRAPH_WORKER_MEMORY_MB") {
                Some(v) => v
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|&n| n > 0)
                    .with_context(|| {
                        format!("CAREGRAPH_WORKER_MEMORY_MB must be a positive number, got `{v}`")
                    })?,
                None => crate::memory::DEFAULT_WORKER_MB,
            },
            replication: ReplicationSettings::load(
                src,
                &src.get("CAREGRAPH_DB_PATH")
                    .unwrap_or_else(|| crate::DEFAULT_DB_PATH.to_string()),
            )?,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation. In production every refusal below is a hard
    /// startup error, never a warning.
    pub fn validate(&self) -> Result<()> {
        let tls_set = [&self.tls_cert, &self.tls_key, &self.tls_client_ca]
            .iter()
            .filter(|p| p.is_some())
            .count();
        if tls_set != 0 && tls_set != 3 {
            bail!(
                "CAREGRAPH_TLS_CERT, CAREGRAPH_TLS_KEY and CAREGRAPH_TLS_CLIENT_CA must be set \
                 together — partial config would silently downgrade mutual TLS"
            );
        }
        if let Some(hex) = &self.encryption_key_hex {
            crate::storage::decode_hex_key(hex).map_err(anyhow::Error::msg)?;
        }
        if self.kek_file.is_some() != self.wrapped_dek.is_some() {
            bail!("CAREGRAPH_KEK_FILE and CAREGRAPH_WRAPPED_DEK must be set together");
        }
        if self.kek_file.is_some() && self.encryption_key_hex.is_some() {
            bail!("set either CAREGRAPH_ENCRYPTION_KEY or the KEK/wrapped-key pair, not both");
        }
        {
            use crate::replication::Role;
            let r = &self.replication;
            if r.role == Role::Follower && r.leader_addr.is_none() {
                bail!("CAREGRAPH_LEADER_ADDR is required when CAREGRAPH_ROLE=follower");
            }
            if r.role == Role::Follower
                && r.leader_addr
                    .as_deref()
                    .is_some_and(|a| a.starts_with("https://"))
                && (r.cert.is_none() || r.key.is_none())
            {
                bail!(
                    "a follower reaching its leader over https needs CAREGRAPH_REPLICATION_CERT \
                     and CAREGRAPH_REPLICATION_KEY (and normally CAREGRAPH_REPLICATION_CA)"
                );
            }
        }
        if self.environment == Environment::Production {
            let mut problems = Vec::new();
            if tls_set != 3 {
                problems.push("mTLS is required (set the TLS cert, key and client CA)");
            }
            if self.encryption_key_hex.is_none() && self.kek_file.is_none() {
                problems.push(
                    "encryption at rest is required (CAREGRAPH_ENCRYPTION_KEY[_FILE], or                      CAREGRAPH_KEK_FILE with CAREGRAPH_WRAPPED_DEK)",
                );
            }
            if !self.sync_writes {
                problems.push("CAREGRAPH_SYNC_WRITES must be true");
            }
            if self.ts_policy == TsPolicy::Legacy {
                problems.push("CAREGRAPH_TS_POLICY=legacy is not allowed");
            }
            if self.rbac_policy.is_none() {
                problems.push("CAREGRAPH_RBAC_POLICY is required");
            }
            if self.audit_dir.is_none() {
                problems.push("CAREGRAPH_AUDIT_DIR is required (read auditing is fail-closed)");
            }
            if self.replication.role == crate::replication::Role::Follower
                && !self
                    .replication
                    .leader_addr
                    .as_deref()
                    .is_some_and(|a| a.starts_with("https://"))
            {
                problems.push(
                    "CAREGRAPH_LEADER_ADDR must be an https:// address: the replication stream                      carries decrypted data",
                );
            }
            if !problems.is_empty() {
                bail!("refusing to start in production: {}", problems.join("; "));
            }
        }
        Ok(())
    }

    /// Human-readable dump with every secret replaced by `<redacted>`.
    pub fn redacted(&self) -> String {
        let red = |o: &Option<String>| if o.is_some() { "<redacted>" } else { "<unset>" };
        let path = |o: &Option<PathBuf>| {
            o.as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<unset>".into())
        };
        format!(
            "environment      = {:?}\n\
             db_path          = {}\n\
             grpc_addr        = {}\n\
             metrics_addr     = {}\n\
             api_key          = {}\n\
             encryption_key   = {}\n\
             kek_file         = {}\n\
             wrapped_dek      = {}\n\
             tls_cert         = {}\n\
             tls_key          = {}\n\
             tls_client_ca    = {}\n\
             tls_crl          = {}\n\
             tokens_file      = {}\n\
             audit_dir        = {}\n\
             backup_dir       = {}\n\
             replication      = {}\n\
             memory_mb        = {}\n\
             pod_name         = {}\n\
             graphsage_model  = {}\n\
             gat_model        = {}\n\
             ml_dir           = {}\n\
             python           = {}\n\
             ts_policy        = {:?}\n\
             sync_writes      = {}\n\
             explain_enabled  = {}\n\
             rbac_policy      = {}\n\
             log_json         = {}\n\
             otel             = {}\n\
             request_timeout  = {}s\n\
             max_request_bytes= {}\n\
             drain_deadline   = {}s\n\
             prestop_delay    = {}s\n\
             rate_limit       = {} rps (burst {})\n\
             max_properties   = {} bytes, depth {}\n",
            self.environment,
            self.db_path,
            self.grpc_addr,
            self.metrics_addr,
            red(&self.api_key),
            red(&self.encryption_key_hex),
            path(&self.kek_file),
            path(&self.wrapped_dek),
            path(&self.tls_cert),
            path(&self.tls_key),
            path(&self.tls_client_ca),
            path(&self.tls_crl),
            path(&self.tokens_file),
            path(&self.audit_dir),
            path(&self.backup_dir),
            self.replication.summary(),
            self.memory_mb.map_or_else(
                || "<unset: historical defaults>".to_string(),
                |m| m.to_string()
            ),
            self.pod_name,
            self.graphsage_model,
            self.gat_model,
            self.ml_dir.display(),
            self.python,
            self.ts_policy,
            self.sync_writes,
            self.explain_enabled,
            path(&self.rbac_policy),
            self.log_json,
            self.otel_endpoint.as_deref().map_or_else(
                || "<off>".to_string(),
                |e| format!("{e} (sample {})", self.otel_sample_ratio)
            ),
            self.server.request_timeout.as_secs(),
            self.server.max_decoding_bytes,
            self.server.drain_deadline.as_secs(),
            self.server.prestop_delay.as_secs(),
            self.server.rate_limit_rps,
            self.server.rate_limit_burst,
            self.input.max_properties_bytes,
            self.input.max_properties_depth,
        )
    }

    pub fn model_dir(&self, model_id: &str) -> PathBuf {
        self.ml_dir.join("deployed").join(model_id)
    }

    /// How many embedding model pools this configuration will start: one per
    /// model whose `model.pt` is deployed.
    pub fn deployed_pools(&self) -> usize {
        [&self.graphsage_model, &self.gat_model]
            .iter()
            .filter(|m| self.model_dir(m).join("model.pt").exists())
            .count()
    }

    /// The memory plan for this configuration, or why the budget cannot be met.
    pub fn memory_plan(&self) -> Result<crate::memory::MemoryPlan> {
        crate::memory::MemoryPlan::derive(
            &crate::memory::Inputs {
                budget_mb: self.memory_mb,
                pools: self.deployed_pools(),
                workers_per_pool: crate::embedding::worker::pool::PoolConfig::from_env().workers,
                worker_mb: self.worker_memory_mb,
                overrides: crate::memory::Overrides::from_env(),
            },
            std::thread::available_parallelism().map_or(2, |n| n.get()),
        )
        .map_err(anyhow::Error::msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Map(HashMap<&'static str, &'static str>);
    impl Source for Map {
        fn get(&self, key: &str) -> Option<String> {
            self.0.get(key).map(|s| s.to_string())
        }
    }

    const KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    fn prod_ok() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("CAREGRAPH_ENV", "production"),
            ("CAREGRAPH_TLS_CERT", "/c"),
            ("CAREGRAPH_TLS_KEY", "/k"),
            ("CAREGRAPH_TLS_CLIENT_CA", "/ca"),
            ("CAREGRAPH_ENCRYPTION_KEY", KEY),
            ("CAREGRAPH_RBAC_POLICY", "/rbac.toml"),
            ("CAREGRAPH_AUDIT_DIR", "/audit"),
        ])
    }

    #[test]
    fn a_follower_needs_its_leader_and_in_production_a_tls_connection_to_it() {
        // Development: a follower must know where its leader is.
        let mut m = HashMap::from([("CAREGRAPH_ROLE", "follower")]);
        assert!(Config::load(&Map(m.clone()))
            .unwrap_err()
            .to_string()
            .contains("CAREGRAPH_LEADER_ADDR"));
        m.insert("CAREGRAPH_LEADER_ADDR", "http://leader:50051");
        let cfg = Config::load(&Map(m)).unwrap();
        assert_eq!(cfg.replication.role, crate::replication::Role::Follower);
        assert_eq!(
            cfg.replication.min_acks, 0,
            "only a leader waits for standbys"
        );
        assert!(cfg.replication.state_file.is_some());

        // Production: plaintext to the leader is refused, https needs a client identity.
        let mut p = prod_ok();
        p.insert("CAREGRAPH_ROLE", "follower");
        p.insert("CAREGRAPH_LEADER_ADDR", "http://leader:50051");
        let err = Config::load(&Map(p.clone())).unwrap_err().to_string();
        assert!(err.contains("https://"), "{err}");
        p.insert("CAREGRAPH_LEADER_ADDR", "https://leader:50051");
        let err = Config::load(&Map(p.clone())).unwrap_err().to_string();
        assert!(err.contains("REPLICATION_CERT"), "{err}");
        p.insert("CAREGRAPH_REPLICATION_CERT", "/rc");
        p.insert("CAREGRAPH_REPLICATION_KEY", "/rk");
        Config::load(&Map(p)).unwrap();
    }

    #[test]
    fn a_leader_waits_for_one_standby_by_default_and_a_standalone_node_never_does() {
        let leader = Config::load(&Map(HashMap::from([("CAREGRAPH_ROLE", "leader")]))).unwrap();
        assert_eq!(leader.replication.min_acks, 1);
        assert_eq!(leader.replication.wal_ttl_secs, 6 * 3600);
        let alone = Config::load(&Map(HashMap::new())).unwrap();
        assert_eq!(alone.replication.min_acks, 0);
        assert_eq!(
            alone.replication.wal_ttl_secs, 0,
            "no log is retained without replication"
        );
        assert!(alone.replication.state_file.is_none());
        let err = Config::load(&Map(HashMap::from([("CAREGRAPH_ROLE", "demoted")])))
            .unwrap_err()
            .to_string();
        assert!(err.contains("standalone|leader|follower"), "{err}");
    }

    #[test]
    fn the_memory_budget_is_validated_and_turned_into_a_plan() {
        let none = Config::load(&Map(HashMap::new())).unwrap();
        assert_eq!(none.memory_mb, None);
        assert_eq!(none.worker_memory_mb, crate::memory::DEFAULT_WORKER_MB);

        for bad in ["abc", "0", "-5", "1.5"] {
            let err = Config::load(&Map(HashMap::from([("CAREGRAPH_MEMORY_MB", bad)])))
                .unwrap_err()
                .to_string();
            assert!(err.contains("CAREGRAPH_MEMORY_MB"), "{bad}: {err}");
        }
        let bad_worker = Config::load(&Map(HashMap::from([("CAREGRAPH_WORKER_MEMORY_MB", "0")])));
        assert!(bad_worker.is_err());

        // The deployed models in the repository decide how many workers the
        // budget has to reserve; turning one off frees a worker's share.
        let both = Config::load(&Map(HashMap::from([("CAREGRAPH_MEMORY_MB", "4096")]))).unwrap();
        let one = Config::load(&Map(HashMap::from([
            ("CAREGRAPH_MEMORY_MB", "4096"),
            ("CAREGRAPH_GAT_MODEL", "none"),
        ])))
        .unwrap();
        if both.deployed_pools() == 2 {
            assert_eq!(one.deployed_pools(), 1);
            let (a, b) = (both.memory_plan().unwrap(), one.memory_plan().unwrap());
            assert_eq!(
                a.workers_mb - b.workers_mb,
                crate::memory::DEFAULT_WORKER_MB
            );
            assert!(
                b.rocksdb_mb > a.rocksdb_mb,
                "the freed memory goes to the database"
            );

            let tiny = Config::load(&Map(HashMap::from([("CAREGRAPH_MEMORY_MB", "300")]))).unwrap();
            let err = tiny.memory_plan().unwrap_err().to_string();
            assert!(err.contains("too small"), "{err}");
        }
    }

    #[test]
    fn development_defaults_load() {
        let cfg = Config::load(&Map(HashMap::new())).unwrap();
        assert_eq!(cfg.environment, Environment::Development);
        assert!(!cfg.sync_writes);
        assert_eq!(cfg.ts_policy, TsPolicy::Legacy);
    }

    #[test]
    fn a_fully_configured_production_node_loads_with_safe_defaults() {
        let cfg = Config::load(&Map(prod_ok())).unwrap();
        assert!(cfg.sync_writes);
        assert!(cfg.log_json);
        assert_eq!(cfg.ts_policy, TsPolicy::Strict);
        assert!(!cfg.explain_enabled);
    }

    #[test]
    fn production_refuses_each_unsafe_setting() {
        for (remove, expect) in [
            ("CAREGRAPH_TLS_CERT", "TLS"),
            ("CAREGRAPH_ENCRYPTION_KEY", "encryption"),
            ("CAREGRAPH_RBAC_POLICY", "RBAC"),
            ("CAREGRAPH_AUDIT_DIR", "AUDIT_DIR"),
        ] {
            let mut m = prod_ok();
            m.remove(remove);
            let err = Config::load(&Map(m)).unwrap_err().to_string();
            assert!(err.contains(expect), "{remove}: {err}");
        }
        let mut m = prod_ok();
        m.insert("CAREGRAPH_SYNC_WRITES", "false");
        assert!(Config::load(&Map(m))
            .unwrap_err()
            .to_string()
            .contains("SYNC_WRITES"));
        let mut m = prod_ok();
        m.insert("CAREGRAPH_TS_POLICY", "legacy");
        assert!(Config::load(&Map(m))
            .unwrap_err()
            .to_string()
            .contains("legacy"));
    }

    #[test]
    fn partial_tls_is_refused_even_in_development() {
        let m = HashMap::from([("CAREGRAPH_TLS_CERT", "/c")]);
        assert!(Config::load(&Map(m)).is_err());
    }

    #[test]
    fn a_malformed_encryption_key_is_refused() {
        let m = HashMap::from([("CAREGRAPH_ENCRYPTION_KEY", "abc")]);
        assert!(Config::load(&Map(m)).is_err());
    }

    #[test]
    fn redacted_output_never_contains_a_secret() {
        let mut m = prod_ok();
        m.insert("CAREGRAPH_API_KEY", "super-secret-token");
        let out = Config::load(&Map(m)).unwrap().redacted();
        assert!(!out.contains("super-secret-token"));
        assert!(!out.contains(KEY));
        assert!(out.contains("<redacted>"));
    }

    #[test]
    fn secret_file_indirection_reads_and_trims() {
        let dir = std::env::temp_dir().join(format!("cg_cfg_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("api_key");
        std::fs::write(&f, "  from-file\n").unwrap();
        let path: &'static str = Box::leak(f.display().to_string().into_boxed_str());
        let m = HashMap::from([("CAREGRAPH_API_KEY_FILE", path)]);
        let cfg = Config::load(&Map(m)).unwrap();
        assert_eq!(cfg.api_key.as_deref(), Some("from-file"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn env_overrides_the_toml_layer() {
        let toml = "db_path = \"/from/file\"\ngrpc_addr = \"127.0.0.1:1\"\n";
        let top = Map(HashMap::from([("CAREGRAPH_DB_PATH", "/from/env")]));
        let layered = Layered::new(Some(toml), top).unwrap();
        let cfg = Config::load(&layered).unwrap();
        assert_eq!(cfg.db_path, "/from/env");
        assert_eq!(cfg.grpc_addr.to_string(), "127.0.0.1:1");
    }
}
