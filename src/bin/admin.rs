//! `caregraph-admin` — operations on a CareGraph database.
//!
//! Two kinds of command:
//!
//! * **Offline** commands open database files directly and so need the server
//!   stopped (RocksDB allows one opener): `backup list|verify|restore|purge`,
//!   `audit-verify`, `repair-status`, `reencrypt`.
//! * **`remote`** commands call the running server's admin service over gRPC
//!   (with mTLS, or the development API key): backup creation, checkpoints,
//!   audit verification and status, which need the live database handle.
//!
//! Key material for offline commands comes from exactly one of: `--plaintext`;
//! `--key-hex` (or `CAREGRAPH_ENCRYPTION_KEY`); or `--kek-file` with
//! `--wrapped-dek` (or their env vars). There is no default, so a forgotten key
//! can never silently mean "unencrypted".

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use caregraph::api::admin::proto::admin_service_client::AdminServiceClient;
use caregraph::api::admin::proto::{
    CreateBackupRequest, CreateCheckpointRequest, DemoteRequest, ListBackupsRequest,
    PromoteRequest, StatusRequest, VerifyAuditRequest,
};
use caregraph::embedding::repair::pending_repairs;
use caregraph::security::audit::verify_store;
use caregraph::security::keys::{self, FileKekProvider, SecretKey};
use caregraph::storage::backup::BackupManager;
use caregraph::storage::maintenance::{check_database, reencrypt};
use caregraph::storage::{decode_hex_key, RocksKv};
use clap::{Args, Parser, Subcommand};
use serde_json::json;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::Request;

#[derive(Parser)]
#[command(
    name = "caregraph-admin",
    version,
    about = "Operate a CareGraph database"
)]
struct Cli {
    /// Print machine-readable JSON instead of text.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Clone)]
struct KeyArgs {
    /// The database is not encrypted.
    #[arg(long)]
    plaintext: bool,
    /// 64 hex characters: the data-encryption key itself.
    #[arg(long, env = "CAREGRAPH_ENCRYPTION_KEY", hide_env_values = true)]
    key_hex: Option<String>,
    /// Key-encryption key file (hex), used with --wrapped-dek.
    #[arg(long, env = "CAREGRAPH_KEK_FILE")]
    kek_file: Option<PathBuf>,
    /// The wrapped data-encryption key file.
    #[arg(long, env = "CAREGRAPH_WRAPPED_DEK")]
    wrapped_dek: Option<PathBuf>,
}

impl KeyArgs {
    /// The data key, or `None` for `--plaintext`. Exactly one source must be
    /// given.
    fn resolve(&self) -> Result<Option<SecretKey>> {
        let by_wrap = self.kek_file.is_some() || self.wrapped_dek.is_some();
        match (self.plaintext, self.key_hex.is_some(), by_wrap) {
            (true, false, false) => Ok(None),
            (false, true, false) => {
                let key = decode_hex_key(self.key_hex.as_deref().unwrap_or_default())
                    .map_err(anyhow::Error::msg)?;
                Ok(Some(zeroize::Zeroizing::new(key)))
            }
            (false, false, true) => {
                let (Some(kek), Some(wrapped)) = (&self.kek_file, &self.wrapped_dek) else {
                    bail!("--kek-file and --wrapped-dek must be given together");
                };
                Ok(Some(keys::load_dek(&FileKekProvider::new(kek), wrapped)?))
            }
            (false, false, false) => {
                bail!("no key given: pass --plaintext, --key-hex, or --kek-file with --wrapped-dek")
            }
            _ => bail!("give exactly one of --plaintext, --key-hex, or --kek-file/--wrapped-dek"),
        }
    }
}

fn slice(key: &Option<SecretKey>) -> Option<&[u8]> {
    key.as_ref().map(|k| &k[..])
}

#[derive(Args, Clone)]
struct RemoteArgs {
    /// Server address, e.g. https://caregraph-write:50051
    #[arg(
        long,
        env = "CAREGRAPH_ADMIN_ADDR",
        default_value = "http://127.0.0.1:50051"
    )]
    addr: String,
    /// CA that signed the server certificate.
    #[arg(long, env = "CAREGRAPH_ADMIN_CA")]
    ca: Option<PathBuf>,
    /// Client certificate (its identity needs the admin role).
    #[arg(long, env = "CAREGRAPH_ADMIN_CERT")]
    cert: Option<PathBuf>,
    #[arg(long, env = "CAREGRAPH_ADMIN_KEY")]
    key: Option<PathBuf>,
    /// Name to verify in the server certificate, if it differs from the host.
    #[arg(long)]
    server_name: Option<String>,
    /// Bearer token: the development API key, or a second-factor token.
    #[arg(long, env = "CAREGRAPH_ADMIN_TOKEN", hide_env_values = true)]
    token: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Backups of a stopped database's backup directory.
    #[command(subcommand)]
    Backup(BackupCmd),
    /// Verify the mutation audit chain of a stopped database.
    AuditVerify {
        #[arg(long)]
        db: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
    },
    /// Read every key of a stopped database and verify its audit chain.
    Check {
        #[arg(long)]
        db: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
    },
    /// List nodes whose embedding is stale because a forward pass failed.
    RepairStatus {
        #[arg(long)]
        db: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
        /// Show at most this many node ids.
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Copy a database under a freshly generated data key (data-key rotation).
    ///
    /// The source is untouched. The new data key is written wrapped under
    /// --new-kek-file to --new-wrapped-dek; start the server against `--dst`
    /// with that file, then retire the old database.
    Reencrypt {
        #[arg(long)]
        src: PathBuf,
        #[arg(long)]
        dst: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
        #[arg(long)]
        new_kek_file: PathBuf,
        #[arg(long)]
        new_wrapped_dek: PathBuf,
    },
    /// Key-encryption-key operations.
    #[command(subcommand)]
    Kek(KekCmd),
    /// Bearer-token generation.
    #[command(subcommand)]
    Token(TokenCmd),
    /// Commands against a running server's admin service.
    #[command(subcommand)]
    Remote(RemoteCmd),
}

#[derive(Subcommand)]
enum BackupCmd {
    List {
        #[arg(long)]
        dir: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
    },
    /// Restore, open and read everything in a backup, and verify its audit chain.
    Verify {
        #[arg(long)]
        dir: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
        /// Backup id (default: the newest).
        #[arg(long)]
        id: Option<u32>,
        /// Scratch directory for the trial restore (removed afterwards).
        #[arg(long)]
        scratch: Option<PathBuf>,
    },
    /// Restore into a new (or empty) directory. Never overwrites a database.
    Restore {
        #[arg(long)]
        dir: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
        #[arg(long)]
        to: PathBuf,
        #[arg(long)]
        id: Option<u32>,
    },
    /// Keep the newest N backups and delete the rest.
    Purge {
        #[arg(long)]
        dir: PathBuf,
        #[command(flatten)]
        key: KeyArgs,
        #[arg(long)]
        keep: usize,
    },
}

#[derive(Subcommand)]
enum KekCmd {
    /// Write a new random key-encryption key (hex) to a file, mode 0600 on Unix.
    New {
        #[arg(long)]
        out: PathBuf,
    },
    /// Wrap a new random data key under a KEK and write it to a file.
    NewDek {
        #[arg(long)]
        kek_file: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Re-wrap a data key from the old KEK to a new one. The database is not
    /// touched. The wrapped file is replaced atomically.
    Rotate {
        #[arg(long)]
        wrapped_dek: PathBuf,
        #[arg(long)]
        old_kek_file: PathBuf,
        #[arg(long)]
        new_kek_file: PathBuf,
    },
}

#[derive(Subcommand)]
enum TokenCmd {
    /// Generate a token. The token is shown once; the entry for the tokens file
    /// (which holds only its hash) is printed beside it.
    New {
        #[arg(long)]
        id: String,
        #[arg(long)]
        principal: String,
        /// Days until it expires.
        #[arg(long, default_value_t = 90)]
        days: u64,
    },
}

#[derive(Subcommand)]
enum RemoteCmd {
    Status {
        #[command(flatten)]
        remote: RemoteArgs,
    },
    CreateBackup {
        #[command(flatten)]
        remote: RemoteArgs,
    },
    ListBackups {
        #[command(flatten)]
        remote: RemoteArgs,
    },
    Checkpoint {
        #[command(flatten)]
        remote: RemoteArgs,
        name: String,
    },
    VerifyAudit {
        #[command(flatten)]
        remote: RemoteArgs,
    },
    /// Stop a leader accepting writes (permanently; it must be rebuilt from a
    /// checkpoint of the new leader to rejoin) and print the last sequence
    /// number it committed.
    Demote {
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u32,
    },
    /// Make a follower the leader.
    Promote {
        #[command(flatten)]
        remote: RemoteArgs,
        /// Wait until the follower has applied at least this sequence (the old
        /// leader's final sequence, from `demote`).
        #[arg(long, default_value_t = 0)]
        wait_for_sequence: u64,
        /// Promote even if that sequence was not reached. Whatever the old
        /// leader committed beyond what this node has is lost.
        #[arg(long)]
        force: bool,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u32,
    },
    /// Planned failover: demote the leader at --addr, then promote the follower
    /// at --new-addr with the sequence the demotion reported.
    ///
    /// If the old leader cannot be reached, confirm it is down (or its volume is
    /// detached) and pass --old-leader-down: the follower is then promoted
    /// without waiting. With synchronous replication every write the old leader
    /// acknowledged is already on the follower, but a write it committed and had
    /// not yet acknowledged may be lost.
    Failover {
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long)]
        new_addr: String,
        #[arg(long)]
        old_leader_down: bool,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u32,
    },
}

/// Adds the bearer token, if any, to every request.
#[derive(Clone)]
struct Bearer(Option<MetadataValue<tonic::metadata::Ascii>>);

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

type Admin = AdminServiceClient<InterceptedService<Channel, Bearer>>;

async fn admin_client(remote: &RemoteArgs) -> Result<Admin> {
    let channel = connect(remote).await?;
    let bearer = match &remote.token {
        Some(t) => Some(
            format!("Bearer {t}")
                .parse()
                .map_err(|_| anyhow::anyhow!("the token is not a valid header value"))?,
        ),
        None => None,
    };
    Ok(AdminServiceClient::with_interceptor(
        channel,
        Bearer(bearer),
    ))
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Remote(cmd) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(remote(cmd, cli.json))
        }
        other => offline(other, cli.json),
    }
}

fn emit(json: bool, value: serde_json::Value, text: impl FnOnce() -> String) {
    if json {
        println!("{value}");
    } else {
        println!("{}", text());
    }
}

fn open_db(path: &Path, key: &Option<SecretKey>) -> Result<RocksKv> {
    match key {
        Some(k) => RocksKv::open_encrypted(path, &k[..]),
        None => RocksKv::open(path),
    }
    .with_context(|| format!("opening the database at {}", path.display()))
}

fn offline(command: Command, json: bool) -> Result<()> {
    match command {
        Command::Backup(cmd) => backup(cmd, json),
        Command::AuditVerify { db, key } => {
            let store = open_db(&db, &key.resolve()?)?;
            match verify_store(&store)? {
                Ok(s) => {
                    emit(
                        json,
                        json!({"intact": true, "records": s.records, "head": s.head_hash}),
                        || {
                            format!(
                                "audit chain intact: {} records, head {}",
                                s.records, s.head_hash
                            )
                        },
                    );
                    Ok(())
                }
                Err(e) => bail!("audit chain BROKEN at record {}: {}", e.at_seq, e.reason),
            }
        }
        Command::Check { db, key } => {
            let store = open_db(&db, &key.resolve()?)?;
            let check = check_database(&store)?;
            emit(
                json,
                json!({
                    "keys": check.keys.iter().map(|(n, c)| (n.to_string(), *c)).collect::<std::collections::BTreeMap<_, _>>(),
                    "audit_records": check.audit_records,
                    "audit_head": check.audit_head,
                }),
                || {
                    let mut out = String::new();
                    for (name, n) in &check.keys {
                        out.push_str(&format!("{name:<16} {n}\n"));
                    }
                    out.push_str(&format!(
                        "all keys read; audit chain intact ({} records)",
                        check.audit_records
                    ));
                    out
                },
            );
            Ok(())
        }
        Command::RepairStatus { db, key, limit } => {
            let store = open_db(&db, &key.resolve()?)?;
            let pending = pending_repairs(&store)?;
            emit(
                json,
                json!({
                    "pending": pending.len(),
                    "nodes": pending.iter().take(limit).map(|p| p.node.as_u64()).collect::<Vec<_>>(),
                }),
                || {
                    let mut out = format!("{} node(s) awaiting a fresh embedding", pending.len());
                    for p in pending.iter().take(limit) {
                        out.push_str(&format!(
                            "\n  node {} marked at {} (latest embedding: {})",
                            p.node.as_u64(),
                            p.marked_at.as_u64(),
                            p.latest_embedding
                                .map(|t| t.as_u64().to_string())
                                .unwrap_or_else(|| "none".into())
                        ));
                    }
                    out
                },
            );
            Ok(())
        }
        Command::Reencrypt {
            src,
            dst,
            key,
            new_kek_file,
            new_wrapped_dek,
        } => {
            let old = key.resolve()?;
            let provider = FileKekProvider::new(&new_kek_file);
            let new_key = keys::generate_dek();
            // The wrapped key is written first: if the copy fails nothing is
            // lost, and if the wrapped file could not be written the copy never
            // starts under a key nobody can recover.
            keys::write_wrapped(&new_wrapped_dek, &keys::wrap(&provider, &new_key)?)?;
            let check = reencrypt(&src, slice(&old), &dst, Some(&new_key[..]))?;
            emit(
                json,
                json!({"keys": check.total_keys(), "audit_records": check.audit_records}),
                || {
                    format!(
                        "copied {} keys and {} audit records to {}; verified equal.\n\
                         new wrapped data key: {}",
                        check.total_keys(),
                        check.audit_records,
                        dst.display(),
                        new_wrapped_dek.display()
                    )
                },
            );
            Ok(())
        }
        Command::Kek(cmd) => kek(cmd),
        Command::Token(TokenCmd::New {
            id,
            principal,
            days,
        }) => {
            let token = hex(&keys::generate_dek()[..]);
            let hash: String = caregraph::security::tokens::hash_token(&token)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            let not_after = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
                + days * 86_400;
            println!("token (shown once; give it to the client): {token}\n");
            println!(
                "[[token]]\nid = \"{id}\"\nsha256 = \"{hash}\"\nprincipal = \"{principal}\"\nnot_after_unix = {not_after}"
            );
            Ok(())
        }
        Command::Remote(_) => unreachable!("handled in main"),
    }
}

fn backup(cmd: BackupCmd, json: bool) -> Result<()> {
    match cmd {
        BackupCmd::List { dir, key } => {
            let key = key.resolve()?;
            let manager = BackupManager::open(&dir, slice(&key))?;
            let list = manager.list();
            emit(
                json,
                json!(list.iter().map(info_json).collect::<Vec<_>>()),
                || {
                    if list.is_empty() {
                        return "no backups".to_string();
                    }
                    list.iter()
                        .map(|b| {
                            format!(
                                "id {:<4} sequence {:<12} {:>12} bytes  {} files  created {}",
                                b.backup_id, b.sequence, b.size_bytes, b.num_files, b.created_unix
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                },
            );
            Ok(())
        }
        BackupCmd::Verify {
            dir,
            key,
            id,
            scratch,
        } => {
            let key = key.resolve()?;
            let manager = BackupManager::open(&dir, slice(&key))?;
            let id = match id {
                Some(id) => id,
                None => manager
                    .list()
                    .last()
                    .map(|b| b.backup_id)
                    .context("there are no backups")?,
            };
            let scratch = scratch.unwrap_or_else(|| {
                let mut p = dir.clone().into_os_string();
                p.push(format!(".verify-{id}"));
                PathBuf::from(p)
            });
            let report = manager.verify(id, &scratch)?;
            emit(
                json,
                json!({
                    "backup_id": report.backup_id,
                    "keys": report.database.total_keys(),
                    "audit_records": report.database.audit_records,
                }),
                || {
                    format!(
                        "backup {} verified: {} keys read, audit chain intact ({} records)",
                        report.backup_id,
                        report.database.total_keys(),
                        report.database.audit_records
                    )
                },
            );
            Ok(())
        }
        BackupCmd::Restore { dir, key, to, id } => {
            let key = key.resolve()?;
            let manager = BackupManager::open(&dir, slice(&key))?;
            let id = manager.restore(id, &to)?;
            emit(json, json!({"restored": id, "to": to}), || {
                format!("restored backup {id} to {}", to.display())
            });
            Ok(())
        }
        BackupCmd::Purge { dir, key, keep } => {
            let key = key.resolve()?;
            let manager = BackupManager::open(&dir, slice(&key))?;
            manager.purge(keep)?;
            emit(json, json!({"remaining": manager.list().len()}), || {
                format!("{} backup(s) remain", manager.list().len())
            });
            Ok(())
        }
    }
}

fn info_json(b: &caregraph::storage::backup::BackupInfo) -> serde_json::Value {
    json!({
        "backup_id": b.backup_id,
        "created_unix": b.created_unix,
        "size_bytes": b.size_bytes,
        "num_files": b.num_files,
        "sequence": b.sequence,
    })
}

fn kek(cmd: KekCmd) -> Result<()> {
    match cmd {
        KekCmd::New { out } => {
            let key = keys::generate_dek();
            write_secret(&out, &hex(&key[..]))?;
            println!("wrote a new key-encryption key to {}", out.display());
            Ok(())
        }
        KekCmd::NewDek { kek_file, out } => {
            let provider = FileKekProvider::new(&kek_file);
            keys::write_wrapped(&out, &keys::wrap(&provider, &keys::generate_dek())?)?;
            println!("wrote a new wrapped data key to {}", out.display());
            Ok(())
        }
        KekCmd::Rotate {
            wrapped_dek,
            old_kek_file,
            new_kek_file,
        } => {
            let old = FileKekProvider::new(&old_kek_file);
            let new = FileKekProvider::new(&new_kek_file);
            let rotated = keys::rewrap(&old, &new, &keys::read_wrapped(&wrapped_dek)?)?;
            keys::write_wrapped(&wrapped_dek, &rotated)?;
            println!(
                "re-wrapped {} under the new key-encryption key (id {})",
                wrapped_dek.display(),
                rotated.kek_id
            );
            Ok(())
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Writes a secret so only its owner can read it (Unix: mode 0600 from the
/// start, never briefly world-readable).
fn write_secret(path: &Path, text: &str) -> Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {} (it must not already exist)", path.display()))?;
    file.write_all(text.as_bytes())?;
    Ok(())
}

async fn connect(remote: &RemoteArgs) -> Result<Channel> {
    let mut endpoint = Channel::from_shared(remote.addr.clone()).context("invalid --addr")?;
    if remote.addr.starts_with("https://") {
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = &remote.ca {
            tls = tls.ca_certificate(Certificate::from_pem(std::fs::read(ca)?));
        }
        if let (Some(cert), Some(key)) = (&remote.cert, &remote.key) {
            tls = tls.identity(Identity::from_pem(
                std::fs::read(cert)?,
                std::fs::read(key)?,
            ));
        }
        if let Some(name) = &remote.server_name {
            tls = tls.domain_name(name.clone());
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    endpoint
        .connect()
        .await
        .with_context(|| format!("connecting to {}", remote.addr))
}

async fn remote(cmd: RemoteCmd, json: bool) -> Result<()> {
    let args = match &cmd {
        RemoteCmd::Status { remote }
        | RemoteCmd::CreateBackup { remote }
        | RemoteCmd::ListBackups { remote }
        | RemoteCmd::Checkpoint { remote, .. }
        | RemoteCmd::VerifyAudit { remote }
        | RemoteCmd::Demote { remote, .. }
        | RemoteCmd::Promote { remote, .. }
        | RemoteCmd::Failover { remote, .. } => remote.clone(),
    };
    let mut client = admin_client(&args).await?;
    match cmd {
        RemoteCmd::Status { .. } => {
            let s = client.status(StatusRequest {}).await?.into_inner();
            emit(
                json,
                json!({
                    "latest_sequence": s.latest_sequence, "hwm_us": s.hwm_us,
                    "audit_records": s.audit_records, "audit_head": s.audit_head,
                    "backups": s.backups, "last_backup_unix": s.last_backup_unix,
                    "backups_configured": s.backups_configured,
                    "role": s.role, "epoch": s.epoch, "leader_sequence": s.leader_sequence,
                    "lag": s.lag, "needs_bootstrap": s.needs_bootstrap,
                    "ready_for_reads": s.ready_for_reads,
                    "followers": s.followers.iter().map(|f| json!({"id": f.id, "acked_sequence": f.acked_sequence})).collect::<Vec<_>>(),
                }),
                || {
                    let mut replication = format!("role {}  epoch {}", s.role, s.epoch);
                    if s.role == "follower" {
                        replication.push_str(&format!(
                            "  lag {} (leader at {}){}",
                            s.lag,
                            s.leader_sequence,
                            if s.needs_bootstrap {
                                "  NEEDS RE-BOOTSTRAP"
                            } else {
                                ""
                            }
                        ));
                    }
                    for f in &s.followers {
                        replication.push_str(&format!(
                            "\n  follower {} confirmed {}",
                            f.id, f.acked_sequence
                        ));
                    }
                    format!(
                        "{replication}\nsequence {}  commit high-water mark {}\naudit: {} records, head {}\nbackups: {} (newest {}){}",
                        s.latest_sequence,
                        s.hwm_us,
                        s.audit_records,
                        s.audit_head,
                        s.backups,
                        s.last_backup_unix,
                        if s.backups_configured { "" } else { "  [no backup directory configured]" }
                    )
                },
            );
        }
        RemoteCmd::CreateBackup { .. } => {
            let b = client
                .create_backup(CreateBackupRequest {})
                .await?
                .into_inner();
            emit(
                json,
                json!({"backup_id": b.backup_id, "sequence": b.sequence, "size_bytes": b.size_bytes}),
                || format!("backup {} created at sequence {}", b.backup_id, b.sequence),
            );
        }
        RemoteCmd::ListBackups { .. } => {
            let list = client
                .list_backups(ListBackupsRequest {})
                .await?
                .into_inner();
            emit(
                json,
                json!(list
                    .backups
                    .iter()
                    .map(|b| json!({
                        "backup_id": b.backup_id, "created_unix": b.created_unix,
                        "size_bytes": b.size_bytes, "num_files": b.num_files, "sequence": b.sequence
                    }))
                    .collect::<Vec<_>>()),
                || {
                    if list.backups.is_empty() {
                        return "no backups".to_string();
                    }
                    list.backups
                        .iter()
                        .map(|b| {
                            format!(
                                "id {:<4} sequence {:<12} {:>12} bytes  created {}",
                                b.backup_id, b.sequence, b.size_bytes, b.created_unix
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                },
            );
        }
        RemoteCmd::Checkpoint { name, .. } => {
            let c = client
                .create_checkpoint(CreateCheckpointRequest { name })
                .await?
                .into_inner();
            emit(
                json,
                json!({"name": c.name, "path": c.path, "sequence": c.sequence}),
                || {
                    format!(
                        "checkpoint {} at sequence {} in {}",
                        c.name, c.sequence, c.path
                    )
                },
            );
        }
        RemoteCmd::Demote { timeout_secs, .. } => {
            let d = client
                .demote(DemoteRequest {
                    timeout_ms: timeout_secs.saturating_mul(1000),
                })
                .await?
                .into_inner();
            emit(
                json,
                json!({"final_sequence": d.final_sequence, "epoch": d.epoch, "follower_caught_up": d.follower_caught_up}),
                || {
                    format!(
                        "demoted: final sequence {}, epoch {}, follower caught up: {}\n\
                         this node must be rebuilt from a checkpoint of the new leader to rejoin",
                        d.final_sequence, d.epoch, d.follower_caught_up
                    )
                },
            );
        }
        RemoteCmd::Promote {
            wait_for_sequence,
            force,
            timeout_secs,
            ..
        } => {
            let p = client
                .promote(PromoteRequest {
                    wait_for_sequence,
                    wait_timeout_ms: timeout_secs.saturating_mul(1000),
                    force,
                })
                .await?
                .into_inner();
            emit(
                json,
                json!({"epoch": p.epoch, "sequence": p.sequence}),
                || format!("promoted: epoch {}, sequence {}", p.epoch, p.sequence),
            );
        }
        RemoteCmd::Failover {
            new_addr,
            old_leader_down,
            timeout_secs,
            ..
        } => {
            let timeout_ms = timeout_secs.saturating_mul(1000);
            let final_sequence = if old_leader_down {
                eprintln!(
                    "warning: promoting without contacting the old leader; a write it committed \
                     but had not acknowledged may be lost"
                );
                0
            } else {
                let d = client
                    .demote(DemoteRequest { timeout_ms })
                    .await?
                    .into_inner();
                eprintln!(
                    "old leader demoted: final sequence {}, follower caught up: {}",
                    d.final_sequence, d.follower_caught_up
                );
                d.final_sequence
            };
            let mut target = args.clone();
            target.addr = new_addr;
            let mut new_leader = admin_client(&target).await?;
            let p = new_leader
                .promote(PromoteRequest {
                    wait_for_sequence: final_sequence,
                    wait_timeout_ms: timeout_ms,
                    force: old_leader_down,
                })
                .await?
                .into_inner();
            emit(
                json,
                json!({"epoch": p.epoch, "sequence": p.sequence, "old_final_sequence": final_sequence}),
                || {
                    format!(
                        "failover complete: new leader is at epoch {}, sequence {}.\n\
                         next: move the write Service to the new leader, and rebuild the old one \
                         from a checkpoint of it before it rejoins",
                        p.epoch, p.sequence
                    )
                },
            );
        }
        RemoteCmd::VerifyAudit { .. } => {
            let v = client
                .verify_audit(VerifyAuditRequest {})
                .await?
                .into_inner();
            if !v.intact {
                bail!(
                    "audit chain BROKEN at record {}: {}",
                    v.first_bad_seq,
                    v.detail
                );
            }
            emit(
                json,
                json!({"intact": true, "records": v.records, "head": v.head}),
                || format!("audit chain intact: {} records, head {}", v.records, v.head),
            );
        }
    }
    Ok(())
}
