//! Role-based access control.
//!
//! A policy maps a principal (the name taken from the caller's client
//! certificate, see [`crate::security::identity`]) to roles; each RPC requires
//! one role. The policy file is TOML and is re-read when it changes, so access
//! can be granted or revoked without restarting the server:
//!
//! ```toml
//! [principals]
//! "spiffe://caregraph/ingest"  = ["writer"]
//! "spiffe://caregraph/analyst" = ["reader"]
//! "spiffe://caregraph/ops"     = ["admin"]
//! ```
//!
//! Roles are deliberately narrow (minimum necessary access): `writer` can read
//! and mutate, `reader` can only read, `admin` runs administrative RPCs and
//! nothing else, `replicator` may stream the write-ahead log to a follower.
//! A principal absent from the policy has no access.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Reader,
    Writer,
    Admin,
    Replicator,
}

/// Every callable RPC, one variant each, so that authorization is an
/// exhaustive `match` the compiler checks when an RPC is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rpc {
    AddEdge,
    RemoveEdge,
    Traverse,
    Snapshot,
    SimilarCarePathways,
    SimilarityDelta,
    /// Administrative service (backup, audit verification, promotion).
    Admin,
    /// Write-ahead-log streaming to a follower.
    Replicate,
}

impl Rpc {
    /// The gRPC method path, as it appears on the wire and in audit records.
    pub fn name(self) -> &'static str {
        match self {
            Rpc::AddEdge => "AddEdge",
            Rpc::RemoveEdge => "RemoveEdge",
            Rpc::Traverse => "Traverse",
            Rpc::Snapshot => "Snapshot",
            Rpc::SimilarCarePathways => "SimilarCarePathways",
            Rpc::SimilarityDelta => "SimilarityDelta",
            Rpc::Admin => "Admin",
            Rpc::Replicate => "Replicate",
        }
    }

    /// Whether `role` may call this RPC.
    pub fn permitted_to(self, role: Role) -> bool {
        match self {
            Rpc::Traverse | Rpc::Snapshot | Rpc::SimilarCarePathways | Rpc::SimilarityDelta => {
                matches!(role, Role::Reader | Role::Writer)
            }
            Rpc::AddEdge | Rpc::RemoveEdge => role == Role::Writer,
            Rpc::Admin => role == Role::Admin,
            Rpc::Replicate => role == Role::Replicator,
        }
    }

    pub const ALL: [Rpc; 8] = [
        Rpc::AddEdge,
        Rpc::RemoveEdge,
        Rpc::Traverse,
        Rpc::Snapshot,
        Rpc::SimilarCarePathways,
        Rpc::SimilarityDelta,
        Rpc::Admin,
        Rpc::Replicate,
    ];
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Policy {
    #[serde(default)]
    principals: HashMap<String, HashSet<Role>>,
    /// Development only: every authenticated caller may call everything.
    #[serde(skip)]
    allow_all: bool,
}

impl Policy {
    pub fn parse(text: &str) -> Result<Self> {
        let policy: Policy = toml::from_str(text).context("parsing RBAC policy as TOML")?;
        Ok(policy)
    }

    /// The development policy used when no policy file is configured.
    pub fn allow_all() -> Self {
        Policy {
            principals: HashMap::new(),
            allow_all: true,
        }
    }

    pub fn is_allow_all(&self) -> bool {
        self.allow_all
    }

    pub fn allows(&self, principal: &str, rpc: Rpc) -> bool {
        if self.allow_all {
            return true;
        }
        self.principals
            .get(principal)
            .is_some_and(|roles| roles.iter().any(|r| rpc.permitted_to(*r)))
    }

    pub fn principal_count(&self) -> usize {
        self.principals.len()
    }
}

/// A policy that follows its file on disk.
pub struct PolicyHandle {
    path: Option<PathBuf>,
    current: RwLock<Arc<Policy>>,
    loaded_mtime: RwLock<Option<SystemTime>>,
}

impl PolicyHandle {
    pub fn fixed(policy: Policy) -> Arc<Self> {
        Arc::new(PolicyHandle {
            path: None,
            current: RwLock::new(Arc::new(policy)),
            loaded_mtime: RwLock::new(None),
        })
    }

    pub fn from_file(path: &Path) -> Result<Arc<Self>> {
        let (policy, mtime) = read_policy(path)?;
        Ok(Arc::new(PolicyHandle {
            path: Some(path.to_path_buf()),
            current: RwLock::new(Arc::new(policy)),
            loaded_mtime: RwLock::new(Some(mtime)),
        }))
    }

    pub fn current(&self) -> Arc<Policy> {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn allows(&self, principal: &str, rpc: Rpc) -> bool {
        self.current().allows(principal, rpc)
    }

    /// Re-reads the file if its modification time changed. A file that fails to
    /// parse leaves the previous policy in force (a typo must not lock every
    /// client out, nor silently widen access) and the error is returned.
    pub fn reload_if_changed(&self) -> Result<bool> {
        let Some(path) = &self.path else {
            return Ok(false);
        };
        let mtime = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .with_context(|| format!("stat {}", path.display()))?;
        if *self.loaded_mtime.read().unwrap_or_else(|e| e.into_inner()) == Some(mtime) {
            return Ok(false);
        }
        let (policy, mtime) = read_policy(path)?;
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(policy);
        *self.loaded_mtime.write().unwrap_or_else(|e| e.into_inner()) = Some(mtime);
        Ok(true)
    }

    /// Polls for changes until the process exits.
    pub async fn watch(self: Arc<Self>, every: std::time::Duration) {
        loop {
            tokio::time::sleep(every).await;
            match self.reload_if_changed() {
                Ok(true) => tracing::info!("RBAC policy reloaded"),
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(error = %e, "RBAC policy reload failed; keeping the previous policy")
                }
            }
        }
    }
}

fn read_policy(path: &Path) -> Result<(Policy, SystemTime)> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading RBAC policy at {}", path.display()))?;
    let mtime = std::fs::metadata(path)?.modified()?;
    Ok((Policy::parse(&text)?, mtime))
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: &str = r#"
        [principals]
        "ingest"  = ["writer"]
        "analyst" = ["reader"]
        "ops"     = ["admin"]
        "follower" = ["replicator"]
        "both"    = ["reader", "admin"]
    "#;

    #[test]
    fn every_role_and_rpc_pair_is_decided_as_documented() {
        let p = Policy::parse(POLICY).unwrap();
        let expect = |who: &str, allowed: &[Rpc]| {
            for rpc in Rpc::ALL {
                assert_eq!(
                    p.allows(who, rpc),
                    allowed.contains(&rpc),
                    "{who} on {}",
                    rpc.name()
                );
            }
        };
        let reads = [
            Rpc::Traverse,
            Rpc::Snapshot,
            Rpc::SimilarCarePathways,
            Rpc::SimilarityDelta,
        ];
        expect("analyst", &reads);
        expect(
            "ingest",
            &[&reads[..], &[Rpc::AddEdge, Rpc::RemoveEdge]].concat(),
        );
        expect("ops", &[Rpc::Admin]);
        expect("follower", &[Rpc::Replicate]);
        expect("both", &[&reads[..], &[Rpc::Admin]].concat());
        expect("stranger", &[]);
    }

    #[test]
    fn the_development_policy_allows_everything() {
        let p = Policy::allow_all();
        assert!(Rpc::ALL.iter().all(|r| p.allows("anyone", *r)));
    }

    #[test]
    fn an_unknown_role_is_a_parse_error_not_a_silent_grant() {
        assert!(Policy::parse("[principals]\n\"a\" = [\"superuser\"]").is_err());
        assert!(Policy::parse("not toml at all =").is_err());
    }

    #[test]
    fn a_policy_file_is_reloaded_when_it_changes_and_a_bad_edit_keeps_the_old_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rbac.toml");
        std::fs::write(&path, "[principals]\n\"a\" = [\"reader\"]\n").unwrap();
        let h = PolicyHandle::from_file(&path).unwrap();
        assert!(h.allows("a", Rpc::Snapshot));
        assert!(!h.allows("a", Rpc::AddEdge));
        assert!(
            !h.reload_if_changed().unwrap(),
            "unchanged file is not re-read"
        );

        // mtime granularity can be coarse; make the change unambiguous.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::write(&path, "[principals]\n\"a\" = [\"writer\"]\n").unwrap();
        assert!(h.reload_if_changed().unwrap());
        assert!(h.allows("a", Rpc::AddEdge));

        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::write(&path, "[principals]\n\"a\" = [\"nonsense\"]\n").unwrap();
        assert!(h.reload_if_changed().is_err());
        assert!(
            h.allows("a", Rpc::AddEdge),
            "the previous policy stays in force"
        );
    }
}
