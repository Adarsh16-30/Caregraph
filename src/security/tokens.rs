//! Bearer tokens as a second factor.
//!
//! The server stores only the SHA-256 of each token, never the token, so a
//! leaked tokens file does not leak working credentials. Several tokens can be
//! valid at once (each with its own expiry), which is how rotation works: add
//! the new token, move clients over, remove the old one.
//!
//! Every presented token is compared against every stored hash in constant
//! time, so response time does not reveal which entry (if any) was close.
//!
//! ```toml
//! [[token]]
//! id = "ingest-2026-10"
//! sha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
//! principal = "spiffe://caregraph/ingest"
//! not_after_unix = 1798675200
//! ```

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

#[derive(Debug, Clone, Deserialize)]
struct TokenEntry {
    id: String,
    sha256: String,
    principal: String,
    /// Seconds since the Unix epoch; the token is rejected at and after this
    /// instant.
    not_after_unix: u64,
}

#[derive(Debug, Deserialize)]
struct TokenFile {
    #[serde(default)]
    token: Vec<TokenEntry>,
}

struct Parsed {
    id: String,
    hash: [u8; 32],
    principal: String,
    not_after: u64,
}

#[derive(Default)]
pub struct TokenSet {
    entries: Vec<Parsed>,
}

/// A token that verified: who it belongs to and which entry matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedToken {
    pub id: String,
    pub principal: String,
}

pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

impl TokenSet {
    pub fn parse(text: &str) -> Result<Self> {
        let file: TokenFile = toml::from_str(text).context("parsing tokens file as TOML")?;
        let mut entries = Vec::with_capacity(file.token.len());
        for t in file.token {
            let bytes = hex_decode(&t.sha256)
                .with_context(|| format!("token `{}`: sha256 is not 64 hex characters", t.id))?;
            let Ok(hash) = <[u8; 32]>::try_from(bytes.as_slice()) else {
                bail!("token `{}`: sha256 must be 32 bytes", t.id);
            };
            entries.push(Parsed {
                id: t.id,
                hash,
                principal: t.principal,
                not_after: t.not_after_unix,
            });
        }
        Ok(TokenSet { entries })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Checks `presented` against every entry without stopping early.
    pub fn verify(&self, presented: &str, now_unix: u64) -> Option<VerifiedToken> {
        let digest = hash_token(presented);
        let mut found: Option<&Parsed> = None;
        for e in &self.entries {
            let same = bool::from(e.hash.ct_eq(&digest));
            let live = now_unix < e.not_after;
            if same && live && found.is_none() {
                found = Some(e);
            }
        }
        found.map(|e| VerifiedToken {
            id: e.id.clone(),
            principal: e.principal.clone(),
        })
    }
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        bail!("odd length");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| anyhow::anyhow!("not hex")))
        .collect()
}

/// A token set that follows its file on disk.
pub struct TokenHandle {
    path: std::path::PathBuf,
    current: RwLock<Arc<TokenSet>>,
    loaded_mtime: RwLock<Option<SystemTime>>,
}

impl TokenHandle {
    pub fn from_file(path: &Path) -> Result<Arc<Self>> {
        let (set, mtime) = read(path)?;
        Ok(Arc::new(TokenHandle {
            path: path.to_path_buf(),
            current: RwLock::new(Arc::new(set)),
            loaded_mtime: RwLock::new(Some(mtime)),
        }))
    }

    pub fn verify(&self, presented: &str, now_unix: u64) -> Option<VerifiedToken> {
        let set = self
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        set.verify(presented, now_unix)
    }

    pub fn reload_if_changed(&self) -> Result<bool> {
        let mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .with_context(|| format!("stat {}", self.path.display()))?;
        if *self.loaded_mtime.read().unwrap_or_else(|e| e.into_inner()) == Some(mtime) {
            return Ok(false);
        }
        let (set, mtime) = read(&self.path)?;
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(set);
        *self.loaded_mtime.write().unwrap_or_else(|e| e.into_inner()) = Some(mtime);
        Ok(true)
    }

    pub async fn watch(self: Arc<Self>, every: std::time::Duration) {
        loop {
            tokio::time::sleep(every).await;
            match self.reload_if_changed() {
                Ok(true) => tracing::info!("tokens file reloaded"),
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(error = %e, "tokens reload failed; keeping the previous set")
                }
            }
        }
    }
}

fn read(path: &Path) -> Result<(TokenSet, SystemTime)> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading tokens file at {}", path.display()))?;
    let mtime = std::fs::metadata(path)?.modified()?;
    Ok((TokenSet::parse(&text)?, mtime))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(h: [u8; 32]) -> String {
        h.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn file(entries: &[(&str, &str, &str, u64)]) -> String {
        entries
            .iter()
            .map(|(id, token, principal, exp)| {
                format!(
                    "[[token]]\nid = \"{id}\"\nsha256 = \"{}\"\nprincipal = \"{principal}\"\nnot_after_unix = {exp}\n",
                    hex(hash_token(token))
                )
            })
            .collect()
    }

    #[test]
    fn a_valid_token_resolves_to_its_principal_and_others_do_not() {
        let set = TokenSet::parse(&file(&[("a", "secret-a", "alice", 2000)])).unwrap();
        let ok = set.verify("secret-a", 1000).unwrap();
        assert_eq!((ok.id.as_str(), ok.principal.as_str()), ("a", "alice"));
        assert!(set.verify("secret-b", 1000).is_none());
        assert!(set.verify("", 1000).is_none());
    }

    #[test]
    fn an_expired_token_is_refused_at_and_after_its_expiry() {
        let set = TokenSet::parse(&file(&[("a", "s", "alice", 2000)])).unwrap();
        assert!(set.verify("s", 1999).is_some());
        assert!(set.verify("s", 2000).is_none());
        assert!(set.verify("s", 9999).is_none());
    }

    #[test]
    fn overlapping_tokens_support_rotation() {
        let set = TokenSet::parse(&file(&[
            ("old", "old-secret", "alice", 1500),
            ("new", "new-secret", "alice", 9000),
        ]))
        .unwrap();
        assert_eq!(set.verify("old-secret", 1000).unwrap().id, "old");
        assert_eq!(set.verify("new-secret", 1000).unwrap().id, "new");
        assert!(
            set.verify("old-secret", 2000).is_none(),
            "old token retired"
        );
        assert!(set.verify("new-secret", 2000).is_some());
    }

    #[test]
    fn a_malformed_hash_is_rejected_at_load() {
        let bad = "[[token]]\nid=\"x\"\nsha256=\"abc\"\nprincipal=\"p\"\nnot_after_unix=1\n";
        assert!(TokenSet::parse(bad).is_err());
        let short = format!(
            "[[token]]\nid=\"x\"\nsha256=\"{}\"\nprincipal=\"p\"\nnot_after_unix=1\n",
            "ab".repeat(16)
        );
        assert!(TokenSet::parse(&short).is_err());
    }
}
