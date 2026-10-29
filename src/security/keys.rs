//! Data-encryption-key management.
//!
//! The database is encrypted under a 256-bit data-encryption key (DEK). Keeping
//! that key in a configuration variable means rotating it requires re-encrypting
//! the whole database and that the key sits in plaintext wherever configuration
//! is stored. This module separates the two roles:
//!
//! - the DEK is stored **wrapped** (AES-256-GCM) beside the database, in a small
//!   JSON file;
//! - the wrapping key (KEK) comes from a [`KeyProvider`]. The provider here reads
//!   it from a file, which is how a Kubernetes Secret or an external-secrets
//!   mount delivers it; the trait is where a KMS or Vault transit provider plugs
//!   in.
//!
//! **KEK rotation** is cheap and online: unwrap with the old KEK, wrap with the
//! new one, replace the file ([`rewrap`]). The database is untouched. **DEK
//! rotation** means every file is re-encrypted (`caregraph-admin reencrypt`, or
//! bootstrap a replica under a new DEK and promote it).
//!
//! Every wrapped file records the id of the KEK that wrapped it, and the AES-GCM
//! associated data binds the ciphertext to that id and to the format version, so
//! a wrapped key cannot be silently relabelled.

use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::storage::KEY_LEN;

const FORMAT_VERSION: u32 = 1;
const NONCE_LEN: usize = 12;

/// A 256-bit key that is wiped from memory when dropped.
pub type SecretKey = Zeroizing<[u8; KEY_LEN]>;

/// Where the key-encryption key comes from.
pub trait KeyProvider: Send + Sync {
    /// Identifier recorded in wrapped files (not secret).
    fn kek_id(&self) -> String;
    /// The key-encryption key itself.
    fn kek(&self) -> Result<SecretKey>;
}

/// A KEK stored as 64 hex characters in a file (a mounted Secret). The id is
/// the first 8 bytes of the SHA-256 of the key, so two different keys never
/// share an id and the id reveals nothing useful about the key.
pub struct FileKekProvider {
    path: PathBuf,
}

impl FileKekProvider {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        FileKekProvider { path: path.into() }
    }
}

fn read_hex_key(path: &Path) -> Result<SecretKey> {
    let text = Zeroizing::new(
        std::fs::read_to_string(path)
            .with_context(|| format!("reading key file {}", path.display()))?,
    );
    let key = crate::storage::decode_hex_key(text.trim()).map_err(|e| anyhow!(e))?;
    Ok(Zeroizing::new(key))
}

fn id_of(key: &SecretKey) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(key.as_ref());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

impl KeyProvider for FileKekProvider {
    fn kek_id(&self) -> String {
        read_hex_key(&self.path)
            .map(|k| id_of(&k))
            .unwrap_or_else(|_| "unreadable".to_string())
    }

    fn kek(&self) -> Result<SecretKey> {
        read_hex_key(&self.path)
    }
}

/// A wrapped DEK as stored on disk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WrappedDek {
    pub version: u32,
    /// Id of the KEK that wrapped it.
    pub kek_id: String,
    /// 12-byte GCM nonce, hex.
    pub nonce: String,
    /// Ciphertext plus 16-byte tag, hex.
    pub ciphertext: String,
}

fn aad(version: u32, kek_id: &str) -> Vec<u8> {
    format!("caregraph-dek-v{version}:{kek_id}").into_bytes()
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn from_hex(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        bail!("odd-length hex");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| anyhow!("not hex")))
        .collect()
}

/// Generates a fresh random DEK.
pub fn generate_dek() -> SecretKey {
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    use aes_gcm::aead::rand_core::RngCore;
    OsRng.fill_bytes(key.as_mut());
    key
}

/// Wraps `dek` under the provider's KEK.
pub fn wrap(provider: &dyn KeyProvider, dek: &SecretKey) -> Result<WrappedDek> {
    let kek = provider.kek()?;
    let kek_id = id_of(&kek);
    let cipher = Aes256Gcm::new_from_slice(kek.as_ref()).map_err(|e| anyhow!("{e}"))?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: dek.as_ref(),
                aad: &aad(FORMAT_VERSION, &kek_id),
            },
        )
        .map_err(|_| anyhow!("wrapping the data key failed"))?;
    Ok(WrappedDek {
        version: FORMAT_VERSION,
        kek_id,
        nonce: to_hex(&nonce),
        ciphertext: to_hex(&ciphertext),
    })
}

/// Unwraps a DEK. Fails — without saying which part was wrong — if the KEK is
/// not the one that wrapped it or the file was altered.
pub fn unwrap(provider: &dyn KeyProvider, wrapped: &WrappedDek) -> Result<SecretKey> {
    if wrapped.version != FORMAT_VERSION {
        bail!("unsupported wrapped-key version {}", wrapped.version);
    }
    let kek = provider.kek()?;
    let cipher = Aes256Gcm::new_from_slice(kek.as_ref()).map_err(|e| anyhow!("{e}"))?;
    let nonce_bytes = from_hex(&wrapped.nonce)?;
    if nonce_bytes.len() != NONCE_LEN {
        bail!("malformed wrapped key");
    }
    let ciphertext = from_hex(&wrapped.ciphertext)?;
    let plain = Zeroizing::new(
        cipher
            .decrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: &ciphertext,
                    aad: &aad(wrapped.version, &wrapped.kek_id),
                },
            )
            .map_err(|_| anyhow!("could not unwrap the data key (wrong key-encryption key, or the file was altered)"))?,
    );
    let key: [u8; KEY_LEN] = plain
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("malformed wrapped key"))?;
    Ok(Zeroizing::new(key))
}

/// KEK rotation: the same DEK, wrapped under a new KEK. The database is not
/// touched.
pub fn rewrap(
    old: &dyn KeyProvider,
    new: &dyn KeyProvider,
    wrapped: &WrappedDek,
) -> Result<WrappedDek> {
    let dek = unwrap(old, wrapped)?;
    wrap(new, &dek)
}

pub fn read_wrapped(path: &Path) -> Result<WrappedDek> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading wrapped key at {}", path.display()))?;
    serde_json::from_str(&text).context("parsing the wrapped key file")
}

/// Writes atomically (temp file + rename) so a crash never leaves a half-written
/// key file next to the database.
pub fn write_wrapped(path: &Path, wrapped: &WrappedDek) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(wrapped)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))?;
    Ok(())
}

/// Resolves the database key at startup: unwrap the stored DEK with the
/// provider's KEK.
pub fn load_dek(provider: &dyn KeyProvider, wrapped_path: &Path) -> Result<SecretKey> {
    unwrap(provider, &read_wrapped(wrapped_path)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kek_file(dir: &Path, name: &str, byte: u8) -> FileKekProvider {
        let p = dir.join(name);
        std::fs::write(&p, to_hex(&[byte; KEY_LEN])).unwrap();
        FileKekProvider::new(p)
    }

    #[test]
    fn a_wrapped_key_unwraps_to_the_same_key_and_is_not_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let kek = kek_file(dir.path(), "kek", 0x11);
        let dek = generate_dek();
        let wrapped = wrap(&kek, &dek).unwrap();
        assert!(!wrapped.ciphertext.contains(&to_hex(dek.as_ref())));
        assert_eq!(*unwrap(&kek, &wrapped).unwrap(), *dek);
    }

    #[test]
    fn the_wrong_kek_or_a_tampered_file_fails_to_unwrap() {
        let dir = tempfile::tempdir().unwrap();
        let kek = kek_file(dir.path(), "kek", 0x11);
        let other = kek_file(dir.path(), "other", 0x22);
        let wrapped = wrap(&kek, &generate_dek()).unwrap();
        assert!(unwrap(&other, &wrapped).is_err());

        let mut flipped = wrapped.clone();
        let mut ct = from_hex(&flipped.ciphertext).unwrap();
        ct[0] ^= 1;
        flipped.ciphertext = to_hex(&ct);
        assert!(unwrap(&kek, &flipped).is_err());

        let mut relabelled = wrapped.clone();
        relabelled.kek_id = "0000000000000000".into();
        assert!(
            unwrap(&kek, &relabelled).is_err(),
            "the KEK id is authenticated, so it cannot be swapped"
        );
    }

    #[test]
    fn rotating_the_kek_keeps_the_dek_and_retires_the_old_kek() {
        let dir = tempfile::tempdir().unwrap();
        let old = kek_file(dir.path(), "old", 0x11);
        let new = kek_file(dir.path(), "new", 0x22);
        let dek = generate_dek();
        let wrapped = wrap(&old, &dek).unwrap();

        let rotated = rewrap(&old, &new, &wrapped).unwrap();
        assert_ne!(rotated.kek_id, wrapped.kek_id);
        assert_eq!(*unwrap(&new, &rotated).unwrap(), *dek, "same data key");
        assert!(
            unwrap(&old, &rotated).is_err(),
            "old KEK no longer opens it"
        );
    }

    #[test]
    fn the_wrapped_file_round_trips_through_disk_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let kek = kek_file(dir.path(), "kek", 0x33);
        let path = dir.path().join("dek.wrapped");
        let dek = generate_dek();
        write_wrapped(&path, &wrap(&kek, &dek).unwrap()).unwrap();
        assert!(!dir.path().join("dek.tmp").exists());
        assert_eq!(*load_dek(&kek, &path).unwrap(), *dek);
    }

    #[test]
    fn two_wraps_of_the_same_key_differ() {
        let dir = tempfile::tempdir().unwrap();
        let kek = kek_file(dir.path(), "kek", 0x44);
        let dek = generate_dek();
        assert_ne!(
            wrap(&kek, &dek).unwrap().ciphertext,
            wrap(&kek, &dek).unwrap().ciphertext,
            "a fresh nonce every time"
        );
    }
}
