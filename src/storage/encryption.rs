//! Real RocksDB encryption at rest (PRD Section 0, Rule 8).
//!
//! The `rocksdb` crate exposes no encryption API at all — its C bindings
//! (`librocksdb-sys`) never wrap `rocksdb/env_encryption.h`, even though the
//! C++ core they statically link already compiles `env/env_encryption.cc` in.
//! `native/rocksdb_encryption/shim.cc` bridges to RocksDB's real
//! `EncryptionProvider` / `NewEncryptedEnv` C++ API directly. The block cipher
//! behind it is the RustCrypto `aes` crate (AES-256; hardware AES-NI when the
//! CPU has it, a constant-time software implementation otherwise), handed to
//! the shim as a small table of function pointers — RocksDB's only built-in
//! cipher, `ROT13BlockCipher`, is documented as test-only and unsuitable for
//! production, and no cipher code is written by hand here.
//!
//! [`open_encrypted_env`] is the escape hatch the `rocksdb` crate's own docs
//! point to for exactly this situation (see `Env::from_raw`'s doc comment):
//! it hands a pre-instrumented `Env` — built through the shim, not through
//! any C API this crate's safe surface exposes — to the crate for further
//! use via `Options::set_env`.

use aes::cipher::generic_array::GenericArray;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use aes::Aes256;
use rocksdb::Env;

use crate::error::{CareGraphError, Result};

/// AES-256 key length in bytes. [`CAREGRAPH_ENCRYPTION_KEY`] must decode to
/// exactly this many bytes.
///
/// [`CAREGRAPH_ENCRYPTION_KEY`]: crate::storage::encryption::ENCRYPTION_KEY_ENV
pub const KEY_LEN: usize = 32;

/// The environment variable `RocksKv::open_encrypted` callers read the key
/// from. Named here, not just in `main.rs`, so the fault-injection worker and
/// any other binary that opens an encrypted database read the same variable.
pub const ENCRYPTION_KEY_ENV: &str = "CAREGRAPH_ENCRYPTION_KEY";

/// Function table the C++ shim calls for each 16-byte block. Layout must match
/// `caregraph_cipher_ops` in `native/rocksdb_encryption/shim.cc`.
#[repr(C)]
struct CipherOps {
    ctx: *mut std::ffi::c_void,
    encrypt: unsafe extern "C" fn(*mut std::ffi::c_void, *mut u8),
    decrypt: unsafe extern "C" fn(*mut std::ffi::c_void, *mut u8),
    destroy: unsafe extern "C" fn(*mut std::ffi::c_void),
}

/// AES block size in bytes.
const BLOCK: usize = 16;

unsafe extern "C" fn encrypt_block(ctx: *mut std::ffi::c_void, block: *mut u8) {
    // SAFETY: `ctx` is the `Box<Aes256>` leaked in `open_encrypted_env` and not
    // freed until `destroy_cipher`; `block` points to BLOCK writable bytes,
    // which is the contract of RocksDB's `BlockCipher::Encrypt`.
    let cipher = unsafe { &*(ctx as *const Aes256) };
    let block = unsafe { std::slice::from_raw_parts_mut(block, BLOCK) };
    cipher.encrypt_block(GenericArray::from_mut_slice(block));
}

unsafe extern "C" fn decrypt_block(ctx: *mut std::ffi::c_void, block: *mut u8) {
    // SAFETY: as `encrypt_block`.
    let cipher = unsafe { &*(ctx as *const Aes256) };
    let block = unsafe { std::slice::from_raw_parts_mut(block, BLOCK) };
    cipher.decrypt_block(GenericArray::from_mut_slice(block));
}

unsafe extern "C" fn destroy_cipher(ctx: *mut std::ffi::c_void) {
    // SAFETY: called exactly once by the shim's cipher destructor with the
    // pointer produced by `Box::into_raw` in `open_encrypted_env`. Dropping the
    // `Aes256` zeroizes its round keys (the `aes` crate's `zeroize` feature).
    drop(unsafe { Box::from_raw(ctx as *mut Aes256) });
}

extern "C" {
    // Declared in native/rocksdb_encryption/shim.cc. Returns a real
    // rocksdb_env_t* (RocksDB's C API's own struct layout — see that file's
    // header comment for why that specific layout is required) wrapping a
    // CTR-mode encrypted Env over the supplied block cipher, or null if
    // RocksDB itself refuses to construct the provider/env. Takes ownership of
    // `ops.ctx` in every case.
    fn caregraph_create_encrypted_env(ops: *const CipherOps) -> *mut librocksdb_sys::rocksdb_env_t;
}

/// Builds a real, RocksDB-native encrypted [`Env`] (AES-256 in CTR mode) from
/// a 32-byte key. Pass the result to [`rocksdb::Options::set_env`] before
/// opening a database to have every file RocksDB writes — SST files, the WAL,
/// MANIFEST, everything — encrypted on disk.
///
/// # Errors
///
/// Returns an error if `key` is not exactly [`KEY_LEN`] bytes, or if the
/// underlying RocksDB call fails (should not happen with a valid key; see
/// the shim's own doc comment).
pub fn open_encrypted_env(key: &[u8]) -> Result<Env> {
    if key.len() != KEY_LEN {
        return Err(CareGraphError::Encryption(format!(
            "encryption key must be exactly {KEY_LEN} bytes (AES-256), got {}",
            key.len()
        )));
    }

    let cipher = Box::new(
        Aes256::new_from_slice(key)
            .map_err(|e| CareGraphError::Encryption(format!("invalid AES-256 key: {e}")))?,
    );
    let ops = CipherOps {
        ctx: Box::into_raw(cipher) as *mut std::ffi::c_void,
        encrypt: encrypt_block,
        decrypt: decrypt_block,
        destroy: destroy_cipher,
    };

    // SAFETY: `caregraph_create_encrypted_env` is implemented in
    // native/rocksdb_encryption/shim.cc, compiled and linked by build.rs
    // against the same RocksDB static library the `rocksdb` crate already
    // links. `ops` is a valid, fully initialised table; the shim copies it and
    // takes ownership of `ctx`, calling `destroy_cipher` exactly once.
    let encrypted_env = unsafe { caregraph_create_encrypted_env(&ops) };

    if encrypted_env.is_null() {
        return Err(CareGraphError::Encryption(
            "RocksDB refused to construct the encrypted Env (see              caregraph_create_encrypted_env in shim.cc)"
                .to_string(),
        ));
    }

    // SAFETY: `encrypted_env` is a freshly allocated, non-null rocksdb_env_t*
    // that the shim constructed to match RocksDB's own C API struct layout
    // exactly (`struct rocksdb_env_t { Env* rep; bool is_default; }`, see
    // shim.cc's header comment) with is_default = false, so `Env`'s Drop
    // (which calls `rocksdb_env_destroy`) frees both the wrapper and the
    // encrypted Env it owns, exactly as it would for an Env the C API itself
    // created. Ownership transfers to the returned `Env`; nothing else holds
    // this pointer.
    Ok(unsafe { Env::from_raw(encrypted_env) })
}

/// Decodes a hex-encoded encryption key (as read from
/// [`ENCRYPTION_KEY_ENV`]) into raw bytes, validating its length.
///
/// A separate function from [`open_encrypted_env`] so callers can validate
/// and fail loudly on a malformed `CAREGRAPH_ENCRYPTION_KEY` *before*
/// touching RocksDB at all — matching how `CAREGRAPH_API_KEY` is handled in
/// `main.rs`: an explicitly-set-but-wrong value is refused, never silently
/// treated as "encryption disabled".
pub fn decode_hex_key(hex: &str) -> std::result::Result<[u8; KEY_LEN], String> {
    let hex = hex.trim();
    if hex.len() != KEY_LEN * 2 {
        return Err(format!(
            "{ENCRYPTION_KEY_ENV} must be {} hex characters ({KEY_LEN} bytes); got {} characters",
            KEY_LEN * 2,
            hex.len()
        ));
    }
    let mut out = [0u8; KEY_LEN];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let byte_str = std::str::from_utf8(chunk)
            .map_err(|_| format!("{ENCRYPTION_KEY_ENV} contains non-UTF-8 bytes"))?;
        out[i] = u8::from_str_radix(byte_str, 16).map_err(|_| {
            format!(
                "{ENCRYPTION_KEY_ENV} contains non-hex characters at position {}",
                i * 2
            )
        })?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_hex_key_rejects_wrong_length() {
        let err = decode_hex_key("abcd").unwrap_err();
        assert!(err.contains("64 hex characters"));
    }

    #[test]
    fn decode_hex_key_rejects_non_hex() {
        let bad = "gg".repeat(32);
        let err = decode_hex_key(&bad).unwrap_err();
        assert!(err.contains("non-hex"));
    }

    #[test]
    fn decode_hex_key_round_trips() {
        let hex = "00".repeat(32);
        let key = decode_hex_key(&hex).unwrap();
        assert_eq!(key, [0u8; KEY_LEN]);

        let hex_ff = "ff".repeat(32);
        let key_ff = decode_hex_key(&hex_ff).unwrap();
        assert_eq!(key_ff, [0xffu8; KEY_LEN]);
    }

    #[test]
    fn open_encrypted_env_rejects_wrong_key_length() {
        // `rocksdb::Env` isn't `Debug`, so `.unwrap_err()` (which requires
        // the Ok side to be Debug for its panic message) doesn't type-check
        // here — match manually instead.
        match open_encrypted_env(&[0u8; 16]) {
            Err(err) => assert!(err.to_string().contains("32 bytes")),
            Ok(_) => panic!("expected an error for a 16-byte key"),
        }
    }

    /// FIPS-197 Appendix C.3: AES-256 known-answer test, through the same
    /// function table the shim calls.
    #[test]
    fn the_block_function_matches_the_fips_197_vector() {
        let key: Vec<u8> = (0u8..32).collect();
        let mut block: [u8; 16] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        let expected: [u8; 16] = [
            0x8e, 0xa2, 0xb7, 0xca, 0x51, 0x67, 0x45, 0xbf, 0xea, 0xfc, 0x49, 0x90, 0x4b, 0x49,
            0x60, 0x89,
        ];
        let cipher =
            Box::into_raw(Box::new(Aes256::new_from_slice(&key).unwrap())) as *mut std::ffi::c_void;
        unsafe {
            encrypt_block(cipher, block.as_mut_ptr());
            assert_eq!(block, expected);
            decrypt_block(cipher, block.as_mut_ptr());
            destroy_cipher(cipher);
        }
        assert_eq!(block[0], 0x00);
        assert_eq!(block[15], 0xff);
    }

    #[test]
    fn open_encrypted_env_accepts_a_real_key() {
        // Real call into the shim — this is Rule 8's actual bar: a live call
        // into RocksDB's own encryption API, not just a type that compiles.
        let key = [0x42u8; KEY_LEN];
        open_encrypted_env(&key).expect("a valid 32-byte key must succeed");
    }
}
