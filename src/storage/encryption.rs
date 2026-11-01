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
    /// Fills a buffer from the OS CSPRNG; 0 on success. Used for each new file's prefix
    /// (see the long comment on `SecurePrefixProvider` in the shim for why).
    fill_random: unsafe extern "C" fn(*mut u8, usize) -> i32,
}

// The shim asserts the same size (`static_assert` on `caregraph_cipher_ops`); a mismatch
// between the two declarations would be silent memory corruption.
const _: () = assert!(std::mem::size_of::<CipherOps>() == 5 * std::mem::size_of::<usize>());

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

/// The prefix source in production: the operating system's CSPRNG. Failing closed matters:
/// the shim refuses to create the file if this returns non-zero, instead of using anything
/// weaker.
unsafe extern "C" fn os_random(buf: *mut u8, len: usize) -> i32 {
    use aes_gcm::aead::rand_core::RngCore;
    // SAFETY: the shim passes a writable buffer of `len` bytes (RocksDB's prefix block).
    let buf = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match aes_gcm::aead::OsRng.try_fill_bytes(buf) {
        Ok(()) => 0,
        Err(_) => 1,
    }
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
    open_encrypted_env_with(key, os_random)
}

/// [`open_encrypted_env`] with the prefix source supplied, so a test can show where a file's
/// prefix comes from. Production code goes through `open_encrypted_env`, which always uses
/// the OS CSPRNG.
pub(crate) fn open_encrypted_env_with(
    key: &[u8],
    fill_random: unsafe extern "C" fn(*mut u8, usize) -> i32,
) -> Result<Env> {
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
        fill_random,
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

// ---------------------------------------------------------------------------------------
// Auditing the prefixes of an existing database
// ---------------------------------------------------------------------------------------

/// Length of the prefix RocksDB's CTR provider writes at the start of every encrypted file:
/// the initial counter (block 0), the IV (block 1), then filler.
pub const PREFIX_LEN: u64 = 4096;

/// Two files whose keystreams overlap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixOverlap {
    pub a: String,
    pub b: String,
}

/// Result of [`audit_prefixes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixAudit {
    /// Files that could carry a prefix (at least [`PREFIX_LEN`] bytes) and were checked.
    pub files: usize,
    /// Pairs of files that share a keystream. Anyone holding both ciphertexts can XOR
    /// them to get the XOR of the two plaintexts, with no key. Empty is the goal.
    pub overlaps: Vec<PrefixOverlap>,
}

/// Checks the files of an **encrypted** database directory for keystream reuse.
///
/// A file's keystream is `AES(key, counter + i || nonce)` for block `i`, where the counter
/// starts at the value in the file's first block and the nonce is the last 8 bytes of its
/// second block. Two files collide when they have the same nonce and their counter ranges
/// overlap. Files written by RocksDB's stock provider take both from a generator with 31 bits
/// of state, so this can find real collisions in a database created before the prefix came
/// from the OS CSPRNG. It reads the first 32 bytes and the length of each file; the key is
/// not needed. Run it only on an encrypted directory: on a plaintext one it reads
/// meaningless bytes.
///
/// A range is counted as the larger of the file's data blocks and the 254 blocks the stock
/// provider also spends encrypting its prefix filler, so the answer errs towards reporting.
pub fn audit_prefixes(dir: &std::path::Path) -> std::io::Result<PrefixAudit> {
    use std::collections::HashMap;
    use std::io::Read;

    struct F {
        name: String,
        counter: u64,
        blocks: u64,
    }
    let mut by_nonce: HashMap<[u8; 8], Vec<F>> = HashMap::new();
    let mut files = 0usize;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if !meta.is_file() || meta.len() < PREFIX_LEN {
            continue;
        }
        let mut head = [0u8; 32];
        std::fs::File::open(entry.path())?.read_exact(&mut head)?;
        let counter = u64::from_le_bytes(head[0..8].try_into().unwrap());
        let nonce: [u8; 8] = head[24..32].try_into().unwrap();
        let data_blocks = (meta.len() - PREFIX_LEN).div_ceil(BLOCK as u64);
        files += 1;
        by_nonce.entry(nonce).or_default().push(F {
            name: entry.file_name().to_string_lossy().into_owned(),
            counter,
            blocks: data_blocks.max((PREFIX_LEN - 32) / BLOCK as u64),
        });
    }

    let mut overlaps = Vec::new();
    for group in by_nonce.values().filter(|g| g.len() > 1) {
        for (i, a) in group.iter().enumerate() {
            for b in &group[i + 1..] {
                // Ranges on a ring of 2^64 counters: they overlap if either start lies in the
                // other's range.
                let hit = b.counter.wrapping_sub(a.counter) < a.blocks
                    || a.counter.wrapping_sub(b.counter) < b.blocks;
                if hit {
                    let (x, y) = if a.name <= b.name { (a, b) } else { (b, a) };
                    overlaps.push(PrefixOverlap {
                        a: x.name.clone(),
                        b: y.name.clone(),
                    });
                }
            }
        }
    }
    overlaps.sort_by(|p, q| (&p.a, &p.b).cmp(&(&q.a, &q.b)));
    Ok(PrefixAudit { files, overlaps })
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
    // ---- where a file's prefix comes from -------------------------------------------------

    use std::sync::atomic::{AtomicU64, Ordering};

    static MARKED_CALLS: AtomicU64 = AtomicU64::new(0);

    /// A recognisable "random" source: the whole buffer is 0xA5 except the first 8 bytes,
    /// which count the calls. Nothing else produces a prefix like this.
    unsafe extern "C" fn marked_random(buf: *mut u8, len: usize) -> i32 {
        let n = MARKED_CALLS.fetch_add(1, Ordering::SeqCst);
        let b = unsafe { std::slice::from_raw_parts_mut(buf, len) };
        b.fill(0xA5);
        b[..8].copy_from_slice(&n.to_le_bytes());
        0
    }

    unsafe extern "C" fn failing_random(_buf: *mut u8, _len: usize) -> i32 {
        1
    }

    fn open_db(dir: &std::path::Path, env: &Env) -> Result<rocksdb::DB> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_env(env);
        rocksdb::DB::open(&opts, dir).map_err(|e| CareGraphError::Encryption(e.to_string()))
    }

    /// Regression test for the weak stock prefix. RocksDB's own provider fills the prefix
    /// from a 31-bit time-seeded generator, so a file's prefix would not be what the random
    /// source returned; with the fix every byte of it is.
    #[test]
    fn a_new_files_whole_prefix_comes_from_the_random_source() {
        let dir = tempfile::tempdir().unwrap();
        let env = open_encrypted_env_with(&[0x42u8; KEY_LEN], marked_random).unwrap();
        {
            let db = open_db(dir.path(), &env).unwrap();
            for i in 0..2000u32 {
                db.put(i.to_be_bytes(), vec![7u8; 64]).unwrap();
            }
            db.flush().unwrap();
            // Reading it back shows the data still round-trips under such prefixes.
            assert_eq!(db.get(5u32.to_be_bytes()).unwrap().unwrap(), vec![7u8; 64]);
        }
        let mut checked = 0;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let bytes = std::fs::read(entry.path()).unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            if (bytes.len() as u64) < PREFIX_LEN || name == "LOG" {
                // LOCK is too small to carry a prefix. LOG is RocksDB's own info log; it is
                // opened through a path that does not go through the configured Env's
                // cipher stream (RocksDB writes it in the clear regardless of encryption
                // settings), so it never gets a prefix at all, marked or otherwise.
                continue;
            }
            assert!(
                bytes[8..PREFIX_LEN as usize].iter().all(|&b| b == 0xA5),
                "{}: the prefix is not what the random source produced",
                entry.file_name().to_string_lossy()
            );
            checked += 1;
        }
        assert!(
            checked >= 3,
            "expected several encrypted files, found {checked}"
        );
    }

    #[test]
    fn a_file_is_not_created_when_the_random_source_fails() {
        let dir = tempfile::tempdir().unwrap();
        let env = open_encrypted_env_with(&[0x42u8; KEY_LEN], failing_random).unwrap();
        let err = open_db(dir.path(), &env)
            .err()
            .expect("creating encrypted files must fail without random bytes");
        assert!(
            err.to_string().contains("random number generator"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn a_database_reopens_and_reads_back_with_the_production_random_source() {
        let dir = tempfile::tempdir().unwrap();
        let key = [0x17u8; KEY_LEN];
        {
            let env = open_encrypted_env(&key).unwrap();
            let db = open_db(dir.path(), &env).unwrap();
            db.put(b"k", b"v").unwrap();
            db.flush().unwrap();
        }
        let env = open_encrypted_env(&key).unwrap();
        let db = open_db(dir.path(), &env).unwrap();
        assert_eq!(db.get(b"k").unwrap().unwrap(), b"v");
    }

    // ---- auditing prefixes ----------------------------------------------------------------

    fn write_prefixed(
        dir: &std::path::Path,
        name: &str,
        counter: u64,
        nonce: [u8; 8],
        data_blocks: u64,
    ) {
        let mut bytes = vec![0u8; (PREFIX_LEN + data_blocks * BLOCK as u64) as usize];
        bytes[0..8].copy_from_slice(&counter.to_le_bytes());
        bytes[24..32].copy_from_slice(&nonce);
        std::fs::write(dir.join(name), bytes).unwrap();
    }

    #[test]
    fn the_audit_finds_files_that_share_a_keystream_and_only_those() {
        let dir = tempfile::tempdir().unwrap();
        let n1 = [1u8; 8];
        let n2 = [2u8; 8];
        write_prefixed(dir.path(), "a.sst", 1_000, n1, 500); // blocks 1000..1500
        write_prefixed(dir.path(), "b.sst", 1_400, n1, 500); // overlaps a's range
        write_prefixed(dir.path(), "c.sst", 9_000, n1, 500); // same nonce, disjoint counters
        write_prefixed(dir.path(), "d.sst", 1_000, n2, 500); // blocks 1000..1500
                                                             // Starts 10 blocks before the counter wraps and runs long enough (1100 blocks) to
                                                             // wrap around past 0 and land inside d.sst's range (1000..1500): blocks
                                                             // u64::MAX-10 ..= u64::MAX, then 0..=1089.
        write_prefixed(dir.path(), "wrap.sst", u64::MAX - 10, n2, 1_100);
        std::fs::write(dir.path().join("LOCK"), b"").unwrap(); // not an encrypted file

        let audit = audit_prefixes(dir.path()).unwrap();
        assert_eq!(audit.files, 5);
        assert_eq!(
            audit.overlaps,
            vec![
                PrefixOverlap {
                    a: "a.sst".into(),
                    b: "b.sst".into()
                },
                PrefixOverlap {
                    a: "d.sst".into(),
                    b: "wrap.sst".into()
                },
            ]
        );
    }

    #[test]
    fn a_database_written_now_has_no_overlapping_keystreams() {
        let dir = tempfile::tempdir().unwrap();
        let env = open_encrypted_env(&[0x23u8; KEY_LEN]).unwrap();
        {
            let db = open_db(dir.path(), &env).unwrap();
            for round in 0..6u32 {
                for i in 0..500u32 {
                    db.put((round * 1000 + i).to_be_bytes(), vec![round as u8; 128])
                        .unwrap();
                }
                db.flush().unwrap(); // one new file per round
            }
        }
        let audit = audit_prefixes(dir.path()).unwrap();
        assert!(audit.files >= 6, "{audit:?}");
        assert!(audit.overlaps.is_empty(), "{audit:?}");
    }
}
