//! Compiles `proto/caregraph.proto` into Rust at build time (PRD Section 2.4)
//! and compiles `native/rocksdb_encryption/shim.cc` (PRD Section 0, Rule 8 —
//! see that directory's module doc for why a C++ shim is necessary at all).
//!
//! `tonic_build`'s default output goes to `OUT_DIR` and is pulled in via
//! `tonic::include_proto!` in `src/api/mod.rs` — no generated code is checked
//! in, so the `.proto` file is the only source of truth for the wire schema.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/caregraph.proto")?;
    println!("cargo:rerun-if-changed=proto/caregraph.proto");

    build_encryption_shim();

    Ok(())
}

/// Compiles the RocksDB-encryption C++ shim and links it into this crate's
/// final binaries. The shim's own symbols (`rocksdb::NewEncryptedEnv`,
/// `rocksdb::EncryptionProvider`, ...) resolve at final-link time against the
/// RocksDB static library librocksdb-sys already told Cargo to link via its
/// own `cargo:rustc-link-lib` — this build script only needs to add the shim's
/// object code, not re-link RocksDB itself.
fn build_encryption_shim() {
    let rocksdb_include = librocksdb_sys_include_dir();

    println!("cargo:rerun-if-changed=native/rocksdb_encryption/shim.cc");
    println!("cargo:rerun-if-changed=native/rocksdb_encryption/aes256.h");

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("native/rocksdb_encryption/shim.cc")
        .include(&rocksdb_include)
        .include("native/rocksdb_encryption")
        // RocksDB's own headers require this on Windows/MSVC builds (matches
        // librocksdb-sys's own build.rs flag set for the same headers).
        .define("NOMINMAX", None)
        .warnings(false) // upstream headers, not this shim's own code
        .compile("caregraph_rocksdb_encryption_shim");
}

/// Resolves librocksdb-sys's exact on-disk source directory (which vendors
/// `rocksdb/include/`) from the locked dependency graph, via `cargo metadata`
/// rather than the `links`-propagated DEP_ROCKSDB_* env vars — those turned
/// out not to be visible from this crate's build script in practice (a
/// build-dependency edge alone did not make them appear; verified by
/// printing every DEP_* var and finding none).
fn librocksdb_sys_include_dir() -> String {
    let metadata = cargo_metadata::MetadataCommand::new().exec().expect(
        "`cargo metadata` failed — needed to locate librocksdb-sys's vendored \
             rocksdb/include/ headers for native/rocksdb_encryption/shim.cc",
    );
    let pkg = metadata
        .packages
        .iter()
        .find(|p| p.name.as_str() == "librocksdb-sys")
        .expect(
            "librocksdb-sys not found in the resolved dependency graph — it should be \
             pulled in transitively via the `rocksdb` dependency",
        );
    let manifest_dir = pkg
        .manifest_path
        .parent()
        .expect("librocksdb-sys manifest_path has no parent directory");
    format!("{manifest_dir}/rocksdb/include")
}
