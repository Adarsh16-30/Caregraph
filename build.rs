//! Compiles `proto/caregraph.proto` into Rust at build time (PRD Section 2.4).
//!
//! `tonic_build`'s default output goes to `OUT_DIR` and is pulled in via
//! `tonic::include_proto!` in `src/api/mod.rs` — no generated code is checked
//! in, so the `.proto` file is the only source of truth for the wire schema.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/caregraph.proto")?;
    println!("cargo:rerun-if-changed=proto/caregraph.proto");
    Ok(())
}
