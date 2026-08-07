//! `caregraph-load` — apply a JSONL mutation trace to a real RocksDB instance.
//!
//! Consumes the trace emitted by `data/idpip_ukpds_loader.py` and writes it into
//! the versioned column families. This is the Phase 1 ingestion path; from Phase
//! 6 the same records arrive over gRPC instead.
//!
//! Structural writes only. Embeddings are *not* written here — they become part
//! of the commit at Phase 4/5, at which point every mutation goes through
//! `run_mutation_pipeline` and lands in the same WriteBatch as its embedding
//! update (Rule 5). Loading a graph without embeddings is honest; loading it
//! with placeholder embedding vectors would violate Rule 3.
//!
//! Usage:
//!     caregraph-load --trace benchmarks/traces/ukpds_smoke_100.jsonl \
//!                    --db data/db/caregraph

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::keys::{encode_edge_key, encode_node_key};
use caregraph::types::{EdgeType, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde::Deserialize;

/// Records are flushed in batches of this many to bound memory on a full
/// 5,102-patient load without making every line its own fsync.
const BATCH_SIZE: usize = 1_000;

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Record {
    UpsertNode {
        node_id: u64,
        node_type: String,
        timestamp_us: u64,
        #[serde(default)]
        properties: serde_json::Value,
    },
    AddEdge {
        src: u64,
        dst: u64,
        edge_type: u16,
        timestamp_us: u64,
        #[serde(default)]
        properties: serde_json::Value,
    },
}

#[derive(Default, Debug)]
struct Stats {
    nodes: u64,
    edges: u64,
}

fn parse_args() -> Result<(PathBuf, String)> {
    let mut trace: Option<PathBuf> = None;
    let mut db: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--trace" => trace = args.next().map(PathBuf::from),
            "--db" => db = args.next(),
            "-h" | "--help" => {
                println!("usage: caregraph-load --trace <file.jsonl> [--db <path>]");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let trace = trace.context("--trace is required")?;
    Ok((trace, db.unwrap_or_else(caregraph::db_path_from_env)))
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "caregraph=info".into()),
        )
        .init();

    let (trace_path, db_path) = parse_args()?;

    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let store = RocksKv::open(&db_path).with_context(|| format!("opening RocksDB at {db_path}"))?;

    let file = File::open(&trace_path)
        .with_context(|| format!("opening trace {}", trace_path.display()))?;
    let reader = BufReader::new(file);

    let mut stats = Stats::default();
    let mut batch = WriteBatch::default();
    let mut pending = 0usize;

    let nodes_cf = store.cf_handle(cf::CF_NODES)?;
    let edges_cf = store.cf_handle(cf::CF_EDGES)?;
    let reverse_cf = store.cf_handle(cf::CF_REVERSE)?;

    for (lineno, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading line {}", lineno + 1))?;
        if line.trim().is_empty() {
            continue;
        }

        let record: Record = serde_json::from_str(&line)
            .with_context(|| format!("{}:{}: malformed record", trace_path.display(), lineno + 1))?;

        match record {
            Record::UpsertNode {
                node_id,
                node_type,
                timestamp_us,
                properties,
            } => {
                let key = encode_node_key(NodeId(node_id), Timestamp(timestamp_us));
                let value = serde_json::json!({ "type": node_type, "properties": properties });
                batch.put_cf(&nodes_cf, key, serde_json::to_vec(&value)?);
                pending += 1;
                stats.nodes += 1;
            }
            Record::AddEdge {
                src,
                dst,
                edge_type,
                timestamp_us,
                properties,
            } => {
                // An unrecognised discriminant means the trace and
                // src/types.rs::EdgeType have drifted apart. Fail rather than
                // silently dropping clinical relationships.
                let edge_type = EdgeType::from_u16(edge_type).with_context(|| {
                    format!(
                        "{}:{}: unknown edge_type discriminant {edge_type}",
                        trace_path.display(),
                        lineno + 1
                    )
                })?;
                let (src, dst, ts) = (NodeId(src), NodeId(dst), Timestamp(timestamp_us));
                let value = serde_json::to_vec(&properties)?;

                batch.put_cf(&edges_cf, encode_edge_key(src, edge_type, ts, dst), &value);
                // Mirrored into CF_REVERSE so incoming traversal is a prefix
                // scan rather than a full scan (PRD 3.3).
                batch.put_cf(&reverse_cf, encode_edge_key(dst, edge_type, ts, src), &value);
                pending += 2;
                stats.edges += 1;
            }
        }

        if pending >= BATCH_SIZE {
            store.write(std::mem::take(&mut batch))?;
            pending = 0;
        }
    }

    if pending > 0 {
        store.write(batch)?;
    }

    for name in [cf::CF_NODES, cf::CF_EDGES, cf::CF_REVERSE] {
        store.flush(name)?;
    }

    tracing::info!(
        trace = %trace_path.display(),
        db = %db_path,
        nodes = stats.nodes,
        edges = stats.edges,
        "trace applied"
    );

    if stats.nodes == 0 && stats.edges == 0 {
        bail!("trace contained no records; refusing to report an empty load as success");
    }

    Ok(())
}
