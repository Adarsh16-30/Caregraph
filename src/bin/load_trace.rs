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
//!
//! `--out-dir <DIR>` additionally times the read-plus-batch-write loop below
//! (Section 1's "sustained ingestion throughput" metric) and writes a
//! timestamped raw-results JSON there (Rules 4, 10) — this *is* the real
//! Phase 1 ingestion path, not a separate synthetic ingest benchmark, so the
//! number it produces is the actual bulk-load throughput this binary
//! delivers, structural writes only (no embedding computation is on this
//! path — see the module doc above). The final `flush()` calls are excluded
//! from the timed window: they are a one-time compaction-style call this
//! binary happens to make at the end of a bulk load, not part of the
//! steady-state ingest rate a production system would sustain continuously.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use caregraph::storage::{cf, KvStore, RocksKv};
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::TemporalWriter;
use caregraph::types::{EdgeType, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};

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
    /// A retraction — a diagnosis withdrawn, a prescription stopped. Recorded
    /// as a tombstone version at `timestamp_us`, never as a delete, so the
    /// history before the retraction stays queryable.
    RemoveEdge {
        src: u64,
        dst: u64,
        edge_type: u16,
        timestamp_us: u64,
    },
}

#[derive(Default, Debug)]
struct Stats {
    nodes: u64,
    edges: u64,
    removals: u64,
}

/// An unrecognised discriminant means the trace and `src/types.rs::EdgeType`
/// have drifted apart. Fail rather than silently dropping clinical
/// relationships — a graph quietly missing its diagnoses still loads, still
/// benchmarks, and is wrong.
fn decode_edge_type(raw: u16, trace: &std::path::Path, lineno: usize) -> Result<EdgeType> {
    EdgeType::from_u16(raw).with_context(|| {
        format!(
            "{}:{}: unknown edge_type discriminant {raw}",
            trace.display(),
            lineno + 1
        )
    })
}

fn parse_args() -> Result<(PathBuf, String, Option<PathBuf>)> {
    let mut trace: Option<PathBuf> = None;
    let mut db: Option<String> = None;
    let mut out_dir: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--trace" => trace = args.next().map(PathBuf::from),
            "--db" => db = args.next(),
            "--out-dir" => out_dir = args.next().map(PathBuf::from),
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-load --trace <file.jsonl> [--db <path>] \
                     [--out-dir <dir>]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let trace = trace.context("--trace is required")?;
    Ok((
        trace,
        db.unwrap_or_else(caregraph::db_path_from_env),
        out_dir,
    ))
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[derive(Serialize)]
struct Provenance {
    generated_at_unix: u64,
    git_commit: String,
    git_dirty: bool,
    command: String,
}

#[derive(Serialize)]
struct IngestReport {
    benchmark: &'static str,
    prd_target: &'static str,
    provenance: Provenance,
    trace: String,
    nodes: u64,
    edges: u64,
    removals: u64,
    elapsed_secs: f64,
    edges_per_sec: f64,
    total_mutations_per_sec: f64,
    min_edges_per_sec_target: f64,
    passed: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "caregraph=info".into()),
        )
        .init();

    let (trace_path, db_path, out_dir) = parse_args()?;

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

    let writer = TemporalWriter::new(&store)?;
    let ingest_start = Instant::now();

    for (lineno, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading line {}", lineno + 1))?;
        if line.trim().is_empty() {
            continue;
        }

        let record: Record = serde_json::from_str(&line).with_context(|| {
            format!("{}:{}: malformed record", trace_path.display(), lineno + 1)
        })?;

        match record {
            Record::UpsertNode {
                node_id,
                node_type,
                timestamp_us,
                properties,
            } => {
                writer.put_node(
                    &mut batch,
                    NodeId(node_id),
                    Timestamp(timestamp_us),
                    &NodeValue::new(node_type, properties),
                );
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
                let edge_type = decode_edge_type(edge_type, &trace_path, lineno)?;
                // put_edge mirrors into CF_REVERSE in the same batch, so forward
                // and reverse adjacency can never disagree (PRD 3.3).
                writer.put_edge(
                    &mut batch,
                    NodeId(src),
                    edge_type,
                    NodeId(dst),
                    Timestamp(timestamp_us),
                    &EdgeValue::new(properties),
                );
                pending += 2;
                stats.edges += 1;
            }
            Record::RemoveEdge {
                src,
                dst,
                edge_type,
                timestamp_us,
            } => {
                let edge_type = decode_edge_type(edge_type, &trace_path, lineno)?;
                writer.remove_edge(
                    &mut batch,
                    NodeId(src),
                    edge_type,
                    NodeId(dst),
                    Timestamp(timestamp_us),
                );
                pending += 2;
                stats.removals += 1;
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
    let ingest_elapsed = ingest_start.elapsed();

    for name in [cf::CF_NODES, cf::CF_EDGES, cf::CF_REVERSE] {
        store.flush(name)?;
    }

    tracing::info!(
        trace = %trace_path.display(),
        db = %db_path,
        nodes = stats.nodes,
        edges = stats.edges,
        removals = stats.removals,
        "trace applied"
    );

    if stats.nodes == 0 && stats.edges == 0 && stats.removals == 0 {
        bail!("trace contained no records; refusing to report an empty load as success");
    }

    if let Some(out_dir) = out_dir {
        const MIN_EDGES_PER_SEC_TARGET: f64 = 10_000.0;
        let elapsed_secs = ingest_elapsed.as_secs_f64();
        let edges_per_sec = stats.edges as f64 / elapsed_secs;
        let total_mutations = stats.nodes + stats.edges + stats.removals;
        let total_mutations_per_sec = total_mutations as f64 / elapsed_secs;

        let report = IngestReport {
            benchmark: "sustained_ingestion_throughput",
            prd_target: "Section 1: sustained ingestion throughput 10,000-50,000 edges/sec",
            provenance: Provenance {
                generated_at_unix: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                git_commit: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
                git_dirty: git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty()),
                command: std::env::args().collect::<Vec<_>>().join(" "),
            },
            trace: trace_path.display().to_string(),
            nodes: stats.nodes,
            edges: stats.edges,
            removals: stats.removals,
            elapsed_secs,
            edges_per_sec,
            total_mutations_per_sec,
            min_edges_per_sec_target: MIN_EDGES_PER_SEC_TARGET,
            passed: edges_per_sec >= MIN_EDGES_PER_SEC_TARGET,
        };

        std::fs::create_dir_all(&out_dir)?;
        let out = out_dir.join(format!(
            "ingest_throughput_{}.json",
            report.provenance.generated_at_unix
        ));
        std::fs::write(&out, serde_json::to_string_pretty(&report)? + "\n")?;

        eprintln!(
            "ingest: {} edges, {} nodes, {} removals in {:.3}s — {:.0} edges/sec \
             ({:.0} total mutations/sec), target >= {:.0} edges/sec: {}",
            report.edges,
            report.nodes,
            report.removals,
            report.elapsed_secs,
            report.edges_per_sec,
            report.total_mutations_per_sec,
            report.min_edges_per_sec_target,
            if report.passed { "PASS" } else { "MISS" }
        );
        eprintln!("raw results: {}", out.display());
    }

    Ok(())
}
