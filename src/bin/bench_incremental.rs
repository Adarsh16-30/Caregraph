//! `caregraph-bench-incremental` — incremental vs. full-recompute embedding
//! latency (PRD Phase 4).
//!
//! Measures the speedup the bounded-subgraph forward pass
//! ([`associative::incremental_aggregate`]) gets over the deliberately
//! unbounded reference ([`associative::full_recompute`]) on real mutations
//! sampled from the real clinical graph's own trace, and writes a timestamped
//! raw-results file (Rules 4, 10). Phase 4's success criterion is **at least
//! 5x** median speedup; this binary asserts it rather than reporting it as a
//! hope.
//!
//! Both paths run the same trained model through the same
//! `ml/embedding_server.py` worker — this is not comparing a fast
//! approximation to a slow exact one, it is comparing the same exact
//! computation done over a bounded input against the same exact computation
//! done over an unbounded one. See `docs/benchmark_report.md` for why a
//! mutation touching a high-degree node is the case where that gap narrows,
//! honestly, rather than disappearing into an averaged number.
//!
//! Usage:
//!     caregraph-bench-incremental --db data/db/diabetes130 \
//!         --trace benchmarks/traces/diabetes130_full.jsonl \
//!         --model diabetes130_graphsage --samples 30

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use caregraph::embedding::state::{GraphMutation, MutationContext};
use caregraph::embedding::{associative, EmbeddingModel};
use caregraph::storage::{KvStore, RocksKv};
use caregraph::types::{EdgeType, ModelKind, NodeId, Timestamp};
use serde::Serialize;

/// PRD Phase 4 success criterion: incremental at least this much faster than
/// full recompute, median over sampled mutations.
const DEFAULT_MIN_SPEEDUP: f64 = 5.0;
const FANOUT_CAP: usize = 512;

struct Args {
    db: String,
    trace: PathBuf,
    model: String,
    samples: usize,
    out_dir: PathBuf,
    min_speedup: f64,
}

fn parse_args() -> Result<Args> {
    let mut db = "data/db/diabetes130".to_string();
    let mut trace = None;
    let mut model = "diabetes130_graphsage".to_string();
    let mut samples = 30usize;
    let mut out_dir = PathBuf::from("benchmarks/results");
    let mut min_speedup = DEFAULT_MIN_SPEEDUP;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--db" => db = next()?,
            "--trace" => trace = Some(PathBuf::from(next()?)),
            "--model" => model = next()?,
            "--samples" => samples = next()?.parse()?,
            "--out-dir" => out_dir = PathBuf::from(next()?),
            "--min-speedup" => min_speedup = next()?.parse()?,
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-incremental --trace <file.jsonl> [--db <path>] \
                     [--model <name>] [--samples N] [--out-dir DIR] [--min-speedup X]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Args {
        db,
        trace: trace.context("--trace is required: it supplies the mutation sample")?,
        model,
        samples,
        out_dir,
        min_speedup,
    })
}

/// One real `add_edge` record from the trace, replayed as a mutation.
struct Sample {
    src: NodeId,
    dst: NodeId,
    edge_type: EdgeType,
    ts: Timestamp,
}

/// Every `add_edge` in the trace, so degree-weighted sampling below reflects
/// the real distribution rather than an artificially uniform one.
fn read_samples(trace: &std::path::Path, take: usize) -> Result<Vec<Sample>> {
    let file = File::open(trace).with_context(|| format!("opening trace {}", trace.display()))?;
    let mut all = Vec::new();

    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let r: serde_json::Value = serde_json::from_str(&line)?;
        if r.get("op").and_then(|v| v.as_str()) != Some("add_edge") {
            continue;
        }
        let (Some(src), Some(dst), Some(et), Some(ts)) = (
            r.get("src").and_then(|v| v.as_u64()),
            r.get("dst").and_then(|v| v.as_u64()),
            r.get("edge_type").and_then(|v| v.as_u64()),
            r.get("timestamp_us").and_then(|v| v.as_u64()),
        ) else {
            continue;
        };
        let Some(edge_type) = EdgeType::from_u16(et as u16) else {
            continue;
        };
        all.push(Sample {
            src: NodeId(src),
            dst: NodeId(dst),
            edge_type,
            ts: Timestamp(ts),
        });
    }

    if all.is_empty() {
        bail!("{} has no add_edge records; nothing to sample", trace.display());
    }

    // Evenly spaced through the trace rather than the first N, so the sample
    // is not biased toward whichever node type happened to load first.
    let step = (all.len() / take.max(1)).max(1);
    Ok(all.into_iter().step_by(step).take(take).collect())
}

fn build_full_graph<S: KvStore + ?Sized>(
    store: &S,
    trace: &std::path::Path,
) -> Result<(Vec<NodeId>, Vec<(NodeId, NodeId)>)> {
    let _ = store;
    let file = File::open(trace)?;
    let mut nodes = std::collections::HashSet::new();
    let mut edges = std::collections::HashSet::new();

    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let r: serde_json::Value = serde_json::from_str(&line)?;
        match r.get("op").and_then(|v| v.as_str()) {
            Some("upsert_node") => {
                if let Some(id) = r.get("node_id").and_then(|v| v.as_u64()) {
                    nodes.insert(NodeId(id));
                }
            }
            Some("add_edge") => {
                if let (Some(s), Some(d)) = (
                    r.get("src").and_then(|v| v.as_u64()),
                    r.get("dst").and_then(|v| v.as_u64()),
                ) {
                    let (a, b) = (NodeId(s), NodeId(d));
                    edges.insert(if a.as_u64() <= b.as_u64() { (a, b) } else { (b, a) });
                }
            }
            Some("remove_edge") => {
                if let (Some(s), Some(d)) = (
                    r.get("src").and_then(|v| v.as_u64()),
                    r.get("dst").and_then(|v| v.as_u64()),
                ) {
                    let (a, b) = (NodeId(s), NodeId(d));
                    edges.remove(&if a.as_u64() <= b.as_u64() { (a, b) } else { (b, a) });
                }
            }
            _ => {}
        }
    }

    Ok((nodes.into_iter().collect(), edges.into_iter().collect()))
}

#[derive(Serialize)]
struct Percentiles {
    samples: usize,
    min_ms: f64,
    median_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

fn percentiles(mut ms: Vec<f64>) -> Percentiles {
    assert!(!ms.is_empty());
    ms.sort_by(|a, b| a.total_cmp(b));
    let at = |p: f64| ms[((p * ms.len() as f64).ceil() as usize).clamp(1, ms.len()) - 1];
    Percentiles {
        samples: ms.len(),
        min_ms: ms[0],
        median_ms: at(0.50),
        p95_ms: at(0.95),
        max_ms: ms[ms.len() - 1],
    }
}

#[derive(Serialize)]
struct SampleResult {
    src: u64,
    dst: u64,
    affected_count: usize,
    incremental_ms: f64,
    full_recompute_ms: f64,
    speedup: f64,
}

#[derive(Serialize)]
struct Provenance {
    generated_at_unix: u64,
    git_commit: String,
    git_dirty: bool,
    command: String,
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[derive(Serialize)]
struct Report {
    benchmark: &'static str,
    prd_target: &'static str,
    provenance: Provenance,
    model: String,
    incremental: Percentiles,
    full_recompute: Percentiles,
    median_speedup: f64,
    per_sample: Vec<SampleResult>,
    min_speedup_target: f64,
    passed: bool,
    notes: Vec<String>,
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let store = RocksKv::open(&args.db).with_context(|| format!("opening RocksDB at {}", args.db))?;
    let model = EmbeddingModel::spawn(&args.model)
        .with_context(|| format!("spawning embedding worker for {}", args.model))?;

    eprintln!("building whole-graph reference input from {} ...", args.trace.display());
    let (all_nodes, all_edges) = build_full_graph(&store, &args.trace)?;
    eprintln!("whole graph: {} nodes, {} live edges", all_nodes.len(), all_edges.len());

    let samples = read_samples(&args.trace, args.samples)?;
    eprintln!("sampled {} real mutations from the trace", samples.len());

    let mut results = Vec::with_capacity(samples.len());
    let mut hub_touching = 0usize;

    for s in &samples {
        let mutation = GraphMutation::AddEdge {
            src: s.src,
            dst: s.dst,
            edge_type: s.edge_type,
            ts: s.ts,
        };

        let mut ctx = MutationContext::new(mutation, ModelKind::GraphSAGE);
        let t0 = Instant::now();
        associative::incremental_aggregate(&mut ctx, &store, &model, FANOUT_CAP)?;
        let incremental_elapsed = t0.elapsed();

        if ctx.fallback {
            eprintln!("  skipping sample ({:?} -> {:?}): resolver fallback", s.src, s.dst);
            continue;
        }
        if ctx.affected.len() > 2000 {
            hub_touching += 1;
        }

        let t0 = Instant::now();
        let _reference = associative::full_recompute(
            &store,
            &model,
            &all_nodes,
            &all_edges,
            &ctx.affected,
            ModelKind::GraphSAGE,
            s.ts,
        )?;
        let full_elapsed = t0.elapsed();

        let inc_ms = incremental_elapsed.as_secs_f64() * 1000.0;
        let full_ms = full_elapsed.as_secs_f64() * 1000.0;
        results.push(SampleResult {
            src: s.src.as_u64(),
            dst: s.dst.as_u64(),
            affected_count: ctx.affected.len(),
            incremental_ms: inc_ms,
            full_recompute_ms: full_ms,
            speedup: full_ms / inc_ms.max(1e-9),
        });
        eprintln!(
            "  {} -> {}: affected={} incremental={:.2}ms full={:.2}ms speedup={:.1}x",
            s.src.as_u64(), s.dst.as_u64(), ctx.affected.len(), inc_ms, full_ms, full_ms / inc_ms.max(1e-9)
        );
    }

    if results.is_empty() {
        bail!("every sampled mutation fell back; nothing was measured");
    }

    let incremental_p = percentiles(results.iter().map(|r| r.incremental_ms).collect());
    let full_p = percentiles(results.iter().map(|r| r.full_recompute_ms).collect());
    let mut speedups: Vec<f64> = results.iter().map(|r| r.speedup).collect();
    speedups.sort_by(|a, b| a.total_cmp(b));
    let median_speedup = speedups[speedups.len() / 2];
    let passed = median_speedup >= args.min_speedup;

    let mut notes = vec![format!(
        "{} of {} sampled mutations touched a >2000-node affected set (hub-adjacent)",
        hub_touching, results.len()
    )];
    let provenance = Provenance {
        generated_at_unix: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
        git_commit: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        git_dirty: git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty()),
        command: std::env::args().collect::<Vec<_>>().join(" "),
    };
    if provenance.git_dirty {
        notes.push("working tree was dirty at run time; not reproducible from the commit alone".into());
    }

    let report = Report {
        benchmark: "incremental_vs_full_recompute",
        prd_target: "Phase 4: incremental embedding update at least 5x faster than full recompute",
        provenance,
        model: args.model.clone(),
        incremental: incremental_p,
        full_recompute: full_p,
        median_speedup,
        per_sample: results,
        min_speedup_target: args.min_speedup,
        passed,
        notes,
    };

    std::fs::create_dir_all(&args.out_dir)?;
    let out = args.out_dir.join(format!("incremental_speedup_{}.json", report.provenance.generated_at_unix));
    std::fs::write(&out, serde_json::to_string_pretty(&report)? + "\n")?;

    println!("incremental vs. full recompute ({} samples)", report.incremental.samples);
    println!(
        "  incremental  median {:.2} ms  p95 {:.2} ms",
        report.incremental.median_ms, report.incremental.p95_ms
    );
    println!(
        "  full recompute median {:.2} ms  p95 {:.2} ms",
        report.full_recompute.median_ms, report.full_recompute.p95_ms
    );
    println!("  median speedup: {:.1}x   target: >= {:.1}x", median_speedup, args.min_speedup);
    for note in &report.notes {
        println!("  note: {note}");
    }
    println!("  raw results: {}", out.display());

    if !passed {
        bail!("median speedup {:.1}x is below the {:.1}x target", median_speedup, args.min_speedup);
    }
    Ok(())
}
