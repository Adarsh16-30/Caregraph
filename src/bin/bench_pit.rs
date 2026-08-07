//! `caregraph-bench-pit` — point-in-time read latency (PRD Phase 2).
//!
//! Measures p50/p95/p99 for `node_as_of` and `edges_as_of` against the Section 1
//! target of **p95 < 20 ms**, and writes a timestamped raw-results file
//! recording the exact command, commit, and machine that produced the numbers.
//! No number may enter a report without that record (Rules 4 and 10).
//!
//! Query selection is driven by a seeded PRNG so a run is reproducible: the same
//! `--seed` against the same database issues the same queries. A benchmark
//! whose workload cannot be reproduced cannot be regression-tested.
//!
//! The database must already hold a real clinical graph — the benchmark refuses
//! to run against an empty one rather than reporting the latency of misses
//! (Rule 6).
//!
//! Usage:
//!     caregraph-bench-pit --db data/db/caregraph \
//!         --trace benchmarks/traces/ukpds_smoke_100.jsonl \
//!         --queries 10000

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use caregraph::storage::RocksKv;
use caregraph::temporal::TemporalIndex;
use caregraph::types::{EdgeType, NodeId, Timestamp};
use serde::Serialize;

/// PRD Section 1: point-in-time snapshot query latency (p95) < 20 ms.
const DEFAULT_P95_TARGET_MS: f64 = 20.0;

/// Queries run before measurement starts, so the numbers reflect steady state
/// rather than cold block-cache misses. Reported alongside the results.
const WARMUP_QUERIES: usize = 500;

// ---------------------------------------------------------------------------

struct Args {
    db: String,
    trace: PathBuf,
    queries: usize,
    seed: u64,
    out_dir: PathBuf,
    p95_target_ms: f64,
}

fn parse_args() -> Result<Args> {
    let mut db = None;
    let mut trace = None;
    let mut queries = 10_000usize;
    let mut seed = 0x5EED_C0FFEE_u64;
    let mut out_dir = PathBuf::from("benchmarks/results");
    let mut p95_target_ms = DEFAULT_P95_TARGET_MS;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--db" => db = Some(next()?),
            "--trace" => trace = Some(PathBuf::from(next()?)),
            "--queries" => queries = next()?.parse()?,
            "--seed" => seed = next()?.parse()?,
            "--out-dir" => out_dir = PathBuf::from(next()?),
            "--p95-target-ms" => p95_target_ms = next()?.parse()?,
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-pit --trace <file.jsonl> [--db <path>] \
                     [--queries N] [--seed N] [--out-dir DIR] [--p95-target-ms MS]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Args {
        db: db.unwrap_or_else(caregraph::db_path_from_env),
        trace: trace.context("--trace is required: it supplies the query population")?,
        queries,
        seed,
        out_dir,
        p95_target_ms,
    })
}

// ---------------------------------------------------------------------------

/// xorshift64*. Seeded rather than entropy-based so a run is reproducible.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
}

// ---------------------------------------------------------------------------

/// The query population, read from the trace the database was loaded from.
struct Population {
    /// Nodes that appear as an edge source — the ones with adjacency to read.
    sources: Vec<NodeId>,
    edge_types: Vec<EdgeType>,
    min_ts: u64,
    max_ts: u64,
}

fn read_population(trace: &Path) -> Result<Population> {
    let file = File::open(trace).with_context(|| format!("opening trace {}", trace.display()))?;

    let mut sources = BTreeSet::new();
    let mut edge_types = BTreeSet::new();
    let mut min_ts = u64::MAX;
    let mut max_ts = 0u64;

    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: serde_json::Value = serde_json::from_str(&line)?;
        let Some(ts) = record.get("timestamp_us").and_then(|v| v.as_u64()) else {
            continue;
        };
        min_ts = min_ts.min(ts);
        max_ts = max_ts.max(ts);

        if let Some(src) = record.get("src").and_then(|v| v.as_u64()) {
            sources.insert(NodeId(src));
        }
        if let Some(raw) = record.get("edge_type").and_then(|v| v.as_u64()) {
            if let Some(et) = EdgeType::from_u16(raw as u16) {
                edge_types.insert(et as u16);
            }
        }
    }

    if sources.is_empty() {
        bail!(
            "{} contains no edges. The benchmark needs a real loaded clinical \
             graph; measuring lookups that all miss would not be a measurement.",
            trace.display()
        );
    }

    Ok(Population {
        sources: sources.into_iter().collect(),
        edge_types: edge_types
            .into_iter()
            .filter_map(EdgeType::from_u16)
            .collect(),
        min_ts,
        max_ts,
    })
}

// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Percentiles {
    samples: usize,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    mean_ms: f64,
}

fn percentiles(mut samples: Vec<Duration>) -> Percentiles {
    assert!(!samples.is_empty(), "no samples collected");
    samples.sort_unstable();

    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    // Nearest-rank: the smallest value at or above which p% of samples fall.
    let at = |p: f64| {
        let rank = ((p * samples.len() as f64).ceil() as usize).clamp(1, samples.len());
        ms(samples[rank - 1])
    };
    let total: f64 = samples.iter().map(|d| ms(*d)).sum();

    Percentiles {
        samples: samples.len(),
        min_ms: ms(samples[0]),
        p50_ms: at(0.50),
        p95_ms: at(0.95),
        p99_ms: at(0.99),
        max_ms: ms(samples[samples.len() - 1]),
        mean_ms: total / samples.len() as f64,
    }
}

#[derive(Serialize)]
struct Provenance {
    generated_at_unix: u64,
    generated_at_iso: String,
    git_commit: String,
    git_dirty: bool,
    command: String,
    os: String,
    arch: String,
    available_parallelism: usize,
    /// Present only if the runner exported it. Recorded rather than guessed —
    /// Rule 4 requires the hardware spec, and an invented one is worse than an
    /// absent one.
    hardware_note: Option<String>,
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn provenance() -> Provenance {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    Provenance {
        generated_at_unix: now,
        generated_at_iso: format_unix_iso(now),
        git_commit: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        git_dirty: git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty()),
        command: std::env::args().collect::<Vec<_>>().join(" "),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        available_parallelism: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        hardware_note: std::env::var("CAREGRAPH_HARDWARE").ok(),
    }
}

/// Minimal UTC formatting — avoids a chrono dependency for one string.
fn format_unix_iso(secs: u64) -> String {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);

    // Civil-from-days (Howard Hinnant's algorithm), epoch-shifted to 0000-03-01.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mth <= 2);

    format!("{y:04}-{mth:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[derive(Serialize)]
struct Report {
    benchmark: &'static str,
    prd_target: &'static str,
    provenance: Provenance,
    database_path: String,
    trace_path: String,
    seed: u64,
    warmup_queries: usize,
    node_as_of: Percentiles,
    edges_as_of: Percentiles,
    p95_target_ms: f64,
    passed: bool,
    notes: Vec<String>,
}

// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = parse_args()?;
    if args.queries == 0 {
        bail!("--queries must be at least 1; there is nothing to measure otherwise");
    }
    let population = read_population(&args.trace)?;
    let store = RocksKv::open(&args.db).with_context(|| format!("opening RocksDB at {}", args.db))?;
    let index = TemporalIndex::new(&store);

    let span = population.max_ts.saturating_sub(population.min_ts).max(1);
    let mut rng = Rng::new(args.seed);

    let pick = |rng: &mut Rng| {
        let node = population.sources[rng.below(population.sources.len() as u64) as usize];
        let edge_type = population.edge_types[rng.below(population.edge_types.len() as u64) as usize];
        // Sample across the whole timeline plus a margin, so queries land both
        // inside recorded history and after its end.
        let as_of = Timestamp(population.min_ts + rng.below(span + span / 10));
        (node, edge_type, as_of)
    };

    // Warm-up: exclude cold block-cache misses from the reported distribution.
    for _ in 0..WARMUP_QUERIES {
        let (node, edge_type, as_of) = pick(&mut rng);
        let _ = index.node_as_of(node, as_of)?;
        let _ = index.edges_as_of(node, edge_type, as_of)?;
    }

    let mut node_samples = Vec::with_capacity(args.queries);
    let mut edge_samples = Vec::with_capacity(args.queries);
    let mut hits = 0usize;

    for _ in 0..args.queries {
        let (node, edge_type, as_of) = pick(&mut rng);

        let t0 = Instant::now();
        let node_result = index.node_as_of(node, as_of)?;
        node_samples.push(t0.elapsed());

        let t1 = Instant::now();
        let edges = index.edges_as_of(node, edge_type, as_of)?;
        edge_samples.push(t1.elapsed());

        if node_result.is_some() || !edges.is_empty() {
            hits += 1;
        }
    }

    // If almost nothing was found, the numbers describe empty lookups rather
    // than real reads, and reporting them would be misleading.
    let hit_rate = hits as f64 / args.queries as f64;
    if hit_rate < 0.5 {
        bail!(
            "only {:.1}% of queries returned data. The database at {} does not \
             appear to hold the graph described by {}; refusing to report \
             latencies dominated by misses.",
            hit_rate * 100.0,
            args.db,
            args.trace.display()
        );
    }

    let node_stats = percentiles(node_samples);
    let edge_stats = percentiles(edge_samples);
    let passed =
        node_stats.p95_ms < args.p95_target_ms && edge_stats.p95_ms < args.p95_target_ms;

    let provenance = provenance();
    let mut notes = vec![format!("query hit rate {:.1}%", hit_rate * 100.0)];
    if provenance.git_dirty {
        notes.push(
            "working tree was dirty at run time; this result is not reproducible \
             from the recorded commit alone"
                .into(),
        );
    }
    if provenance.hardware_note.is_none() {
        notes.push(
            "CAREGRAPH_HARDWARE was not set, so no hardware spec is recorded; \
             Rule 4 requires one before this result is quoted in a report"
                .into(),
        );
    }
    notes.push(
        "embedding point-in-time latency is not measured here — CF_EMBEDDINGS is \
         populated from Phase 4"
            .into(),
    );

    let report = Report {
        benchmark: "point_in_time_read_latency",
        prd_target: "Section 1: point-in-time snapshot query latency (p95) < 20 ms",
        provenance,
        database_path: args.db.clone(),
        trace_path: args.trace.display().to_string(),
        seed: args.seed,
        warmup_queries: WARMUP_QUERIES,
        node_as_of: node_stats,
        edges_as_of: edge_stats,
        p95_target_ms: args.p95_target_ms,
        passed,
        notes,
    };

    std::fs::create_dir_all(&args.out_dir)?;
    let out = args
        .out_dir
        .join(format!("pit_latency_{}.json", report.provenance.generated_at_unix));
    std::fs::write(&out, serde_json::to_string_pretty(&report)? + "\n")?;

    println!("point-in-time read latency ({} queries)", args.queries);
    println!(
        "  node_as_of   p50 {:.3} ms   p95 {:.3} ms   p99 {:.3} ms",
        report.node_as_of.p50_ms, report.node_as_of.p95_ms, report.node_as_of.p99_ms
    );
    println!(
        "  edges_as_of  p50 {:.3} ms   p95 {:.3} ms   p99 {:.3} ms",
        report.edges_as_of.p50_ms, report.edges_as_of.p95_ms, report.edges_as_of.p99_ms
    );
    println!("  target: p95 < {:.1} ms", args.p95_target_ms);
    for note in &report.notes {
        println!("  note: {note}");
    }
    println!("  raw results: {}", out.display());

    if !passed {
        bail!(
            "p95 exceeded the {:.1} ms target (node {:.3} ms, edges {:.3} ms)",
            args.p95_target_ms,
            report.node_as_of.p95_ms,
            report.edges_as_of.p95_ms
        );
    }

    Ok(())
}
