//! `caregraph-bench-diff` — point-in-time similarity delta latency
//! (Phase 9, Feature 3): `caregraph::api::diff::similarity_delta`, run
//! directly against the storage layer the same way `bench_pit.rs` measures
//! `TemporalIndex` reads, rather than round-tripping through gRPC.
//!
//! There is no PRD target for this — Feature 3 is a Phase 9 addition beyond
//! the PRD's original Section 1 metrics, the same reason
//! `caregraph-bench-attribution` reports rather than gates.
//!
//! Query nodes are drawn from whichever nodes actually hold an embedding at
//! `Timestamp::MAX` (i.e. currently) — a similarity delta needs a query
//! embedding at both ends of the window, so this benchmark refuses to
//! measure the latency of misses (the same Rule 6 argument `bench_pit.rs`
//! makes) by requiring a real hit rate before reporting.
//!
//! Usage:
//!     caregraph-bench-diff --db data/db/diabetes130 \
//!         --trace benchmarks/traces/diabetes130_full.jsonl --queries 500

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use caregraph::api::diff::similarity_delta;
use caregraph::storage::RocksKv;
use caregraph::temporal::TemporalIndex;
use caregraph::types::{NodeId, Timestamp};
use serde::Serialize;

struct Args {
    db: String,
    trace: PathBuf,
    queries: usize,
    seed: u64,
    top_k: usize,
    out_dir: PathBuf,
}

fn parse_args() -> Result<Args> {
    let mut db = "data/db/diabetes130".to_string();
    let mut trace = None;
    let mut queries = 500usize;
    let mut seed = 0x5EED_C0FFEE_u64;
    let mut top_k = 10usize;
    let mut out_dir = PathBuf::from("benchmarks/results");

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--db" => db = next()?,
            "--trace" => trace = Some(PathBuf::from(next()?)),
            "--queries" => queries = next()?.parse()?,
            "--seed" => seed = next()?.parse()?,
            "--top-k" => top_k = next()?.parse()?,
            "--out-dir" => out_dir = PathBuf::from(next()?),
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-diff --trace <file.jsonl> [--db <path>] \
                     [--queries N] [--seed N] [--top-k N] [--out-dir DIR]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Args {
        db,
        trace: trace.context("--trace is required: it supplies the timestamp window")?,
        queries,
        seed,
        top_k,
        out_dir,
    })
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
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

fn read_timestamp_span(trace: &Path) -> Result<(u64, u64)> {
    let file = File::open(trace).with_context(|| format!("opening trace {}", trace.display()))?;
    let (mut min_ts, mut max_ts) = (u64::MAX, 0u64);
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let r: serde_json::Value = serde_json::from_str(&line)?;
        if let Some(ts) = r.get("timestamp_us").and_then(|v| v.as_u64()) {
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
        }
    }
    if min_ts > max_ts {
        bail!("{} has no timestamped records", trace.display());
    }
    Ok((min_ts, max_ts))
}

#[derive(Serialize)]
struct Percentiles {
    samples: usize,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn percentiles(mut ms: Vec<f64>) -> Percentiles {
    assert!(!ms.is_empty());
    ms.sort_by(|a, b| a.total_cmp(b));
    let at = |p: f64| ms[((p * ms.len() as f64).ceil() as usize).clamp(1, ms.len()) - 1];
    Percentiles {
        samples: ms.len(),
        min_ms: ms[0],
        p50_ms: at(0.50),
        p95_ms: at(0.95),
        p99_ms: at(0.99),
        max_ms: ms[ms.len() - 1],
    }
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
    database_path: String,
    trace_path: String,
    seed: u64,
    top_k: usize,
    query_window_us: (u64, u64),
    latency: Percentiles,
    hit_rate: f64,
    notes: Vec<String>,
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let (min_ts, max_ts) = read_timestamp_span(&args.trace)?;

    let store =
        RocksKv::open(&args.db).with_context(|| format!("opening RocksDB at {}", args.db))?;
    let index = TemporalIndex::new(&store);

    eprintln!("collecting query population (nodes with an embedding at Timestamp::MAX) ...");
    let population: Vec<NodeId> = index
        .all_embeddings_as_of(Timestamp::MAX)?
        .into_iter()
        .map(|(node, _)| node)
        .collect();
    if population.is_empty() {
        bail!(
            "no node in {} holds an embedding — similarity_delta needs a query embedding at \
             both ends of the window; load the graph and run at least one mutation first",
            args.db
        );
    }
    eprintln!("population: {} nodes with an embedding", population.len());

    let mut rng = Rng::new(args.seed);
    let mut latencies = Vec::with_capacity(args.queries);
    let mut hits = 0usize;

    for _ in 0..args.queries {
        let node = population[rng.below(population.len() as u64) as usize];
        let t0 = Instant::now();
        let result = similarity_delta(
            &store,
            node,
            Timestamp(min_ts),
            Timestamp(max_ts),
            0.0,
            args.top_k,
        )?;
        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
        if !result.query_missing_at_from && !result.query_missing_at_to {
            hits += 1;
        }
    }

    let hit_rate = hits as f64 / args.queries as f64;
    if hit_rate < 0.5 {
        bail!(
            "only {:.1}% of queries had a query embedding at both ends of the window \
             [{min_ts}, {max_ts}]us; refusing to report latencies dominated by misses",
            hit_rate * 100.0
        );
    }

    let stats = percentiles(latencies);
    let provenance = Provenance {
        generated_at_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        git_commit: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        git_dirty: git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty()),
        command: std::env::args().collect::<Vec<_>>().join(" "),
    };

    let mut notes = vec![format!("query hit rate {:.1}%", hit_rate * 100.0)];
    if provenance.git_dirty {
        notes.push(
            "working tree was dirty at run time; not reproducible from the commit alone".into(),
        );
    }
    notes.push(
        "no PRD target exists for this query — Feature 3 is a Phase 9 addition beyond \
         the original PRD's Section 1 metrics"
            .into(),
    );

    let report = Report {
        benchmark: "similarity_delta_latency",
        prd_target: "none (Phase 9 addition beyond the original PRD's Section 1 metrics)",
        provenance,
        database_path: args.db.clone(),
        trace_path: args.trace.display().to_string(),
        seed: args.seed,
        top_k: args.top_k,
        query_window_us: (min_ts, max_ts),
        latency: stats,
        hit_rate,
        notes,
    };

    std::fs::create_dir_all(&args.out_dir)?;
    let out = args.out_dir.join(format!(
        "similarity_delta_latency_{}.json",
        report.provenance.generated_at_unix
    ));
    std::fs::write(&out, serde_json::to_string_pretty(&report)? + "\n")?;

    println!("similarity delta latency ({} queries)", args.queries);
    println!(
        "  p50 {:.3} ms   p95 {:.3} ms   p99 {:.3} ms",
        report.latency.p50_ms, report.latency.p95_ms, report.latency.p99_ms
    );
    for note in &report.notes {
        println!("  note: {note}");
    }
    println!("  raw results: {}", out.display());

    Ok(())
}
