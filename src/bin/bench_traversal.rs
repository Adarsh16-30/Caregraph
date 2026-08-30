//! `caregraph-bench-traversal` — 2-hop bounded traversal latency (PRD Phase 3).
//!
//! Measures p50/p95/p99 for hop-bounded traversal against the Section 1 target
//! of **p95 < 50 ms**, and writes a timestamped raw-results file recording the
//! commit, command, and machine (Rules 4 and 10).
//!
//! The result file also records the truncation profile of the run — how many
//! traversals hit a bound. A latency number produced entirely by traversals
//! that stopped early at the fan-out cap says more about the cap than about the
//! engine, so the two numbers must be read together and are reported together.
//!
//! Usage:
//!     caregraph-bench-traversal --db data/db/caregraph \
//!         --trace benchmarks/traces/ukpds_full.jsonl \
//!         --hops 2 --queries 5000

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use caregraph::graph::{Direction, TraversalLimits, TraversalRequest, Traverser};
use caregraph::storage::RocksKv;
use caregraph::types::{NodeId, Timestamp};
use serde::Serialize;

/// PRD Section 1: 2-hop bounded traversal latency (p95) < 50 ms.
const DEFAULT_P95_TARGET_MS: f64 = 50.0;
const WARMUP_QUERIES: usize = 200;

struct Args {
    db: String,
    trace: PathBuf,
    queries: usize,
    hops: u8,
    seed: u64,
    out_dir: PathBuf,
    p95_target_ms: f64,
    /// Optional *tightenings* of the server caps, for measuring the latency
    /// against completeness trade-off. Neither may exceed the server ceiling —
    /// a benchmark that raised a bound would be reporting a configuration the
    /// server would never actually serve.
    max_neighbors_per_node: Option<usize>,
    max_expanded_nodes: Option<usize>,
}

fn parse_args() -> Result<Args> {
    let mut db = None;
    let mut trace = None;
    let mut queries = 5_000usize;
    let mut hops = 2u8;
    let mut seed = 0x5EED_C0FFEE_u64;
    let mut out_dir = PathBuf::from("benchmarks/results");
    let mut p95_target_ms = DEFAULT_P95_TARGET_MS;
    let mut max_neighbors_per_node = None;
    let mut max_expanded_nodes = None;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--db" => db = Some(next()?),
            "--trace" => trace = Some(PathBuf::from(next()?)),
            "--queries" => queries = next()?.parse()?,
            "--hops" => hops = next()?.parse()?,
            "--seed" => seed = next()?.parse()?,
            "--out-dir" => out_dir = PathBuf::from(next()?),
            "--p95-target-ms" => p95_target_ms = next()?.parse()?,
            "--max-neighbors-per-node" => max_neighbors_per_node = Some(next()?.parse()?),
            "--max-expanded-nodes" => max_expanded_nodes = Some(next()?.parse()?),
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-traversal --trace <file.jsonl> [--db <path>] \
                     [--hops N] [--queries N] [--seed N] [--out-dir DIR] [--p95-target-ms MS] \
                     [--max-neighbors-per-node N] [--max-expanded-nodes N]"
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
        hops,
        seed,
        out_dir,
        p95_target_ms,
        max_neighbors_per_node,
        max_expanded_nodes,
    })
}

/// xorshift64*, seeded so a run is reproducible.
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

/// Patients to start traversals from, and the timeline to sample `as_of` over.
struct Population {
    starts: Vec<NodeId>,
    min_ts: u64,
    max_ts: u64,
}

fn read_population(trace: &Path) -> Result<Population> {
    let file = File::open(trace).with_context(|| format!("opening trace {}", trace.display()))?;

    let mut starts = BTreeSet::new();
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
            starts.insert(NodeId(src));
        }
    }

    if starts.is_empty() {
        bail!(
            "{} contains no edges. A traversal benchmark needs a real loaded \
             clinical graph; timing traversals of an empty graph is not a \
             measurement.",
            trace.display()
        );
    }

    Ok(Population {
        starts: starts.into_iter().collect(),
        min_ts,
        max_ts,
    })
}

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
    assert!(!samples.is_empty());
    samples.sort_unstable();
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
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

/// How often bounds bound. Reported next to the latency, because a fast number
/// produced by traversals that all stopped early is measuring the cap.
#[derive(Default, Serialize)]
struct TruncationProfile {
    queries_truncated: usize,
    queries_hit_node_limit: usize,
    queries_hit_edge_limit: usize,
    queries_fanout_capped: usize,
    total_neighbors_dropped: usize,
}

#[derive(Serialize)]
struct Provenance {
    generated_at_unix: u64,
    git_commit: String,
    git_dirty: bool,
    command: String,
    os: String,
    arch: String,
    available_parallelism: usize,
    hardware_note: Option<String>,
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn provenance() -> Provenance {
    Provenance {
        generated_at_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
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

#[derive(Serialize)]
struct Report {
    benchmark: &'static str,
    system: &'static str,
    prd_target: &'static str,
    provenance: Provenance,
    database_path: String,
    trace_path: String,
    hops: u8,
    seed: u64,
    warmup_queries: usize,
    limits: LimitsRecord,
    latency: Percentiles,
    mean_nodes_returned: f64,
    mean_edges_returned: f64,
    truncation: TruncationProfile,
    p95_target_ms: f64,
    passed: bool,
    notes: Vec<String>,
}

/// The limits in force, recorded so the number can be interpreted later.
#[derive(Serialize)]
struct LimitsRecord {
    max_hops: u8,
    max_result_nodes: usize,
    max_result_edges: usize,
    max_neighbors_per_node: usize,
    max_expanded_nodes: usize,
}

impl From<TraversalLimits> for LimitsRecord {
    fn from(l: TraversalLimits) -> Self {
        LimitsRecord {
            max_hops: l.max_hops,
            max_result_nodes: l.max_result_nodes,
            max_result_edges: l.max_result_edges,
            max_neighbors_per_node: l.max_neighbors_per_node,
            max_expanded_nodes: l.max_expanded_nodes,
        }
    }
}

fn main() -> Result<()> {
    let args = parse_args()?;
    if args.queries == 0 {
        bail!("--queries must be at least 1");
    }

    let population = read_population(&args.trace)?;
    let store =
        RocksKv::open(&args.db).with_context(|| format!("opening RocksDB at {}", args.db))?;

    // Server limits are not relaxed to accommodate the benchmark. If the
    // requested depth exceeds the ceiling, the traversal would be silently
    // clamped and the report would claim a depth it never ran at.
    let mut limits = TraversalLimits::default();

    // A cap may be tightened to measure the latency/completeness curve, never
    // loosened: the same rule `clamp` enforces for clients applies to the
    // benchmark itself. Reporting a latency measured at a bound the server
    // would refuse to serve would be reporting a configuration that does not
    // exist.
    if let Some(requested) = args.max_neighbors_per_node {
        if requested > limits.max_neighbors_per_node {
            bail!(
                "--max-neighbors-per-node {} exceeds the server ceiling of {}; a \
                 benchmark may tighten a bound but never raise one",
                requested,
                limits.max_neighbors_per_node
            );
        }
        if requested == 0 {
            bail!("--max-neighbors-per-node must be at least 1");
        }
        limits.max_neighbors_per_node = requested;
    }
    if let Some(requested) = args.max_expanded_nodes {
        if requested > limits.max_expanded_nodes {
            bail!(
                "--max-expanded-nodes {} exceeds the server ceiling of {}; a \
                 benchmark may tighten a bound but never raise one",
                requested,
                limits.max_expanded_nodes
            );
        }
        if requested == 0 {
            bail!("--max-expanded-nodes must be at least 1");
        }
        limits.max_expanded_nodes = requested;
    }

    if args.hops > limits.max_hops {
        bail!(
            "--hops {} exceeds the server ceiling of {}; the traversal would be \
             clamped and the reported depth would be wrong",
            args.hops,
            limits.max_hops
        );
    }
    let traverser = Traverser::new(&store, limits);

    let span = population.max_ts.saturating_sub(population.min_ts).max(1);
    let mut rng = Rng::new(args.seed);

    let pick = |rng: &mut Rng| {
        let start = population.starts[rng.below(population.starts.len() as u64) as usize];
        let as_of = Timestamp(population.min_ts + rng.below(span + span / 10));
        TraversalRequest::new(start, as_of, args.hops).direction(Direction::Both)
    };

    for _ in 0..WARMUP_QUERIES {
        let _ = traverser.traverse(&pick(&mut rng))?;
    }

    let mut samples = Vec::with_capacity(args.queries);
    let mut profile = TruncationProfile::default();
    let mut total_nodes = 0u64;
    let mut total_edges = 0u64;
    let mut nonempty = 0usize;

    for _ in 0..args.queries {
        let request = pick(&mut rng);

        let t0 = Instant::now();
        let result = traverser.traverse(&request)?;
        samples.push(t0.elapsed());

        total_nodes += result.nodes.len() as u64;
        total_edges += result.edges.len() as u64;
        // A traversal that found nothing is fast for the wrong reason, so track
        // how many actually did work.
        if !result.edges.is_empty() {
            nonempty += 1;
        }

        let t = &result.truncation;
        if t.is_truncated() {
            profile.queries_truncated += 1;
        }
        if t.hit_node_limit {
            profile.queries_hit_node_limit += 1;
        }
        if t.hit_edge_limit {
            profile.queries_hit_edge_limit += 1;
        }
        if t.fanout_capped_nodes > 0 {
            profile.queries_fanout_capped += 1;
        }
        profile.total_neighbors_dropped += t.fanout_dropped_neighbors;
    }

    let productive = nonempty as f64 / args.queries as f64;
    if productive < 0.5 {
        bail!(
            "only {:.1}% of traversals returned any edge. The database at {} does \
             not appear to hold the graph described by {}; refusing to report \
             latencies dominated by empty traversals.",
            productive * 100.0,
            args.db,
            args.trace.display()
        );
    }

    let latency = percentiles(samples);
    let passed = latency.p95_ms < args.p95_target_ms;
    let provenance = provenance();

    let mut notes = vec![format!(
        "{:.1}% of traversals returned at least one edge",
        productive * 100.0
    )];
    if profile.queries_truncated > 0 {
        notes.push(format!(
            "{} of {} traversals hit a bound ({} at the fan-out cap, {} neighbours \
             dropped in total); this latency is the latency of bounded traversal, \
             not of exhaustive traversal",
            profile.queries_truncated,
            args.queries,
            profile.queries_fanout_capped,
            profile.total_neighbors_dropped
        ));
    }
    if args.max_neighbors_per_node.is_some() || args.max_expanded_nodes.is_some() {
        let defaults = TraversalLimits::default();
        notes.push(format!(
            "bounds were tightened for this run (fan-out {} vs default {}, expanded \
             nodes {} vs default {}); this latency is not comparable to a run at \
             the default bounds without also comparing what each returned",
            limits.max_neighbors_per_node,
            defaults.max_neighbors_per_node,
            limits.max_expanded_nodes,
            defaults.max_expanded_nodes
        ));
    }
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

    let report = Report {
        benchmark: "bounded_traversal_latency",
        system: "caregraph",
        prd_target: "Section 1: 2-hop bounded traversal latency (p95) < 50 ms",
        provenance,
        database_path: args.db.clone(),
        trace_path: args.trace.display().to_string(),
        hops: args.hops,
        seed: args.seed,
        warmup_queries: WARMUP_QUERIES,
        limits: limits.into(),
        latency,
        mean_nodes_returned: total_nodes as f64 / args.queries as f64,
        mean_edges_returned: total_edges as f64 / args.queries as f64,
        truncation: profile,
        p95_target_ms: args.p95_target_ms,
        passed,
        notes,
    };

    std::fs::create_dir_all(&args.out_dir)?;
    let out = args.out_dir.join(format!(
        "traversal_{}hop_caregraph_{}.json",
        args.hops, report.provenance.generated_at_unix
    ));
    std::fs::write(&out, serde_json::to_string_pretty(&report)? + "\n")?;

    println!(
        "{}-hop bounded traversal ({} queries)",
        args.hops, args.queries
    );
    println!(
        "  p50 {:.3} ms   p95 {:.3} ms   p99 {:.3} ms   max {:.3} ms",
        report.latency.p50_ms, report.latency.p95_ms, report.latency.p99_ms, report.latency.max_ms
    );
    println!(
        "  mean result: {:.1} nodes, {:.1} edges",
        report.mean_nodes_returned, report.mean_edges_returned
    );
    println!("  target: p95 < {:.1} ms", args.p95_target_ms);
    for note in &report.notes {
        println!("  note: {note}");
    }
    println!("  raw results: {}", out.display());

    if !passed {
        bail!(
            "p95 {:.3} ms exceeded the {:.1} ms target",
            report.latency.p95_ms,
            args.p95_target_ms
        );
    }

    Ok(())
}
