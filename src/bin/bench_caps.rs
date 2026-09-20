//! `caregraph-bench-caps` — the A/B that decides whether Phase 9's self-tuning
//! cap controller (Feature 2) actually closes the incremental-update p95
//! latency miss `docs/benchmark_report.md` §2.3-§2.4 discloses (GraphSAGE
//! 1548.96ms, GAT 1531.04ms against a 100ms target): the same real sampled
//! mutations, same seed, same machine, run once against
//! [`CapController::pinned`]'s fixed `(512, 1500)` and once against a live
//! [`CapController::new`] that is allowed to hill-climb — see
//! `src/embedding/caps.rs`'s own module doc for why the ladder is a discrete
//! hill-climb rather than a continuous controller.
//!
//! Reports both arms' p95 side by side, plus the adaptive arm's final rung
//! and the truncation cost that rung actually paid (`expansion_capped`
//! frequency) — the latency win, honestly, is not free: a tighter
//! `max_expanded_nodes` means more ring-two truncation.
//!
//! The two arms are two separate, explicit loops rather than one shared
//! helper — `bench_incremental.rs`'s own associative/staged dispatch is
//! written the same way, for the same reason: the loops differ in exactly
//! one line (where the cap pair and the post-mutation feedback come from),
//! and threading that through a generic closure pair fights the borrow
//! checker (`next_caps` and `on_observe` both need `&mut CapController`) for
//! no real reduction in size.
//!
//! Usage:
//!     caregraph-bench-caps --db data/db/diabetes130 \
//!         --trace benchmarks/traces/diabetes130_full.jsonl \
//!         --model diabetes130_graphsage --samples 60

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use caregraph::embedding::caps::CapController;
use caregraph::embedding::resolver::{patch_subgraph_for_mutation, AffectedSubgraphResolver};
use caregraph::embedding::staged_incremental;
use caregraph::embedding::state::{GraphMutation, MutationContext};
use caregraph::embedding::{associative, EmbeddingModel};
use caregraph::storage::RocksKv;
use caregraph::temporal::TemporalIndex;
use caregraph::types::{ComputationPath, EdgeType, ModelKind, NodeId, Timestamp};
use serde::Serialize;

struct Args {
    db: String,
    trace: PathBuf,
    model: String,
    model_kind: ModelKind,
    samples: usize,
    out_dir: PathBuf,
}

fn parse_model_kind(s: &str) -> Result<ModelKind> {
    Ok(match s {
        "graphsage" => ModelKind::GraphSAGE,
        "gcn" => ModelKind::GCN,
        "gat" => ModelKind::GAT,
        other => bail!("unknown --model-kind {other} (expected graphsage, gcn, or gat)"),
    })
}

fn parse_args() -> Result<Args> {
    let mut db = "data/db/diabetes130".to_string();
    let mut trace = None;
    let mut model = "diabetes130_graphsage".to_string();
    let mut model_kind = ModelKind::GraphSAGE;
    let mut samples = 60usize;
    let mut out_dir = PathBuf::from("benchmarks/results");

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--db" => db = next()?,
            "--trace" => trace = Some(PathBuf::from(next()?)),
            "--model" => model = next()?,
            "--model-kind" => model_kind = parse_model_kind(&next()?)?,
            "--samples" => samples = next()?.parse()?,
            "--out-dir" => out_dir = PathBuf::from(next()?),
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-caps --trace <file.jsonl> [--db <path>] \
                     [--model <name>] [--model-kind graphsage|gcn|gat] [--samples N] [--out-dir DIR]"
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
        model_kind,
        samples,
        out_dir,
    })
}

struct Sample {
    src: NodeId,
    dst: NodeId,
    edge_type: EdgeType,
    ts: Timestamp,
}

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
        bail!(
            "{} has no add_edge records; nothing to sample",
            trace.display()
        );
    }
    let step = (all.len() / take.max(1)).max(1);
    Ok(all.into_iter().step_by(step).take(take).collect())
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

/// Run one incremental-update pass per sample, dispatching by the model's
/// manifest exactly as `atomic_commit.rs` does. `is_associative`/
/// `architecture` are passed in rather than re-read from `model` on every
/// call — both arms share the same spawned model, so this is a fixed fact
/// for the whole run.
#[allow(clippy::too_many_arguments)]
fn run_one(
    store: &RocksKv,
    index: &TemporalIndex<'_, RocksKv>,
    model: &EmbeddingModel,
    model_kind: ModelKind,
    mutation: GraphMutation,
    ts: Timestamp,
    fanout_cap: usize,
    max_expanded_nodes: usize,
) -> Result<MutationContext> {
    let mut ctx = MutationContext::new(mutation, model_kind);
    if model.manifest.is_associative {
        associative::incremental_aggregate(&mut ctx, store, model, fanout_cap, max_expanded_nodes)?;
    } else {
        let path = if model.manifest.architecture == "GAT" {
            ComputationPath::GatConstrained
        } else {
            ComputationPath::NonAssociative
        };
        let resolver = AffectedSubgraphResolver::new(store, fanout_cap, max_expanded_nodes);
        let mut subgraph = resolver.resolve(mutation)?;
        patch_subgraph_for_mutation(&mut subgraph, index, mutation)?;
        staged_incremental::staged_incremental_update(
            &mut ctx, store, model, subgraph, ts, model_kind, path,
        )?;
    }
    Ok(ctx)
}

#[derive(Serialize)]
struct ArmReport {
    latency: Percentiles,
    fallbacks: usize,
    expansion_capped_count: usize,
    expansion_capped_rate: f64,
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
    model_kind: &'static str,
    pinned: ArmReport,
    adaptive: ArmReport,
    adaptive_final_rung: Option<usize>,
    p95_speedup_adaptive_over_pinned: f64,
    notes: Vec<String>,
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let store =
        RocksKv::open(&args.db).with_context(|| format!("opening RocksDB at {}", args.db))?;
    let index = TemporalIndex::new(&store);
    let model = EmbeddingModel::spawn(&args.model)
        .with_context(|| format!("spawning embedding worker for {}", args.model))?;

    if model.manifest.is_associative != args.model_kind.is_associative() {
        bail!(
            "model {} manifest says is_associative={}, which disagrees with --model-kind {:?}",
            args.model,
            model.manifest.is_associative,
            args.model_kind
        );
    }

    let samples = read_samples(&args.trace, args.samples)?;
    eprintln!("sampled {} real mutations from the trace", samples.len());

    // --- Arm A: pinned (512, 1500), the Phase 4/5 production default ---
    eprintln!("--- arm A: pinned (512, 1500) ---");
    let pinned = CapController::pinned(512, 1_500).current().0;
    let mut pinned_latencies = Vec::with_capacity(samples.len());
    let mut pinned_fallbacks = 0usize;
    let mut pinned_expansion_capped = 0usize;
    for s in &samples {
        let mutation = GraphMutation::AddEdge {
            src: s.src,
            dst: s.dst,
            edge_type: s.edge_type,
            ts: s.ts,
        };
        let t0 = Instant::now();
        let ctx = run_one(
            &store,
            &index,
            &model,
            args.model_kind,
            mutation,
            s.ts,
            pinned.fanout_cap,
            pinned.max_expanded_nodes,
        )?;
        let elapsed = t0.elapsed();
        if ctx.fallback {
            pinned_fallbacks += 1;
            continue;
        }
        if ctx.truncation.expansion_capped {
            pinned_expansion_capped += 1;
        }
        pinned_latencies.push(elapsed.as_secs_f64() * 1000.0);
    }

    // --- Arm B: adaptive, hill-climbing CapController::new() ---
    eprintln!("--- arm B: adaptive (CapController::new) ---");
    let mut controller = CapController::new();
    let mut adaptive_latencies = Vec::with_capacity(samples.len());
    let mut adaptive_fallbacks = 0usize;
    let mut adaptive_expansion_capped = 0usize;
    for s in &samples {
        let mutation = GraphMutation::AddEdge {
            src: s.src,
            dst: s.dst,
            edge_type: s.edge_type,
            ts: s.ts,
        };
        let (caps, _rung) = controller.current();
        let t0 = Instant::now();
        let ctx = run_one(
            &store,
            &index,
            &model,
            args.model_kind,
            mutation,
            s.ts,
            caps.fanout_cap,
            caps.max_expanded_nodes,
        )?;
        let elapsed = t0.elapsed();
        controller.observe(elapsed);

        if ctx.fallback {
            adaptive_fallbacks += 1;
            continue;
        }
        if ctx.truncation.expansion_capped {
            adaptive_expansion_capped += 1;
        }
        adaptive_latencies.push(elapsed.as_secs_f64() * 1000.0);
    }
    let adaptive_final_rung = controller.current().1;

    if pinned_latencies.is_empty() || adaptive_latencies.is_empty() {
        bail!("every sampled mutation fell back on one arm or the other; nothing was measured");
    }

    let pinned_p = percentiles(pinned_latencies);
    let adaptive_p = percentiles(adaptive_latencies);
    let speedup = pinned_p.p95_ms / adaptive_p.p95_ms.max(1e-9);

    let provenance = Provenance {
        generated_at_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        git_commit: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        git_dirty: git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty()),
        command: std::env::args().collect::<Vec<_>>().join(" "),
    };

    let mut notes = vec![
        "a lower adaptive p95 trades accuracy for latency: expansion_capped_rate is the \
         honest cost of that trade, not a free win — see src/embedding/caps.rs's module doc"
            .to_string(),
    ];
    if provenance.git_dirty {
        notes.push(
            "working tree was dirty at run time; not reproducible from the commit alone".into(),
        );
    }

    let model_kind_str = match args.model_kind {
        ModelKind::GraphSAGE => "graphsage",
        ModelKind::GCN => "gcn",
        ModelKind::GAT => "gat",
    };

    let n_pinned = pinned_p.samples as f64;
    let n_adaptive = adaptive_p.samples as f64;
    let report = Report {
        benchmark: "cap_controller_ab",
        prd_target: "informational: does the Feature 2 controller reduce p95 vs. the Phase 4/5 pinned default",
        provenance,
        model: args.model.clone(),
        model_kind: model_kind_str,
        pinned: ArmReport {
            latency: pinned_p,
            fallbacks: pinned_fallbacks,
            expansion_capped_count: pinned_expansion_capped,
            expansion_capped_rate: pinned_expansion_capped as f64 / n_pinned,
        },
        adaptive: ArmReport {
            latency: adaptive_p,
            fallbacks: adaptive_fallbacks,
            expansion_capped_count: adaptive_expansion_capped,
            expansion_capped_rate: adaptive_expansion_capped as f64 / n_adaptive,
        },
        adaptive_final_rung,
        p95_speedup_adaptive_over_pinned: speedup,
        notes,
    };

    std::fs::create_dir_all(&args.out_dir)?;
    let out = args.out_dir.join(format!(
        "cap_controller_ab_{}.json",
        report.provenance.generated_at_unix
    ));
    std::fs::write(&out, serde_json::to_string_pretty(&report)? + "\n")?;

    println!(
        "cap controller A/B ({} samples, model {})",
        args.samples, model_kind_str
    );
    println!(
        "  pinned (512,1500)    p95 {:.2} ms   expansion_capped_rate {:.1}%",
        report.pinned.latency.p95_ms,
        report.pinned.expansion_capped_rate * 100.0
    );
    println!(
        "  adaptive (rung {:?}) p95 {:.2} ms   expansion_capped_rate {:.1}%",
        report.adaptive_final_rung,
        report.adaptive.latency.p95_ms,
        report.adaptive.expansion_capped_rate * 100.0
    );
    println!("  p95 speedup (adaptive vs pinned): {:.2}x", speedup);
    for note in &report.notes {
        println!("  note: {note}");
    }
    println!("  raw results: {}", out.display());

    Ok(())
}
