//! `caregraph-bench-attribution` — integrated-gradients attribution overhead
//! (Phase 9, Feature 1) on top of the ordinary incremental embedding update,
//! plus the real completeness-residual distribution this feature produces on
//! the full clinical graph (as opposed to `tests/embedding/attribution_completeness_test.rs`'s
//! small synthetic fixture).
//!
//! There is no PRD target for this — attribution is a Phase 9 addition, not
//! one of the PRD's original Section 1 metrics — so this binary reports
//! measured overhead rather than asserting a pass/fail threshold that was
//! never actually specified (Rule 10 is about not inventing numbers, and an
//! invented target would be exactly that).
//!
//! Usage:
//!     caregraph-bench-attribution --db data/db/diabetes130 \
//!         --trace benchmarks/traces/diabetes130_full.jsonl \
//!         --model diabetes130_graphsage --samples 30
//!
//!     caregraph-bench-attribution --db data/db/diabetes130 \
//!         --trace benchmarks/traces/diabetes130_full.jsonl \
//!         --model diabetes130_gat --model-kind gat --samples 30

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
use caregraph::embedding::{associative, attribute_targets, EmbeddingModel};
use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::{TemporalIndex, TemporalWriter};
use caregraph::types::{ComputationPath, EdgeType, ModelKind, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde::Serialize;

const DEFAULT_STEPS: u32 = 16;
const DEFAULT_TOP_K: usize = 20;

struct Args {
    db: String,
    trace: PathBuf,
    model: String,
    model_kind: ModelKind,
    samples: usize,
    steps: u32,
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
    let mut samples = 30usize;
    let mut steps = DEFAULT_STEPS;
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
            "--steps" => steps = next()?.parse()?,
            "--out-dir" => out_dir = PathBuf::from(next()?),
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-attribution --trace <file.jsonl> [--db <path>] \
                     [--model <name>] [--model-kind graphsage|gcn|gat] [--samples N] \
                     [--steps N] [--out-dir DIR]"
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
        steps,
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

#[derive(Serialize)]
struct SampleResult {
    src: u64,
    dst: u64,
    incremental_only_ms: f64,
    incremental_plus_attribution_ms: f64,
    attribution_overhead_ms: f64,
    delta_norm: f32,
    baseline_delta: f32,
    completeness_residual: f32,
    completeness_residual_relative: f32,
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
    steps: u32,
    top_k: usize,
    incremental_only: Percentiles,
    incremental_plus_attribution: Percentiles,
    attribution_overhead: Percentiles,
    completeness_residual_relative_max: f32,
    per_sample: Vec<SampleResult>,
    notes: Vec<String>,
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let store =
        RocksKv::open(&args.db).with_context(|| format!("opening RocksDB at {}", args.db))?;
    let model = EmbeddingModel::spawn(&args.model)
        .with_context(|| format!("spawning embedding worker for {}", args.model))?;
    let index = TemporalIndex::new(&store);

    if model.manifest.is_associative != args.model_kind.is_associative() {
        bail!(
            "model {} manifest says is_associative={}, which disagrees with --model-kind {:?}",
            args.model,
            model.manifest.is_associative,
            args.model_kind
        );
    }

    let (caps, _rung) = CapController::pinned(512, 1_500).current();
    let samples = read_samples(&args.trace, args.samples)?;
    eprintln!("sampled {} real mutations from the trace", samples.len());

    let mut results = Vec::with_capacity(samples.len());

    for s in &samples {
        // `--trace` was already applied in full by `caregraph-load` at this
        // exact timestamp, so replaying it unchanged would be a structural
        // no-op — the newest version at or before any `as_of >= s.ts` is
        // already "present," and attribution would correctly (but
        // uselessly) report zero change to explain. A tombstone written
        // *before* `s.ts` would not fix this: temporal keys resolve to the
        // newest version at or before `as_of`, so an earlier removal is
        // simply superseded by the original (later) add. The removal has to
        // be the newest version instead — written *after* `s.ts` — and the
        // benchmark's own "AddEdge" mutation timestamped later still, so the
        // real sequence is add (original, `s.ts`) → remove (`ts_remove`) →
        // add again (`ts_mutation`, what this benchmark actually measures).
        let ts_remove = Timestamp(s.ts.as_u64() + 1);
        let ts_mutation = Timestamp(s.ts.as_u64() + 2);
        let mutation = GraphMutation::AddEdge {
            src: s.src,
            dst: s.dst,
            edge_type: s.edge_type,
            ts: ts_mutation,
        };

        if index
            .edge_as_of(s.src, s.edge_type, s.dst, ts_remove)?
            .is_some()
        {
            let writer = TemporalWriter::new(&store)?;
            let mut batch = WriteBatch::default();
            writer.remove_edge(&mut batch, s.src, s.edge_type, s.dst, ts_remove);
            store.write(batch)?;
        }

        let resolver =
            AffectedSubgraphResolver::new(&store, caps.fanout_cap, caps.max_expanded_nodes);
        let mut subgraph = resolver.resolve(mutation)?;
        let edges_before = subgraph.edges.clone();
        patch_subgraph_for_mutation(&mut subgraph, &index, mutation)?;
        let edges_after = subgraph.edges.clone();
        let nodes = subgraph.nodes.clone();

        let mut ctx = MutationContext::new(mutation, args.model_kind);
        let t0 = Instant::now();
        if model.manifest.is_associative {
            associative::incremental_aggregate(
                &mut ctx,
                &store,
                &model,
                caps.fanout_cap,
                caps.max_expanded_nodes,
            )?;
        } else {
            let path = if model.manifest.architecture == "GAT" {
                ComputationPath::GatConstrained
            } else {
                ComputationPath::NonAssociative
            };
            let resolver2 =
                AffectedSubgraphResolver::new(&store, caps.fanout_cap, caps.max_expanded_nodes);
            let mut subgraph2 = resolver2.resolve(mutation)?;
            patch_subgraph_for_mutation(&mut subgraph2, &index, mutation)?;
            staged_incremental::staged_incremental_update(
                &mut ctx,
                &store,
                &model,
                subgraph2,
                ts_mutation,
                args.model_kind,
                path,
            )?;
        }
        let incremental_only = t0.elapsed();

        if ctx.fallback {
            eprintln!(
                "  skipping sample ({:?} -> {:?}): resolver fallback",
                s.src, s.dst
            );
            continue;
        }

        let t1 = Instant::now();
        let attribution = attribute_targets(
            &store,
            &model,
            &nodes,
            &edges_before,
            &edges_after,
            &[s.src, s.dst],
            ts_mutation,
            args.steps,
            DEFAULT_TOP_K,
        )?;
        let attribution_elapsed = t1.elapsed();
        let incremental_plus_attribution = incremental_only + attribution_elapsed;

        let Some(record) = attribution.get(&s.src).or_else(|| attribution.get(&s.dst)) else {
            eprintln!(
                "  skipping sample ({:?} -> {:?}): no non-degenerate embedding change to attribute",
                s.src, s.dst
            );
            continue;
        };

        let inc_ms = incremental_only.as_secs_f64() * 1000.0;
        let total_ms = incremental_plus_attribution.as_secs_f64() * 1000.0;
        let overhead_ms = attribution_elapsed.as_secs_f64() * 1000.0;
        let rel_residual = if record.delta_norm > 1e-9 {
            record.completeness_residual.abs() / record.delta_norm
        } else {
            0.0
        };

        results.push(SampleResult {
            src: s.src.as_u64(),
            dst: s.dst.as_u64(),
            incremental_only_ms: inc_ms,
            incremental_plus_attribution_ms: total_ms,
            attribution_overhead_ms: overhead_ms,
            delta_norm: record.delta_norm,
            baseline_delta: record.baseline_delta,
            completeness_residual: record.completeness_residual,
            completeness_residual_relative: rel_residual,
        });
        eprintln!(
            "  {} -> {}: incremental={:.2}ms +attribution={:.2}ms overhead={:.2}ms residual_rel={:.4}",
            s.src.as_u64(), s.dst.as_u64(), inc_ms, total_ms, overhead_ms, rel_residual
        );
    }

    if results.is_empty() {
        bail!("every sampled mutation fell back or had a degenerate delta; nothing was measured");
    }

    let incremental_only_p = percentiles(results.iter().map(|r| r.incremental_only_ms).collect());
    let total_p = percentiles(
        results
            .iter()
            .map(|r| r.incremental_plus_attribution_ms)
            .collect(),
    );
    let overhead_p = percentiles(results.iter().map(|r| r.attribution_overhead_ms).collect());
    let max_rel_residual = results
        .iter()
        .map(|r| r.completeness_residual_relative)
        .fold(0.0f32, f32::max);

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
        "no PRD target exists for attribution overhead — this is a Phase 9 addition; \
         reported as a measurement, not a pass/fail gate"
            .to_string(),
        "attribution computed for the mutation's two endpoints only, not every affected node \
         — see src/embedding/attribution.rs's module doc for the cost this bounds"
            .to_string(),
        "this run wrote real removal tombstones ahead of each sampled edge (one microsecond \
         earlier) so replaying it as AddEdge was a genuine topology change rather than a \
         structural no-op against the already-fully-loaded trace — run against a scratch copy \
         of the database, not one relied on by other benchmarks"
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

    let report = Report {
        benchmark: "attribution_overhead",
        prd_target: "none (Phase 9 addition beyond the original PRD's Section 1 metrics)",
        provenance,
        model: args.model.clone(),
        model_kind: model_kind_str,
        steps: args.steps,
        top_k: DEFAULT_TOP_K,
        incremental_only: incremental_only_p,
        incremental_plus_attribution: total_p,
        attribution_overhead: overhead_p,
        completeness_residual_relative_max: max_rel_residual,
        per_sample: results,
        notes,
    };

    std::fs::create_dir_all(&args.out_dir)?;
    let out = args.out_dir.join(format!(
        "attribution_overhead_{}.json",
        report.provenance.generated_at_unix
    ));
    std::fs::write(&out, serde_json::to_string_pretty(&report)? + "\n")?;

    println!(
        "attribution overhead ({} samples, model {})",
        report.incremental_only.samples, model_kind_str
    );
    println!(
        "  incremental only        median {:.2} ms  p95 {:.2} ms",
        report.incremental_only.median_ms, report.incremental_only.p95_ms
    );
    println!(
        "  incremental+attribution median {:.2} ms  p95 {:.2} ms",
        report.incremental_plus_attribution.median_ms, report.incremental_plus_attribution.p95_ms
    );
    println!(
        "  attribution overhead    median {:.2} ms  p95 {:.2} ms",
        report.attribution_overhead.median_ms, report.attribution_overhead.p95_ms
    );
    println!(
        "  max relative completeness residual observed: {:.4}",
        max_rel_residual
    );
    for note in &report.notes {
        println!("  note: {note}");
    }
    println!("  raw results: {}", out.display());

    Ok(())
}
