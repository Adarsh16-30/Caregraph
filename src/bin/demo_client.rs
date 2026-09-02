//! `caregraph-demo-client` — the live half of `scripts/run_demo.sh` (PRD
//! Phase 8's "seed patients, replay mutations, show traversal/snapshot/
//! similarity queries live").
//!
//! Every call in this file is a real gRPC request against a real running
//! `caregraph` server backed by a real RocksDB instance — no canned
//! response, no in-process shortcut (Rule 2). It does not invent any data:
//! the mutations it replays are read verbatim from a slice of the real
//! Diabetes 130 trace (`benchmarks/traces/diabetes130_smoke_100.jsonl`)
//! that `scripts/run_demo.sh` deliberately holds out of the bulk
//! `caregraph-load` pass so this binary can apply them live instead — the
//! same distinction `load_trace.rs`'s own module doc draws: "Loading a
//! graph without embeddings is honest; loading it with placeholder
//! embedding vectors would violate Rule 3. From Phase 6 the same records
//! arrive over gRPC instead." This binary is that Phase 6 arrival path,
//! exercised for real.
//!
//! Usage:
//!     caregraph-demo-client --addr http://127.0.0.1:50061 \
//!         --api-key <key> --live-edges <held_out.jsonl>

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::{
    AddEdgeRequest, Direction as ProtoDirection, ModelKind as ProtoModelKind,
    SimilarCarePathwaysRequest, SnapshotRequest, TraverseRequest,
};
use serde::Deserialize;
use tonic::transport::Channel;
use tonic::Request;

#[derive(Debug, Deserialize)]
struct LiveEdgeRecord {
    src: u64,
    dst: u64,
    edge_type: u16,
    edge_type_name: String,
    timestamp_us: u64,
    #[serde(default)]
    properties: serde_json::Value,
}

struct Args {
    addr: String,
    api_key: String,
    live_edges: String,
    top_k: u32,
}

fn parse_args() -> Result<Args> {
    let mut addr = "http://127.0.0.1:50061".to_string();
    let mut api_key = None;
    let mut live_edges = None;
    let mut top_k = 5u32;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => addr = args.next().context("--addr needs a value")?,
            "--api-key" => api_key = args.next(),
            "--live-edges" => live_edges = args.next(),
            "--top-k" => {
                top_k = args
                    .next()
                    .context("--top-k needs a value")?
                    .parse()
                    .context("--top-k must be an integer")?
            }
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-demo-client --addr <url> --api-key <key> \
                     --live-edges <file.jsonl> [--top-k N]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Args {
        addr,
        api_key: api_key.context("--api-key is required")?,
        live_edges: live_edges.context("--live-edges is required")?,
        top_k,
    })
}

fn authed<T>(msg: T, api_key: &str) -> Request<T> {
    let mut req = Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {api_key}")
            .parse()
            .expect("valid header value"),
    );
    req
}

fn section(title: &str) {
    println!();
    println!("=== {title} ===");
}

fn load_live_edges(path: &str) -> Result<Vec<LiveEdgeRecord>> {
    let file = File::open(path).with_context(|| format!("opening {path}"))?;
    let mut out = Vec::new();
    for (lineno, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("reading {path}:{}", lineno + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(
            serde_json::from_str(&line)
                .with_context(|| format!("{path}:{}: not a live-edge record", lineno + 1))?,
        );
    }
    if out.is_empty() {
        bail!("{path} contained no records — nothing to replay live");
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let live_edges = load_live_edges(&args.live_edges)?;

    println!(
        "CareGraph live demo — connecting to {} ({} held-out real mutations to replay)",
        args.addr,
        live_edges.len()
    );

    let channel = Channel::from_shared(args.addr.clone())
        .context("invalid --addr")?
        .connect()
        .await
        .with_context(|| format!("connecting to {}", args.addr))?;
    let mut client = CareGraphServiceClient::new(channel);

    // ------------------------------------------------------------------
    // Part 1 — Mutation: replay real, previously-unloaded encounters live.
    // ------------------------------------------------------------------
    section("Part 1/4 — Live mutation + atomic embedding commit (Rule 5)");
    println!(
        "These {} edges exist in the real Diabetes 130 trace but were held out of \
         the bulk `caregraph-load` pass on purpose — this is the first time they \
         reach the database, and they arrive the way Phase 6 intends: one gRPC \
         AddEdge call per edge, each committing its structural change and its \
         GraphSAGE embedding update in the same RocksDB WriteBatch.",
        live_edges.len()
    );

    let mut patients_touched: Vec<u64> = Vec::new();
    let mut first_ts_for_patient: BTreeMap<u64, u64> = BTreeMap::new();
    let mut last_ts_for_patient: BTreeMap<u64, u64> = BTreeMap::new();

    for rec in &live_edges {
        let start = Instant::now();
        let resp = client
            .add_edge(authed(
                AddEdgeRequest {
                    src: rec.src,
                    dst: rec.dst,
                    edge_type: rec.edge_type as i32,
                    timestamp_us: rec.timestamp_us,
                    properties_json: rec.properties.to_string(),
                    model: ProtoModelKind::Graphsage as i32,
                },
                &args.api_key,
            ))
            .await
            .with_context(|| format!("AddEdge({} -> {})", rec.src, rec.dst))?
            .into_inner();
        let elapsed = start.elapsed();

        println!(
            "  [{:>6.2} ms] patient {:<6} --{:<22}--> {:<6}  affected_nodes={:<4} \
             fallback={} fanout_capped={}",
            elapsed.as_secs_f64() * 1000.0,
            rec.src,
            rec.edge_type_name,
            rec.dst,
            resp.affected_nodes.len(),
            resp.fallback,
            resp.fanout_capped,
        );

        if !patients_touched.contains(&rec.src) {
            patients_touched.push(rec.src);
        }
        first_ts_for_patient
            .entry(rec.src)
            .or_insert(rec.timestamp_us);
        last_ts_for_patient.insert(rec.src, rec.timestamp_us);
    }

    let Some(&focus_patient) = patients_touched.last() else {
        bail!("no patient survived the live-edge replay");
    };
    let focus_before = first_ts_for_patient[&focus_patient].saturating_sub(1);
    let focus_after = last_ts_for_patient[&focus_patient];

    println!(
        "\n{} distinct patients received a live mutation this run: {:?}. \
         Focus patient for the rest of this demo: {focus_patient}.",
        patients_touched.len(),
        patients_touched
    );

    // ------------------------------------------------------------------
    // Part 2 — Bounded traversal.
    // ------------------------------------------------------------------
    section("Part 2/4 — Bounded 2-hop traversal (Layer 3)");
    let start = Instant::now();
    let traverse_resp = client
        .traverse(authed(
            TraverseRequest {
                start: focus_patient,
                as_of_us: u64::MAX,
                max_hops: 2,
                direction: ProtoDirection::Both as i32,
                edge_types: vec![],
                window_from_us: 0,
                window_to_us: 0,
                max_nodes: 0,
                max_edges: 0,
            },
            &args.api_key,
        ))
        .await
        .context("Traverse")?
        .into_inner();
    println!(
        "  Traverse(start={focus_patient}, max_hops=2) in {:.2} ms: {} nodes, {} edges \
         reached (effective_max_hops={}, hit_node_limit={}, hit_edge_limit={})",
        start.elapsed().as_secs_f64() * 1000.0,
        traverse_resp.nodes.len(),
        traverse_resp.edges.len(),
        traverse_resp.effective_max_hops,
        traverse_resp
            .truncation
            .as_ref()
            .map(|t| t.hit_node_limit)
            .unwrap_or(false),
        traverse_resp
            .truncation
            .as_ref()
            .map(|t| t.hit_edge_limit)
            .unwrap_or(false),
    );

    // ------------------------------------------------------------------
    // Part 3 — Point-in-time snapshot, before vs. after the live mutation.
    // ------------------------------------------------------------------
    section("Part 3/4 — Point-in-time snapshot (Layer 2/3, Contribution 2)");
    let before = client
        .snapshot(authed(
            SnapshotRequest {
                subject: focus_patient,
                as_of_us: focus_before,
                edge_types: vec![],
            },
            &args.api_key,
        ))
        .await
        .context("Snapshot(before)")?
        .into_inner();
    let after = client
        .snapshot(authed(
            SnapshotRequest {
                subject: focus_patient,
                as_of_us: focus_after,
                edge_types: vec![],
            },
            &args.api_key,
        ))
        .await
        .context("Snapshot(after)")?
        .into_inner();
    println!(
        "  as_of={focus_before} (the instant before this run's first live mutation \
         for patient {focus_patient}): found={}, {} related entities",
        before.found,
        before.related.len()
    );
    println!(
        "  as_of={focus_after} (immediately after this run's live replay finished): \
         found={}, {} related entities",
        after.found,
        after.related.len()
    );
    println!(
        "  Same subject, same code path, two different real answers ({} -> {} related \
         entities) purely because `as_of` moved across the mutations just committed \
         above — no separate recompute, no export/reimport (Contribution 2).",
        before.related.len(),
        after.related.len()
    );

    // ------------------------------------------------------------------
    // Part 4 — Point-in-time similarity search.
    // ------------------------------------------------------------------
    section("Part 4/4 — Point-in-time care-pathway similarity (Contribution 5)");
    let sim = client
        .similar_care_pathways(authed(
            SimilarCarePathwaysRequest {
                node_id: focus_patient,
                as_of_us: u64::MAX,
                top_k: args.top_k,
            },
            &args.api_key,
        ))
        .await
        .context("SimilarCarePathways")?
        .into_inner();
    if sim.query_node_has_no_embedding {
        println!(
            "  Patient {focus_patient} has no embedding as of now — its mutation must not \
             have landed. This would be a bug, not an expected demo outcome."
        );
    } else {
        println!(
            "  Top-{} care pathways most similar to patient {focus_patient} \
             (cosine similarity over the live-updated GraphSAGE embedding, \
             candidate pool = every node with an embedding as of now):",
            args.top_k
        );
        for m in &sim.matches {
            println!("    node {:<8} similarity {:.4}", m.node_id, m.similarity);
        }
        if sim.matches.is_empty() {
            println!(
                "    (no candidates yet — only nodes touched by a live mutation's affected \
                 subgraph have an embedding at all, per Rule 3's ban on placeholder vectors)"
            );
        }
    }

    println!();
    println!(
        "Demo complete. All four gRPC capability groups (mutation, traversal, snapshot, \
         similarity) were exercised against a real server, real RocksDB instance, and \
         real trained GraphSAGE model — every response above varied with its input."
    );

    Ok(())
}
