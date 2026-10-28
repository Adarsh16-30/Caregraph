//! `caregraph-bench-load` — open-loop concurrent load generator.
//!
//! Every benchmark before this one was sequential: one request at a time, so
//! it measured per-request latency but said nothing about throughput, queueing,
//! or what happens past saturation. This generator drives a *running* server
//! over real gRPC at a fixed offered rate, independent of how fast the server
//! answers.
//!
//! **Open loop, no coordinated omission.** Request *i* has an *intended* send
//! time fixed in advance (constant spacing, or seeded Poisson arrivals).
//! Latency is measured from that intended instant, not from when the client
//! actually got around to sending. A slow server therefore cannot hide its
//! slowness by making the generator wait (a closed-loop client's classic
//! flaw): queueing delay is part of the measured latency. In-flight requests
//! are never capped to protect the server; a cap exists only to protect this
//! process from running out of memory, and anything refused by it is counted
//! as `dropped`, never silently skipped.
//!
//! **Real workload.** Node ids and mutations are sampled from the real
//! Diabetes 130 trace (Rule 6) — nothing is generated.
//!
//! Usage:
//!     caregraph-bench-load --addr http://127.0.0.1:50051 --api-key KEY \
//!         --trace benchmarks/traces/diabetes130_full.jsonl \
//!         --rates 100,200,400 --duration-secs 20 \
//!         [--mix snapshot=85,traverse=10,similar=5,add_edge=0] [--poisson]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::{
    AddEdgeRequest, Direction, ModelKind, SimilarCarePathwaysRequest, SnapshotRequest,
    TraverseRequest,
};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

/// "Now": larger than any stored timestamp, so the newest version wins.
const AS_OF_NOW: u64 = u64::MAX;
const MAX_INFLIGHT: usize = 20_000;
/// A scheduler wake-up later than this past its slot counts as a missed slot.
const LATE_SLOT: Duration = Duration::from_millis(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
enum Op {
    Snapshot,
    Traverse,
    Similar,
    AddEdge,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Snapshot => "snapshot",
            Op::Traverse => "traverse",
            Op::Similar => "similar",
            Op::AddEdge => "add_edge",
        }
    }
}

struct Args {
    addr: String,
    api_key: String,
    trace: PathBuf,
    rates: Vec<f64>,
    duration: Duration,
    warmup: Duration,
    mix: Vec<(Op, u32)>,
    poisson: bool,
    /// Send timestamp_us = 0 so the server assigns mutation timestamps (required
    /// under the strict timestamp policy, where concurrent client-side
    /// wall-clock stamps can arrive microseconds out of order and be refused).
    server_timestamps: bool,
    connections: usize,
    seed: u64,
    out_dir: PathBuf,
    max_sample_nodes: usize,
    max_sample_edges: usize,
}

fn parse_mix(s: &str) -> Result<Vec<(Op, u32)>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let (k, v) = part
            .split_once('=')
            .context("--mix entries look like name=weight")?;
        let op = match k {
            "snapshot" => Op::Snapshot,
            "traverse" => Op::Traverse,
            "similar" => Op::Similar,
            "add_edge" => Op::AddEdge,
            other => bail!("unknown op in --mix: {other}"),
        };
        out.push((op, v.parse()?));
    }
    if out.iter().all(|(_, w)| *w == 0) {
        bail!("--mix has no non-zero weight");
    }
    Ok(out)
}

fn parse_args() -> Result<Args> {
    let mut a = Args {
        addr: "http://127.0.0.1:50051".into(),
        api_key: String::new(),
        trace: PathBuf::new(),
        rates: vec![100.0],
        duration: Duration::from_secs(20),
        warmup: Duration::from_secs(3),
        mix: parse_mix("snapshot=85,traverse=10,similar=5,add_edge=0")?,
        poisson: false,
        server_timestamps: false,
        connections: 4,
        seed: 0xC0FFEE,
        out_dir: PathBuf::from("benchmarks/results"),
        max_sample_nodes: 50_000,
        max_sample_edges: 5_000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--addr" => a.addr = next()?,
            "--api-key" => a.api_key = next()?,
            "--trace" => a.trace = PathBuf::from(next()?),
            "--rates" => {
                a.rates = next()?
                    .split(',')
                    .map(|r| r.trim().parse::<f64>())
                    .collect::<std::result::Result<_, _>>()?
            }
            "--duration-secs" => a.duration = Duration::from_secs(next()?.parse()?),
            "--warmup-secs" => a.warmup = Duration::from_secs(next()?.parse()?),
            "--mix" => a.mix = parse_mix(&next()?)?,
            "--poisson" => a.poisson = true,
            "--server-timestamps" => a.server_timestamps = true,
            "--connections" => a.connections = next()?.parse()?,
            "--seed" => a.seed = next()?.parse()?,
            "--out-dir" => a.out_dir = PathBuf::from(next()?),
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-load --api-key KEY --trace FILE.jsonl \
                     [--addr URL] [--rates r1,r2,..] [--duration-secs N] [--warmup-secs N] \
                     [--mix snapshot=85,traverse=10,similar=5,add_edge=0] [--poisson] \
                     [--connections N] [--seed N] [--out-dir DIR]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    if a.api_key.is_empty() {
        a.api_key = std::env::var("CAREGRAPH_API_KEY").unwrap_or_default();
    }
    if a.api_key.is_empty() {
        bail!("--api-key (or CAREGRAPH_API_KEY) is required");
    }
    if a.trace.as_os_str().is_empty() {
        bail!("--trace is required");
    }
    Ok(a)
}

/// xorshift64* — same reproducible PRNG the other bench binaries use.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[derive(Debug, Deserialize)]
struct TraceRecord {
    op: String,
    #[serde(default)]
    node_id: Option<u64>,
    #[serde(default)]
    node_type: Option<String>,
    #[serde(default)]
    src: Option<u64>,
    #[serde(default)]
    dst: Option<u64>,
    #[serde(default)]
    edge_type: Option<u16>,
    #[serde(default)]
    timestamp_us: Option<u64>,
}

struct Workload {
    patients: Vec<u64>,
    edges: Vec<(u64, u64, u16, u64)>,
}

fn load_workload(a: &Args) -> Result<Workload> {
    let file = File::open(&a.trace).with_context(|| format!("opening {}", a.trace.display()))?;
    let mut patients = Vec::new();
    let mut edges = Vec::new();
    let mut seen_edges = 0u64;
    let mut rng = Rng(a.seed | 1);
    for line in BufReader::new(file).lines() {
        let line = line?;
        let Ok(rec) = serde_json::from_str::<TraceRecord>(&line) else {
            continue;
        };
        match rec.op.as_str() {
            "upsert_node" if rec.node_type.as_deref() == Some("Patient") => {
                if let Some(id) = rec.node_id {
                    // Reservoir-sample so a 100k-patient trace doesn't bias to its head.
                    if patients.len() < a.max_sample_nodes {
                        patients.push(id);
                    }
                }
            }
            "add_edge" => {
                if let (Some(s), Some(d), Some(t), Some(ts)) =
                    (rec.src, rec.dst, rec.edge_type, rec.timestamp_us)
                {
                    seen_edges += 1;
                    if edges.len() < a.max_sample_edges {
                        edges.push((s, d, t, ts));
                    } else {
                        let j = (rng.next() % seen_edges) as usize;
                        if j < a.max_sample_edges {
                            edges[j] = (s, d, t, ts);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if patients.is_empty() {
        bail!("trace has no Patient nodes to sample");
    }
    Ok(Workload { patients, edges })
}

fn authed<T>(msg: T, key: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        format!("Bearer {key}").parse().expect("valid header"),
    );
    r
}

struct Sample {
    op: Op,
    latency_us: u64,
    outcome: Outcome,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    Ok,
    Err(String),
}

async fn fire(
    mut client: CareGraphServiceClient<Channel>,
    key: Arc<str>,
    op: Op,
    patient: u64,
    edge: (u64, u64, u16, u64),
    live_ts: u64,
) -> Outcome {
    let res = match op {
        Op::Snapshot => client
            .snapshot(authed(
                SnapshotRequest {
                    subject: patient,
                    as_of_us: AS_OF_NOW,
                    edge_types: vec![],
                },
                &key,
            ))
            .await
            .map(|_| ()),
        Op::Traverse => client
            .traverse(authed(
                TraverseRequest {
                    start: patient,
                    as_of_us: AS_OF_NOW,
                    max_hops: 2,
                    direction: Direction::Both as i32,
                    edge_types: vec![],
                    window_from_us: 0,
                    window_to_us: 0,
                    max_nodes: 0,
                    max_edges: 0,
                },
                &key,
            ))
            .await
            .map(|_| ()),
        Op::Similar => client
            .similar_care_pathways(authed(
                SimilarCarePathwaysRequest {
                    node_id: patient,
                    as_of_us: AS_OF_NOW,
                    top_k: 10,
                },
                &key,
            ))
            .await
            .map(|_| ()),
        Op::AddEdge => client
            .add_edge(authed(
                AddEdgeRequest {
                    src: edge.0,
                    dst: edge.1,
                    edge_type: i32::from(edge.2),
                    // Live-arrival semantics: stamped at wall-clock now, so the
                    // resolver sees the full current graph. Replaying the trace's
                    // historical timestamp would resolve against a tiny past
                    // neighbourhood and understate real mutation cost.
                    timestamp_us: live_ts,
                    properties_json: String::new(),
                    model: ModelKind::Graphsage as i32,
                    explain: false,
                    request_id: String::new(),
                },
                &key,
            ))
            .await
            .map(|_| ()),
    };
    match res {
        Ok(()) => Outcome::Ok,
        Err(s) => Outcome::Err(format!("{:?}", s.code())),
    }
}

#[derive(Serialize, Default)]
struct OpStats {
    count: u64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
    mean_ms: f64,
}

#[derive(Serialize)]
struct RateResult {
    offered_rps: f64,
    achieved_ok_rps: f64,
    sent: u64,
    ok: u64,
    errors_by_code: BTreeMap<String, u64>,
    /// Refused by the client's own in-flight memory guard, never sent.
    dropped: u64,
    /// Scheduler wake-ups more than 5ms past their slot: if this is large the
    /// *generator* is the bottleneck and the result is not trustworthy.
    late_slots: u64,
    per_op: BTreeMap<String, OpStats>,
}

fn stats(h: &Histogram<u64>) -> OpStats {
    let ms = |us: u64| us as f64 / 1000.0;
    OpStats {
        count: h.len(),
        p50_ms: ms(h.value_at_quantile(0.50)),
        p95_ms: ms(h.value_at_quantile(0.95)),
        p99_ms: ms(h.value_at_quantile(0.99)),
        p999_ms: ms(h.value_at_quantile(0.999)),
        max_ms: ms(h.max()),
        mean_ms: h.mean() / 1000.0,
    }
}

fn precise_wait(target: Instant) {
    let now = Instant::now();
    if target > now + Duration::from_millis(2) {
        std::thread::sleep(target - now - Duration::from_millis(1));
    }
    while Instant::now() < target {
        std::hint::spin_loop();
    }
}

async fn run_rate(
    a: &Args,
    wl: &Workload,
    clients: &[CareGraphServiceClient<Channel>],
    rate: f64,
    seed: u64,
) -> Result<RateResult> {
    let key: Arc<str> = Arc::from(a.api_key.as_str());
    let (tx, mut rx) = mpsc::unbounded_channel::<Sample>();
    let inflight = Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT));
    let mut rng = Rng(seed | 1);
    let total_weight: u32 = a.mix.iter().map(|(_, w)| *w).sum();

    let measure_from = a.warmup;
    let end = a.warmup + a.duration;
    let interval = 1.0 / rate;

    let start = tokio::time::Instant::now();
    let mut t = 0.0f64; // intended offset in seconds
    let mut i = 0usize;
    let (mut sent, mut dropped, mut late_slots) = (0u64, 0u64, 0u64);

    while Duration::from_secs_f64(t) < end {
        let intended = start + Duration::from_secs_f64(t);
        // tokio's timer wheel is ~15ms-granular on Windows, which would add up to
        // that much phantom queueing delay to every latency (measured from the
        // intended instant). Wait with an OS sleep that stops 1ms short, then spin.
        let target = intended.into_std();
        tokio::task::block_in_place(|| precise_wait(target));
        if std::time::Instant::now().saturating_duration_since(target) > LATE_SLOT {
            late_slots += 1;
        }

        let pick = (rng.next() % total_weight as u64) as u32;
        let mut acc = 0;
        let mut op = a.mix[0].0;
        for (o, w) in &a.mix {
            acc += w;
            if pick < acc {
                op = *o;
                break;
            }
        }
        let patient = wl.patients[(rng.next() % wl.patients.len() as u64) as usize];
        let edge = if wl.edges.is_empty() {
            (0, 0, 1, 0)
        } else {
            wl.edges[(rng.next() % wl.edges.len() as u64) as usize]
        };
        if op == Op::AddEdge && wl.edges.is_empty() {
            bail!("--mix includes add_edge but the trace has no add_edge records");
        }

        let live_ts = if a.server_timestamps {
            0
        } else {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64
        };
        let in_measure = Duration::from_secs_f64(t) >= measure_from;
        match inflight.clone().try_acquire_owned() {
            Ok(permit) => {
                let client = clients[i % clients.len()].clone();
                let key = key.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let outcome = fire(client, key, op, patient, edge, live_ts).await;
                    // Measured from the INTENDED send time (open loop).
                    let latency = tokio::time::Instant::elapsed(&intended);
                    drop(permit);
                    if in_measure {
                        let _ = tx.send(Sample {
                            op,
                            latency_us: latency.as_micros() as u64,
                            outcome,
                        });
                    }
                });
                if in_measure {
                    sent += 1;
                }
            }
            Err(_) => {
                if in_measure {
                    dropped += 1;
                }
            }
        }
        i += 1;
        t += if a.poisson {
            -(1.0 - rng.unit()).ln() * interval
        } else {
            interval
        };
    }
    drop(tx);

    // Drain: wait for stragglers, bounded so a wedged server can't hang the run.
    let mut hists: BTreeMap<Op, Histogram<u64>> = BTreeMap::new();
    let mut all = Histogram::<u64>::new_with_max(3_600_000_000, 3)?;
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    let mut ok = 0u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(s)) => {
                let v = s.latency_us.max(1);
                hists
                    .entry(s.op)
                    .or_insert_with(|| Histogram::new_with_max(3_600_000_000, 3).unwrap())
                    .saturating_record(v);
                all.saturating_record(v);
                match s.outcome {
                    Outcome::Ok => ok += 1,
                    Outcome::Err(c) => *errors.entry(c).or_default() += 1,
                }
            }
            Ok(None) => break,
            Err(_) => {
                *errors.entry("DRAIN_TIMEOUT".into()).or_default() += 1;
                break;
            }
        }
    }

    let mut per_op: BTreeMap<String, OpStats> = hists
        .iter()
        .map(|(op, h)| (op.name().to_string(), stats(h)))
        .collect();
    if !all.is_empty() {
        per_op.insert("all".into(), stats(&all));
    }
    Ok(RateResult {
        offered_rps: rate,
        achieved_ok_rps: ok as f64 / a.duration.as_secs_f64(),
        sent,
        ok,
        errors_by_code: errors,
        dropped,
        late_slots,
        per_op,
    })
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

#[derive(Serialize)]
struct Report {
    benchmark: &'static str,
    provenance: Provenance,
    addr: String,
    trace: String,
    mix: BTreeMap<String, u32>,
    arrivals: &'static str,
    server_timestamps: bool,
    duration_secs: u64,
    warmup_secs: u64,
    connections: usize,
    patients_sampled: usize,
    edges_sampled: usize,
    results: Vec<RateResult>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let a = parse_args()?;
    let wl = load_workload(&a)?;
    eprintln!(
        "workload: {} patients, {} add_edge records sampled from {}",
        wl.patients.len(),
        wl.edges.len(),
        a.trace.display()
    );

    let mut clients = Vec::new();
    for _ in 0..a.connections.max(1) {
        let ch = Endpoint::from_shared(a.addr.clone())?
            .connect()
            .await
            .with_context(|| format!("connecting to {}", a.addr))?;
        clients.push(CareGraphServiceClient::new(ch));
    }

    let mut results = Vec::new();
    for (n, rate) in a.rates.iter().enumerate() {
        eprintln!(
            "-- offered {rate} req/s for {}s (+{}s warm-up)",
            a.duration.as_secs(),
            a.warmup.as_secs()
        );
        let r = run_rate(&a, &wl, &clients, *rate, a.seed.wrapping_add(n as u64)).await?;
        if let Some(all) = r.per_op.get("all") {
            eprintln!(
                "   achieved {:.1} ok/s  p50 {:.2}ms  p95 {:.2}ms  p99 {:.2}ms  errors {:?}  dropped {}  late_slots {}",
                r.achieved_ok_rps, all.p50_ms, all.p95_ms, all.p99_ms, r.errors_by_code, r.dropped, r.late_slots
            );
        }
        results.push(r);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let report = Report {
        benchmark: "concurrent_load",
        provenance: Provenance {
            generated_at_unix: now,
            git_commit: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
            git_dirty: git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty()),
            command: std::env::args().collect::<Vec<_>>().join(" "),
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            available_parallelism: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(0),
            hardware_note: std::env::var("CAREGRAPH_HARDWARE").ok(),
        },
        addr: a.addr.clone(),
        trace: a.trace.display().to_string(),
        mix: a
            .mix
            .iter()
            .map(|(o, w)| (o.name().to_string(), *w))
            .collect(),
        arrivals: if a.poisson { "poisson" } else { "constant" },
        server_timestamps: a.server_timestamps,
        duration_secs: a.duration.as_secs(),
        warmup_secs: a.warmup.as_secs(),
        connections: a.connections,
        patients_sampled: wl.patients.len(),
        edges_sampled: wl.edges.len(),
        results,
    };
    std::fs::create_dir_all(&a.out_dir)?;
    let path = a.out_dir.join(format!("load_{now}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    eprintln!("wrote {}", path.display());
    Ok(())
}
