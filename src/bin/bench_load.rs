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
//! **Comparing systems.** `--target neo4j|terminusdb` (feature `baselines`) drives the
//! same open-loop generator against Neo4j over Bolt or TerminusDB over HTTP, so the
//! client, the arrival process and the latency definition are identical. Only the
//! operations that mean the same thing on all three are available there:
//!
//! * `snapshot` — a node and its outgoing edges, with the type of each far-end node
//!   (one hop, current state);
//! * `traverse` — every node within two hops, edges taken as undirected;
//! * `ping` — the cheapest request the system can answer (`RETURN 1`, `GET /api/info`,
//!   the gRPC health check): the floor for protocol, network and container overhead.
//!
//! Each result records how many items came back (`mean_items`) and, for CareGraph, how
//! often its fan-out caps truncated a traversal, because the systems do not return the
//! same amount of data and a latency is only comparable next to the work it paid for.
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
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

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
    /// The cheapest request each system can answer: the overhead floor.
    Ping,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Snapshot => "snapshot",
            Op::Traverse => "traverse",
            Op::Similar => "similar",
            Op::AddEdge => "add_edge",
            Op::Ping => "ping",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    CareGraph,
    Neo4j,
    TerminusDb,
}

impl Target {
    fn name(self) -> &'static str {
        match self {
            Target::CareGraph => "caregraph",
            Target::Neo4j => "neo4j",
            Target::TerminusDb => "terminusdb",
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
    target: Target,
    neo4j_uri: String,
    neo4j_user: String,
    neo4j_password: String,
    terminus_url: String,
    terminus_db: String,
    terminus_user: String,
    terminus_password: String,
    /// Client-side deadline for one request to a baseline; a slower one is an error
    /// (`TIMEOUT`), counted, never silently dropped.
    request_timeout: Duration,
    /// CareGraph traversal caps (0 = the server default).
    traverse_max_nodes: u64,
    traverse_max_edges: u64,
    /// End the rate sweep at the first rate the system cannot sustain, so a saturated
    /// system is not driven further into overload than needed to see where it broke.
    stop_when_saturated: bool,
    /// Neo4j traversal: return at most this many nodes (0 = all of them). CareGraph's
    /// traversal returns at most 10,000, so this gives Neo4j the same bound on the answer.
    neo4j_limit: u64,
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
            "ping" => Op::Ping,
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
        target: Target::CareGraph,
        neo4j_uri: "127.0.0.1:7687".into(),
        neo4j_user: "neo4j".into(),
        neo4j_password: std::env::var("NEO4J_PASSWORD")
            .unwrap_or_else(|_| "caregraph-dev-only".into()),
        terminus_url: "http://127.0.0.1:6363".into(),
        terminus_db: "caregraph_baseline".into(),
        terminus_user: "admin".into(),
        terminus_password: std::env::var("TERMINUSDB_PASSWORD")
            .unwrap_or_else(|_| "caregraph-dev-only".into()),
        request_timeout: Duration::from_secs(30),
        traverse_max_nodes: 0,
        traverse_max_edges: 0,
        stop_when_saturated: false,
        neo4j_limit: 0,
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
            "--target" => {
                a.target = match next()?.as_str() {
                    "caregraph" => Target::CareGraph,
                    "neo4j" => Target::Neo4j,
                    "terminusdb" => Target::TerminusDb,
                    other => bail!("--target is caregraph, neo4j or terminusdb, not {other}"),
                }
            }
            "--neo4j-uri" => a.neo4j_uri = next()?,
            "--neo4j-user" => a.neo4j_user = next()?,
            "--neo4j-password" => a.neo4j_password = next()?,
            "--terminus-url" => a.terminus_url = next()?,
            "--terminus-db" => a.terminus_db = next()?,
            "--terminus-user" => a.terminus_user = next()?,
            "--terminus-password" => a.terminus_password = next()?,
            "--request-timeout-secs" => a.request_timeout = Duration::from_secs(next()?.parse()?),
            "--neo4j-limit" => a.neo4j_limit = next()?.parse()?,
            "--stop-when-saturated" => a.stop_when_saturated = true,
            "--traverse-max-nodes" => a.traverse_max_nodes = next()?.parse()?,
            "--traverse-max-edges" => a.traverse_max_edges = next()?.parse()?,
            "-h" | "--help" => {
                println!(
                    "usage: caregraph-bench-load --api-key KEY --trace FILE.jsonl \
                     [--addr URL] [--rates r1,r2,..] [--duration-secs N] [--warmup-secs N] \
                     [--mix snapshot=85,traverse=10,similar=5,add_edge=0] [--poisson] \
                     [--connections N] [--seed N] [--out-dir DIR] \
                     [--target caregraph|neo4j|terminusdb] [--neo4j-uri HOST:PORT] \
                     [--terminus-url URL] [--request-timeout-secs N] \
                     [--traverse-max-nodes N] [--traverse-max-edges N] [--stop-when-saturated]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    if a.api_key.is_empty() {
        a.api_key = std::env::var("CAREGRAPH_API_KEY").unwrap_or_default();
    }
    if a.target == Target::CareGraph && a.api_key.is_empty() {
        bail!("--api-key (or CAREGRAPH_API_KEY) is required");
    }
    if a.target != Target::CareGraph {
        if let Some((op, _)) = a
            .mix
            .iter()
            .find(|(o, w)| *w > 0 && matches!(o, Op::Similar | Op::AddEdge))
        {
            bail!(
                "{} has no equivalent of `{}`: use snapshot, traverse and ping for {}",
                a.target.name(),
                op.name(),
                a.target.name()
            );
        }
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
    /// Items the answer carried (edges for a snapshot, nodes for a traversal).
    items: u64,
    /// CareGraph only: the answer was cut short by a fan-out or size cap.
    truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    Ok,
    Err(String),
}

struct Reply {
    outcome: Outcome,
    items: u64,
    truncated: bool,
}

impl Reply {
    fn ok(items: u64, truncated: bool) -> Self {
        Reply {
            outcome: Outcome::Ok,
            items,
            truncated,
        }
    }
    fn err(code: impl Into<String>) -> Self {
        Reply {
            outcome: Outcome::Err(code.into()),
            items: 0,
            truncated: false,
        }
    }
}

/// The system under test and what it takes to talk to it.
enum Backend {
    CareGraph {
        clients: Vec<CareGraphServiceClient<Channel>>,
        health: Vec<HealthClient<Channel>>,
        key: Arc<str>,
        max_nodes: u64,
        max_edges: u64,
    },
    #[cfg(feature = "baselines")]
    Neo4j {
        graph: neo4rs::Graph,
        timeout: Duration,
        limit: u64,
    },
    #[cfg(feature = "baselines")]
    Terminus {
        http: reqwest::Client,
        url: String,
        db: String,
        user: String,
        password: String,
        timeout: Duration,
    },
}

async fn connect(a: &Args) -> Result<Backend> {
    match a.target {
        Target::CareGraph => {
            let mut clients = Vec::new();
            let mut health = Vec::new();
            for _ in 0..a.connections.max(1) {
                let ch = Endpoint::from_shared(a.addr.clone())?
                    .connect()
                    .await
                    .with_context(|| format!("connecting to {}", a.addr))?;
                clients.push(CareGraphServiceClient::new(ch.clone()));
                health.push(HealthClient::new(ch));
            }
            Ok(Backend::CareGraph {
                clients,
                health,
                key: Arc::from(a.api_key.as_str()),
                max_nodes: a.traverse_max_nodes,
                max_edges: a.traverse_max_edges,
            })
        }
        #[cfg(feature = "baselines")]
        Target::Neo4j => {
            let config = neo4rs::ConfigBuilder::default()
                .uri(a.neo4j_uri.as_str())
                .user(a.neo4j_user.as_str())
                .password(a.neo4j_password.as_str())
                .max_connections(a.connections.max(1) * 16)
                .build()?;
            let graph = neo4rs::Graph::connect(config)
                .await
                .with_context(|| format!("connecting to Neo4j at {}", a.neo4j_uri))?;
            Ok(Backend::Neo4j {
                graph,
                timeout: a.request_timeout,
                limit: a.neo4j_limit,
            })
        }
        #[cfg(feature = "baselines")]
        Target::TerminusDb => Ok(Backend::Terminus {
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(a.connections.max(1) * 16)
                .timeout(a.request_timeout)
                .build()?,
            url: a.terminus_url.trim_end_matches('/').to_string(),
            db: a.terminus_db.clone(),
            user: a.terminus_user.clone(),
            password: a.terminus_password.clone(),
            timeout: a.request_timeout,
        }),
        #[cfg(not(feature = "baselines"))]
        _ => bail!("this build has no baseline drivers: rebuild with --features baselines"),
    }
}

impl Backend {
    async fn fire(
        &self,
        slot: usize,
        op: Op,
        patient: u64,
        edge: (u64, u64, u16, u64),
        live_ts: u64,
    ) -> Reply {
        match self {
            Backend::CareGraph {
                clients,
                health,
                key,
                max_nodes,
                max_edges,
            } => {
                let client = clients[slot % clients.len()].clone();
                let health = health[slot % health.len()].clone();
                fire_caregraph(
                    client,
                    health,
                    key.clone(),
                    op,
                    patient,
                    edge,
                    live_ts,
                    *max_nodes,
                    *max_edges,
                )
                .await
            }
            #[cfg(feature = "baselines")]
            Backend::Neo4j {
                graph,
                timeout,
                limit,
            } => {
                match tokio::time::timeout(*timeout, baselines::neo4j(graph, op, patient, *limit))
                    .await
                {
                    Ok(r) => r,
                    Err(_) => Reply::err("TIMEOUT"),
                }
            }
            #[cfg(feature = "baselines")]
            Backend::Terminus {
                http,
                url,
                db,
                user,
                password,
                timeout,
            } => {
                match tokio::time::timeout(
                    *timeout,
                    baselines::terminus(http, url, db, user, password, op, patient),
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => Reply::err("TIMEOUT"),
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn fire_caregraph(
    mut client: CareGraphServiceClient<Channel>,
    mut health: HealthClient<Channel>,
    key: Arc<str>,
    op: Op,
    patient: u64,
    edge: (u64, u64, u16, u64),
    live_ts: u64,
    max_nodes: u64,
    max_edges: u64,
) -> Reply {
    let code = |s: tonic::Status| Reply::err(format!("{:?}", s.code()));
    match op {
        Op::Snapshot => match client
            .snapshot(authed(
                SnapshotRequest {
                    min_sequence: 0,
                    subject: patient,
                    as_of_us: AS_OF_NOW,
                    edge_types: vec![],
                },
                &key,
            ))
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                let capped = r.truncation.is_some_and(|t| {
                    t.hit_node_limit
                        || t.hit_edge_limit
                        || t.hit_expansion_limit
                        || t.fanout_capped_nodes > 0
                });
                Reply::ok(r.related.len() as u64, capped)
            }
            Err(s) => code(s),
        },
        Op::Traverse => match client
            .traverse(authed(
                TraverseRequest {
                    min_sequence: 0,
                    start: patient,
                    as_of_us: AS_OF_NOW,
                    max_hops: 2,
                    direction: Direction::Both as i32,
                    edge_types: vec![],
                    window_from_us: 0,
                    window_to_us: 0,
                    max_nodes,
                    max_edges,
                },
                &key,
            ))
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                let capped = r.truncation.is_some_and(|t| {
                    t.hit_node_limit
                        || t.hit_edge_limit
                        || t.hit_expansion_limit
                        || t.fanout_capped_nodes > 0
                });
                Reply::ok(r.nodes.len() as u64, capped)
            }
            Err(s) => code(s),
        },
        Op::Similar => match client
            .similar_care_pathways(authed(
                SimilarCarePathwaysRequest {
                    min_sequence: 0,
                    node_id: patient,
                    as_of_us: AS_OF_NOW,
                    top_k: 10,
                },
                &key,
            ))
            .await
        {
            Ok(r) => Reply::ok(r.into_inner().matches.len() as u64, false),
            Err(s) => code(s),
        },
        Op::AddEdge => match client
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
        {
            Ok(_) => Reply::ok(1, false),
            Err(s) => code(s),
        },
        Op::Ping => match health
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
        {
            Ok(_) => Reply::ok(0, false),
            Err(s) => code(s),
        },
    }
}

/// Drivers for the baselines. Each runs the query that does the same work as the
/// CareGraph operation of the same name and reads the whole answer before returning,
/// so the latency includes moving the result to the client.
#[cfg(feature = "baselines")]
mod baselines {
    use super::{Op, Reply};
    use neo4rs::query;
    use serde_json::{json, Value};

    pub async fn neo4j(graph: &neo4rs::Graph, op: Op, patient: u64, limit: u64) -> Reply {
        let id = patient as i64;
        let result: Result<(u64, bool), neo4rs::Error> = async {
            match op {
                Op::Snapshot => {
                    let mut rows = graph
                        .execute(
                            query(
                                "MATCH (a:GraphNode {id: $id}) \
                                 OPTIONAL MATCH (a)-[r]->(b:GraphNode) \
                                 RETURN a.type AS t, \
                                        collect([type(r), b.id, b.type, r.event_us]) AS rel, \
                                        count(r) AS n",
                            )
                            .param("id", id),
                        )
                        .await?;
                    let mut n = 0;
                    while let Some(row) = rows.next().await? {
                        n += row.get::<i64>("n").unwrap_or(0) as u64;
                    }
                    Ok((n, false))
                }
                Op::Traverse => {
                    let bound = if limit > 0 {
                        format!("LIMIT {limit} ")
                    } else {
                        String::new()
                    };
                    let mut rows = graph
                        .execute(
                            query(&format!(
                                "MATCH (a:GraphNode {{id: $id}})-[*1..2]-(b:GraphNode) \
                                 WITH DISTINCT b {bound}\
                                 RETURN collect(b.id) AS ids, count(b) AS n"
                            ))
                            .param("id", id),
                        )
                        .await?;
                    let mut n = 0;
                    while let Some(row) = rows.next().await? {
                        n += row.get::<i64>("n").unwrap_or(0) as u64;
                    }
                    Ok((n, false))
                }
                Op::Ping => {
                    let mut rows = graph.execute(query("RETURN 1 AS one")).await?;
                    while rows.next().await?.is_some() {}
                    Ok((0, false))
                }
                _ => unreachable!("rejected at argument parsing"),
            }
        }
        .await;
        match result {
            Ok((items, truncated)) => Reply::ok(items, truncated),
            Err(e) => Reply::err(format!("neo4j:{}", short(&e.to_string()))),
        }
    }

    fn short(s: &str) -> String {
        s.chars().take(40).collect()
    }

    fn triple_data(var: &str, predicate: &str, value: u64) -> Value {
        json!({
            "@type": "Triple",
            "subject": {"@type": "NodeValue", "variable": var},
            "predicate": {"@type": "NodeValue", "node": format!("@schema:{predicate}")},
            "object": {"@type": "Value", "data": {"@type": "xsd:integer", "@value": value}},
        })
    }

    fn triple_var(var: &str, predicate: &str, object: &str) -> Value {
        json!({
            "@type": "Triple",
            "subject": {"@type": "NodeValue", "variable": var},
            "predicate": {"@type": "NodeValue", "node": format!("@schema:{predicate}")},
            "object": {"@type": "Value", "variable": object},
        })
    }

    /// A node and its outgoing edges with each far-end node's type.
    fn snapshot_query(id: u64) -> Value {
        json!({
            "@type": "Select",
            "variables": ["ET", "D", "TS", "NT"],
            "query": {"@type": "And", "and": [
                triple_data("E", "src", id),
                triple_var("E", "dst", "D"),
                triple_var("E", "edge_type", "ET"),
                triple_var("E", "event_us", "TS"),
                triple_var("N", "node_id", "D"),
                triple_var("N", "node_type", "NT"),
            ]},
        })
    }

    /// Every node within two hops, edges taken as undirected: the second hop joins on
    /// the node reached by the first.
    fn traverse_query(id: u64) -> Value {
        let hop1 = json!({"@type": "Or", "or": [
            {"@type": "And", "and": [triple_data("E1", "src", id), triple_var("E1", "dst", "M")]},
            {"@type": "And", "and": [triple_data("E1", "dst", id), triple_var("E1", "src", "M")]},
        ]});
        let hop2 = json!({"@type": "Or", "or": [
            {"@type": "And", "and": [triple_var("E2", "src", "M"), triple_var("E2", "dst", "R")]},
            {"@type": "And", "and": [triple_var("E2", "dst", "M"), triple_var("E2", "src", "R")]},
        ]});
        json!({
            "@type": "Distinct",
            "variables": ["R"],
            "query": {"@type": "Select", "variables": ["R"], "query": {"@type": "And", "and": [hop1, hop2]}},
        })
    }

    #[cfg(test)]
    pub fn snapshot_query_for_test(id: u64) -> Value {
        snapshot_query(id)
    }

    #[cfg(test)]
    pub fn traverse_query_for_test(id: u64) -> Value {
        traverse_query(id)
    }

    pub async fn terminus(
        http: &reqwest::Client,
        url: &str,
        db: &str,
        user: &str,
        password: &str,
        op: Op,
        patient: u64,
    ) -> Reply {
        let request = match op {
            Op::Ping => http.get(format!("{url}/api/info")),
            Op::Snapshot | Op::Traverse => {
                let q = if op == Op::Snapshot {
                    snapshot_query(patient)
                } else {
                    traverse_query(patient)
                };
                http.post(format!("{url}/api/woql/{user}/{db}"))
                    .json(&json!({"query": q}))
            }
            _ => unreachable!("rejected at argument parsing"),
        }
        .basic_auth(user, Some(password));
        let response = match request.send().await {
            Ok(r) => r,
            Err(e) if e.is_timeout() => return Reply::err("TIMEOUT"),
            Err(_) => return Reply::err("terminus:connect"),
        };
        if !response.status().is_success() {
            return Reply::err(format!("terminus:http{}", response.status().as_u16()));
        }
        let body: Value = match response.json().await {
            Ok(b) => b,
            Err(_) => return Reply::err("terminus:body"),
        };
        if op == Op::Ping {
            return Reply::ok(0, false);
        }
        if body.get("api:status").and_then(Value::as_str) != Some("api:success") {
            return Reply::err("terminus:woql");
        }
        let n = body
            .get("bindings")
            .and_then(Value::as_array)
            .map_or(0, |b| b.len() as u64);
        Reply::ok(n, false)
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
    /// Mean items per answer (edges / nodes), over the answers that succeeded.
    mean_items: f64,
    /// Share of answers cut short by a CareGraph fan-out or size cap.
    truncated_share: f64,
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

fn stats(h: &Histogram<u64>, mean_items: f64, truncated_share: f64) -> OpStats {
    let ms = |us: u64| us as f64 / 1000.0;
    OpStats {
        mean_items,
        truncated_share,
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
    backend: &Arc<Backend>,
    rate: f64,
    seed: u64,
) -> Result<RateResult> {
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
                let backend = backend.clone();
                let tx = tx.clone();
                let slot = i;
                tokio::spawn(async move {
                    let reply = backend.fire(slot, op, patient, edge, live_ts).await;
                    // Measured from the INTENDED send time (open loop).
                    let latency = tokio::time::Instant::elapsed(&intended);
                    drop(permit);
                    if in_measure {
                        let _ = tx.send(Sample {
                            op,
                            latency_us: latency.as_micros() as u64,
                            outcome: reply.outcome,
                            items: reply.items,
                            truncated: reply.truncated,
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
    // Per op: (answers counted, items summed, truncated answers).
    let mut work: BTreeMap<Op, (u64, u64, u64)> = BTreeMap::new();
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
                    Outcome::Ok => {
                        ok += 1;
                        let w = work.entry(s.op).or_default();
                        w.0 += 1;
                        w.1 += s.items;
                        w.2 += u64::from(s.truncated);
                    }
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
        .map(|(op, h)| {
            let (n, items, truncated) = work.get(op).copied().unwrap_or_default();
            let per = |x: u64| if n == 0 { 0.0 } else { x as f64 / n as f64 };
            (op.name().to_string(), stats(h, per(items), per(truncated)))
        })
        .collect();
    if !all.is_empty() {
        per_op.insert("all".into(), stats(&all, 0.0, 0.0));
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
    target: &'static str,
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

    let backend = Arc::new(connect(&a).await?);

    let mut results = Vec::new();
    for (n, rate) in a.rates.iter().enumerate() {
        eprintln!(
            "-- offered {rate} req/s for {}s (+{}s warm-up)",
            a.duration.as_secs(),
            a.warmup.as_secs()
        );
        let r = run_rate(&a, &wl, &backend, *rate, a.seed.wrapping_add(n as u64)).await?;
        if let Some(all) = r.per_op.get("all") {
            eprintln!(
                "   achieved {:.1} ok/s  p50 {:.2}ms  p95 {:.2}ms  p99 {:.2}ms  errors {:?}  dropped {}  late_slots {}",
                r.achieved_ok_rps, all.p50_ms, all.p95_ms, all.p99_ms, r.errors_by_code, r.dropped, r.late_slots
            );
        }
        // "Saturated": it completed under 90% of what was offered, or its p99 passed
        // two seconds, or more than 5% of requests failed.
        let saturated = r.per_op.get("all").is_some_and(|all| {
            let failed: u64 = r.errors_by_code.values().sum();
            r.achieved_ok_rps < 0.9 * r.offered_rps
                || all.p99_ms > 2_000.0
                || failed as f64 > 0.05 * r.sent.max(1) as f64
        });
        results.push(r);
        if a.stop_when_saturated && saturated {
            eprintln!("   saturated: ending the sweep here");
            break;
        }
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
        target: a.target.name(),
        addr: match a.target {
            Target::CareGraph => a.addr.clone(),
            Target::Neo4j => a.neo4j_uri.clone(),
            Target::TerminusDb => a.terminus_url.clone(),
        },
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
    let path = a
        .out_dir
        .join(format!("load_{}_{now}.json", a.target.name()));
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    eprintln!("wrote {}", path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mix_accepts_every_operation_including_ping() {
        let mix = parse_mix("snapshot=80,traverse=15,ping=5").unwrap();
        assert_eq!(
            mix,
            vec![(Op::Snapshot, 80), (Op::Traverse, 15), (Op::Ping, 5)]
        );
        assert!(
            parse_mix("snapshot=0").is_err(),
            "an all-zero mix is refused"
        );
        assert!(parse_mix("nonsense=1").is_err());
    }

    #[cfg(feature = "baselines")]
    #[test]
    fn the_terminusdb_queries_ask_for_the_same_work_as_the_other_systems() {
        let snapshot = baselines::snapshot_query_for_test(42).to_string();
        // One hop out of the subject, with the far-end node's type joined in.
        assert!(snapshot.contains("@schema:src") && snapshot.contains("@schema:node_type"));
        assert!(snapshot.contains("\"@value\":42"));

        let traverse = baselines::traverse_query_for_test(42).to_string();
        // Two hops, each in both directions, answers distinct.
        assert!(traverse.contains("\"Distinct\""));
        assert_eq!(traverse.matches("\"Or\"").count(), 2);
        assert!(traverse.contains("@schema:src") && traverse.contains("@schema:dst"));
        assert_eq!(traverse.matches("\"@value\":42").count(), 2);
    }
}
