//! Layer 5 — Query & API (PRD 3.1, Phase 6): the tonic gRPC service wrapping
//! the graph-semantics (`graph/`) and embedding (`embedding/`) layers behind
//! the RPC surface `proto/caregraph.proto` defines.
//!
//! # Rule 2 — no fake query endpoints
//!
//! Every RPC below reads or writes through the real layers underneath: the
//! mutation RPCs are `run_mutation_pipeline`/`AtomicCommitter`'s first live
//! caller anywhere in this codebase (until now, both were only ever driven
//! by tests and benchmarks); traversal and snapshot wrap `Traverser` and
//! `SnapshotReader` unchanged; similarity wraps `api::similarity` unchanged.
//! None of these build a response from literals — `check_rules.sh`'s Rule 2
//! gate greps for exactly that pattern.
//!
//! # Auth — reconciling a one-line PRD mention with real code
//!
//! Section 3.1 names "auth" as a Layer 5 responsibility in a five-word table
//! cell and never specifies a mechanism anywhere else in the document — no
//! scheme, no token format, no library. Rather than inventing an elaborate
//! auth system the PRD never asked for, [`AuthInterceptor`] does the
//! smallest real thing that mechanism-agnostic sentence supports: every RPC
//! requires a bearer token in the `authorization` metadata that matches
//! `CAREGRAPH_API_KEY`, checked before any handler runs. The server refuses
//! to start if that variable is unset — an API with auth silently disabled
//! is a fake auth endpoint in exactly the sense Rule 2 exists to forbid,
//! even though Rule 2's own grep only checks RPC response bodies.
//!
//! # Result limits
//!
//! `max_hops`/`max_result_nodes`/`max_result_edges` are the PRD's
//! `max_hops`/`max_result_size` (Section 7, Phase 3's task list) — already
//! implemented and enforced server-side by [`TraversalLimits`] since Phase
//! 3. This layer's job is only to carry a client's requested values through
//! to that existing clamp, never to construct a `TraversalLimits` from
//! anything the client sends.

// `tonic::Status` is 176 bytes — clippy's `result_large_err` (default
// threshold ~128 bytes) flags every small helper in this module that
// returns `Result<_, Status>`, which is most of them: `Status` is tonic's
// own required error type for every RPC handler's return type, not a
// choice made here, so every helper that composes into an RPC response via
// `?` inherits it. Boxing `Status` in these helpers would not shrink
// anything — it would just relocate the same 176 bytes behind a pointer at
// every one of their many call sites, in exchange for an extra
// allocation and a `*` at each `?`. Allowed crate-wide within this module
// rather than repeated on every function for the same one reason.
#![allow(clippy::result_large_err)]

pub mod diff;
pub mod metrics;
pub mod similarity;

pub mod proto {
    tonic::include_proto!("caregraph.v1");
}

use tonic::{Request, Response, Status};

use crate::api::metrics::ApiMetrics;
use crate::embedding::caps::CapController;
use crate::embedding::metrics::EmbeddingMetrics;
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::pipeline::run_mutation_pipeline;
use crate::embedding::sequencer::Sequencer;
use crate::embedding::state::GraphMutation;
use crate::error::CareGraphError;
use crate::graph::{Direction, SnapshotReader, TraversalLimits, TraversalRequest, Traverser};
use crate::server::{InputLimits, ModelStatus, RateLimiter, RpcClass};
use crate::storage::RocksKv;
use crate::temporal::record::EdgeValue;
use crate::types::{EdgeType, ModelKind, NodeId, Timestamp};

use proto::care_graph_service_server::CareGraphService;
use proto::{
    AddEdgeRequest, EdgeMsg, MutationResponse, NodeMsg, RelatedEntityMsg, RemoveEdgeRequest,
    SimilarCarePathwaysRequest, SimilarCarePathwaysResponse, SimilarPathwayMatch,
    SimilarityDeltaMatch, SimilarityDeltaRequest, SimilarityDeltaResponse, SnapshotRequest,
    SnapshotResponse, TraverseRequest, TraverseResponse, TruncationMsg, VisitedNodeMsg,
};

/// The authenticated caller, attached to every request by [`AuthInterceptor`]
/// and used to key rate limits (and, with mTLS identities, authorization).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal(pub std::sync::Arc<str>);

/// Everything beyond the stores and models that shapes how requests are
/// admitted: timestamp policy, input bounds, rate limiting.
pub struct ServiceOptions {
    pub ts_policy: crate::config::TsPolicy,
    pub input: InputLimits,
    pub rate_limiter: Option<RateLimiter>,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        ServiceOptions {
            ts_policy: crate::config::TsPolicy::Legacy,
            input: InputLimits::default(),
            rate_limiter: None,
        }
    }
}

fn rate_limited(wait: std::time::Duration) -> Status {
    let mut status = Status::resource_exhausted(format!(
        "rate limit exceeded; retry in {} ms",
        wait.as_millis().max(1)
    ));
    if let Ok(v) = wait.as_millis().max(1).to_string().parse() {
        status.metadata_mut().insert("retry-after-ms", v);
    }
    status
}

fn internal(err: CareGraphError) -> Status {
    match err {
        // Retryable: workers restarting, breaker open, queue full.
        CareGraphError::Unavailable(m) => Status::unavailable(m),
        CareGraphError::Timeout(m) => Status::deadline_exceeded(m),
        // Ordering-rule violations: retry with a fresh timestamp / later.
        CareGraphError::Aborted(m) => Status::aborted(m),
        other => Status::internal(other.to_string()),
    }
}

/// Default result count when a request leaves `top_k` at zero.
const DEFAULT_TOP_K: u32 = 10;
/// Largest `top_k` a request may ask for; above this is `INVALID_ARGUMENT`
/// rather than a silent clamp, so a caller never gets fewer rows than it
/// believes it asked for.
pub const MAX_TOP_K: u32 = 1000;

fn clamp_top_k(requested: u32) -> Result<usize, Status> {
    match requested {
        0 => Ok(DEFAULT_TOP_K as usize),
        n if n > MAX_TOP_K => Err(Status::invalid_argument(format!(
            "top_k {n} exceeds the maximum of {MAX_TOP_K}"
        ))),
        n => Ok(n as usize),
    }
}

fn env_permits(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn proto_edge_type(raw: i32) -> Result<EdgeType, Status> {
    match proto::EdgeType::try_from(raw).unwrap_or(proto::EdgeType::Unspecified) {
        proto::EdgeType::DiagnosedWith => Ok(EdgeType::DiagnosedWith),
        proto::EdgeType::PrescribedMedication => Ok(EdgeType::PrescribedMedication),
        proto::EdgeType::UnderwentProcedure => Ok(EdgeType::UnderwentProcedure),
        proto::EdgeType::TreatedByProvider => Ok(EdgeType::TreatedByProvider),
        proto::EdgeType::HasLabResult => Ok(EdgeType::HasLabResult),
        proto::EdgeType::HasEncounter => Ok(EdgeType::HasEncounter),
        proto::EdgeType::Unspecified => Err(Status::invalid_argument("edge_type is required")),
    }
}

fn domain_edge_type(t: EdgeType) -> proto::EdgeType {
    match t {
        EdgeType::DiagnosedWith => proto::EdgeType::DiagnosedWith,
        EdgeType::PrescribedMedication => proto::EdgeType::PrescribedMedication,
        EdgeType::UnderwentProcedure => proto::EdgeType::UnderwentProcedure,
        EdgeType::TreatedByProvider => proto::EdgeType::TreatedByProvider,
        EdgeType::HasLabResult => proto::EdgeType::HasLabResult,
        EdgeType::HasEncounter => proto::EdgeType::HasEncounter,
    }
}

fn proto_model_kind(raw: i32) -> Result<ModelKind, Status> {
    match proto::ModelKind::try_from(raw).unwrap_or(proto::ModelKind::Unspecified) {
        proto::ModelKind::Graphsage => Ok(ModelKind::GraphSAGE),
        proto::ModelKind::Gcn => Ok(ModelKind::GCN),
        proto::ModelKind::Gat => Ok(ModelKind::GAT),
        proto::ModelKind::Unspecified => Err(Status::invalid_argument("model is required")),
    }
}

fn edge_to_msg(edge: &crate::temporal::index::Edge) -> EdgeMsg {
    EdgeMsg {
        src: edge.src.as_u64(),
        dst: edge.dst.as_u64(),
        edge_type: domain_edge_type(edge.edge_type) as i32,
        timestamp_us: edge.timestamp.as_u64(),
        properties_json: edge.properties.to_string(),
    }
}

fn node_to_msg(node: &crate::temporal::index::Node) -> NodeMsg {
    NodeMsg {
        node_id: node.node_id.as_u64(),
        node_type: node.node_type.clone(),
        timestamp_us: node.timestamp.as_u64(),
        properties_json: node.properties.to_string(),
    }
}

fn truncation_to_msg(t: crate::graph::Truncation) -> TruncationMsg {
    TruncationMsg {
        hit_node_limit: t.hit_node_limit,
        hit_edge_limit: t.hit_edge_limit,
        hit_expansion_limit: t.hit_expansion_limit,
        fanout_capped_nodes: t.fanout_capped_nodes as u64,
        fanout_dropped_neighbors: t.fanout_dropped_neighbors as u64,
    }
}

/// Real service state — held behind an `Arc` (see [`CareGraphApi`]) so every
/// concurrent RPC shares one open database and one pair of already-spawned
/// model workers, rather than each request paying to open or spawn its own.
struct Inner {
    store: RocksKv,
    /// `None` when `ml/deployed/<name>/` doesn't exist — a request for that
    /// model kind then fails loudly (Status::failed_precondition) rather
    /// than silently running a different model's weights under its name.
    graphsage_model: Option<EmbeddingModel>,
    gat_model: Option<EmbeddingModel>,
    metrics: EmbeddingMetrics,
    api_metrics: ApiMetrics,
    limits: TraversalLimits,
    /// Self-tuning receptive-field caps (Phase 9, Feature 2) — replaces the
    /// fixed `FANOUT_CAP`/`MAX_EXPANDED_NODES` constants this service used to
    /// declare locally. `Mutex`-guarded because every concurrent mutation RPC
    /// shares one controller, the same reason `store` and the model workers
    /// are shared rather than per-request.
    caps: std::sync::Mutex<CapController>,
    /// Bulkheads. All store reads and the whole mutation pipeline are blocking
    /// work (RocksDB iteration, a Python round trip), so they run on tokio's
    /// blocking pool, never on an async worker thread, and each class is
    /// bounded so a flood of one cannot starve the other or exhaust memory.
    /// Exhaustion is `RESOURCE_EXHAUSTED`, a fast, retryable refusal.
    read_permits: std::sync::Arc<tokio::sync::Semaphore>,
    write_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// Orders commits by timestamp and detects conflicting concurrent ones.
    sequencer: Sequencer,
    input: InputLimits,
    rate_limiter: Option<RateLimiter>,
    /// Attribution costs tens of seconds per request; one at a time.
    explain_permits: std::sync::Arc<tokio::sync::Semaphore>,
    read_capacity: usize,
    write_capacity: usize,
}

/// The tonic service implementation. Cheap to clone — tonic clones the
/// service once per accepted connection, and everything real lives behind
/// the shared `Arc<Inner>`.
#[derive(Clone)]
pub struct CareGraphApi {
    inner: std::sync::Arc<Inner>,
}

impl CareGraphApi {
    /// Opens `store`, spawns whichever of the two known model kinds are
    /// actually deployed (`graphsage_model_id`/`gat_model_id`, either may be
    /// `None` to skip spawning it), and builds the metrics registry this
    /// service's mutation path records into.
    pub fn new(
        store: RocksKv,
        graphsage_model_id: Option<&str>,
        gat_model_id: Option<&str>,
        registry: &prometheus::Registry,
    ) -> crate::error::Result<Self> {
        Self::with_options(
            store,
            graphsage_model_id,
            gat_model_id,
            registry,
            ServiceOptions::default(),
        )
    }

    /// As [`Self::new`], with an explicit mutation-timestamp policy (see
    /// `embedding/sequencer.rs`). `new` keeps the historical `Legacy`
    /// behaviour (any client timestamp, no ordering); production configs use
    /// `Strict`.
    pub fn new_with_policy(
        store: RocksKv,
        graphsage_model_id: Option<&str>,
        gat_model_id: Option<&str>,
        registry: &prometheus::Registry,
        ts_policy: crate::config::TsPolicy,
    ) -> crate::error::Result<Self> {
        Self::with_options(
            store,
            graphsage_model_id,
            gat_model_id,
            registry,
            ServiceOptions {
                ts_policy,
                ..ServiceOptions::default()
            },
        )
    }

    /// The full constructor: [`Self::new`] and [`Self::new_with_policy`] are
    /// shorthands for it.
    pub fn with_options(
        store: RocksKv,
        graphsage_model_id: Option<&str>,
        gat_model_id: Option<&str>,
        registry: &prometheus::Registry,
        options: ServiceOptions,
    ) -> crate::error::Result<Self> {
        let sequencer = Sequencer::open(&store, options.ts_policy)?;
        let read_capacity = env_permits("CAREGRAPH_MAX_CONCURRENT_READS", 512);
        let write_capacity = env_permits("CAREGRAPH_MAX_CONCURRENT_WRITES", 64);
        let graphsage_model = graphsage_model_id.map(EmbeddingModel::spawn).transpose()?;
        let gat_model = gat_model_id.map(EmbeddingModel::spawn).transpose()?;
        let metrics = EmbeddingMetrics::new(registry)
            .map_err(|e| CareGraphError::Io(std::io::Error::other(e.to_string())))?;
        let api_metrics = ApiMetrics::new(registry)
            .map_err(|e| CareGraphError::Io(std::io::Error::other(e.to_string())))?;

        Ok(CareGraphApi {
            inner: std::sync::Arc::new(Inner {
                store,
                graphsage_model,
                gat_model,
                metrics,
                api_metrics,
                limits: TraversalLimits::default(),
                caps: std::sync::Mutex::new(CapController::new()),
                read_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(read_capacity)),
                write_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(write_capacity)),
                sequencer,
                input: options.input,
                rate_limiter: options.rate_limiter,
                explain_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
                read_capacity,
                write_capacity,
            }),
        })
    }

    /// Charges one request against the caller's rate-limit budget.
    fn admit<T>(&self, request: &Request<T>, class: RpcClass) -> Result<(), Status> {
        let Some(limiter) = &self.inner.rate_limiter else {
            return Ok(());
        };
        let principal = request
            .extensions()
            .get::<Principal>()
            .map(|p| p.0.as_ref())
            .unwrap_or("anonymous");
        limiter.check(principal, class).map_err(rate_limited)
    }

    /// Each deployed model's worker-pool state, for readiness and metrics.
    pub fn model_statuses(&self) -> Vec<ModelStatus> {
        let mut out = Vec::new();
        for (name, model) in [
            ("graphsage", &self.inner.graphsage_model),
            ("gat", &self.inner.gat_model),
        ] {
            if let Some(m) = model {
                out.push(ModelStatus {
                    name: name.to_string(),
                    stats: m.stats(),
                });
            }
        }
        out
    }

    /// Whether the database answers a read. Used by the liveness probe.
    pub fn store_responds(&self) -> bool {
        use crate::storage::KvStore;
        self.inner
            .store
            .get(crate::storage::CF_META, b"hwm")
            .is_ok()
    }

    /// Waits until no read or write is executing on the blocking pool, up to
    /// `deadline`. Returns whether it went idle. Requests whose client already
    /// went away keep running on the blocking pool after the transport drains,
    /// so shutdown must wait for them before closing the database.
    pub async fn quiesce(&self, deadline: std::time::Duration) -> bool {
        let wait = async {
            let reads = self
                .inner
                .read_permits
                .acquire_many(self.inner.read_capacity as u32)
                .await;
            let writes = self
                .inner
                .write_permits
                .acquire_many(self.inner.write_capacity as u32)
                .await;
            drop((reads, writes));
        };
        tokio::time::timeout(deadline, wait).await.is_ok()
    }

    /// Forces the write-ahead log to stable storage.
    pub fn flush(&self) -> crate::error::Result<()> {
        use crate::storage::KvStore;
        self.inner.store.flush_wal()
    }

    /// Runs `f` on the blocking pool under a permit from `permits`. A refused
    /// permit is an immediate `RESOURCE_EXHAUSTED`, not a queue.
    async fn blocking<T, F>(
        &self,
        permits: &std::sync::Arc<tokio::sync::Semaphore>,
        f: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&Inner) -> Result<T, Status> + Send + 'static,
    {
        let permit = permits.clone().try_acquire_owned().map_err(|_| {
            Status::resource_exhausted("server is at its concurrency limit; retry with backoff")
        })?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&inner)
        })
        .await
        .map_err(|e| Status::internal(format!("request task failed: {e}")))?
    }

    fn model_in(inner: &Inner, kind: ModelKind) -> Result<&EmbeddingModel, Status> {
        match kind {
            ModelKind::GraphSAGE | ModelKind::GCN => inner.graphsage_model.as_ref().ok_or_else(|| {
                Status::failed_precondition(
                    "no GraphSAGE model deployed on this server (ml/deployed/<name>/ not configured)",
                )
            }),
            ModelKind::GAT => inner.gat_model.as_ref().ok_or_else(|| {
                Status::failed_precondition(
                    "no GAT model deployed on this server (ml/deployed/<name>/ not configured)",
                )
            }),
        }
    }

    async fn run_mutation(
        &self,
        mutation: GraphMutation,
        edge_value: EdgeValue,
        model_kind: ModelKind,
        explain: bool,
        request_id: String,
    ) -> Result<MutationResponse, Status> {
        // GCN has no trained, deployed model anywhere in this codebase (only
        // GraphSAGE and GAT were ever trained — see ml/train_graphsage.py,
        // ml/train_gat.py) — routing it through the GraphSAGE worker would
        // silently run the wrong weights under GCN's name. Reject explicitly
        // rather than let `model_in`'s associative-path fallback do that.
        if model_kind == ModelKind::GCN {
            return Err(Status::failed_precondition(
                "ModelKind::GCN has no trained model in this deployment; use GRAPHSAGE or GAT",
            ));
        }
        // Attribution is two to three orders of magnitude slower than a plain
        // mutation, so it is off unless configured on and runs one at a time.
        if explain && !self.inner.input.explain_enabled {
            return Err(Status::permission_denied(
                "explain is disabled on this server (CAREGRAPH_EXPLAIN_ENABLED=false)",
            ));
        }

        // Fail fast, before taking a permit, when the model is not deployed.
        Self::model_in(&self.inner, model_kind)?;

        let explain_permit = if explain {
            Some(
                self.inner
                    .explain_permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| {
                        Status::resource_exhausted(
                            "another explain request is running; retry with backoff",
                        )
                    })?,
            )
        } else {
            None
        };

        let permits = self.inner.write_permits.clone();
        self.blocking(&permits, move |inner| {
            let _explain_permit = explain_permit;
            let model = Self::model_in(inner, model_kind)?;
            let request_id = (!request_id.is_empty()).then_some(request_id.as_str());
            let outcome = run_mutation_pipeline(
                mutation,
                &edge_value,
                model_kind,
                &inner.store,
                model,
                &inner.metrics,
                &inner.caps,
                &inner.sequencer,
                request_id,
                explain,
            )
            .map_err(internal)?;
            let ctx = &outcome.ctx;

            Ok(MutationResponse {
                affected_nodes: ctx.affected.iter().map(|n| n.as_u64()).collect(),
                fallback: ctx.fallback,
                fanout_capped: ctx.truncation.fanout_capped,
                neighbors_dropped: ctx.truncation.neighbors_dropped as u64,
                expansion_capped: ctx.truncation.expansion_capped,
                commit_seq: outcome.commit_seq,
                timestamp_us: outcome.timestamp.as_u64(),
                replayed: outcome.replayed,
            })
        })
        .await
    }
}

#[tonic::async_trait]
impl CareGraphService for CareGraphApi {
    async fn add_edge(
        &self,
        request: Request<AddEdgeRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.admit(&request, RpcClass::Write)?;
        let req = request.into_inner();
        let edge_type = proto_edge_type(req.edge_type)?;
        let model_kind = proto_model_kind(req.model)?;
        self.inner
            .input
            .check_timestamp(req.timestamp_us, Timestamp::now().as_u64())?;
        let properties = self.inner.input.parse_properties(&req.properties_json)?;

        let mutation = GraphMutation::AddEdge {
            src: NodeId(req.src),
            dst: NodeId(req.dst),
            edge_type,
            ts: Timestamp(req.timestamp_us),
        };
        let response = self
            .run_mutation(
                mutation,
                EdgeValue::new(properties),
                model_kind,
                req.explain,
                req.request_id,
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn remove_edge(
        &self,
        request: Request<RemoveEdgeRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.admit(&request, RpcClass::Write)?;
        let req = request.into_inner();
        let edge_type = proto_edge_type(req.edge_type)?;
        self.inner
            .input
            .check_timestamp(req.timestamp_us, Timestamp::now().as_u64())?;
        let model_kind = proto_model_kind(req.model)?;

        let mutation = GraphMutation::RemoveEdge {
            src: NodeId(req.src),
            dst: NodeId(req.dst),
            edge_type,
            ts: Timestamp(req.timestamp_us),
        };
        // Ignored for a removal (a tombstone is staged instead) — see
        // AtomicCommitter::stage_mutation.
        let response = self
            .run_mutation(
                mutation,
                EdgeValue::new(serde_json::Value::Null),
                model_kind,
                req.explain,
                req.request_id,
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn traverse(
        &self,
        request: Request<TraverseRequest>,
    ) -> Result<Response<TraverseResponse>, Status> {
        self.admit(&request, RpcClass::Read)?;
        let req = request.into_inner();

        let direction = match req.direction() {
            proto::Direction::Outgoing => Direction::Outgoing,
            proto::Direction::Incoming => Direction::Incoming,
            proto::Direction::Both | proto::Direction::Unspecified => Direction::Both,
        };

        let mut edge_types = Vec::new();
        for raw in &req.edge_types {
            edge_types.push(proto_edge_type(*raw)?);
        }

        let mut traversal_request = TraversalRequest::new(
            NodeId(req.start),
            Timestamp(req.as_of_us),
            req.max_hops as u8,
        )
        .direction(direction)
        .edge_types(edge_types)
        .max_nodes(req.max_nodes as usize)
        .max_edges(req.max_edges as usize);
        if req.window_from_us != 0 || req.window_to_us != 0 {
            traversal_request = traversal_request
                .window(Timestamp(req.window_from_us), Timestamp(req.window_to_us));
        }

        // Always the server's own limits — a client's request only supplies
        // *values* TraversalLimits clamps, never the limits themselves. See
        // this module's doc on result limits.
        let permits = self.inner.read_permits.clone();
        let result = self
            .blocking(&permits, move |inner| {
                let traverser = Traverser::new(&inner.store, inner.limits);
                let started = std::time::Instant::now();
                let result = traverser.traverse(&traversal_request).map_err(internal)?;
                // Labeled by the *effective* (server-clamped) hop count, not the
                // client's requested value — a client asking for an absurd depth
                // that gets clamped should not pollute a different label's series
                // with a latency it never actually paid for.
                inner
                    .api_metrics
                    .traversal_latency_seconds
                    .with_label_values(&[&result.effective_max_hops.to_string()])
                    .observe(started.elapsed().as_secs_f64());
                Ok(result)
            })
            .await?;

        Ok(Response::new(TraverseResponse {
            start: result.start.as_u64(),
            as_of_us: result.as_of.as_u64(),
            nodes: result
                .nodes
                .iter()
                .map(|n| VisitedNodeMsg {
                    node_id: n.node_id.as_u64(),
                    hops: n.hops as u32,
                })
                .collect(),
            edges: result.edges.iter().map(edge_to_msg).collect(),
            effective_max_hops: result.effective_max_hops as u32,
            truncation: Some(truncation_to_msg(result.truncation)),
        }))
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        self.admit(&request, RpcClass::Read)?;
        let req = request.into_inner();
        let mut edge_types = Vec::new();
        for raw in &req.edge_types {
            edge_types.push(proto_edge_type(*raw)?);
        }
        let edge_types = if edge_types.is_empty() {
            EdgeType::ALL.to_vec()
        } else {
            edge_types
        };

        let (subject, as_of) = (NodeId(req.subject), Timestamp(req.as_of_us));
        let permits = self.inner.read_permits.clone();
        let snapshot = self
            .blocking(&permits, move |inner| {
                let reader = SnapshotReader::new(&inner.store, inner.limits);
                let timer = inner.api_metrics.point_in_time_query_seconds.start_timer();
                let snapshot = reader
                    .snapshot_of_types(subject, as_of, &edge_types)
                    .map_err(internal)?;
                timer.observe_duration();
                Ok(snapshot)
            })
            .await?;

        let Some(snapshot) = snapshot else {
            return Ok(Response::new(SnapshotResponse {
                found: false,
                subject: None,
                as_of_us: req.as_of_us,
                related: Vec::new(),
                truncation: None,
            }));
        };

        Ok(Response::new(SnapshotResponse {
            found: true,
            subject: Some(node_to_msg(&snapshot.subject)),
            as_of_us: snapshot.as_of.as_u64(),
            related: snapshot
                .related
                .iter()
                .map(|r| RelatedEntityMsg {
                    edge: Some(edge_to_msg(&r.edge)),
                    node: r.node.as_ref().map(node_to_msg),
                })
                .collect(),
            truncation: Some(truncation_to_msg(snapshot.truncation)),
        }))
    }

    async fn similar_care_pathways(
        &self,
        request: Request<SimilarCarePathwaysRequest>,
    ) -> Result<Response<SimilarCarePathwaysResponse>, Status> {
        self.admit(&request, RpcClass::Read)?;
        let req = request.into_inner();
        let top_k = clamp_top_k(req.top_k)?;

        let (node, as_of) = (NodeId(req.node_id), Timestamp(req.as_of_us));
        let permits = self.inner.read_permits.clone();
        let result = self
            .blocking(&permits, move |inner| {
                similarity::similar_care_pathways(&inner.store, node, as_of, top_k)
                    .map_err(internal)
            })
            .await?;

        let Some(matches) = result else {
            return Ok(Response::new(SimilarCarePathwaysResponse {
                matches: Vec::new(),
                query_node_has_no_embedding: true,
            }));
        };

        Ok(Response::new(SimilarCarePathwaysResponse {
            matches: matches
                .into_iter()
                .map(|(node, similarity)| SimilarPathwayMatch {
                    node_id: node.as_u64(),
                    similarity,
                })
                .collect(),
            query_node_has_no_embedding: false,
        }))
    }

    async fn similarity_delta(
        &self,
        request: Request<SimilarityDeltaRequest>,
    ) -> Result<Response<SimilarityDeltaResponse>, Status> {
        self.admit(&request, RpcClass::Read)?;
        let req = request.into_inner();
        let top_k = clamp_top_k(req.top_k)?;

        let (node, from, to, min_abs) = (
            NodeId(req.node_id),
            Timestamp(req.from_us),
            Timestamp(req.to_us),
            req.min_abs_delta,
        );
        let permits = self.inner.read_permits.clone();
        let result = self
            .blocking(&permits, move |inner| {
                let timer = inner.api_metrics.similarity_delta_seconds.start_timer();
                let result = diff::similarity_delta(&inner.store, node, from, to, min_abs, top_k)
                    .map_err(internal)?;
                timer.observe_duration();
                Ok(result)
            })
            .await?;

        Ok(Response::new(SimilarityDeltaResponse {
            matches: result
                .matches
                .into_iter()
                .map(|m| SimilarityDeltaMatch {
                    node_id: m.node_id.as_u64(),
                    similarity_from: m.similarity_from,
                    similarity_to: m.similarity_to,
                    delta: m.delta,
                    present_at_from: m.present_at_from,
                    present_at_to: m.present_at_to,
                })
                .collect(),
            query_node_has_no_embedding_at_from: result.query_missing_at_from,
            query_node_has_no_embedding_at_to: result.query_missing_at_to,
        }))
    }
}

/// Bearer-token check applied to every RPC before it reaches [`CareGraphApi`]
/// — see this module's doc for why this, specifically, is the real (not
/// fake) minimum the PRD's one-line "auth" mention supports.
#[derive(Clone)]
pub struct AuthInterceptor {
    expected_token: std::sync::Arc<str>,
}

impl AuthInterceptor {
    /// `expected_token` is compared byte-for-byte against the bearer token
    /// on every request. Building one is the caller's declaration that auth
    /// is intentionally configured — see `main.rs` for why the server
    /// refuses to start rather than default this to an empty/always-pass
    /// value.
    pub fn new(expected_token: impl Into<std::sync::Arc<str>>) -> Self {
        AuthInterceptor {
            expected_token: expected_token.into(),
        }
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?;
        let value = header
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization metadata is not valid UTF-8"))?;
        let token = value
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("authorization must be \"Bearer <token>\""))?;

        if constant_time_eq(token.as_bytes(), self.expected_token.as_bytes()) {
            let mut request = request;
            request
                .extensions_mut()
                .insert(Principal(std::sync::Arc::from("api-key")));
            Ok(request)
        } else {
            Err(Status::unauthenticated("invalid bearer token"))
        }
    }
}

/// Equality that does not stop at the first differing byte, so response time
/// does not reveal how much of a guessed token was right. Length is not hidden.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
