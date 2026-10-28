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

use serde_json::Value as JsonValue;
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

fn env_permits(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn parse_properties(raw: &str) -> Result<JsonValue, Status> {
    if raw.is_empty() {
        return Ok(JsonValue::Object(Default::default()));
    }
    serde_json::from_str(raw).map_err(|e| Status::invalid_argument(format!("properties_json: {e}")))
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
        Self::new_with_policy(
            store,
            graphsage_model_id,
            gat_model_id,
            registry,
            crate::config::TsPolicy::Legacy,
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
        let sequencer = Sequencer::open(&store, ts_policy)?;
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
                read_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(env_permits(
                    "CAREGRAPH_MAX_CONCURRENT_READS",
                    512,
                ))),
                write_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(env_permits(
                    "CAREGRAPH_MAX_CONCURRENT_WRITES",
                    64,
                ))),
                sequencer,
            }),
        })
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
        // Fail fast, before taking a permit, when the model is not deployed.
        Self::model_in(&self.inner, model_kind)?;

        let permits = self.inner.write_permits.clone();
        self.blocking(&permits, move |inner| {
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
        let req = request.into_inner();
        let edge_type = proto_edge_type(req.edge_type)?;
        let model_kind = proto_model_kind(req.model)?;
        let properties = parse_properties(&req.properties_json)?;

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
        let req = request.into_inner();
        let edge_type = proto_edge_type(req.edge_type)?;
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
                EdgeValue::new(JsonValue::Null),
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
        let req = request.into_inner();
        let top_k = if req.top_k == 0 {
            10
        } else {
            req.top_k as usize
        };

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
        let req = request.into_inner();
        let top_k = if req.top_k == 0 {
            10
        } else {
            req.top_k as usize
        };

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

        if token == self.expected_token.as_ref() {
            Ok(request)
        } else {
            Err(Status::unauthenticated("invalid bearer token"))
        }
    }
}
