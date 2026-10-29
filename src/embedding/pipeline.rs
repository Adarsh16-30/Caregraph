//! `run_mutation_pipeline` (PRD 4.3) — resolve, aggregate, persist, count.
//!
//! "Persist" is `AtomicCommitter::commit` (`atomic_commit.rs`): the
//! structural mutation and its embedding update land in one `WriteBatch`,
//! not two separately-durable writes, regardless of which model is active —
//! `AtomicCommitter` itself dispatches between the associative
//! (GraphSAGE/GCN) and staged aggregations (`staged_incremental.rs`) based on
//! the active model's own manifest (Phase 9), not a hardcoded model list.
//! This module owns orchestration and metrics only — timing the call,
//! counting mutations and fallbacks (Rule 7) — not the write path itself.
//!
//! # Feeding the cap controller (Phase 9, Feature 2)
//!
//! This is the one place in production that both knows the real
//! embedding-update duration and runs once per mutation, so it is where
//! [`CapController::observe`] is fed — never from `metrics`, whose
//! `Histogram` has no quantile accessor to read a duration back out of. The
//! duration fed in is `ctx.embedding_duration`, which `AtomicCommitter::commit`
//! times narrowly around dispatch alone — excluding resolution/patching,
//! excluding attribution (Feature 1), and excluding the batch write — so the
//! controller only ever reacts to the latency its own caps actually govern.
//! `embedding_update_latency_seconds` is fed the same value, for the same
//! reason: that metric predates attribution, and its recording rule targets
//! the incremental-computation p95 miss specifically, not a request that
//! opted into an explanation of its own result.

use std::sync::Mutex;
use std::time::Instant;

use crate::config::TsPolicy;
use crate::embedding::atomic_commit::AtomicCommitter;
use crate::embedding::caps::CapController;
use crate::embedding::idempotency::{self, StoredOutcome};
use crate::embedding::metrics::EmbeddingMetrics;
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::sequencer::{stage_hwm, Sequencer};
use crate::embedding::state::{GraphMutation, MutationContext, ResolutionTruncation};
use crate::error::Result;
use crate::security::audit::{Fields as AuditFields, MutationAudit};
use crate::storage::RocksKv;
use crate::temporal::record::EdgeValue;
use crate::types::{ModelKind, NodeId, Timestamp};

// `associative::full_recompute` exists for the correctness test — proving the
// incremental path's output is exact by comparing it against an unbounded
// forward pass — and is not called from this pipeline. On a model failure this
// phase gives up on the mutation's embedding update rather than silently
// retrying against the whole graph: affected nodes keep their pre-mutation
// embedding, stale rather than wrong, `ctx.fallback` records that plainly, and
// each stale node gets a durable repair marker (see `repair.rs`).

/// Who is committing, for the mutation audit chain. When present, the audit
/// record is staged into the same batch as the mutation itself.
pub struct AuditContext<'a> {
    pub chain: &'a MutationAudit,
    pub principal: &'a str,
    pub rpc: &'static str,
}

/// What one pass through the pipeline produced.
pub struct PipelineOutcome {
    pub ctx: MutationContext,
    /// The timestamp the mutation committed at (server-assigned when the client
    /// sent `0`, or the client's own under `strict`).
    pub timestamp: Timestamp,
    /// Position in this process's commit order.
    pub commit_seq: u64,
    /// True when this is the stored outcome of an earlier execution of the same
    /// `request_id`; nothing was written this time.
    pub replayed: bool,
}

/// A mutation that has passed admission: its timestamp is fixed and it holds a
/// place in the commit order. Dropping it without executing releases that place.
pub struct Admitted<'a> {
    ticket: Option<crate::embedding::sequencer::Ticket<'a>>,
    mutation: GraphMutation,
    /// Keeps the request id marked in-flight until execution finishes.
    _request: Option<crate::embedding::sequencer::RequestGuard<'a>>,
    request_id: Option<String>,
    replay: Option<PipelineOutcome>,
}

/// Admission: refuse a concurrent duplicate `request_id`, replay a finished
/// one, otherwise fix the timestamp and take a place in the commit order.
pub fn admit_mutation<'a>(
    mutation: GraphMutation,
    store: &RocksKv,
    sequencer: &'a Sequencer,
    request_id: Option<&str>,
) -> Result<Admitted<'a>> {
    let request_id = request_id.filter(|id| !id.is_empty());
    let guard = request_id
        .map(|id| sequencer.begin_request(id))
        .transpose()?;
    if let Some(id) = request_id {
        if let Some(stored) = idempotency::lookup(store, id)? {
            return Ok(Admitted {
                ticket: None,
                mutation,
                _request: guard,
                request_id: Some(id.to_string()),
                replay: Some(replay(mutation, stored)),
            });
        }
    }
    let ticket = sequencer.admit(mutation.timestamp())?;
    let mutation = mutation.with_timestamp(ticket.ts);
    Ok(Admitted {
        ticket: Some(ticket),
        mutation,
        _request: guard,
        request_id: request_id.map(str::to_string),
        replay: None,
    })
}

/// Admit and execute in one step — what a server request does.
///
/// `edge_value` carries the properties for an `AddEdge`; ignored for a
/// `RemoveEdge`. `store` is concrete `RocksKv`, not generic over `KvStore` —
/// `AtomicCommitter` needs `TemporalWriter`'s real column-family handles to
/// stage a `WriteBatch`, same reason `TemporalWriter` itself is concrete.
///
/// `caps` is the self-tuning receptive-field cap controller (Phase 9): this
/// reads the pair currently in force, commits with it, and feeds the real
/// duration back in. `explain` requests an integrated-gradients attribution
/// for the mutation's two endpoints — see `AtomicCommitter::commit`'s doc for
/// why that is opt-in.
#[allow(clippy::too_many_arguments)]
pub fn run_mutation_pipeline(
    mutation: GraphMutation,
    edge_value: &EdgeValue,
    active_model: ModelKind,
    store: &RocksKv,
    model: &EmbeddingModel,
    metrics: &EmbeddingMetrics,
    caps: &Mutex<CapController>,
    sequencer: &Sequencer,
    request_id: Option<&str>,
    explain: bool,
    audit: Option<AuditContext<'_>>,
) -> Result<PipelineOutcome> {
    let admitted = admit_mutation(mutation, store, sequencer, request_id)?;
    execute_admitted(
        admitted,
        edge_value,
        active_model,
        store,
        model,
        metrics,
        caps,
        sequencer,
        explain,
        audit,
    )
}

/// # Ordering
///
/// Computed with no locks held, then committed in timestamp order; if a
/// mutation that committed in the meantime touched anything this one read, it
/// is recomputed once while holding the turn. See `sequencer.rs`.
#[allow(clippy::too_many_arguments)]
pub fn execute_admitted(
    admitted: Admitted<'_>,
    edge_value: &EdgeValue,
    active_model: ModelKind,
    store: &RocksKv,
    model: &EmbeddingModel,
    metrics: &EmbeddingMetrics,
    caps: &Mutex<CapController>,
    sequencer: &Sequencer,
    explain: bool,
    audit: Option<AuditContext<'_>>,
) -> Result<PipelineOutcome> {
    if let Some(replayed) = admitted.replay {
        return Ok(replayed);
    }
    let start = Instant::now();
    let ticket = admitted
        .ticket
        .expect("a non-replayed admission holds a ticket");
    let mutation = admitted.mutation;
    let request_id = admitted.request_id.as_deref();
    metrics.mutations_total.inc();

    let (rung_caps, cap_rung) = caps.lock().unwrap_or_else(|e| e.into_inner()).current();
    let committer = AtomicCommitter::new(store)?;
    let prepare = || {
        committer.prepare(
            mutation,
            active_model,
            model,
            rung_caps.fanout_cap,
            rung_caps.max_expanded_nodes,
            cap_rung,
            explain,
        )
    };

    // Compute with nothing held; remember where the commit log was.
    let pos0 = sequencer.head();
    let mut prepared = prepare()?;

    let waited = sequencer.wait_turn(&ticket)?;
    metrics.commit_wait_seconds.observe(waited.as_secs_f64());

    // Everything with a smaller timestamp has now committed or been dropped.
    // If any of it touched a node this computation read, the result is stale.
    // Nobody else can commit while we hold the turn, so one recompute is final.
    if sequencer.conflicts_since(pos0, &prepared.fetched) {
        metrics.mutation_conflict_retries_total.inc();
        prepared = prepare()?;
    }

    let ts = ticket.ts;
    let (a, b) = mutation.endpoints();
    let (a_node, b_node) = (a, b);
    let policy = sequencer.policy();
    let expected_seq = sequencer.head();
    let outcome = StoredOutcome {
        created_us: Timestamp::now().as_u64(),
        timestamp_us: ts.as_u64(),
        commit_seq: expected_seq,
        affected: prepared.ctx.affected.iter().map(|n| n.as_u64()).collect(),
        fallback: prepared.ctx.fallback,
        fanout_capped: prepared.ctx.truncation.fanout_capped,
        neighbors_dropped: prepared.ctx.truncation.neighbors_dropped as u64,
        expansion_capped: prepared.ctx.truncation.expansion_capped,
    };

    let affected_count = prepared.ctx.affected.len() as u64;
    let stale_embeddings = prepared.ctx.fallback;
    let mut pending_audit = None;
    let ctx = committer.finish(prepared, edge_value, &mut |batch, store| {
        if policy != TsPolicy::Legacy {
            stage_hwm(batch, store, ts)?;
        }
        if let Some(id) = request_id {
            idempotency::stage_outcome(batch, store, id, &outcome)?;
        }
        if let Some(a) = &audit {
            pending_audit = Some(a.chain.stage(
                batch,
                store,
                AuditFields {
                    ts_us: ts.as_u64(),
                    principal: a.principal.to_string(),
                    rpc: a.rpc.to_string(),
                    request_id: request_id.unwrap_or_default().to_string(),
                    src: a_node.as_u64(),
                    dst: b_node.as_u64(),
                    affected_count,
                    outcome: if stale_embeddings {
                        "committed_stale_embeddings".to_string()
                    } else {
                        "committed".to_string()
                    },
                    commit_seq: expected_seq,
                },
            )?);
        }
        Ok(())
    })?;
    // The batch is durable: the audit chain head may now move.
    if let (Some(a), Some(p)) = (&audit, pending_audit) {
        a.chain.advance(p);
    }
    let commit_seq = sequencer.complete(ticket, (a, b));

    metrics
        .embedding_update_latency_seconds
        .observe(ctx.embedding_duration.as_secs_f64());
    caps.lock()
        .unwrap_or_else(|e| e.into_inner())
        .observe(ctx.embedding_duration);

    if ctx.fallback {
        metrics.incremental_fallback_total.inc();
    }

    metrics
        .mutation_latency_seconds
        .observe(start.elapsed().as_secs_f64());

    Ok(PipelineOutcome {
        ctx,
        timestamp: ts,
        commit_seq,
        replayed: false,
    })
}

fn replay(mutation: GraphMutation, stored: StoredOutcome) -> PipelineOutcome {
    let mut ctx = MutationContext::new(
        mutation.with_timestamp(Timestamp(stored.timestamp_us)),
        // The model kind is not part of the stored outcome: a replay reports what
        // happened, it does not recompute anything.
        ModelKind::GraphSAGE,
    );
    ctx.affected = stored.affected.iter().map(|&n| NodeId(n)).collect();
    ctx.fallback = stored.fallback;
    ctx.truncation = ResolutionTruncation {
        fanout_capped: stored.fanout_capped,
        neighbors_dropped: stored.neighbors_dropped as usize,
        expansion_capped: stored.expansion_capped,
    };
    PipelineOutcome {
        ctx,
        timestamp: Timestamp(stored.timestamp_us),
        commit_seq: stored.commit_seq,
        replayed: true,
    }
}
