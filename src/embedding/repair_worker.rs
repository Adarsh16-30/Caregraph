//! The repair worker: refreshes embeddings that were left stale, off the write path.
//!
//! # What it consumes
//!
//! `CF_REPAIR` markers (`repair.rs`). A marker is written, in the same atomic
//! batch as the mutation, for a node whose embedding either could not be
//! refreshed because the forward pass failed, or was deliberately deferred
//! because it sits on a hub (`deferral.rs`). Until now nothing consumed them:
//! they were only listed by `caregraph-admin`. This worker is the consumer.
//!
//! # Why this is safe to run without the commit sequencer
//!
//! A repair writes only embeddings (and their provenance). Mutations never read
//! embeddings: the resolver reads adjacency lists and node types, and the model
//! input is built from those. So a repair cannot change what a concurrent
//! mutation computes, and a mutation cannot invalidate a repair beyond making it
//! redundant, which the worker detects (a node whose newest embedding is already
//! at or after its marker is skipped).
//!
//! The recomputation reads the graph *as of the marker's timestamp*, which is
//! committed and immutable under the `strict` and `server_assigned` timestamp
//! policies. It is the same computation a synchronous update performs (same
//! resolver, same model, same caps), only done later, so the result is what the
//! synchronous path would have stored. Each repaired embedding is written at the
//! marker's timestamp, which is what makes `pending_repairs` see it as resolved
//! and keeps point-in-time reads correct.
//!
//! # Coalescing
//!
//! Many mutations around one hub mark the same neighbours again and again. The
//! queue keeps only the newest marker per node, so a burst of N mutations on a
//! hub costs one repair of its neighbours, not N. Older timestamps in between
//! are never given their own embedding version: a read as of such a timestamp
//! returns the previous version, which is stale by exactly the marker that says
//! so. That is the deliberate trade for keeping up with a write-heavy hub.
//!
//! # What it does not do
//!
//! A repair that fails because the embedding service is unavailable (workers
//! restarting, breaker open, deadline) is retried without limit and does not use up
//! an attempt: an outage must not strand nodes. A batch that fails for any other
//! reason is dropped from the in-memory queue after [`MAX_ATTEMPTS`] and left as a
//! marker, to be picked up again by the periodic re-scan (`repair_task.rs`), the
//! next process start or a promotion. It runs only on a node
//! that accepts writes. A repair is replicated like any other write batch but is
//! not held for a standby's acknowledgement: it can be re-derived from the
//! markers, which do replicate.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use rocksdb::WriteBatch;

use crate::embedding::meta::{CommitMeta, DispatchDecision, EffectiveCaps};
use crate::embedding::metrics::EmbeddingMetrics;
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::repair::{latest_embedding_ts, pending_repairs};
use crate::embedding::resolver::{build_model_input, AffectedSubgraphResolver};
use crate::error::{CareGraphError, Result};
use crate::storage::{cf, KvStore, RocksKv};
use crate::temporal::keys::{encode_commit_meta_key, encode_embedding_key};
use crate::types::{ComputationPath, Embedding, ModelKind, NodeId, Timestamp};

/// A batch that fails this many times is dropped from the in-memory queue (its
/// markers stay in `CF_REPAIR`).
pub const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone)]
struct Entry {
    marked_at: Timestamp,
    model_id: String,
    attempts: u32,
}

/// One node taken from the queue for repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimed {
    pub node: NodeId,
    pub marked_at: Timestamp,
    pub model_id: String,
    pub attempts: u32,
}

/// Nodes waiting for a fresh embedding, newest marker per node.
#[derive(Default)]
pub struct RepairQueue {
    pending: Mutex<HashMap<NodeId, Entry>>,
    wake: Condvar,
}

impl RepairQueue {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<NodeId, Entry>> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Queue `node` for repair as of `marked_at`. A newer marker replaces an
    /// older one (and resets its attempts); an older one is ignored.
    pub fn push(&self, node: NodeId, marked_at: Timestamp, model_id: &str) {
        let mut pending = self.lock();
        Self::insert(&mut pending, node, marked_at, model_id, 0);
        drop(pending);
        self.wake.notify_one();
    }

    pub fn push_all(
        &self,
        nodes: impl IntoIterator<Item = NodeId>,
        marked_at: Timestamp,
        model_id: &str,
    ) {
        let mut pending = self.lock();
        for node in nodes {
            Self::insert(&mut pending, node, marked_at, model_id, 0);
        }
        drop(pending);
        self.wake.notify_one();
    }

    fn insert(
        pending: &mut HashMap<NodeId, Entry>,
        node: NodeId,
        marked_at: Timestamp,
        model_id: &str,
        attempts: u32,
    ) {
        match pending.get_mut(&node) {
            Some(e) if e.marked_at >= marked_at => {}
            Some(e) => {
                e.marked_at = marked_at;
                e.model_id = model_id.to_string();
                e.attempts = attempts;
            }
            None => {
                pending.insert(
                    node,
                    Entry {
                        marked_at,
                        model_id: model_id.to_string(),
                        attempts,
                    },
                );
            }
        }
    }

    /// Rebuild the queue from `CF_REPAIR` (start-up, and on becoming leader).
    /// Returns how many nodes are pending.
    pub fn rebuild(&self, store: &RocksKv) -> Result<usize> {
        let pending = pending_repairs(store)?;
        let mut map = self.lock();
        for p in &pending {
            Self::insert(&mut map, p.node, p.marked_at, &p.model_id, 0);
        }
        let n = map.len();
        drop(map);
        if n > 0 {
            self.wake.notify_one();
        }
        Ok(n)
    }

    /// Take up to `max` nodes, oldest marker first within a model, so the nodes
    /// one mutation marked (which share a timestamp) arrive together and can
    /// share a forward pass.
    pub fn claim(&self, max: usize) -> Vec<Claimed> {
        let mut pending = self.lock();
        let mut all: Vec<(NodeId, Entry)> = pending.iter().map(|(n, e)| (*n, e.clone())).collect();
        all.sort_by(|a, b| {
            (&a.1.model_id, a.1.marked_at, a.0).cmp(&(&b.1.model_id, b.1.marked_at, b.0))
        });
        all.truncate(max);
        for (node, _) in &all {
            pending.remove(node);
        }
        all.into_iter()
            .map(|(node, e)| Claimed {
                node,
                marked_at: e.marked_at,
                model_id: e.model_id,
                attempts: e.attempts,
            })
            .collect()
    }

    /// Return a claimed node after a failure that says nothing about the node
    /// itself (the embedding service was unavailable). It keeps its attempts.
    pub fn requeue_transient(&self, claimed: &Claimed) {
        let mut pending = self.lock();
        Self::insert(
            &mut pending,
            claimed.node,
            claimed.marked_at,
            &claimed.model_id,
            claimed.attempts,
        );
    }

    /// Return a claimed node after a failure. A newer marker that arrived while
    /// it was claimed wins; a node out of attempts is dropped (returns `false`).
    pub fn requeue(&self, claimed: &Claimed) -> bool {
        if claimed.attempts + 1 >= MAX_ATTEMPTS {
            return false;
        }
        let mut pending = self.lock();
        Self::insert(
            &mut pending,
            claimed.node,
            claimed.marked_at,
            &claimed.model_id,
            claimed.attempts + 1,
        );
        true
    }

    /// Block until something is queued or `timeout` passes.
    pub fn wait(&self, timeout: Duration) {
        let pending = self.lock();
        if pending.is_empty() {
            let _ = self.wake.wait_timeout(pending, timeout);
        }
    }
}

/// What one pass over the queue achieved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PassStats {
    pub repaired: usize,
    /// Already refreshed by a newer write, so nothing to do.
    pub superseded: usize,
    pub failed: usize,
    /// Of `failed`, how many were an unavailable embedding service: requeued with
    /// their attempts intact, and the worker backs off before the next pass.
    pub unavailable: usize,
    /// Dropped from the queue after [`MAX_ATTEMPTS`].
    pub abandoned: usize,
}

/// Limits for repair work.
#[derive(Debug, Clone, Copy)]
pub struct RepairParams {
    /// Nodes claimed per pass.
    pub batch: usize,
    /// Targets per forward pass. The nodes one mutation defers share a timestamp
    /// and, around a hub endpoint, can number up to twice the fan-out cap, so the
    /// default keeps such a group in one pass: it is then resolved with the same
    /// receptive-field budget as the synchronous update would have had, and the
    /// overlapping neighbourhoods are read once instead of once per chunk.
    pub chunk: usize,
    pub fanout_cap: usize,
    pub max_expanded_nodes: usize,
}

impl Default for RepairParams {
    fn default() -> Self {
        let rung = crate::embedding::caps::LADDER[0];
        RepairParams {
            batch: 2 * rung.fanout_cap,
            chunk: 2 * rung.fanout_cap,
            fanout_cap: rung.fanout_cap,
            max_expanded_nodes: rung.max_expanded_nodes,
        }
    }
}

/// A deployed model the worker can repair with.
pub struct RepairModel<'a> {
    pub model: &'a EmbeddingModel,
    pub kind: ModelKind,
}

/// Recompute `nodes` as of `ts` with `model` and write the embeddings and their
/// provenance in one batch. `nodes` should share a timestamp. Returns how many
/// embeddings were written.
pub fn repair_group(
    store: &RocksKv,
    model: &RepairModel<'_>,
    nodes: &[NodeId],
    ts: Timestamp,
    params: &RepairParams,
) -> Result<usize> {
    if nodes.is_empty() {
        return Ok(0);
    }
    // Never deferred: this *is* the deferred work.
    let resolver =
        AffectedSubgraphResolver::new(store, params.fanout_cap, params.max_expanded_nodes)
            .with_hub_degree(None);
    let subgraph = resolver.resolve_targets(nodes, ts)?;
    let (features, edge_index, local) =
        build_model_input(store, &subgraph.nodes, &subgraph.edges, ts)?;
    let targets: Vec<usize> = subgraph
        .affected
        .iter()
        .filter_map(|n| local.get(n).copied())
        .collect();
    let vectors = model.model.forward(&features, &edge_index, &targets)?;

    let manifest = &model.model.manifest;
    let path = if manifest.is_associative {
        ComputationPath::Associative
    } else if manifest.architecture == "GAT" {
        ComputationPath::GatConstrained
    } else {
        ComputationPath::NonAssociative
    };

    let mut meta = CommitMeta::new(model.model.model_id.clone());
    meta.dispatch = DispatchDecision {
        computation_path: Some(path),
        manifest_is_associative: manifest.is_associative,
        manifest_architecture: manifest.architecture.clone(),
        checkpoint_architecture: model.model.checkpoint_architecture.clone(),
    };
    meta.caps = EffectiveCaps {
        fanout_cap: params.fanout_cap,
        max_expanded_nodes: params.max_expanded_nodes,
        rung: None,
    };
    meta.truncation = subgraph.truncation;
    meta.repair = true;
    let meta_bytes = meta.serialize();

    let embeddings_cf = store.cf_handle(cf::CF_EMBEDDINGS)?;
    let meta_cf = store.cf_handle(cf::CF_COMMIT_META)?;
    let mut batch = WriteBatch::default();
    let mut written = 0usize;
    for (node, vector) in subgraph.affected.iter().zip(vectors) {
        let embedding = Embedding::new(vector, model.model.model_id.clone(), model.kind, path);
        batch.put_cf(
            &embeddings_cf,
            encode_embedding_key(*node, ts),
            embedding.serialize(),
        );
        batch.put_cf(&meta_cf, encode_commit_meta_key(*node, ts), &meta_bytes);
        written += 1;
    }
    store.write(batch)?;
    Ok(written)
}

/// Claim a batch and repair it. The caller is responsible for being allowed to
/// write (leader, write permit held) before calling this.
pub fn run_pass(
    store: &RocksKv,
    queue: &RepairQueue,
    metrics: &EmbeddingMetrics,
    models: &[RepairModel<'_>],
    params: &RepairParams,
) -> PassStats {
    let mut stats = PassStats::default();
    let claimed = queue.claim(params.batch);
    if claimed.is_empty() {
        return stats;
    }

    // Nodes marked by the same mutation share a timestamp: group them so one
    // forward pass serves the whole group.
    let mut groups: Vec<(String, Timestamp, Vec<Claimed>)> = Vec::new();
    for c in claimed {
        match groups
            .iter_mut()
            .find(|(m, t, _)| *m == c.model_id && *t == c.marked_at)
        {
            Some((_, _, v)) => v.push(c),
            None => groups.push((c.model_id.clone(), c.marked_at, vec![c])),
        }
    }

    for (model_id, ts, group) in groups {
        // An empty id (an unreadable marker) falls back to the first model.
        let model = models
            .iter()
            .find(|m| m.model.model_id == model_id)
            .or_else(|| {
                if model_id.is_empty() {
                    models.first()
                } else {
                    None
                }
            });
        let Some(model) = model else {
            tracing::warn!(model = %model_id, nodes = group.len(), "repair: model not deployed");
            for c in &group {
                stats.failed += 1;
                if !queue.requeue(c) {
                    stats.abandoned += 1;
                }
            }
            metrics.repair_failures_total.inc();
            continue;
        };

        for chunk in group.chunks(params.chunk.max(1)) {
            let started = Instant::now();
            // Skip nodes a newer write already refreshed.
            let mut todo: Vec<&Claimed> = Vec::with_capacity(chunk.len());
            for c in chunk {
                match latest_embedding_ts(store, c.node) {
                    Ok(Some(latest)) if latest >= ts => stats.superseded += 1,
                    _ => todo.push(c),
                }
            }
            if todo.is_empty() {
                continue;
            }
            let nodes: Vec<NodeId> = todo.iter().map(|c| c.node).collect();
            match repair_group(store, model, &nodes, ts, params) {
                Ok(n) => {
                    stats.repaired += n;
                    metrics.repairs_completed_total.inc_by(n as u64);
                    metrics
                        .repair_batch_seconds
                        .observe(started.elapsed().as_secs_f64());
                }
                Err(e) => {
                    let transient = matches!(
                        e,
                        CareGraphError::Unavailable(_) | CareGraphError::Timeout(_)
                    );
                    tracing::error!(error = %e, nodes = nodes.len(), transient, "repair batch failed");
                    metrics.repair_failures_total.inc();
                    for c in todo {
                        stats.failed += 1;
                        if transient {
                            stats.unavailable += 1;
                            queue.requeue_transient(c);
                        } else if !queue.requeue(c) {
                            stats.abandoned += 1;
                        }
                    }
                }
            }
        }
    }
    metrics.repair_pending.set(queue.len() as i64);
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(t: u64) -> Timestamp {
        Timestamp(t)
    }

    #[test]
    fn the_queue_keeps_only_the_newest_marker_per_node() {
        let q = RepairQueue::new();
        q.push(NodeId(1), ts(10), "m");
        q.push(NodeId(1), ts(30), "m");
        q.push(NodeId(1), ts(20), "m");
        assert_eq!(q.len(), 1);
        let c = q.claim(10);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].marked_at, ts(30));
        assert!(q.is_empty());
    }

    #[test]
    fn a_claim_returns_nodes_marked_together_next_to_each_other() {
        let q = RepairQueue::new();
        q.push_all([NodeId(5), NodeId(3), NodeId(4)], ts(100), "m");
        q.push_all([NodeId(1), NodeId(2)], ts(50), "m");
        let c = q.claim(10);
        let order: Vec<(u64, u64)> = c
            .iter()
            .map(|c| (c.marked_at.as_u64(), c.node.as_u64()))
            .collect();
        assert_eq!(order, vec![(50, 1), (50, 2), (100, 3), (100, 4), (100, 5)]);
    }

    #[test]
    fn a_claim_takes_at_most_the_batch_and_leaves_the_rest() {
        let q = RepairQueue::new();
        q.push_all((1..=10).map(NodeId), ts(1), "m");
        assert_eq!(q.claim(4).len(), 4);
        assert_eq!(q.len(), 6);
    }

    #[test]
    fn a_failed_node_is_requeued_until_it_runs_out_of_attempts() {
        let q = RepairQueue::new();
        q.push(NodeId(7), ts(5), "m");
        let mut c = q.claim(1).remove(0);
        for _ in 0..(MAX_ATTEMPTS - 1) {
            assert!(q.requeue(&c));
            c = q.claim(1).remove(0);
        }
        assert!(!q.requeue(&c), "the last attempt abandons the node");
        assert!(q.is_empty());
    }

    #[test]
    fn an_outage_never_uses_up_a_nodes_attempts() {
        let q = RepairQueue::new();
        q.push(NodeId(7), ts(5), "m");
        for _ in 0..(MAX_ATTEMPTS * 10) {
            let c = q.claim(1).remove(0);
            q.requeue_transient(&c);
        }
        let c = q.claim(1).remove(0);
        assert_eq!(c.attempts, 0);
        assert!(q.requeue(&c), "the node must still have attempts left");
    }

    #[test]
    fn a_marker_that_arrived_during_a_failed_repair_is_not_overwritten() {
        let q = RepairQueue::new();
        q.push(NodeId(7), ts(5), "m");
        let c = q.claim(1).remove(0);
        q.push(NodeId(7), ts(9), "m");
        assert!(q.requeue(&c));
        let again = q.claim(1).remove(0);
        assert_eq!(again.marked_at, ts(9));
        assert_eq!(again.attempts, 0);
    }
}
