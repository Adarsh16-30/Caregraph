//! `AtomicCommitter` (PRD Section 9.2, Contribution 1) — the structural
//! mutation and its embedding update land in one `WriteBatch`, committed with
//! a single call into the storage engine's own atomic-batch write path.
//! Rule 5: commit atomically, or not at all.
//!
//! # Reconciling PRD 9.2 with the real code
//!
//! The PRD's own atomic-commit sketch is written against names this crate
//! doesn't have — a `write_mutation` free function, a `cf_embeddings(db)`
//! accessor, `MutationEvent::committed()`, `ctx.audit_trail` — none of which
//! exist on the real `MutationContext` (`state.rs`) or anywhere else in this
//! codebase. Rather than inventing types to match unused pseudocode, this
//! module is built on the primitives every other layer already anticipated
//! needing it: [`TemporalWriter`], whose own doc says its staged writes are
//! how `write_mutation` "is implemented on top of these primitives at
//! Phase 5"; [`RocksKv::raw`], documented as "needed by atomic_commit, which
//! builds a `WriteBatch` against live CF handles"; and
//! [`encode_embedding_key`]/[`Embedding::serialize`][ser], both already
//! annotated "called by atomic_commit". What Rule 5 actually requires — one
//! write, all or nothing — is what this module delivers; the PRD's specific
//! identifier names were never load-bearing.
//!
//! [ser]: crate::types::Embedding::serialize
//!
//! # The read-before-write problem this exists to solve
//!
//! Phase 4's pipeline (`pipeline.rs`) could not do this in one write:
//! computing an embedding update needs to read the *post-mutation* graph
//! (`resolver.rs`'s two-ring expansion), but a `WriteBatch` is invisible to
//! reads until it commits. So Phase 4 committed the structural mutation
//! first, computed embeddings against the now-visible new state, and
//! persisted them in a second, separate batch. Real, but not atomic: a crash
//! between the two commits leaves a structural change durable on disk with
//! nothing recording that its embedding update never ran — unlike a
//! fan-out-capped truncation, that staleness is not marked anywhere.
//!
//! This module resolves the same way Phase 4 did, but against the graph as
//! it stood the instant *before* the mutation — which is naturally what
//! [`AffectedSubgraphResolver::resolve`] sees, since nothing has been staged
//! yet — and then patches the one edge this mutation changes directly into
//! the resolved subgraph before the forward pass runs (see
//! [`patch_subgraph_for_mutation`]). No snapshot machinery and no read-your-
//! own-writes overlay over the storage engine: exactly one edge changes per
//! mutation, and it is already known precisely from the [`GraphMutation`]
//! itself, so patching it in is exact — not an approximation of reading it
//! back.

use std::collections::HashMap;

use rocksdb::WriteBatch;

use crate::embedding::associative::aggregate_over_subgraph;
use crate::embedding::attribution::{attribute_targets, DEFAULT_STEPS, DEFAULT_TOP_K};
use crate::embedding::meta::{Attribution, CommitMeta, DispatchDecision, EffectiveCaps};
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::resolver::{patch_subgraph_for_mutation, AffectedSubgraphResolver};
use crate::embedding::staged_incremental::staged_incremental_update;
use crate::embedding::state::{GraphMutation, MutationContext};
use crate::error::Result;
use crate::storage::{cf, KvStore, RocksKv};
use crate::temporal::keys::{encode_commit_meta_key, encode_embedding_key};
use crate::temporal::record::EdgeValue;
use crate::temporal::{TemporalIndex, TemporalWriter};
use crate::types::{ComputationPath, ModelKind, NodeId};

/// `CAREGRAPH_ATTRIBUTION_STEPS` overrides [`DEFAULT_STEPS`] server-wide —
/// see `attribution.rs`'s own doc for why this is a blunt, global knob rather
/// than a per-request accuracy/latency trade a client can dial in.
fn attribution_steps() -> u32 {
    std::env::var("CAREGRAPH_ATTRIBUTION_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_STEPS)
}

/// Commits one structural mutation and its embedding update as a single
/// atomic write.
pub struct AtomicCommitter<'a> {
    store: &'a RocksKv,
    writer: TemporalWriter<'a>,
    index: TemporalIndex<'a, RocksKv>,
}

impl<'a> AtomicCommitter<'a> {
    pub fn new(store: &'a RocksKv) -> Result<Self> {
        Ok(AtomicCommitter {
            store,
            writer: TemporalWriter::new(store)?,
            index: TemporalIndex::new(store),
        })
    }

    /// Stage the mutation, resolve and compute its embedding update against
    /// the pre-mutation graph plus the one edge this call already knows is
    /// changing, then commit everything together.
    ///
    /// `edge_value` carries the properties for an `AddEdge`; ignored — a
    /// tombstone is staged instead — for a `RemoveEdge`.
    ///
    /// `cap_rung` is `Some(i)` when `fanout_cap`/`max_expanded_nodes` came
    /// from ladder rung `i` of a [`crate::embedding::caps::CapController`]
    /// (Phase 9, Feature 2), and `None` when they were supplied directly by a
    /// pinned caller (benchmarks, correctness tests, fault injection) — it is
    /// recorded verbatim on [`CommitMeta`] and never inferred from the values
    /// themselves, since a pinned caller could coincidentally pass a pair
    /// that also happens to be a ladder rung.
    ///
    /// `explain` requests an integrated-gradients edge attribution (Phase 9,
    /// Feature 1) for the mutation's two endpoints, folded into the
    /// [`CommitMeta`] each of them gets in this same batch. Opt-in per
    /// request, not a server-wide switch: measured overhead is substantial
    /// (`docs/benchmark_report.md`'s attribution section) on top of an
    /// incremental-update p95 that already misses its own target, so a bulk
    /// ingest replaying thousands of mutations does not pay it by default.
    #[allow(clippy::too_many_arguments)]
    pub fn commit(
        &self,
        mutation: GraphMutation,
        edge_value: &EdgeValue,
        active_model: ModelKind,
        model: &EmbeddingModel,
        fanout_cap: usize,
        max_expanded_nodes: usize,
        cap_rung: Option<usize>,
        explain: bool,
    ) -> Result<MutationContext> {
        let mut ctx = MutationContext::new(mutation, active_model);
        let as_of = mutation.timestamp();

        // Reads whatever is durably committed right now. Nothing below has
        // staged this mutation into the database yet, so that is exactly the
        // graph as it stood the instant before it.
        let resolver = AffectedSubgraphResolver::new(self.store, fanout_cap, max_expanded_nodes);
        let mut subgraph = resolver.resolve(mutation)?;
        // Captured before patching (Feature 1): the pre-mutation edge list,
        // over the same `nodes` this call will use throughout — the "before"
        // half of the attribution comparison. `subgraph` itself is moved into
        // the associative/staged dispatch below, so anything needed after
        // that call has to be cloned out first, not read back off it.
        let edges_before: Vec<(NodeId, NodeId)> = subgraph.edges.clone();
        patch_subgraph_for_mutation(&mut subgraph, &self.index, mutation)?;
        let edges_after: Vec<(NodeId, NodeId)> = subgraph.edges.clone();
        let nodes_for_attribution: Vec<NodeId> = subgraph.nodes.clone();

        // Phase 9 (Feature 4): dispatch is driven by the deployed model's own
        // manifest — `model.manifest.is_associative`, read from
        // `dataset_manifest.json` and cross-checked at spawn time against
        // what the worker self-reported from model.pt — rather than a
        // hand-maintained match on ModelKind. `ModelKind::is_associative()`
        // still exists, but now serves only as a consistency check: it must
        // agree with the manifest, or something is mislabeled and this fails
        // loudly instead of silently trusting whichever side is wrong.
        if model.manifest.is_associative != active_model.is_associative() {
            return Err(crate::error::CareGraphError::Io(std::io::Error::other(
                format!(
                    "model {} (manifest architecture {:?}) declares is_associative={}, \
                 which disagrees with the requested ModelKind {active_model:?} (is_associative={})",
                    model.model_id,
                    model.manifest.architecture,
                    model.manifest.is_associative,
                    active_model.is_associative(),
                ),
            )));
        }

        // Resolution and patching are identical either way — only which
        // aggregation ran, and therefore which ComputationPath tag the
        // result carries, depends on the manifest (see
        // staged_incremental.rs's module doc for why a non-associative model
        // still shares this exact mechanism).
        //
        // Timed narrowly around dispatch alone — not resolution/patching
        // above, and not attribution/the batch write below — because this is
        // exactly the duration `ctx.embedding_duration`'s own doc promises
        // the cap controller: what the caps this call was given actually
        // govern, nothing else.
        let dispatch_start = std::time::Instant::now();
        let computation_path = if model.manifest.is_associative {
            aggregate_over_subgraph(&mut ctx, self.store, model, subgraph, as_of)?;
            ComputationPath::Associative
        } else {
            // GAT keeps its own historically-stable tag so every embedding
            // stored before Phase 9 keeps its exact original meaning; any
            // other manifest-declared non-associative architecture tags
            // NonAssociative instead of being folded into GAT's tag.
            let path = if model.manifest.architecture == "GAT" {
                ComputationPath::GatConstrained
            } else {
                ComputationPath::NonAssociative
            };
            staged_incremental_update(
                &mut ctx,
                self.store,
                model,
                subgraph,
                as_of,
                active_model,
                path,
            )?;
            path
        };
        ctx.embedding_duration = dispatch_start.elapsed();

        let dispatch = DispatchDecision {
            computation_path: Some(computation_path),
            manifest_is_associative: model.manifest.is_associative,
            manifest_architecture: model.manifest.architecture.clone(),
            checkpoint_architecture: model.checkpoint_architecture.clone(),
        };
        let caps = EffectiveCaps {
            fanout_cap,
            max_expanded_nodes,
            rung: cap_rung,
        };
        let mut meta = CommitMeta::new(model.model_id.clone());
        meta.dispatch = dispatch;
        meta.caps = caps;
        meta.truncation = ctx.truncation;

        // Feature 1: only the mutation's own two endpoints, never every
        // affected node — see attribution.rs's module doc for the measured
        // cost that boundary exists to bound. `subgraph` is gone by now (it
        // was moved into the dispatch above), which is exactly why
        // `edges_before`/`edges_after`/`nodes_for_attribution` were cloned
        // out ahead of it.
        let attribution_by_node: HashMap<NodeId, Attribution> = if explain {
            let (src, dst) = mutation.endpoints();
            attribute_targets(
                self.store,
                model,
                &nodes_for_attribution,
                &edges_before,
                &edges_after,
                &[src, dst],
                as_of,
                attribution_steps(),
                DEFAULT_TOP_K,
            )?
        } else {
            HashMap::new()
        };

        let mut batch = WriteBatch::default();
        self.stage_mutation(&mut batch, mutation, edge_value);
        self.stage_embeddings(&mut batch, &ctx)?;
        self.stage_commit_meta(&mut batch, &ctx, &meta, &attribution_by_node)?;

        // The whole point of building one shared batch above instead of
        // issuing separate writes: this single call either lands every entry
        // staged into it or none of them — the storage engine's own
        // guarantee for one batch, and the entirety of what Rule 5 requires.
        self.store.write(batch)?;

        Ok(ctx)
    }

    fn stage_mutation(
        &self,
        batch: &mut WriteBatch,
        mutation: GraphMutation,
        edge_value: &EdgeValue,
    ) {
        match mutation {
            GraphMutation::AddEdge {
                src,
                dst,
                edge_type,
                ts,
            } => {
                self.writer
                    .put_edge(batch, src, edge_type, dst, ts, edge_value);
            }
            GraphMutation::RemoveEdge {
                src,
                dst,
                edge_type,
                ts,
            } => {
                self.writer.remove_edge(batch, src, edge_type, dst, ts);
            }
        }
    }

    fn stage_embeddings(&self, batch: &mut WriteBatch, ctx: &MutationContext) -> Result<()> {
        let embeddings_cf = self.store.cf_handle(cf::CF_EMBEDDINGS)?;
        let ts = ctx.mutation.timestamp();
        for (node, embedding) in &ctx.embeddings_after {
            let key = encode_embedding_key(*node, ts);
            batch.put_cf(&embeddings_cf, &key, embedding.serialize());
        }
        Ok(())
    }

    /// Stage one `CommitMeta` record per node whose embedding this mutation
    /// updated, into the same batch `stage_mutation`/`stage_embeddings` wrote
    /// into — Rule 5 requires the whole commit to reach the store through a
    /// single write call, so provenance has to ride in this same batch or not
    /// be atomic with the change it explains.
    ///
    /// `meta` carries the parts common to every affected node (dispatch, caps,
    /// truncation); `attribution_by_node`, when non-empty, is per-node and is
    /// spliced into that node's own copy rather than being part of the shared
    /// `meta` value — most affected nodes were never attribution targets and
    /// must keep `attribution: None`, not silently inherit someone else's.
    fn stage_commit_meta(
        &self,
        batch: &mut WriteBatch,
        ctx: &MutationContext,
        meta: &CommitMeta,
        attribution_by_node: &HashMap<NodeId, Attribution>,
    ) -> Result<()> {
        let meta_cf = self.store.cf_handle(cf::CF_COMMIT_META)?;
        let ts = ctx.mutation.timestamp();
        for (node, _embedding) in &ctx.embeddings_after {
            let key = encode_commit_meta_key(*node, ts);
            let record = match attribution_by_node.get(node) {
                Some(attribution) => {
                    let mut with_attribution = meta.clone();
                    with_attribution.attribution = Some(attribution.clone());
                    with_attribution.serialize()
                }
                None => meta.serialize(),
            };
            batch.put_cf(&meta_cf, &key, record);
        }
        Ok(())
    }
}
