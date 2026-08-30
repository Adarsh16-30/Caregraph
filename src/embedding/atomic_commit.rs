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

use rocksdb::WriteBatch;

use crate::embedding::associative::aggregate_over_subgraph;
use crate::embedding::gat_incremental::gat_incremental_update;
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::resolver::{patch_subgraph_for_mutation, AffectedSubgraphResolver};
use crate::embedding::state::{GraphMutation, MutationContext};
use crate::error::Result;
use crate::storage::{cf, KvStore, RocksKv};
use crate::temporal::keys::encode_embedding_key;
use crate::temporal::record::EdgeValue;
use crate::temporal::{TemporalIndex, TemporalWriter};
use crate::types::ModelKind;

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
    pub fn commit(
        &self,
        mutation: GraphMutation,
        edge_value: &EdgeValue,
        active_model: ModelKind,
        model: &EmbeddingModel,
        fanout_cap: usize,
        max_expanded_nodes: usize,
    ) -> Result<MutationContext> {
        let mut ctx = MutationContext::new(mutation, active_model);
        let as_of = mutation.timestamp();

        // Reads whatever is durably committed right now. Nothing below has
        // staged this mutation into the database yet, so that is exactly the
        // graph as it stood the instant before it.
        let resolver = AffectedSubgraphResolver::new(self.store, fanout_cap, max_expanded_nodes);
        let mut subgraph = resolver.resolve(mutation)?;
        patch_subgraph_for_mutation(&mut subgraph, &self.index, mutation)?;

        // Resolution and patching are identical either way — only which
        // aggregation ran, and therefore which ComputationPath tag the
        // result carries, depends on the active model (see
        // gat_incremental.rs's module doc for why GAT still shares this
        // exact mechanism despite not being associative).
        match active_model {
            ModelKind::GraphSAGE | ModelKind::GCN => {
                aggregate_over_subgraph(&mut ctx, self.store, model, subgraph, as_of)?;
            }
            ModelKind::GAT => {
                gat_incremental_update(&mut ctx, self.store, model, subgraph, as_of)?;
            }
        }

        let mut batch = WriteBatch::default();
        self.stage_mutation(&mut batch, mutation, edge_value);
        self.stage_embeddings(&mut batch, &ctx)?;

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
}
