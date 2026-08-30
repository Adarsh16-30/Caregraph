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
use crate::embedding::model_bridge::EmbeddingModel;
use crate::embedding::resolver::{AffectedSubgraphResolver, ResolvedSubgraph};
use crate::embedding::state::{GraphMutation, MutationContext};
use crate::error::Result;
use crate::storage::{cf, KvStore, RocksKv};
use crate::temporal::keys::encode_embedding_key;
use crate::temporal::record::EdgeValue;
use crate::temporal::{TemporalIndex, TemporalWriter};
use crate::types::{EdgeType, ModelKind};

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
        debug_assert!(
            active_model.is_associative(),
            "AtomicCommitter::commit is the GraphSAGE/GCN path; a GAT-routed \
             mutation needs gat_incremental_update, not yet implemented"
        );

        let mut ctx = MutationContext::new(mutation, active_model);
        let as_of = mutation.timestamp();

        // Reads whatever is durably committed right now. Nothing below has
        // staged this mutation into the database yet, so that is exactly the
        // graph as it stood the instant before it.
        let resolver = AffectedSubgraphResolver::new(self.store, fanout_cap, max_expanded_nodes);
        let mut subgraph = resolver.resolve(mutation)?;
        self.patch_subgraph_for_mutation(&mut subgraph, mutation)?;

        aggregate_over_subgraph(&mut ctx, self.store, model, subgraph, as_of)?;

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

    /// Patch the one edge this mutation changes directly into a subgraph
    /// resolved from pre-mutation state — see the module doc.
    fn patch_subgraph_for_mutation(
        &self,
        subgraph: &mut ResolvedSubgraph,
        mutation: GraphMutation,
    ) -> Result<()> {
        let (src, dst) = mutation.endpoints();
        let pair = if src.as_u64() <= dst.as_u64() {
            (src, dst)
        } else {
            (dst, src)
        };

        match mutation {
            GraphMutation::AddEdge { .. } => {
                // src and dst are always present in `subgraph.nodes` — resolve()
                // seeds `affected` with both endpoints, and `nodes` starts from
                // `affected`. Only the edge between them is missing, because
                // resolve() read the graph before this mutation landed. The
                // model's own topology is edge-type-agnostic
                // (`build_model_input` symmetrises without regard to type — see
                // resolver.rs), so adding the pair is correct regardless of
                // which of the six edge types this mutation is.
                if !subgraph.edges.contains(&pair) {
                    subgraph.edges.push(pair);
                }
            }
            GraphMutation::RemoveEdge { edge_type, .. } => {
                // `pair` is already in `subgraph.edges` if resolve() discovered
                // src and dst as each other's neighbours — the edge being
                // removed was still live in the state resolve() read. Strip it
                // only if no OTHER edge type still connects the same two node
                // ids: two nodes related two different ways stay connected in
                // the model's topology after removing just one relationship,
                // and that case is cheap enough to check for that assuming it
                // away is not worth the risk.
                let ts = mutation.timestamp();
                let mut still_connected = false;
                for &other_type in EdgeType::ALL.iter() {
                    if other_type == edge_type {
                        continue;
                    }
                    if self.index.edge_as_of(src, other_type, dst, ts)?.is_some() {
                        still_connected = true;
                        break;
                    }
                }
                if !still_connected {
                    subgraph.edges.retain(|&e| e != pair);
                }
            }
        }
        Ok(())
    }
}
