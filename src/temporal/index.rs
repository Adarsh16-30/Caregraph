//! Point-in-time reads over the versioned column families (PRD Phase 2).
//!
//! # What the key layout gives, and what it costs
//!
//! Edge keys are `[src_id | edge_type | timestamp_desc | dst_id]` (PRD 3.3,
//! provided). The timestamp sorts *before* `dst_id`, so within one adjacency
//! list the versions of **different** destination nodes are interleaved in time
//! rather than grouped per destination.
//!
//! That ordering is what makes a time-windowed scan cheap: "every change to
//! this patient's diagnoses between T1 and T2" is one contiguous key range, and
//! [`edge_versions_in_window`](TemporalIndex::edge_versions_in_window) reads it
//! with a single seek. Phase 2 requires exactly that, and Phase 4's
//! affected-subgraph resolution depends on it.
//!
//! The cost is that reconstructing a whole adjacency list at a timestamp cannot
//! be a seek per edge. [`edges_as_of`](TemporalIndex::edges_as_of) seeks once to
//! `as_of` and walks backward through time, keeping the **first** version it
//! sees for each `dst` — which, walking newest-first, is that edge's newest
//! version at or before `as_of`. It must walk the full adjacency history,
//! because there is no way to know a given `dst` has been seen without reaching
//! it. So the cost is O(versions in the adjacency list), not O(live edges).
//!
//! Single-node reads (`CF_NODES`, `CF_EMBEDDINGS`) have no such issue: their
//! keys are `[node_id | timestamp_desc]` with nothing after the timestamp, so
//! one seek lands exactly on the answer. That is the O(log n) retrieval of
//! Contribution 2, and it is the path the point-in-time embedding queries take.

use std::collections::HashSet;
use std::ops::ControlFlow;

use serde_json::Value;

use crate::error::{CareGraphError, Result};
use crate::storage::{cf, KvStore};
use crate::temporal::keys::{
    as_of_prefix, decode_edge_key, decode_node_key, edge_prefix, node_prefix, EDGE_KEY_LEN,
    NODE_KEY_LEN,
};
use crate::temporal::record::{EdgeValue, NodeValue};
use crate::types::{EdgeType, Embedding, NodeId, Timestamp};

/// A live edge, as it stood at some timestamp.
#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    pub src: NodeId,
    pub dst: NodeId,
    pub edge_type: EdgeType,
    /// When this version was written — the clinical event time, not ingest time.
    pub timestamp: Timestamp,
    pub properties: Value,
}

/// A live node, as it stood at some timestamp.
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub node_id: NodeId,
    pub node_type: String,
    pub timestamp: Timestamp,
    pub properties: Value,
}

/// One version of an edge, including tombstones.
///
/// Window scans return this rather than [`Edge`] because "what changed in this
/// window" has to include removals — a diagnosis being retracted is a change.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeVersion {
    pub src: NodeId,
    pub dst: NodeId,
    pub edge_type: EdgeType,
    pub timestamp: Timestamp,
    pub properties: Value,
    pub deleted: bool,
}

/// Read-side view over the temporal column families.
pub struct TemporalIndex<'a, S: KvStore + ?Sized> {
    store: &'a S,
}

impl<'a, S: KvStore + ?Sized> TemporalIndex<'a, S> {
    pub fn new(store: &'a S) -> Self {
        TemporalIndex { store }
    }

    // -----------------------------------------------------------------------
    // Single-key reads — one seek, O(log n)
    // -----------------------------------------------------------------------

    /// Node state at `as_of`, or `None` if the node did not exist then or was
    /// retracted at or before then.
    pub fn node_as_of(&self, node: NodeId, as_of: Timestamp) -> Result<Option<Node>> {
        let prefix = node_prefix(node);
        let seek = as_of_prefix(&prefix, as_of);

        let Some((key, value)) = self.store.seek(cf::CF_NODES, &prefix, &seek)? else {
            return Ok(None);
        };
        let (node_id, timestamp) = decode_node_key(&key).ok_or(CareGraphError::MalformedKey {
            cf: cf::CF_NODES,
            expected: NODE_KEY_LEN,
            found: key.len(),
        })?;

        let stored = NodeValue::decode(&value)?;
        if stored.deleted {
            return Ok(None);
        }
        Ok(Some(Node {
            node_id,
            node_type: stored.node_type,
            timestamp,
            properties: stored.properties,
        }))
    }

    /// The node's embedding as it stood at `as_of` — read directly from
    /// `CF_EMBEDDINGS`, never recomputed.
    ///
    /// This is the read that `similar_care_pathways` (PRD 5.3) is built on.
    pub fn embedding_as_of(&self, node: NodeId, as_of: Timestamp) -> Result<Option<Embedding>> {
        let prefix = node_prefix(node);
        let seek = as_of_prefix(&prefix, as_of);

        match self.store.seek(cf::CF_EMBEDDINGS, &prefix, &seek)? {
            Some((_, value)) => Ok(Some(Embedding::deserialize(&value)?)),
            None => Ok(None),
        }
    }

    /// Whether a node existed at `as_of`, without decoding its properties.
    pub fn node_exists_as_of(&self, node: NodeId, as_of: Timestamp) -> Result<bool> {
        Ok(self.node_as_of(node, as_of)?.is_some())
    }

    // -----------------------------------------------------------------------
    // Adjacency reconstruction — one seek, then a walk back through time
    // -----------------------------------------------------------------------

    /// Every live outgoing edge of `(src, edge_type)` at `as_of`.
    pub fn edges_as_of(
        &self,
        src: NodeId,
        edge_type: EdgeType,
        as_of: Timestamp,
    ) -> Result<Vec<Edge>> {
        self.adjacency_as_of(cf::CF_EDGES, src, edge_type, as_of, false, None)
    }

    /// Every live incoming edge of `(dst, edge_type)` at `as_of`.
    ///
    /// Reads `CF_REVERSE`, where the same edge is stored with src/dst swapped,
    /// so "which patients are on metformin?" is a prefix scan and not a full
    /// table scan.
    pub fn incoming_edges_as_of(
        &self,
        dst: NodeId,
        edge_type: EdgeType,
        as_of: Timestamp,
    ) -> Result<Vec<Edge>> {
        self.adjacency_as_of(cf::CF_REVERSE, dst, edge_type, as_of, true, None)
    }

    /// Like [`edges_as_of`](Self::edges_as_of), but stops the scan once `limit`
    /// live neighbours have been resolved.
    ///
    /// The scan walks backward through time, so the neighbours kept are the
    /// most recently updated ones. This is what makes a hop through a
    /// high-degree reference node cost O(limit) instead of O(degree): a
    /// condition referenced by 18,000 patients is never fully materialised just
    /// to discard all but a few hundred of its edges.
    pub fn edges_as_of_limited(
        &self,
        src: NodeId,
        edge_type: EdgeType,
        as_of: Timestamp,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        self.adjacency_as_of(cf::CF_EDGES, src, edge_type, as_of, false, Some(limit))
    }

    /// Incoming-direction counterpart of
    /// [`edges_as_of_limited`](Self::edges_as_of_limited).
    pub fn incoming_edges_as_of_limited(
        &self,
        dst: NodeId,
        edge_type: EdgeType,
        as_of: Timestamp,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        self.adjacency_as_of(cf::CF_REVERSE, dst, edge_type, as_of, true, Some(limit))
    }

    /// Every live outgoing edge of `src`, **across every edge type**, at
    /// `as_of`, capped at `limit` live neighbours total.
    ///
    /// One RocksDB scan instead of `EdgeType::ALL.len()` separate ones. Edge
    /// type sorts immediately after `src_id` in the key layout
    /// (`temporal/keys.rs`), so every edge type's data for one node is one
    /// contiguous byte range — scanning it as a unit is what turns
    /// [`AffectedSubgraphResolver`](crate::embedding::resolver::AffectedSubgraphResolver)'s
    /// per-node cost from 12 calls into 2 (see `embedding/resolver.rs`, "Why
    /// resolution issues two calls per node, not twelve").
    pub fn all_edges_as_of_limited(
        &self,
        src: NodeId,
        as_of: Timestamp,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        self.all_adjacency_as_of(cf::CF_EDGES, src, as_of, false, limit)
    }

    /// Incoming-direction counterpart of
    /// [`all_edges_as_of_limited`](Self::all_edges_as_of_limited).
    pub fn all_incoming_edges_as_of_limited(
        &self,
        dst: NodeId,
        as_of: Timestamp,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        self.all_adjacency_as_of(cf::CF_REVERSE, dst, as_of, true, limit)
    }

    /// Shared walk for [`all_edges_as_of_limited`] and
    /// [`all_incoming_edges_as_of_limited`].
    ///
    /// Unlike [`adjacency_as_of`](Self::adjacency_as_of), this cannot seek
    /// straight to the `as_of` frontier: that frontier sits at a different
    /// byte offset within each of the six edge-type sub-ranges, and there is
    /// only one seek to spend. So the scan starts at the very beginning of
    /// `anchor`'s key range — the globally newest version of its first edge
    /// type, which may postdate `as_of` — and filters every entry against
    /// `as_of` explicitly rather than relying on seek position to do it.
    /// Dedup keys on `(edge_type, other)` rather than `other` alone, because a
    /// single scan now mixes edge types where a plain `HashSet<NodeId>` could
    /// conflate two different relationships that happen to share a
    /// destination id.
    fn all_adjacency_as_of(
        &self,
        family: &'static str,
        anchor: NodeId,
        as_of: Timestamp,
        swap: bool,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        let prefix = node_prefix(anchor);

        let mut resolved: HashSet<(EdgeType, NodeId)> = HashSet::new();
        let mut live: Vec<Edge> = Vec::new();
        let mut failure: Option<CareGraphError> = None;

        // `prefix` here is `[node_id]` (8 bytes) — narrower than CF_EDGES /
        // CF_REVERSE's configured fixed-prefix extractor width of
        // `EDGE_PREFIX_LEN` (10 bytes, `[node_id | edge_type]`). `scan_from`'s
        // `prefix_same_as_start` read option can't establish a bound from a
        // key shorter than the extractor's own width and silently returns
        // nothing — verified the hard way, see `scan_from_narrow_prefix`'s
        // doc comment in `storage/kv.rs`.
        self.store
            .scan_from_narrow_prefix(family, &prefix, &prefix, &mut |key, value| {
                let Some((_, key_edge_type, timestamp, other)) = decode_edge_key(key) else {
                    failure = Some(CareGraphError::MalformedKey {
                        cf: family,
                        expected: EDGE_KEY_LEN,
                        found: key.len(),
                    });
                    return ControlFlow::Break(());
                };

                if timestamp > as_of {
                    return ControlFlow::Continue(()); // written after the instant we're reading
                }
                if !resolved.insert((key_edge_type, other)) {
                    return ControlFlow::Continue(()); // superseded version
                }

                match EdgeValue::decode(value) {
                    Ok(stored) => {
                        if !stored.deleted {
                            let (src, dst) = if swap {
                                (other, anchor)
                            } else {
                                (anchor, other)
                            };
                            live.push(Edge {
                                src,
                                dst,
                                edge_type: key_edge_type,
                                timestamp,
                                properties: stored.properties,
                            });
                            if live.len() >= limit {
                                return ControlFlow::Break(());
                            }
                        }
                        ControlFlow::Continue(())
                    }
                    Err(e) => {
                        failure = Some(e);
                        ControlFlow::Break(())
                    }
                }
            })?;

        if let Some(e) = failure {
            return Err(e);
        }
        Ok(live)
    }

    /// Shared walk for both directions.
    ///
    /// `swap` un-mirrors `CF_REVERSE` keys so the returned [`Edge`] always
    /// reads in the graph's natural direction regardless of which family it
    /// came from.
    ///
    /// `limit` stops the walk after that many *live* neighbours. Tombstoned
    /// versions still consume a step — they resolve a destination to "absent",
    /// which is an answer — but they do not count toward the limit, so a
    /// caller asking for N live neighbours gets N whenever N exist.
    fn adjacency_as_of(
        &self,
        family: &'static str,
        anchor: NodeId,
        edge_type: EdgeType,
        as_of: Timestamp,
        swap: bool,
        limit: Option<usize>,
    ) -> Result<Vec<Edge>> {
        let prefix = edge_prefix(anchor, edge_type);
        let seek = as_of_prefix(&prefix, as_of);

        // Walking forward from `seek` walks backward through time, so the first
        // version seen for a given `other` is its newest at or before `as_of`.
        // Any later sighting of the same `other` is an older version, superseded.
        let mut resolved: HashSet<NodeId> = HashSet::new();
        let mut live: Vec<Edge> = Vec::new();
        let mut failure: Option<CareGraphError> = None;

        self.store
            .scan_from(family, &prefix, &seek, &mut |key, value| {
                let Some((_, key_edge_type, timestamp, other)) = decode_edge_key(key) else {
                    failure = Some(CareGraphError::MalformedKey {
                        cf: family,
                        expected: EDGE_KEY_LEN,
                        found: key.len(),
                    });
                    return ControlFlow::Break(());
                };

                if !resolved.insert(other) {
                    return ControlFlow::Continue(()); // superseded version
                }

                match EdgeValue::decode(value) {
                    Ok(stored) => {
                        if !stored.deleted {
                            let (src, dst) = if swap {
                                (other, anchor)
                            } else {
                                (anchor, other)
                            };
                            live.push(Edge {
                                src,
                                dst,
                                edge_type: key_edge_type,
                                timestamp,
                                properties: stored.properties,
                            });
                            if limit.is_some_and(|l| live.len() >= l) {
                                return ControlFlow::Break(());
                            }
                        }
                        ControlFlow::Continue(())
                    }
                    Err(e) => {
                        failure = Some(e);
                        ControlFlow::Break(())
                    }
                }
            })?;

        if let Some(e) = failure {
            return Err(e);
        }

        // Scan order is newest-version-first, which is arbitrary with respect to
        // dst. Sort so results are deterministic across runs — the property
        // tests and the benchmark harness both compare them directly.
        live.sort_by_key(|e| if swap { e.src } else { e.dst });
        Ok(live)
    }

    /// The state of one specific edge at `as_of`.
    ///
    /// Note the cost: because `dst_id` sorts *after* the timestamp, this cannot
    /// seek straight to the edge and must walk the adjacency history until it
    /// finds `dst`. Callers reconstructing a whole neighbourhood should use
    /// [`edges_as_of`], which resolves every edge in the same single walk.
    pub fn edge_as_of(
        &self,
        src: NodeId,
        edge_type: EdgeType,
        dst: NodeId,
        as_of: Timestamp,
    ) -> Result<Option<Edge>> {
        let prefix = edge_prefix(src, edge_type);
        let seek = as_of_prefix(&prefix, as_of);

        let mut found: Option<Edge> = None;
        let mut failure: Option<CareGraphError> = None;

        self.store
            .scan_from(cf::CF_EDGES, &prefix, &seek, &mut |key, value| {
                let Some((_, key_edge_type, timestamp, other)) = decode_edge_key(key) else {
                    failure = Some(CareGraphError::MalformedKey {
                        cf: cf::CF_EDGES,
                        expected: EDGE_KEY_LEN,
                        found: key.len(),
                    });
                    return ControlFlow::Break(());
                };
                if other != dst {
                    return ControlFlow::Continue(());
                }

                // First sighting of `dst` walking backwards — this is its state
                // at `as_of`, tombstone or not. Stop either way.
                match EdgeValue::decode(value) {
                    Ok(stored) => {
                        if !stored.deleted {
                            found = Some(Edge {
                                src,
                                dst,
                                edge_type: key_edge_type,
                                timestamp,
                                properties: stored.properties,
                            });
                        }
                    }
                    Err(e) => failure = Some(e),
                }
                ControlFlow::Break(())
            })?;

        if let Some(e) = failure {
            return Err(e);
        }
        Ok(found)
    }

    // -----------------------------------------------------------------------
    // Time-windowed range scans
    // -----------------------------------------------------------------------

    /// Every edge version written in `[from, to]`, newest first — tombstones
    /// included.
    ///
    /// This is the query the key layout is optimised for: the window is one
    /// contiguous key range, so it costs a single seek plus a walk proportional
    /// to the number of versions actually in the window.
    pub fn edge_versions_in_window(
        &self,
        src: NodeId,
        edge_type: EdgeType,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<EdgeVersion>> {
        if from > to {
            return Ok(Vec::new());
        }

        let prefix = edge_prefix(src, edge_type);
        let seek = as_of_prefix(&prefix, to);

        let mut out = Vec::new();
        let mut failure: Option<CareGraphError> = None;

        self.store
            .scan_from(cf::CF_EDGES, &prefix, &seek, &mut |key, value| {
                let Some((_, key_edge_type, timestamp, dst)) = decode_edge_key(key) else {
                    failure = Some(CareGraphError::MalformedKey {
                        cf: cf::CF_EDGES,
                        expected: EDGE_KEY_LEN,
                        found: key.len(),
                    });
                    return ControlFlow::Break(());
                };

                // Walking forward means walking back in time; once we fall out
                // of the window everything after is older still.
                if timestamp < from {
                    return ControlFlow::Break(());
                }

                match EdgeValue::decode(value) {
                    Ok(stored) => {
                        out.push(EdgeVersion {
                            src,
                            dst,
                            edge_type: key_edge_type,
                            timestamp,
                            properties: stored.properties,
                            deleted: stored.deleted,
                        });
                        ControlFlow::Continue(())
                    }
                    Err(e) => {
                        failure = Some(e);
                        ControlFlow::Break(())
                    }
                }
            })?;

        if let Some(e) = failure {
            return Err(e);
        }
        Ok(out)
    }

    /// Every version of a node in `[from, to]`, newest first.
    pub fn node_versions_in_window(
        &self,
        node: NodeId,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<(Timestamp, NodeValue)>> {
        if from > to {
            return Ok(Vec::new());
        }

        let prefix = node_prefix(node);
        let seek = as_of_prefix(&prefix, to);

        let mut out = Vec::new();
        let mut failure: Option<CareGraphError> = None;

        self.store
            .scan_from(cf::CF_NODES, &prefix, &seek, &mut |key, value| {
                let Some((_, timestamp)) = decode_node_key(key) else {
                    failure = Some(CareGraphError::MalformedKey {
                        cf: cf::CF_NODES,
                        expected: NODE_KEY_LEN,
                        found: key.len(),
                    });
                    return ControlFlow::Break(());
                };
                if timestamp < from {
                    return ControlFlow::Break(());
                }
                match NodeValue::decode(value) {
                    Ok(stored) => {
                        out.push((timestamp, stored));
                        ControlFlow::Continue(())
                    }
                    Err(e) => {
                        failure = Some(e);
                        ControlFlow::Break(())
                    }
                }
            })?;

        if let Some(e) = failure {
            return Err(e);
        }
        Ok(out)
    }
}
