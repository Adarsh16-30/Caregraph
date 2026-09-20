//! `similarity_delta` (Phase 9, Feature 3) — point-in-time similarity *diff*,
//! not a second single-timestamp lookup.
//!
//! # Why this is a different query shape from Contribution 5
//!
//! [`crate::api::similarity::similar_care_pathways`] answers "who is close to
//! this care pathway right now (or as of one chosen instant)." This answers a
//! materially different question: "whose similarity to this care pathway
//! *changed*, and by how much, between two chosen instants" — a delta over
//! two versioned snapshots of the same relationship, not a ranking of one
//! snapshot. Nothing in the PRD's own Contribution 5 or its prior-art
//! comparison (`docs/novelty_analysis.md`) computes this; a vector index
//! answering "most similar now" has no notion of "now" changing under it at
//! all, since it holds only current state.
//!
//! # Mechanism — entirely reused primitives, no new storage
//!
//! Built from exactly the same versioned reads
//! [`similar_care_pathways`](crate::api::similarity::similar_care_pathways)
//! already uses, called twice — once at `from`, once at `to` — plus the same
//! [`cosine_similarity`](crate::api::similarity::cosine_similarity) scoring
//! function, promoted to `pub(crate)` rather than duplicated. No new column
//! family, no new index: the "diff" is computed in memory over two point-in-time
//! scans this engine already had a single-seek primitive for.
//!
//! # One-sided candidates are surfaced, never silently dropped
//!
//! A node may have an embedding at `to` but not at `from` (mutated into
//! existence in between), or vice versa (retracted, or simply not yet
//! reachable by the resolver at `from`). Such a candidate's delta is not
//! merely small — it is undefined, because one side of the subtraction does
//! not exist. This module reports it with `present_at_from`/`present_at_to`
//! set accordingly and a `delta` of exactly `0.0`, rather than omitting the
//! candidate or fabricating a comparison against a value that was never
//! computed. `min_abs_delta` filtering only ever applies to a candidate
//! present at *both* timestamps — the same honesty convention
//! `RelatedEntityMsg.node`'s absence follows for a dangling edge target.

use std::collections::{HashMap, HashSet};

use crate::api::similarity::cosine_similarity;
use crate::error::Result;
use crate::storage::KvStore;
use crate::temporal::TemporalIndex;
use crate::types::{Embedding, NodeId, Timestamp};

/// One candidate's similarity at each timestamp and the change between them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeltaMatch {
    pub node_id: NodeId,
    /// `cos(query@from, candidate@from)`. Only meaningful when `present_at_from`.
    pub similarity_from: f32,
    /// `cos(query@to, candidate@to)`. Only meaningful when `present_at_to`.
    pub similarity_to: f32,
    /// `similarity_to - similarity_from` when both sides are present;
    /// exactly `0.0`, by convention, when only one side is.
    pub delta: f32,
    pub present_at_from: bool,
    pub present_at_to: bool,
}

/// Result of [`similarity_delta`]. The two `query_missing_at_*` flags are
/// independent (unlike `Option`, both can be inspected even though only one
/// or neither may be true) because a caller reporting "why is this empty"
/// needs to know *which* endpoint the query node was missing at.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SimilarityDeltaResult {
    pub query_missing_at_from: bool,
    pub query_missing_at_to: bool,
    /// Ranked descending by `|delta|`, `NodeId` ascending as a tiebreaker —
    /// mirrors `rank_by_cosine_similarity`'s own reproducibility convention.
    /// Empty whenever either `query_missing_at_*` flag is set.
    pub matches: Vec<DeltaMatch>,
}

/// Compute the similarity delta for `node_id` between `from` and `to`.
///
/// `min_abs_delta` filters out candidates present at *both* timestamps whose
/// `|delta|` falls below it; a candidate present at only one timestamp is
/// never filtered by it (its delta is undefined, not small — see the module
/// doc). `top_k` truncates the final ranked list, applied after filtering.
pub fn similarity_delta<S: KvStore + ?Sized>(
    store: &S,
    node_id: NodeId,
    from: Timestamp,
    to: Timestamp,
    min_abs_delta: f32,
    top_k: usize,
) -> Result<SimilarityDeltaResult> {
    let index = TemporalIndex::new(store);

    let query_from = index.embedding_as_of(node_id, from)?;
    let query_to = index.embedding_as_of(node_id, to)?;

    let (Some(query_from), Some(query_to)) = (&query_from, &query_to) else {
        return Ok(SimilarityDeltaResult {
            query_missing_at_from: query_from.is_none(),
            query_missing_at_to: query_to.is_none(),
            matches: Vec::new(),
        });
    };

    let map_at =
        |candidates: Vec<(NodeId, Embedding)>, query: &Embedding| -> HashMap<NodeId, Embedding> {
            candidates
                .into_iter()
                .filter(|(n, e)| *n != node_id && e.model_id == query.model_id)
                .collect()
        };
    let map_from = map_at(index.all_embeddings_as_of(from)?, query_from);
    let map_to = map_at(index.all_embeddings_as_of(to)?, query_to);

    let mut candidate_ids: HashSet<NodeId> = HashSet::with_capacity(map_from.len() + map_to.len());
    candidate_ids.extend(map_from.keys().copied());
    candidate_ids.extend(map_to.keys().copied());

    let mut matches: Vec<DeltaMatch> = candidate_ids
        .into_iter()
        .filter_map(|node_id| {
            let from_embedding = map_from.get(&node_id);
            let to_embedding = map_to.get(&node_id);
            let similarity_from =
                from_embedding.map(|e| cosine_similarity(&query_from.vector, &e.vector));
            let similarity_to =
                to_embedding.map(|e| cosine_similarity(&query_to.vector, &e.vector));

            let (delta, present_both) = match (similarity_from, similarity_to) {
                (Some(a), Some(b)) => (b - a, true),
                _ => (0.0, false),
            };
            if present_both && delta.abs() < min_abs_delta {
                return None;
            }

            Some(DeltaMatch {
                node_id,
                similarity_from: similarity_from.unwrap_or(0.0),
                similarity_to: similarity_to.unwrap_or(0.0),
                delta,
                present_at_from: from_embedding.is_some(),
                present_at_to: to_embedding.is_some(),
            })
        })
        .collect();

    matches.sort_by(|a, b| {
        b.delta
            .abs()
            .partial_cmp(&a.delta.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    matches.truncate(top_k);

    Ok(SimilarityDeltaResult {
        query_missing_at_from: false,
        query_missing_at_to: false,
        matches,
    })
}

// `similarity_delta`'s ranking and one-sided-candidate handling are exercised
// end to end against a real RocksDB fixture in
// tests/integration/api_endpoint_test.rs (Rule 2's response-variation proof:
// `(t1, t1)` yields no deltas, `(t1, t2)` yields real ones). Rule 1 forbids a
// second `KvStore` implementation anywhere in `src/` — including inside a
// `#[cfg(test)]` module — so this function is not unit-tested against a fake
// store; `cosine_similarity` and the ranking-comparator logic it reuses are
// already covered by `similarity.rs`'s own in-file tests.
