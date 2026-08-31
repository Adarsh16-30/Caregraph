//! `similar_care_pathways` (PRD Section 5.3, Contribution 5) — care-pathway
//! similarity search evaluated *as of* a historical timestamp, not only on
//! current state.
//!
//! # Reconciling the PRD's pseudocode with the real code
//!
//! Section 5.3 gives:
//!
//! ```text
//! pub async fn similar_care_pathways(node_id, as_of, top_k) -> Result<Vec<(NodeId, f32)>> {
//!     let embedding = embeddings::get_versioned(node_id, as_of)?; // no recompute
//!     let candidates = embeddings::scan_as_of(as_of)?;            // same timestamp
//!     Ok(rank_by_cosine_similarity(embedding, candidates, top_k))
//! }
//! ```
//!
//! This function is exactly that, against the real types: `get_versioned` is
//! [`TemporalIndex::embedding_as_of`] (already annotated in `temporal/index.rs`
//! as "the read `similar_care_pathways` is built on"), and `scan_as_of` is
//! [`TemporalIndex::all_embeddings_as_of`], added alongside it. Two
//! departures from the pseudocode's literal shape, both made explicit rather
//! than silently assumed:
//!
//! 1. **The query node is excluded from its own results.** The pseudocode's
//!    `candidates` includes every node with an embedding, the query node
//!    included — whose cosine similarity to itself is always exactly 1.0.
//!    Ranking that first is never the answer a "find similar pathways"
//!    caller wants, so it is filtered out here.
//! 2. **Candidates from a different model are excluded.** Two embeddings
//!    from different architectures (GraphSAGE vs. GAT) live in unrelated
//!    vector spaces — the same dimension does not mean the same geometry.
//!    Comparing across them would produce a number that looks like a
//!    similarity score without being one. Only candidates whose
//!    `model_id` matches the query embedding's own are ranked.
//!
//! Not addressed because the PRD does not address it either (see the module
//! this function is called from, `src/api/mod.rs`, for how the gRPC layer
//! surfaces it instead): what happens when the query node has no embedding
//! at `as_of` at all. That is not an error — it means the node either did
//! not exist yet or has never been through the mutation pipeline — so this
//! function returns `Ok(None)` rather than an error, and the caller decides
//! how to report that distinctly from "zero similar nodes found."

use crate::error::Result;
use crate::storage::KvStore;
use crate::temporal::TemporalIndex;
use crate::types::{Embedding, NodeId, Timestamp};

/// `Ok(None)` means the query node had no embedding at or before `as_of` —
/// see the module doc. `Ok(Some(matches))` is ranked descending by
/// similarity, `matches.len() <= top_k`.
pub fn similar_care_pathways<S: KvStore + ?Sized>(
    store: &S,
    node_id: NodeId,
    as_of: Timestamp,
    top_k: usize,
) -> Result<Option<Vec<(NodeId, f32)>>> {
    let index = TemporalIndex::new(store);

    let Some(query_embedding) = index.embedding_as_of(node_id, as_of)? else {
        return Ok(None);
    };

    let candidates = index.all_embeddings_as_of(as_of)?;

    Ok(Some(rank_by_cosine_similarity(
        node_id,
        &query_embedding,
        candidates,
        top_k,
    )))
}

fn rank_by_cosine_similarity(
    query_node: NodeId,
    query: &Embedding,
    candidates: Vec<(NodeId, Embedding)>,
    top_k: usize,
) -> Vec<(NodeId, f32)> {
    let mut scored: Vec<(NodeId, f32)> = candidates
        .into_iter()
        .filter(|(node, embedding)| *node != query_node && embedding.model_id == query.model_id)
        .map(|(node, embedding)| (node, cosine_similarity(&query.vector, &embedding.vector)))
        .collect();

    // Descending by similarity; NodeId as a deterministic tiebreaker so the
    // result order does not depend on the column family's own scan order —
    // the same reproducibility argument `traversal.rs` makes for sorting
    // neighbours before a fan-out cap applies.
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored.truncate(top_k);
    scored
}

/// `0.0` for a zero vector rather than `NaN` — a node with an all-zero
/// embedding (never observed in practice, but not ruled out by the type)
/// is reported as maximally dissimilar to everything, not as an error.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emb(model_id: &str, vector: Vec<f32>) -> Embedding {
        Embedding::new(
            vector,
            model_id,
            crate::types::ModelKind::GraphSAGE,
            crate::types::ComputationPath::Associative,
        )
    }

    #[test]
    fn identical_vectors_score_one() {
        let a = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &a) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn orthogonal_vectors_score_zero() {
        assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0])).abs() < 1e-6);
    }

    #[test]
    fn opposite_vectors_score_minus_one() {
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 1e-6);
    }

    #[test]
    fn a_zero_vector_scores_zero_not_nan() {
        let s = cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]);
        assert_eq!(s, 0.0);
    }

    #[test]
    fn ranking_excludes_the_query_node_itself() {
        let query = emb("m", vec![1.0, 0.0]);
        let candidates = vec![
            (NodeId(1), emb("m", vec![1.0, 0.0])), // identical to query, would rank #1 if not excluded
            (NodeId(2), emb("m", vec![0.9, 0.1])),
        ];
        let ranked = rank_by_cosine_similarity(NodeId(1), &query, candidates, 10);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].0, NodeId(2));
    }

    #[test]
    fn ranking_excludes_candidates_from_a_different_model() {
        let query = emb("graphsage_model", vec![1.0, 0.0]);
        let candidates = vec![
            (NodeId(2), emb("gat_model", vec![1.0, 0.0])), // identical vector, wrong model
            (NodeId(3), emb("graphsage_model", vec![0.5, 0.5])),
        ];
        let ranked = rank_by_cosine_similarity(NodeId(1), &query, candidates, 10);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].0, NodeId(3));
    }

    #[test]
    fn ranking_respects_top_k_and_descending_order() {
        let query = emb("m", vec![1.0, 0.0]);
        let candidates = vec![
            (NodeId(2), emb("m", vec![0.0, 1.0])),  // orthogonal, score 0
            (NodeId(3), emb("m", vec![0.9, 0.1])),  // close, score high
            (NodeId(4), emb("m", vec![-1.0, 0.0])), // opposite, score -1
        ];
        let ranked = rank_by_cosine_similarity(NodeId(1), &query, candidates, 2);
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].0, NodeId(3));
        assert_eq!(ranked[1].0, NodeId(2));
    }
}
