//! Per-mutation commit provenance (Phase 9).
//!
//! [`CommitMeta`] is the answer to "what did the model know, and when, and
//! *why did it change*" — the regulator-facing question the PRD's five original
//! claims never had to answer. It is committed into `CF_COMMIT_META` in the
//! same `WriteBatch` as the structural mutation and the embedding it explains
//! (`atomic_commit.rs`), so it is exactly as durable as the embedding itself.
//!
//! Every field is `#[serde(default)]`. [`crate::types::Embedding`] has none,
//! which is precisely why adding a required field to it would silently break
//! decoding of every embedding stored before that change — this record carries
//! an explicit `schema` version instead of repeating that mistake.

use serde::{Deserialize, Serialize};

use crate::embedding::state::ResolutionTruncation;
use crate::types::ComputationPath;

/// The current [`CommitMeta`] schema version. Bump when a field's meaning
/// changes in a way that would make an old record misleading to reinterpret
/// under the new logic — not for a purely additive field, which `#[serde(default)]`
/// already handles.
pub const COMMIT_META_SCHEMA: u32 = 1;

/// Which aggregation path ran for this mutation, and why (Feature 4).
///
/// Distinct from [`ComputationPath`], which is the persisted, historically
/// stable discriminant stored on the embedding itself. `DispatchDecision`
/// carries the *reasoning* behind that discriminant — the manifest that was
/// consulted and the checkpoint self-report it was cross-checked against —
/// which is new with Phase 9 and has no backward-compatibility constraint.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DispatchDecision {
    /// Resulting persisted tag (mirrors the embedding's own `computation_path`).
    #[serde(default)]
    pub computation_path: Option<ComputationPath>,
    /// `dataset_manifest.json`'s own `is_associative` flag for this model.
    #[serde(default)]
    pub manifest_is_associative: bool,
    /// Architecture name the manifest declares (e.g. "GraphSAGE", "GAT").
    #[serde(default)]
    pub manifest_architecture: String,
    /// Architecture name self-reported by `model.pt` at spawn time, via the
    /// extended `{"ready": true, ...}` handshake. `None` if the deployed model
    /// predates the handshake extension and never reported one.
    #[serde(default)]
    pub checkpoint_architecture: Option<String>,
}

/// The receptive-field caps actually in force for this mutation (Feature 2).
///
/// Recorded even when the cap controller is pinned to a fixed rung, so a past
/// commit's degree of truncation is always reconstructable without needing to
/// know what today's controller state happens to be.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveCaps {
    pub fanout_cap: usize,
    pub max_expanded_nodes: usize,
    /// Index into the controller's cap ladder this pair came from. `None` for
    /// a caller-pinned pair (benchmarks, correctness tests, fault injection).
    #[serde(default)]
    pub rung: Option<usize>,
}

/// One edge's share of an attributed embedding change (Feature 1).
///
/// `(src, dst)` is the canonicalised undirected pair
/// (`src.as_u64() <= dst.as_u64()`), matching [`crate::embedding::resolver::ResolvedSubgraph::edges`].
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgeAttribution {
    pub src: u64,
    pub dst: u64,
    pub value: f32,
}

/// Integrated-gradients attribution for one affected node's embedding change.
///
/// Every quantity a completeness check needs is stored alongside the ranked
/// edges, so this record is independently auditable without re-running
/// anything: `sum_attr(top) + rest_sum + baseline_delta` should reconstruct
/// `delta_norm` up to `completeness_residual`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Attribution {
    /// Always `"integrated_gradients"` today; named explicitly so a future
    /// method never has to be inferred from a record's shape.
    pub method: String,
    pub steps: u32,
    /// Riemann rule used for the IG quadrature (e.g. `"midpoint"`).
    pub rule: String,
    /// Precise description of what mask value 0 means for this model — for
    /// GAT this is *not* "the empty graph" (self-loops are pinned to mask 1
    /// post-softmax), which is exactly why `baseline_delta` exists below.
    pub baseline: String,
    /// `"unit(z_after - z_before)"` — the direction attribution is projected onto.
    pub direction: String,
    /// SHA-256 of the exact model checkpoint used, so this record survives a
    /// later model redeployment as self-describing evidence.
    pub model_sha256: String,
    /// `||z_after - z_before||` for the attributed node.
    pub delta_norm: f32,
    /// `f_after(0) - f_before(0)` — zero for an associative model; nonzero for
    /// GAT, where pinned self-loop mass makes the zero-mask baseline itself
    /// shift between the pre- and post-mutation graphs.
    pub baseline_delta: f32,
    /// Sum of every edge's attribution actually computed (`top` plus every
    /// edge folded into `rest_sum`) — i.e. `sum(top.value) + rest_sum`.
    pub sum_attr: f32,
    /// `delta_norm - (sum_attr + baseline_delta)`, the quadrature error.
    pub completeness_residual: f32,
    /// Total edges considered (union of pre- and post-mutation local subgraph).
    pub edges_total: usize,
    /// Top edges by `|value|`, descending, capped at the record's `top_k`.
    pub top: Vec<EdgeAttribution>,
    /// Sum of `value` over every edge not in `top`.
    pub rest_sum: f32,
    /// Count of edges not in `top`.
    pub rest_count: usize,
}

/// Per-mutation commit provenance, persisted in `CF_COMMIT_META` alongside the
/// embedding it explains.
///
/// Deliberately does not derive `Default`: a derived `Default` would give
/// `schema: 0`, silently disagreeing with [`COMMIT_META_SCHEMA`]. Use [`CommitMeta::new`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitMeta {
    #[serde(default = "default_schema")]
    pub schema: u32,
    #[serde(default)]
    pub model_id: String,
    #[serde(default)]
    pub dispatch: DispatchDecision,
    #[serde(default)]
    pub caps: EffectiveCaps,
    #[serde(default)]
    pub truncation: ResolutionTruncation,
    /// `None` when attribution was not requested for this mutation (the
    /// per-request `explain` flag was unset) — never a zero-valued placeholder.
    #[serde(default)]
    pub attribution: Option<Attribution>,
}

fn default_schema() -> u32 {
    COMMIT_META_SCHEMA
}

impl CommitMeta {
    pub fn new(model_id: impl Into<String>) -> Self {
        CommitMeta {
            schema: COMMIT_META_SCHEMA,
            model_id: model_id.into(),
            dispatch: DispatchDecision::default(),
            caps: EffectiveCaps::default(),
            truncation: ResolutionTruncation::default(),
            attribution: None,
        }
    }

    /// Value encoding for `CF_COMMIT_META`, mirroring [`crate::types::Embedding::serialize`].
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("CommitMeta is infallibly serializable")
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_with_no_attribution_round_trips_as_none() {
        let meta = CommitMeta::new("diabetes130_graphsage");
        let bytes = meta.serialize();
        let decoded = CommitMeta::deserialize(&bytes).expect("decode");
        assert_eq!(decoded.attribution, None);
        assert_eq!(decoded.schema, COMMIT_META_SCHEMA);
    }

    #[test]
    fn a_bare_json_object_decodes_via_defaults_like_a_pre_phase_9_gap() {
        // Simulates decoding a record from a future, differently-shaped writer,
        // or a hand-constructed minimal value — every field must default rather
        // than fail to decode.
        let decoded = CommitMeta::deserialize(b"{}").expect("decode");
        assert_eq!(decoded.schema, COMMIT_META_SCHEMA);
        assert_eq!(decoded.model_id, "");
        assert_eq!(decoded.attribution, None);
    }
}
