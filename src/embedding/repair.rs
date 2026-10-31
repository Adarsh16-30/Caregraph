//! Failure policy and durable repair markers for the embedding step.
//!
//! When the forward pass cannot produce fresh embeddings for a mutation, two
//! different things can be true, and they get different treatment:
//!
//! * **The embedding service is unavailable** (workers restarting, circuit
//!   breaker open, deadline exceeded). The mutation is *rejected* with a
//!   retryable `UNAVAILABLE`/`DEADLINE_EXCEEDED` and nothing is written —
//!   committing an edge whose affected embeddings cannot be refreshed silently
//!   breaks the system's core promise (graph and embeddings move together).
//!   Operators who prefer write availability over embedding freshness can set
//!   `CAREGRAPH_COMMIT_WITHOUT_EMBEDDING=true`; the mutation then commits and
//!   takes the counted fallback path below.
//! * **The model itself failed** on this input. The structural change commits,
//!   the fallback counter (`incremental_fallback_total`, Rule 7) increments, the
//!   error is logged, and — new — every node left with a stale embedding gets a
//!   durable marker in `CF_REPAIR`, written in the *same* batch as the change.
//!
//! A marker is `[node_id | ts]`: "this node's embedding is stale as of `ts`".
//! It is superseded, never deleted (the timeline is append-only): a node is
//! pending repair while its latest embedding is older than its latest marker.

use serde::{Deserialize, Serialize};

use crate::embedding::state::MutationContext;
use crate::error::{CareGraphError, Result};
use crate::storage::{cf, KvStore, RocksKv};
use crate::temporal::keys::{decode_node_key, encode_repair_key, node_prefix};
use crate::types::{NodeId, Timestamp};

pub const REPAIR_SCHEMA: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairRecord {
    pub schema: u32,
    pub reason: String,
    pub model_id: String,
}

/// Should this forward-pass failure degrade to a stale-embedding commit, or
/// propagate and reject the mutation?
pub fn should_degrade(err: &CareGraphError) -> bool {
    match err {
        CareGraphError::Unavailable(_) | CareGraphError::Timeout(_) => {
            std::env::var("CAREGRAPH_COMMIT_WITHOUT_EMBEDDING")
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false)
        }
        _ => true,
    }
}

/// Why a deferred (not failed) node is marked; see `deferral.rs`.
pub const DEFERRED_REASON: &str = "deferred: hub update left for the repair worker";

/// Stage one `CF_REPAIR` marker per stale or deferred node into the commit's
/// batch. A node that is both gets the stale (failure) reason.
pub fn stage_repairs(
    batch: &mut rocksdb::WriteBatch,
    store: &RocksKv,
    ctx: &MutationContext,
    model_id: &str,
) -> Result<()> {
    if ctx.stale_nodes.is_empty() && ctx.deferred_nodes.is_empty() {
        return Ok(());
    }
    let repair_cf = store.cf_handle(cf::CF_REPAIR)?;
    let ts = ctx.mutation.timestamp();
    let record = |reason: &str| {
        serde_json::to_vec(&RepairRecord {
            schema: REPAIR_SCHEMA,
            reason: reason.to_string(),
            model_id: model_id.to_string(),
        })
    };
    let stale_value = record("forward pass failed; embedding not refreshed")?;
    for node in &ctx.stale_nodes {
        batch.put_cf(&repair_cf, encode_repair_key(*node, ts), &stale_value);
    }
    if !ctx.deferred_nodes.is_empty() {
        let deferred_value = record(DEFERRED_REASON)?;
        let stale: std::collections::HashSet<NodeId> = ctx.stale_nodes.iter().copied().collect();
        for node in ctx.deferred_nodes.iter().filter(|n| !stale.contains(n)) {
            batch.put_cf(&repair_cf, encode_repair_key(*node, ts), &deferred_value);
        }
    }
    Ok(())
}

/// Timestamp of `node`'s newest stored embedding, if it has one. Keys sort
/// newest first, so the first key under the node's prefix is the answer.
pub fn latest_embedding_ts(store: &RocksKv, node: NodeId) -> Result<Option<Timestamp>> {
    let prefix = node_prefix(node);
    let mut latest = None;
    store.scan_from(cf::CF_EMBEDDINGS, &prefix, &prefix, &mut |key, _| {
        latest = decode_node_key(key).map(|(_, ts)| ts);
        std::ops::ControlFlow::Break(())
    })?;
    Ok(latest)
}

/// A node whose newest embedding is older than its newest repair marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRepair {
    pub node: NodeId,
    /// The mutation timestamp of the newest marker.
    pub marked_at: Timestamp,
    /// Timestamp of the node's newest embedding, if it has one.
    pub latest_embedding: Option<Timestamp>,
    /// The model named by the newest marker (empty if the marker cannot be read),
    /// so a repair uses the model whose embedding went stale.
    pub model_id: String,
}

/// Every node still waiting for a fresh embedding, in node order. A marker is
/// superseded, not deleted: a later successful embedding of the node clears it
/// by being newer.
pub fn pending_repairs(store: &RocksKv) -> Result<Vec<PendingRepair>> {
    let mut newest_marker: std::collections::BTreeMap<u64, (Timestamp, String)> =
        Default::default();
    store.scan_from_narrow_prefix(cf::CF_REPAIR, &[], &[], &mut |key, value| {
        if let Some((node, ts)) = decode_node_key(key) {
            let model_id = || {
                serde_json::from_slice::<RepairRecord>(value)
                    .map(|r| r.model_id)
                    .unwrap_or_default()
            };
            match newest_marker.entry(node.as_u64()) {
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert((ts, model_id()));
                }
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    if ts > o.get().0 {
                        o.insert((ts, model_id()));
                    }
                }
            }
        }
        std::ops::ControlFlow::Continue(())
    })?;

    let mut pending = Vec::new();
    for (node, (marked_at, model_id)) in newest_marker {
        let node = NodeId(node);
        let latest = latest_embedding_ts(store, node)?;
        if latest.map_or(true, |e| e < marked_at) {
            pending.push(PendingRepair {
                node,
                marked_at,
                latest_embedding: latest,
                model_id,
            });
        }
    }
    Ok(pending)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn availability_failures_reject_by_default_but_model_errors_degrade() {
        assert!(!should_degrade(&CareGraphError::Unavailable("x".into())));
        assert!(!should_degrade(&CareGraphError::Timeout("x".into())));
        assert!(should_degrade(&CareGraphError::Io(std::io::Error::other(
            "model said no"
        ))));
    }
}
