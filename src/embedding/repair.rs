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

/// Stage one `CF_REPAIR` marker per stale node into the commit's batch.
pub fn stage_repairs(
    batch: &mut rocksdb::WriteBatch,
    store: &RocksKv,
    ctx: &MutationContext,
    model_id: &str,
) -> Result<()> {
    if ctx.stale_nodes.is_empty() {
        return Ok(());
    }
    let repair_cf = store.cf_handle(cf::CF_REPAIR)?;
    let ts = ctx.mutation.timestamp();
    let value = serde_json::to_vec(&RepairRecord {
        schema: REPAIR_SCHEMA,
        reason: "forward pass failed; embedding not refreshed".to_string(),
        model_id: model_id.to_string(),
    })?;
    for node in &ctx.stale_nodes {
        batch.put_cf(&repair_cf, encode_repair_key(*node, ts), &value);
    }
    Ok(())
}

/// A node whose newest embedding is older than its newest repair marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRepair {
    pub node: NodeId,
    /// The mutation timestamp of the newest marker.
    pub marked_at: Timestamp,
    /// Timestamp of the node's newest embedding, if it has one.
    pub latest_embedding: Option<Timestamp>,
}

/// Every node still waiting for a fresh embedding, in node order. A marker is
/// superseded, not deleted: a later successful embedding of the node clears it
/// by being newer.
pub fn pending_repairs(store: &RocksKv) -> Result<Vec<PendingRepair>> {
    let mut newest_marker: std::collections::BTreeMap<u64, Timestamp> = Default::default();
    store.scan_from_narrow_prefix(cf::CF_REPAIR, &[], &[], &mut |key, _| {
        if let Some((node, ts)) = decode_node_key(key) {
            let slot = newest_marker.entry(node.as_u64()).or_insert(ts);
            if ts > *slot {
                *slot = ts;
            }
        }
        std::ops::ControlFlow::Continue(())
    })?;

    let mut pending = Vec::new();
    for (node, marked_at) in newest_marker {
        let node = NodeId(node);
        let prefix = node_prefix(node);
        let mut latest = None;
        store.scan_from(cf::CF_EMBEDDINGS, &prefix, &prefix, &mut |key, _| {
            latest = decode_node_key(key).map(|(_, ts)| ts);
            // Keys sort newest first, so the first one is the newest.
            std::ops::ControlFlow::Break(())
        })?;
        if latest.map_or(true, |e| e < marked_at) {
            pending.push(PendingRepair {
                node,
                marked_at,
                latest_embedding: latest,
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
