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
use crate::storage::{cf, RocksKv};
use crate::temporal::keys::encode_repair_key;

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
