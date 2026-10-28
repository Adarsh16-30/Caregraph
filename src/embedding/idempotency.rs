//! Request-id idempotency for mutations.
//!
//! A client that loses a response (timeout, dropped connection, a replicated
//! commit whose acknowledgement never arrived) cannot know whether its
//! mutation landed. With a `request_id`, retrying is safe: the outcome of the
//! first execution is stored in `CF_META` *in the same batch as the commit*, and
//! a retry returns that stored outcome instead of applying the mutation again.
//!
//! Records expire (`TTL`) so the keyspace does not grow forever; an expired
//! record is treated as "never seen". A concurrent duplicate is refused by the
//! sequencer's in-flight set before it gets here.

use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::storage::{cf, KvStore, RocksKv};
use crate::types::Timestamp;

pub const TTL_US: u64 = 24 * 60 * 60 * 1_000_000;
const PREFIX: &[u8] = b"req:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredOutcome {
    pub created_us: u64,
    pub timestamp_us: u64,
    pub commit_seq: u64,
    pub affected: Vec<u64>,
    pub fallback: bool,
    pub fanout_capped: bool,
    pub neighbors_dropped: u64,
    pub expansion_capped: bool,
}

fn key(id: &str) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(id.as_bytes());
    k
}

pub fn stage_outcome(
    batch: &mut WriteBatch,
    store: &RocksKv,
    request_id: &str,
    outcome: &StoredOutcome,
) -> Result<()> {
    let meta = store.cf_handle(cf::CF_META)?;
    batch.put_cf(&meta, key(request_id), serde_json::to_vec(outcome)?);
    Ok(())
}

/// The stored outcome of an earlier execution of `request_id`, if still live.
pub fn lookup(store: &RocksKv, request_id: &str) -> Result<Option<StoredOutcome>> {
    let Some(raw) = store.get(cf::CF_META, &key(request_id))? else {
        return Ok(None);
    };
    let outcome: StoredOutcome = serde_json::from_slice(&raw)?;
    let age = Timestamp::now().as_u64().saturating_sub(outcome.created_us);
    Ok((age <= TTL_US).then_some(outcome))
}
