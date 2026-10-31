//! Read replicas by write-ahead-log shipping, with operator-triggered failover.
//!
//! See `docs/replication.md` for the model; the modules are:
//!
//! * [`bootstrap`] — building a replica from the leader's checkpoint over the network;
//! * [`batch_decode`] — reads the operations inside a serialized write batch;
//! * [`epoch`] — the leadership epoch history and the fencing rules, as pure
//!   functions;
//! * [`state`] — a node's role, epoch, replication progress and the
//!   synchronous-acknowledgement gate;
//! * [`leader`] / [`follower`] — the two ends of the stream.

// `tonic::Status` is 176 bytes, over clippy's `result_large_err` threshold, and it is
// the error type tonic requires of every RPC handler, so the generated service traits
// and the helpers that compose into them all return it. Newer clippy releases also check
// the generated code in this module's `proto`. Boxing it would only move the same bytes
// behind a pointer at every call site. Same reasoning, and same choice, as in `crate::api`.
#![allow(clippy::result_large_err)]

/// Largest replication message either end will encode or decode. A message is
/// one write batch.
pub const MAX_BATCH_MESSAGE_BYTES: usize = 64 << 20;

pub mod batch_decode;
pub mod bootstrap;
pub mod epoch;
pub mod follower;
pub mod leader;
pub mod state;

pub mod proto {
    tonic::include_proto!("caregraph.internal.v1");
}

pub use state::{AckOutcome, ReplicationConfig, ReplicationState, Role, WriteRefusal};

/// A leader's first start records epoch 1 and the sequence number it begins at,
/// so followers bootstrapped from it afterwards carry the lineage. A database
/// that already has an epoch history is left alone.
pub fn ensure_epoch(
    store: &crate::storage::RocksKv,
    state: &ReplicationState,
) -> crate::error::Result<()> {
    use crate::storage::KvStore;
    if state.role() != Role::Leader {
        return Ok(());
    }
    let history = epoch::EpochHistory::read(store)?;
    if history.current() > 0 {
        state.note_epoch(history.current());
        return Ok(());
    }
    let mut batch = rocksdb::WriteBatch::default();
    epoch::stage_epoch(&mut batch, store, 1, store.latest_sequence() + 1)?;
    store.write(batch)?;
    state.note_epoch(1);
    Ok(())
}
