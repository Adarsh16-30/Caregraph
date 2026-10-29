//! Leadership epochs and the fencing rules.
//!
//! An **epoch** is a number that increases by one each time a different node
//! becomes leader. Each epoch's first write-ahead-log sequence number is recorded
//! in `CF_META` (`epoch:<n>` → start sequence) *in the same batch stream as the
//! data*, so the history replicates and every node knows where each leader's
//! history began.
//!
//! That is enough to refuse the two dangerous reconnects:
//!
//! * a **stale leader** (an old leader that was deposed but did not know): a
//!   follower that has seen a higher epoch than the leader it is talking to will
//!   not take its batches, and a leader that is asked to stream to a follower of
//!   a higher epoch fences itself;
//! * a **diverged follower** (one that holds writes the new leader never had,
//!   e.g. an old leader that kept accepting writes after the standby was
//!   promoted): if the follower's last epoch is older than the leader's and the
//!   follower's sequence reaches the point where the next epoch began, its
//!   history forked there and it must be re-bootstrapped.
//!
//! The decision is a pure function so the rules are testable without a database.

use std::collections::BTreeMap;

use rocksdb::WriteBatch;

use crate::error::Result;
use crate::storage::{cf, KvStore, RocksKv};

const PREFIX: &[u8] = b"epoch:";

fn key(epoch: u64) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(&epoch.to_be_bytes());
    k
}

/// Every epoch this database knows of, with the sequence number its leader's
/// first write received.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EpochHistory {
    starts: BTreeMap<u64, u64>,
}

impl EpochHistory {
    pub fn new(entries: impl IntoIterator<Item = (u64, u64)>) -> Self {
        EpochHistory {
            starts: entries.into_iter().collect(),
        }
    }

    pub fn read(store: &RocksKv) -> Result<Self> {
        let mut starts = BTreeMap::new();
        store.scan_from_narrow_prefix(cf::CF_META, PREFIX, PREFIX, &mut |k, v| {
            if let (Ok(e), Ok(s)) = (
                <[u8; 8]>::try_from(&k[PREFIX.len()..]),
                <[u8; 8]>::try_from(v),
            ) {
                starts.insert(u64::from_be_bytes(e), u64::from_be_bytes(s));
            }
            std::ops::ControlFlow::Continue(())
        })?;
        Ok(EpochHistory { starts })
    }

    /// The highest epoch recorded, or 0 if none.
    pub fn current(&self) -> u64 {
        self.starts.keys().next_back().copied().unwrap_or(0)
    }

    /// First sequence number written under `epoch`.
    pub fn start_of(&self, epoch: u64) -> Option<u64> {
        self.starts.get(&epoch).copied()
    }
}

/// Stages the record that `epoch` begins at `start_seq` into `batch`.
pub fn stage_epoch(
    batch: &mut WriteBatch,
    store: &RocksKv,
    epoch: u64,
    start_seq: u64,
) -> Result<()> {
    let meta = store.cf_handle(cf::CF_META)?;
    batch.put_cf(&meta, key(epoch), start_seq.to_be_bytes());
    Ok(())
}

/// Whether a leader may stream to a follower, and if not why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamDecision {
    Serve,
    /// The follower has seen a newer epoch than this leader holds: this leader
    /// has been replaced and must stop accepting writes.
    StaleLeader {
        follower_epoch: u64,
    },
    /// The follower's history forked from the leader's.
    Diverged(String),
}

/// `follower_latest` is the follower's newest applied sequence number.
pub fn decide(
    leader_epoch: u64,
    leader_latest: u64,
    history: &EpochHistory,
    follower_epoch: u64,
    follower_latest: u64,
) -> StreamDecision {
    if follower_epoch > leader_epoch {
        return StreamDecision::StaleLeader { follower_epoch };
    }
    if follower_latest > leader_latest {
        return StreamDecision::Diverged(format!(
            "the follower is at sequence {follower_latest}, ahead of the leader's {leader_latest}"
        ));
    }
    if follower_epoch < leader_epoch {
        // The next epoch after the follower's began at `fork`. A follower that
        // already holds that sequence number wrote (or received) different
        // data there.
        match history.start_of(follower_epoch + 1) {
            Some(fork) if follower_latest >= fork => {
                return StreamDecision::Diverged(format!(
                    "epoch {} began at sequence {fork} but the follower, still in epoch \
                     {follower_epoch}, is at {follower_latest}",
                    follower_epoch + 1
                ));
            }
            Some(_) => {}
            None => {
                return StreamDecision::Diverged(format!(
                    "the leader has no record of epoch {} that follows the follower's epoch \
                     {follower_epoch}",
                    follower_epoch + 1
                ));
            }
        }
    }
    StreamDecision::Serve
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history() -> EpochHistory {
        // Epoch 1 began at 100, epoch 2 (after a failover) at 5_000.
        EpochHistory::new([(1, 100), (2, 5_000)])
    }

    #[test]
    fn a_follower_in_the_current_epoch_is_served() {
        assert_eq!(
            decide(2, 9_000, &history(), 2, 7_000),
            StreamDecision::Serve
        );
        assert_eq!(
            decide(2, 9_000, &history(), 2, 9_000),
            StreamDecision::Serve
        );
    }

    #[test]
    fn a_follower_that_has_seen_a_newer_epoch_proves_the_leader_stale() {
        assert_eq!(
            decide(1, 9_000, &EpochHistory::new([(1, 100)]), 2, 5_100),
            StreamDecision::StaleLeader { follower_epoch: 2 }
        );
    }

    #[test]
    fn a_follower_behind_the_fork_point_catches_up_across_the_epoch_change() {
        assert_eq!(
            decide(2, 9_000, &history(), 1, 4_999),
            StreamDecision::Serve
        );
    }

    #[test]
    fn a_follower_that_reached_the_fork_point_in_an_older_epoch_has_diverged() {
        // The old leader kept writing after the standby was promoted.
        for latest in [5_000, 5_001, 8_000] {
            assert!(
                matches!(
                    decide(2, 9_000, &history(), 1, latest),
                    StreamDecision::Diverged(_)
                ),
                "{latest}"
            );
        }
    }

    #[test]
    fn a_follower_ahead_of_the_leader_has_diverged() {
        assert!(matches!(
            decide(2, 9_000, &history(), 2, 9_001),
            StreamDecision::Diverged(_)
        ));
    }

    #[test]
    fn an_unknown_epoch_lineage_is_refused_not_guessed() {
        assert!(matches!(
            decide(4, 9_000, &history(), 2, 10),
            StreamDecision::Diverged(_)
        ));
    }

    #[test]
    fn history_round_trips_through_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksKv::open(dir.path()).unwrap();
        assert_eq!(EpochHistory::read(&store).unwrap().current(), 0);
        let mut batch = WriteBatch::default();
        stage_epoch(&mut batch, &store, 1, 100).unwrap();
        stage_epoch(&mut batch, &store, 2, 5_000).unwrap();
        store.write(batch).unwrap();
        let read = EpochHistory::read(&store).unwrap();
        assert_eq!(read, history());
        assert_eq!((read.current(), read.start_of(2)), (2, Some(5_000)));
    }
}
