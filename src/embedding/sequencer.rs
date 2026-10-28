//! Ordered-commit sequencer — what makes parallel embedding workers *safe*.
//!
//! # The hazard
//!
//! With more than one embedding worker, two mutations can be computed at the
//! same time. If both read the graph before either commits, the second one's
//! embeddings are computed from a graph that does not contain the first one's
//! edge, and its (later) write silently overwrites the correct values: a lost
//! update. A *backdated* client timestamp is the same bug even sequentially: the
//! mutation's embeddings are computed as of its timestamp, so every later
//! version of an affected node is left stale.
//!
//! # Why not locks
//!
//! Locking the affected nodes up front needs the affected set, which is only
//! known after `resolve()` (most of the cost), and around a hub node the set
//! covers everything, so it serialises anyway. RocksDB's optimistic
//! transactions track point reads, not the iterator ranges the resolver uses.
//!
//! # The protocol (exact, not heuristic)
//!
//! A mutation's output depends only on the adjacency lists the resolver read
//! (its `fetched` set) as of the mutation's timestamp; node types are immutable
//! through the API. So:
//!
//! 1. **Admit.** Under `strict`, the timestamp must exceed the high-water mark
//!    (HWM) of every admitted mutation, or the request is `ABORTED`; `0` means
//!    "server assigns `max(now, HWM + 1)`". Admission order therefore *is*
//!    timestamp order.
//! 2. **Compute in parallel.** Record the commit-log position, then resolve and
//!    run the forward pass. Nothing is held.
//! 3. **Commit in timestamp order.** Wait until every lower-timestamp ticket has
//!    committed or been dropped. Then check the commit log since step 2: if any
//!    mutation that committed meanwhile touched a node in this one's `fetched`
//!    set, recompute. Nobody else can commit while we hold the turn, so one
//!    retry is always enough — no livelock.
//!
//! A ticket that is dropped without completing (error, panic, client cancel)
//! releases its turn, so a failed mutation never wedges the ones behind it.
//!
//! `legacy` policy bypasses all of this (any client timestamp, no ordering). It
//! is only safe with exactly one worker and is refused in production.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use rocksdb::WriteBatch;

use crate::config::TsPolicy;
use crate::error::{CareGraphError, Result};
use crate::storage::{cf, KvStore, RocksKv};
use crate::temporal::keys::decode_edge_key;
use crate::types::{NodeId, Timestamp};

/// Commit-log entries kept for conflict checks. A mutation whose recorded
/// position has been trimmed away is treated as conflicting (conservative:
/// it recomputes), never silently trusted.
const LOG_CAP: usize = 65_536;
/// Longest a mutation waits for its turn before giving up with `UNAVAILABLE`.
const MAX_TURN_WAIT: Duration = Duration::from_secs(20 * 60);

pub const HWM_KEY: &[u8] = b"hwm";

struct State {
    /// Highest timestamp ever admitted (strict / server-assigned policies).
    hwm_admitted: u64,
    /// Timestamps admitted but not yet committed or dropped.
    pending: BTreeSet<u64>,
    /// Sequence number the *next* commit will get.
    next_seq: u64,
    /// Recent commits, oldest first: `(seq, endpoint a, endpoint b)`.
    log: VecDeque<(u64, NodeId, NodeId)>,
    /// Request ids currently executing, to refuse a concurrent duplicate.
    in_flight: HashSet<String>,
}

pub struct Sequencer {
    policy: TsPolicy,
    state: Mutex<State>,
    turn: Condvar,
}

/// Admission to commit, in timestamp order. Dropping it without calling
/// [`Sequencer::complete`] releases the turn.
pub struct Ticket<'a> {
    seq: &'a Sequencer,
    pub ts: Timestamp,
    registered: bool,
    done: bool,
}

/// Holds a request id as in-flight until dropped.
pub struct RequestGuard<'a> {
    seq: &'a Sequencer,
    id: String,
}

impl Drop for RequestGuard<'_> {
    fn drop(&mut self) {
        let mut st = self.seq.lock();
        st.in_flight.remove(&self.id);
    }
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        if self.registered && !self.done {
            let mut st = self.seq.lock();
            st.pending.remove(&self.ts.as_u64());
            self.seq.turn.notify_all();
        }
    }
}

impl Sequencer {
    /// Pure constructor, used directly by unit tests.
    pub fn new(policy: TsPolicy, initial_hwm: u64) -> Self {
        Sequencer {
            policy,
            state: Mutex::new(State {
                hwm_admitted: initial_hwm,
                pending: BTreeSet::new(),
                next_seq: 1,
                log: VecDeque::new(),
                in_flight: HashSet::new(),
            }),
            turn: Condvar::new(),
        }
    }

    /// Recover the high-water mark from storage: the persisted `hwm` record, or,
    /// if none exists yet (a database that was bulk-loaded and never mutated
    /// through the sequencer), the newest timestamp of any stored edge. The
    /// scan only runs once; every later commit stages the HWM in its own batch.
    pub fn open(store: &RocksKv, policy: TsPolicy) -> Result<Self> {
        let hwm = match store.get(cf::CF_META, HWM_KEY)? {
            Some(bytes) if bytes.len() == 8 => u64::from_be_bytes(bytes.try_into().expect("8")),
            _ => {
                if policy == TsPolicy::Legacy {
                    0
                } else {
                    let started = Instant::now();
                    let mut max_ts = 0u64;
                    store.scan_from_narrow_prefix(cf::CF_EDGES, &[], &[], &mut |key, _| {
                        if let Some((_, _, ts, _)) = decode_edge_key(key) {
                            max_ts = max_ts.max(ts.as_u64());
                        }
                        std::ops::ControlFlow::Continue(())
                    })?;
                    tracing::info!(
                        hwm = max_ts,
                        scan_ms = started.elapsed().as_millis() as u64,
                        "recovered commit high-water mark from the newest stored edge"
                    );
                    max_ts
                }
            }
        };
        Ok(Self::new(policy, hwm))
    }

    pub fn policy(&self) -> TsPolicy {
        self.policy
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn high_water_mark(&self) -> u64 {
        self.lock().hwm_admitted
    }

    /// Refuse a second concurrent execution of the same client request id.
    pub fn begin_request(&self, id: &str) -> Result<RequestGuard<'_>> {
        let mut st = self.lock();
        if !st.in_flight.insert(id.to_string()) {
            return Err(CareGraphError::Aborted(format!(
                "request_id {id} is already being processed; retry after it completes"
            )));
        }
        Ok(RequestGuard {
            seq: self,
            id: id.to_string(),
        })
    }

    /// Admit a mutation and fix its timestamp. See the module doc.
    pub fn admit(&self, requested: Timestamp) -> Result<Ticket<'_>> {
        if self.policy == TsPolicy::Legacy {
            return Ok(Ticket {
                seq: self,
                ts: requested,
                registered: false,
                done: false,
            });
        }
        let mut st = self.lock();
        let ts = match (self.policy, requested.as_u64()) {
            (TsPolicy::Strict, r) if r != 0 => {
                if r <= st.hwm_admitted {
                    return Err(CareGraphError::Aborted(format!(
                        "timestamp {r} is at or below the commit high-water mark {}; send \
                         timestamp_us=0 to have the server assign one, or a larger timestamp",
                        st.hwm_admitted
                    )));
                }
                r
            }
            // `strict` with 0, and `server_assigned` always.
            _ => Timestamp::now().as_u64().max(st.hwm_admitted + 1),
        };
        st.hwm_admitted = ts;
        st.pending.insert(ts);
        Ok(Ticket {
            seq: self,
            ts: Timestamp(ts),
            registered: true,
            done: false,
        })
    }

    /// Commit-log position to record *before* resolving.
    pub fn head(&self) -> u64 {
        self.lock().next_seq
    }

    /// Block until every lower-timestamp ticket has committed or been dropped.
    /// Returns how long this ticket waited.
    pub fn wait_turn(&self, ticket: &Ticket<'_>) -> Result<Duration> {
        if !ticket.registered {
            return Ok(Duration::ZERO);
        }
        let started = Instant::now();
        let ts = ticket.ts.as_u64();
        let mut st = self.lock();
        while st.pending.first() != Some(&ts) {
            let left = MAX_TURN_WAIT.saturating_sub(started.elapsed());
            if left.is_zero() {
                return Err(CareGraphError::Unavailable(
                    "timed out waiting for earlier mutations to commit".into(),
                ));
            }
            st = self
                .turn
                .wait_timeout(st, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        Ok(started.elapsed())
    }

    /// Did a mutation commit since `pos0` that touched a node in `fetched`?
    /// Also true when `pos0` has fallen off the retained log (conservative).
    pub fn conflicts_since(&self, pos0: u64, fetched: &HashSet<NodeId>) -> bool {
        if self.policy == TsPolicy::Legacy {
            return false;
        }
        let st = self.lock();
        if let Some(&(oldest, _, _)) = st.log.front() {
            if pos0 < oldest && oldest > 1 {
                return true;
            }
        } else if pos0 < st.next_seq && st.next_seq > 1 {
            // Commits happened since `pos0` but the log has been emptied/trimmed.
            return true;
        }
        st.log
            .iter()
            .filter(|(seq, _, _)| *seq >= pos0)
            .any(|(_, a, b)| fetched.contains(a) || fetched.contains(b))
    }

    /// Record a durable commit and release the turn. Returns its sequence number.
    pub fn complete(&self, mut ticket: Ticket<'_>, endpoints: (NodeId, NodeId)) -> u64 {
        ticket.done = true;
        let mut st = self.lock();
        let seq = st.next_seq;
        st.next_seq += 1;
        st.log.push_back((seq, endpoints.0, endpoints.1));
        while st.log.len() > LOG_CAP {
            st.log.pop_front();
        }
        if ticket.registered {
            st.pending.remove(&ticket.ts.as_u64());
            self.turn.notify_all();
        }
        seq
    }
}

/// Stage the persisted high-water mark into the commit batch. Staging only —
/// the one durable write stays the committer's single batch write.
pub fn stage_hwm(batch: &mut WriteBatch, store: &RocksKv, ts: Timestamp) -> Result<()> {
    let meta = store.cf_handle(cf::CF_META)?;
    batch.put_cf(&meta, HWM_KEY, ts.as_u64().to_be_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn set(ids: &[u64]) -> HashSet<NodeId> {
        ids.iter().map(|&i| NodeId(i)).collect()
    }

    #[test]
    fn strict_rejects_a_backdated_timestamp_and_keeps_the_hwm() {
        let s = Sequencer::new(TsPolicy::Strict, 100);
        let t = s.admit(Timestamp(150)).unwrap();
        assert_eq!(t.ts.as_u64(), 150);
        for bad in [150u64, 149, 100, 1] {
            assert!(
                matches!(s.admit(Timestamp(bad)), Err(CareGraphError::Aborted(_))),
                "ts {bad} must be refused"
            );
        }
        assert_eq!(s.high_water_mark(), 150, "a refusal must not move the HWM");
    }

    #[test]
    fn zero_means_server_assigned_and_is_strictly_increasing() {
        let s = Sequencer::new(TsPolicy::Strict, 5);
        let a = s.admit(Timestamp(0)).unwrap().ts.as_u64();
        let b = s.admit(Timestamp(0)).unwrap().ts.as_u64();
        assert!(a > 5 && b > a);
    }

    #[test]
    fn server_assigned_ignores_the_client_timestamp() {
        let s = Sequencer::new(TsPolicy::ServerAssigned, 0);
        let t = s.admit(Timestamp(7)).unwrap();
        assert!(
            t.ts.as_u64() > 7,
            "client ts 7 must be replaced by a server ts"
        );
    }

    #[test]
    fn legacy_accepts_anything_and_never_conflicts() {
        let s = Sequencer::new(TsPolicy::Legacy, 0);
        assert_eq!(s.admit(Timestamp(3)).unwrap().ts.as_u64(), 3);
        assert_eq!(s.admit(Timestamp(1)).unwrap().ts.as_u64(), 1);
        assert!(!s.conflicts_since(0, &set(&[1])));
    }

    #[test]
    fn a_commit_to_a_fetched_node_is_a_conflict_and_to_a_stranger_is_not() {
        let s = Sequencer::new(TsPolicy::Strict, 0);
        let pos0 = s.head();
        let other = s.admit(Timestamp(10)).unwrap();
        s.complete(other, (NodeId(1), NodeId(2)));
        assert!(
            s.conflicts_since(pos0, &set(&[2, 99])),
            "node 2 was written"
        );
        assert!(
            !s.conflicts_since(pos0, &set(&[50, 99])),
            "disjoint read set"
        );
        // A reader that started after that commit is not affected by it.
        assert!(!s.conflicts_since(s.head(), &set(&[1, 2])));
    }

    #[test]
    fn commits_are_released_in_timestamp_order_even_if_finished_out_of_order() {
        let s = Arc::new(Sequencer::new(TsPolicy::Strict, 0));
        let first = s.admit(Timestamp(10)).unwrap();
        let second = s.admit(Timestamp(20)).unwrap();

        let order = Arc::new(Mutex::new(Vec::new()));
        std::thread::scope(|scope| {
            let (s2, o2) = (s.clone(), order.clone());
            // The later mutation is ready first and must wait for the earlier one.
            let waiter = scope.spawn(move || {
                s2.wait_turn(&second).unwrap();
                o2.lock().unwrap().push(20);
                s2.complete(second, (NodeId(3), NodeId(4)));
            });
            std::thread::sleep(Duration::from_millis(150));
            assert!(
                order.lock().unwrap().is_empty(),
                "20 must not run before 10"
            );
            s.wait_turn(&first).unwrap();
            order.lock().unwrap().push(10);
            s.complete(first, (NodeId(1), NodeId(2)));
            waiter.join().unwrap();
        });
        assert_eq!(*order.lock().unwrap(), vec![10, 20]);
    }

    #[test]
    fn a_dropped_ticket_releases_its_turn_so_later_ones_are_not_wedged() {
        let s = Sequencer::new(TsPolicy::Strict, 0);
        let doomed = s.admit(Timestamp(10)).unwrap();
        let next = s.admit(Timestamp(20)).unwrap();
        drop(doomed); // e.g. the forward pass errored
        s.wait_turn(&next).unwrap(); // must return, not block
        s.complete(next, (NodeId(1), NodeId(2)));
    }

    #[test]
    fn the_same_request_id_cannot_run_twice_concurrently() {
        let s = Sequencer::new(TsPolicy::Strict, 0);
        let g = s.begin_request("abc").unwrap();
        assert!(matches!(
            s.begin_request("abc"),
            Err(CareGraphError::Aborted(_))
        ));
        drop(g);
        assert!(
            s.begin_request("abc").is_ok(),
            "released once the first finished"
        );
    }

    #[test]
    fn a_position_that_fell_off_the_log_is_treated_as_a_conflict() {
        let s = Sequencer::new(TsPolicy::Strict, 0);
        let pos0 = s.head();
        for i in 0..(LOG_CAP as u64 + 10) {
            let t = s.admit(Timestamp(0)).unwrap();
            s.complete(t, (NodeId(i * 2), NodeId(i * 2 + 1)));
        }
        assert!(
            s.conflicts_since(pos0, &set(&[u64::MAX])),
            "trimmed history cannot prove there was no conflict"
        );
    }
}
