//! The leader's end of the replication stream.
//!
//! A follower connects, proves it can continue from where it is (same column
//! families, compatible history), and then receives the leader's write batches
//! exactly as they were written, starting at the sequence number it asks for.
//! New batches are pushed as they commit; when idle the leader sends a
//! heartbeat so the follower can tell "no writes" from "stream is dead" and
//! measure its lag.
//!
//! The stream reads the write-ahead log. How far back it reaches is bounded by
//! WAL retention (`CAREGRAPH_WAL_TTL_SECS`, `CAREGRAPH_WAL_SIZE_MB`): a follower
//! that asks for something already discarded is told `needs_bootstrap` and must
//! be rebuilt from a checkpoint.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::api::CareGraphApi;
use crate::error::CareGraphError;
use crate::replication::epoch::{decide, EpochHistory, StreamDecision};
use crate::replication::proto::replication_service_server::ReplicationService;
use crate::replication::proto::{AckRequest, AckResponse, ReplicateRequest, ReplicatedBatch};
use crate::security::Rpc;
use crate::storage::RocksKv;

/// How long an idle stream waits before sending a heartbeat.
const HEARTBEAT: Duration = Duration::from_secs(1);
/// Batches read from the log per round, and the byte budget for them.
const CHUNK_BATCHES: usize = 256;
const CHUNK_BYTES: usize = 4 << 20;
const MAX_FOLLOWER_ID: usize = 64;

struct RawBatch {
    seq: u64,
    count: u32,
    data: Vec<u8>,
}

struct Chunk {
    batches: Vec<RawBatch>,
    /// The first sequence number after the last batch read.
    next: u64,
}

enum ChunkError {
    /// The log no longer contains (or has a gap at) the requested sequence.
    Purged(String),
    Other(CareGraphError),
}

/// Reads batches starting exactly at `next` from the write-ahead log.
fn read_chunk(store: &RocksKv, next: u64) -> Result<Chunk, ChunkError> {
    // The log iterator yields batches that start after the sequence it is
    // given, so ask for the one before `next`.
    let iter = store.updates_since(next - 1).map_err(|e| {
        ChunkError::Purged(format!("the log no longer reaches back to {next}: {e}"))
    })?;
    let mut batches = Vec::new();
    let mut bytes = 0usize;
    let mut expected = next;
    for item in iter {
        let (seq, batch) = match item {
            Ok(v) => v,
            Err(e) if batches.is_empty() => {
                return Err(ChunkError::Purged(format!(
                    "the log no longer reaches back to {next}: {e}"
                )))
            }
            Err(e) => return Err(ChunkError::Other(e.into())),
        };
        let count = batch.len() as u32;
        if count == 0 {
            continue;
        }
        if seq != expected {
            return Err(ChunkError::Purged(format!(
                "the log has a gap: expected a batch at {expected}, found one at {seq}"
            )));
        }
        let data = batch.data().to_vec();
        bytes += data.len();
        expected = seq + u64::from(count);
        batches.push(RawBatch { seq, count, data });
        if batches.len() >= CHUNK_BATCHES || bytes >= CHUNK_BYTES {
            break;
        }
    }
    Ok(Chunk {
        batches,
        next: expected,
    })
}

fn precondition(msg: impl Into<String>) -> Status {
    Status::failed_precondition(msg.into())
}

impl CareGraphApi {
    /// Pushes batches to one follower until it goes away or this node stops
    /// being the leader.
    async fn stream_to_follower(
        self,
        tx: mpsc::Sender<Result<ReplicatedBatch, Status>>,
        follower_id: String,
        mut next: u64,
    ) {
        let repl = self.replication().clone();
        let mut commits = self.store().subscribe_commits();
        let _registered = repl.follower_connected(&follower_id, next - 1);
        loop {
            if tx.is_closed() {
                return;
            }
            if !repl.serves_stream() {
                let _ = tx
                    .send(Err(precondition(
                        "stale_leader: this node is no longer the leader",
                    )))
                    .await;
                return;
            }
            let latest = self.store().latest_sequence();
            if next <= latest {
                let from = next;
                // A follower that is keeping up is served from memory; only one
                // catching up reads the log.
                let from_tail = self
                    .store()
                    .recent_since(from, CHUNK_BATCHES, CHUNK_BYTES)
                    .map(|batches| {
                        let next = batches
                            .last()
                            .map_or(from, |b| b.first_seq + u64::from(b.count));
                        Chunk {
                            batches: batches
                                .into_iter()
                                .map(|b| RawBatch {
                                    seq: b.first_seq,
                                    count: b.count,
                                    data: b.data.as_ref().clone(),
                                })
                                .collect(),
                            next,
                        }
                    });
                let read = match from_tail {
                    Some(chunk) => Ok(Ok(chunk)),
                    None => self.on_store(move |s| read_chunk(s, from)).await,
                };
                match read {
                    Ok(Ok(chunk)) => {
                        if chunk.batches.is_empty() {
                            // The log has not caught up with the sequence
                            // number yet; look again shortly.
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            continue;
                        }
                        for b in chunk.batches {
                            let msg = ReplicatedBatch {
                                epoch: repl.epoch(),
                                seq: b.seq,
                                count: b.count,
                                data: b.data,
                                leader_seq: latest,
                            };
                            if tx.send(Ok(msg)).await.is_err() {
                                return;
                            }
                        }
                        next = chunk.next;
                    }
                    Ok(Err(ChunkError::Purged(why))) => {
                        let _ = tx
                            .send(Err(precondition(format!("needs_bootstrap: {why}"))))
                            .await;
                        return;
                    }
                    Ok(Err(ChunkError::Other(e))) => {
                        tracing::error!(error = %e, follower = %follower_id, "reading the log failed");
                        let _ = tx
                            .send(Err(Status::unavailable(
                                "reading the log failed; reconnect",
                            )))
                            .await;
                        return;
                    }
                    Err(_) => return,
                }
            } else {
                tokio::select! {
                    changed = commits.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(HEARTBEAT) => {
                        let beat = ReplicatedBatch {
                            epoch: repl.epoch(),
                            seq: 0,
                            count: 0,
                            data: Vec::new(),
                            leader_seq: self.store().latest_sequence(),
                        };
                        if tx.send(Ok(beat)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl ReplicationService for CareGraphApi {
    type ReplicateStream = ReceiverStream<Result<ReplicatedBatch, Status>>;

    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        self.authorize(&request, Rpc::Replicate)?;
        let req = request.into_inner();
        let repl = self.replication().clone();
        if !repl.serves_stream() {
            return Err(precondition("this node is not the leader"));
        }
        if req.follower_id.is_empty() || req.follower_id.len() > MAX_FOLLOWER_ID {
            return Err(Status::invalid_argument(
                "follower_id must be 1-64 characters",
            ));
        }
        if req.from_seq == 0 {
            return Err(Status::invalid_argument("from_seq starts at 1"));
        }

        let follower_cfs: std::collections::BTreeMap<String, u32> =
            req.column_families.into_iter().collect();
        let (leader_cfs, history, latest) = self
            .on_store(|s| {
                Ok::<_, CareGraphError>((
                    s.cf_ids()?.clone(),
                    EpochHistory::read(s)?,
                    s.latest_sequence(),
                ))
            })
            .await
            .map_err(|e| Status::internal(format!("replication check failed: {e}")))?
            .map_err(|e| Status::internal(format!("replication check failed: {e}")))?;
        if leader_cfs != follower_cfs {
            return Err(precondition(
                "schema_mismatch: the follower's column families differ from the leader's; \
                 bootstrap it from a leader checkpoint",
            ));
        }

        match decide(
            repl.epoch(),
            latest,
            &history,
            req.follower_epoch,
            req.from_seq - 1,
        ) {
            StreamDecision::Serve => {}
            StreamDecision::StaleLeader { follower_epoch } => {
                repl.fence(&format!(
                    "a follower is in epoch {follower_epoch}, newer than this leader's {}",
                    repl.epoch()
                ));
                return Err(precondition(
                    "stale_leader: a newer leader exists; this node has been fenced",
                ));
            }
            StreamDecision::Diverged(why) => {
                return Err(precondition(format!("needs_bootstrap: diverged: {why}")));
            }
        }

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(
            self.clone()
                .stream_to_follower(tx, req.follower_id, req.from_seq),
        );
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        self.authorize(&request, Rpc::Replicate)?;
        let req = request.into_inner();
        let repl = self.replication();
        if req.epoch > repl.epoch() {
            repl.fence("a follower acknowledged from a newer epoch than this leader's");
            return Err(precondition(
                "stale_leader: a newer leader exists; this node has been fenced",
            ));
        }
        repl.record_ack(&req.follower_id, req.applied_seq);
        Ok(Response::new(AckResponse {
            leader_epoch: repl.epoch(),
            leader_seq: self.store().latest_sequence(),
        }))
    }
}
