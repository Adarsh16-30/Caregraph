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
use crate::replication::proto::checkpoint_chunk::Payload;
use crate::replication::proto::replication_service_server::ReplicationService;
use crate::replication::proto::{
    AckRequest, AckResponse, CheckpointChunk, CheckpointFile, CheckpointFooter, CheckpointHeader,
    FetchCheckpointRequest, FileChunk, ReplicateRequest, ReplicatedBatch,
};
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

/// What a demoted leader tells a follower that has everything it will ever commit. A follower
/// treats it like any lost connection and retries; it is not a reason to rebuild.
fn handed_over() -> Status {
    precondition("stale_leader: leadership was handed over; reconnect to the new leader")
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
        // A follower that already has everything a handed-over leader will ever commit is
        // turned away before it registers, so it does not count as a follower either.
        if repl.handed_over() && next > self.store().latest_sequence() {
            let _ = tx.send(Err(handed_over())).await;
            return;
        }
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
            // The handover is complete and this follower has everything: nothing more will
            // ever be sent on this stream, and an idle stream to a demoted leader would keep a
            // replica from ever finding the new one. Ending it makes the follower reconnect
            // through the write Service, which now points at the new leader.
            if repl.handed_over() && next > latest {
                let _ = tx.send(Err(handed_over())).await;
                return;
            }
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

/// Bytes of a checkpoint file per message.
const CHECKPOINT_CHUNK: usize = 1 << 20;

/// Deletes a checkpoint's scratch copy when the transfer ends, however it ends.
struct Scratch(std::path::PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Builds a checkpoint under `dir` and streams it: header, every file's bytes in
/// order, footer. Everything under `dir` named `cp-*` is a leftover of an earlier
/// transfer (only one runs at a time) and is removed first.
async fn send_checkpoint(
    api: &CareGraphApi,
    dir: std::path::PathBuf,
    tx: &mpsc::Sender<Result<CheckpointChunk, Status>>,
) -> Result<(), Status> {
    let internal = |what: &str, e: &dyn std::fmt::Display| {
        tracing::error!(error = %e, "{what}");
        Status::internal(format!("{what}; see the leader's log"))
    };
    std::fs::create_dir_all(&dir).map_err(|e| internal("creating the checkpoint directory", &e))?;
    for entry in std::fs::read_dir(&dir)
        .map_err(|e| internal("reading the checkpoint directory", &e))?
        .flatten()
    {
        if entry.file_name().to_string_lossy().starts_with("cp-") {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let target = dir.join(format!("cp-{unique}"));
    let _scratch = Scratch(target.clone());

    let made = target.clone();
    let sequence = api
        .on_store(move |s| crate::storage::backup::create_checkpoint(s, &made))
        .await
        .map_err(|e| internal("the checkpoint task failed", &e))?
        .map_err(|e| internal("creating the checkpoint failed", &e))?;

    let mut files = Vec::new();
    for entry in std::fs::read_dir(&target).map_err(|e| internal("listing the checkpoint", &e))? {
        let entry = entry.map_err(|e| internal("listing the checkpoint", &e))?;
        let meta = entry
            .metadata()
            .map_err(|e| internal("listing the checkpoint", &e))?;
        if meta.is_file() {
            files.push(CheckpointFile {
                name: entry.file_name().to_string_lossy().into_owned(),
                size: meta.len(),
            });
        }
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));

    let send = |payload: Payload| {
        tx.send(Ok(CheckpointChunk {
            payload: Some(payload),
        }))
    };
    let gone = |_| Status::cancelled("the replica stopped reading the checkpoint");
    send(Payload::Header(CheckpointHeader {
        sequence,
        epoch: api.replication().epoch(),
        files: files.clone(),
    }))
    .await
    .map_err(gone)?;

    let mut bytes = 0u64;
    for file in &files {
        let mut f = tokio::fs::File::open(target.join(&file.name))
            .await
            .map_err(|e| internal("opening a checkpoint file", &e))?;
        let mut offset = 0u64;
        let mut buf = vec![0u8; CHECKPOINT_CHUNK];
        loop {
            use tokio::io::AsyncReadExt;
            let n = f
                .read(&mut buf)
                .await
                .map_err(|e| internal("reading a checkpoint file", &e))?;
            if n == 0 {
                break;
            }
            send(Payload::File(FileChunk {
                name: file.name.clone(),
                offset,
                data: buf[..n].to_vec(),
            }))
            .await
            .map_err(gone)?;
            offset += n as u64;
            bytes += n as u64;
        }
        if offset != file.size {
            return Err(internal(
                "a checkpoint file changed size while it was being sent",
                &file.name,
            ));
        }
    }
    send(Payload::Footer(CheckpointFooter {
        files: files.len() as u64,
        bytes,
    }))
    .await
    .map_err(gone)?;
    Ok(())
}

#[tonic::async_trait]
impl ReplicationService for CareGraphApi {
    type ReplicateStream = ReceiverStream<Result<ReplicatedBatch, Status>>;
    type FetchCheckpointStream = ReceiverStream<Result<CheckpointChunk, Status>>;

    async fn fetch_checkpoint(
        &self,
        request: Request<FetchCheckpointRequest>,
    ) -> Result<Response<Self::FetchCheckpointStream>, Status> {
        self.authorize(&request, Rpc::Replicate)?;
        let req = request.into_inner();
        if req.follower_id.is_empty() || req.follower_id.len() > MAX_FOLLOWER_ID {
            return Err(Status::invalid_argument(
                "follower_id must be 1-64 characters",
            ));
        }
        let repl = self.replication().clone();
        if !repl.serves_stream() {
            return Err(precondition("this node is not the leader"));
        }
        let Some(dir) = repl.config().bootstrap_dir.clone() else {
            return Err(precondition(
                "this server has no bootstrap directory (CAREGRAPH_BOOTSTRAP_DIR)",
            ));
        };
        let Some(transfer) = repl.begin_checkpoint_transfer() else {
            return Err(Status::resource_exhausted(
                "another replica is being bootstrapped; retry shortly",
            ));
        };
        let (tx, rx) = mpsc::channel(4);
        let api = self.clone();
        tokio::spawn(async move {
            let _transfer = transfer;
            if let Err(status) = send_checkpoint(&api, dir, &tx).await {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

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
