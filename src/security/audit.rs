//! Tamper-evident audit.
//!
//! Two hash chains, because two different things have to be true.
//!
//! **Mutation chain** (`CF_AUDIT` in the main database). One record per
//! committed mutation, staged into the *same* `WriteBatch` as the edge, the
//! embeddings and the high-water mark, so a mutation cannot be durable without
//! its audit record or the reverse. Records are chained in commit order (the
//! sequencer serialises commits), and the chain head lives in `CF_META`. The
//! records replicate with the data, so a follower holds the same chain.
//!
//! **Pod chain** (a separate per-pod database). Reads, access denials and
//! administrative actions. These are not part of the replicated data (a replica
//! must stay byte-identical to its leader), so each pod keeps its own log. Reads
//! are *fail-closed*: the event is written durably (group-committed, fsynced)
//! before the response is released, and if the log cannot accept it the read is
//! refused rather than left unaudited.
//!
//! Each record's hash covers the previous record's hash and the record's own
//! fields, so editing, deleting, reordering or inserting any record breaks every
//! hash after it. [`verify_chain`] checks a chain end to end; anchoring the head
//! hash somewhere the database's operator cannot rewrite (the periodic
//! `audit_anchor` log line, shipped to write-once storage) is what lets a
//! verifier detect truncation of the tail.

use std::path::Path;
use std::sync::{Arc, Mutex};

use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{CareGraphError, Result};
use crate::storage::{cf, KvStore, RocksKv};
use crate::types::Timestamp;

/// Chain head of the mutation chain, in `CF_META`.
pub const MUTATION_HEAD_KEY: &[u8] = b"audit_head";
/// Name recorded in mutation-chain records.
pub const MUTATION_CHAIN: &str = "mutations";

pub const GENESIS: [u8; 32] = [0u8; 32];

/// What a record says happened. Everything is a plain field so the hash input
/// is unambiguous.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub chain: String,
    pub seq: u64,
    pub ts_us: u64,
    pub prev_hash: String,
    pub principal: String,
    pub rpc: String,
    pub request_id: String,
    /// For a mutation, the edge's endpoints; for a read, the subject in `src`.
    pub src: u64,
    pub dst: u64,
    pub affected_count: u64,
    pub outcome: String,
    pub commit_seq: u64,
    pub hash: String,
}

/// The caller-supplied part of a record; chain position and hashes are added
/// when it is appended.
#[derive(Debug, Clone, Default)]
pub struct Fields {
    pub ts_us: u64,
    pub principal: String,
    pub rpc: String,
    pub request_id: String,
    pub src: u64,
    pub dst: u64,
    pub affected_count: u64,
    pub outcome: String,
    pub commit_seq: u64,
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

impl AuditRecord {
    fn build(chain: &str, seq: u64, prev: &[u8; 32], f: Fields) -> AuditRecord {
        let mut rec = AuditRecord {
            chain: chain.to_string(),
            seq,
            ts_us: f.ts_us,
            prev_hash: to_hex(prev),
            principal: f.principal,
            rpc: f.rpc,
            request_id: f.request_id,
            src: f.src,
            dst: f.dst,
            affected_count: f.affected_count,
            outcome: f.outcome,
            commit_seq: f.commit_seq,
            hash: String::new(),
        };
        rec.hash = to_hex(&rec.compute_hash());
        rec
    }

    /// SHA-256 over the previous hash and this record's fields (with an empty
    /// `hash`). Field order is the struct's declaration order, which serde keeps
    /// stable, so the same record always hashes the same.
    pub fn compute_hash(&self) -> [u8; 32] {
        let mut unhashed = self.clone();
        unhashed.hash = String::new();
        let body = serde_json::to_vec(&unhashed).expect("audit record serialises");
        let mut h = Sha256::new();
        h.update(b"caregraph-audit-v1");
        h.update(body);
        h.finalize().into()
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("audit record serialises")
    }

    pub fn decode(bytes: &[u8]) -> Result<AuditRecord> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Why a chain failed verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainError {
    pub at_seq: u64,
    pub reason: String,
}

impl std::fmt::Display for ChainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "audit chain broken at seq {}: {}",
            self.at_seq, self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainSummary {
    pub records: u64,
    pub head_seq: u64,
    pub head_hash: String,
}

/// Verifies records in order: contiguous sequence numbers from 1, each
/// `prev_hash` equal to the previous record's hash, each stored hash equal to
/// the recomputed one.
pub fn verify_chain(
    records: impl IntoIterator<Item = AuditRecord>,
) -> std::result::Result<ChainSummary, ChainError> {
    let mut expected_seq = 1u64;
    let mut prev = GENESIS;
    let mut count = 0u64;
    for rec in records {
        if rec.seq != expected_seq {
            return Err(ChainError {
                at_seq: expected_seq,
                reason: format!("expected seq {expected_seq}, found {}", rec.seq),
            });
        }
        if from_hex32(&rec.prev_hash) != Some(prev) {
            return Err(ChainError {
                at_seq: rec.seq,
                reason: "prev_hash does not match the previous record".into(),
            });
        }
        let recomputed = rec.compute_hash();
        if from_hex32(&rec.hash) != Some(recomputed) {
            return Err(ChainError {
                at_seq: rec.seq,
                reason: "record contents do not match its hash".into(),
            });
        }
        prev = recomputed;
        expected_seq += 1;
        count += 1;
    }
    Ok(ChainSummary {
        records: count,
        head_seq: expected_seq - 1,
        head_hash: to_hex(&prev),
    })
}

fn seq_key(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

/// Every record in `CF_AUDIT` of `store`, in sequence order.
pub fn read_chain(store: &RocksKv) -> Result<Vec<AuditRecord>> {
    let mut out = Vec::new();
    let mut failure = None;
    store.scan_from_narrow_prefix(cf::CF_AUDIT, &[], &[], &mut |_, value| {
        match AuditRecord::decode(value) {
            Ok(r) => out.push(r),
            Err(e) => {
                failure = Some(e);
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    })?;
    match failure {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

/// Verifies a database's mutation chain, including that the chain head recorded
/// in `CF_META` is the last record (so deleting the tail is caught as well).
pub fn verify_store(store: &RocksKv) -> Result<std::result::Result<ChainSummary, ChainError>> {
    let records = read_chain(store)?;
    let summary = match verify_chain(records) {
        Ok(s) => s,
        Err(e) => return Ok(Err(e)),
    };
    let head = store.get(cf::CF_META, MUTATION_HEAD_KEY)?;
    let recorded = head.as_deref().and_then(decode_head);
    let ok = match recorded {
        Some((seq, hash)) => seq == summary.head_seq && to_hex(&hash) == summary.head_hash,
        None => summary.records == 0,
    };
    if !ok {
        return Ok(Err(ChainError {
            at_seq: summary.head_seq,
            reason: "the chain head in CF_META does not match the last record (tail removed?)"
                .into(),
        }));
    }
    Ok(Ok(summary))
}

fn encode_head(seq: u64, hash: &[u8; 32]) -> [u8; 40] {
    let mut out = [0u8; 40];
    out[..8].copy_from_slice(&seq.to_be_bytes());
    out[8..].copy_from_slice(hash);
    out
}

fn decode_head(bytes: &[u8]) -> Option<(u64, [u8; 32])> {
    if bytes.len() != 40 {
        return None;
    }
    let seq = u64::from_be_bytes(bytes[..8].try_into().ok()?);
    let hash: [u8; 32] = bytes[8..].try_into().ok()?;
    Some((seq, hash))
}

// ---------------------------------------------------------------------------
// Mutation chain
// ---------------------------------------------------------------------------

struct Head {
    seq: u64,
    hash: [u8; 32],
}

/// The in-memory chain head for the mutation chain, and the staging of records
/// into commit batches. Commits are serialised by the sequencer, so staging and
/// advancing the head never interleave between two mutations.
pub struct MutationAudit {
    head: Mutex<Head>,
}

/// A record staged into a batch but not yet durable.
pub struct Pending {
    seq: u64,
    hash: [u8; 32],
}

impl MutationAudit {
    pub fn open(store: &RocksKv) -> Result<Arc<Self>> {
        let head = match store.get(cf::CF_META, MUTATION_HEAD_KEY)? {
            Some(bytes) => {
                let (seq, hash) = decode_head(&bytes).ok_or_else(|| {
                    CareGraphError::Io(std::io::Error::other("audit_head in CF_META is malformed"))
                })?;
                Head { seq, hash }
            }
            None => Head {
                seq: 0,
                hash: GENESIS,
            },
        };
        Ok(Arc::new(MutationAudit {
            head: Mutex::new(head),
        }))
    }

    /// Stages the record and the new chain head into `batch`. Call
    /// [`advance`](Self::advance) only after the batch is durably written.
    pub fn stage(&self, batch: &mut WriteBatch, store: &RocksKv, f: Fields) -> Result<Pending> {
        let (seq, prev) = {
            let h = self.head.lock().unwrap_or_else(|e| e.into_inner());
            (h.seq + 1, h.hash)
        };
        let rec = AuditRecord::build(MUTATION_CHAIN, seq, &prev, f);
        let hash = rec.compute_hash();
        let audit_cf = store.cf_handle(cf::CF_AUDIT)?;
        let meta_cf = store.cf_handle(cf::CF_META)?;
        batch.put_cf(&audit_cf, seq_key(seq), rec.encode());
        batch.put_cf(&meta_cf, MUTATION_HEAD_KEY, encode_head(seq, &hash));
        Ok(Pending { seq, hash })
    }

    pub fn advance(&self, pending: Pending) {
        let mut h = self.head.lock().unwrap_or_else(|e| e.into_inner());
        h.seq = pending.seq;
        h.hash = pending.hash;
    }

    pub fn head(&self) -> (u64, String) {
        let h = self.head.lock().unwrap_or_else(|e| e.into_inner());
        (h.seq, to_hex(&h.hash))
    }
}

// ---------------------------------------------------------------------------
// Pod chain: reads, denials, admin actions
// ---------------------------------------------------------------------------

struct Event {
    fields: Fields,
    /// Present for events that must be durable before the caller proceeds.
    ack: Option<tokio::sync::oneshot::Sender<std::result::Result<(), String>>>,
}

/// Why an event could not be recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditUnavailable {
    /// The writer's queue is full; the caller should refuse the request.
    QueueFull,
    /// The writer failed or stopped.
    Failed(String),
}

/// The per-pod audit log: a database of its own, one writer thread doing group
/// commits.
pub struct AuditLog {
    pod: String,
    store: Arc<RocksKv>,
    tx: tokio::sync::mpsc::Sender<Event>,
    head: Arc<Mutex<Head>>,
    dropped: std::sync::atomic::AtomicU64,
    queue_full: std::sync::atomic::AtomicU64,
}

/// Events per group commit, at most.
const MAX_BATCH: usize = 512;

impl AuditLog {
    /// Opens (or creates) the audit database in `dir`, recovering the chain
    /// head from its last record, and starts the writer thread. `store` is a
    /// separate [`RocksKv`] (encrypted if the main database is).
    pub fn start(store: RocksKv, pod: &str, queue_capacity: usize) -> Result<Arc<Self>> {
        store.set_sync_writes(true);
        let store = Arc::new(store);
        let existing = read_chain(&store)?;
        let head = match existing.last() {
            Some(last) => Head {
                seq: last.seq,
                hash: from_hex32(&last.hash).unwrap_or(GENESIS),
            },
            None => Head {
                seq: 0,
                hash: GENESIS,
            },
        };
        let head = Arc::new(Mutex::new(head));
        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(queue_capacity.max(1));
        let writer_store = store.clone();
        let log = Arc::new(AuditLog {
            pod: pod.to_string(),
            store,
            tx,
            head: head.clone(),
            dropped: Default::default(),
            queue_full: Default::default(),
        });
        let chain_name = pod.to_string();
        std::thread::Builder::new()
            .name("audit-writer".into())
            .spawn(move || writer_loop(writer_store, rx, head, chain_name))
            .map_err(CareGraphError::Io)?;
        Ok(log)
    }

    /// Opens `dir` as a [`RocksKv`] and starts the log on it.
    pub fn open_dir(dir: &Path, pod: &str, queue_capacity: usize) -> Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;
        Self::start(RocksKv::open(dir)?, pod, queue_capacity)
    }

    /// Records an event and waits until it is durable. Used for reads, where a
    /// failure must stop the request (fail-closed).
    pub async fn record_durable(
        &self,
        mut fields: Fields,
    ) -> std::result::Result<(), AuditUnavailable> {
        fields.ts_us = Timestamp::now().as_u64();
        let (ack, wait) = tokio::sync::oneshot::channel();
        self.tx
            .try_send(Event {
                fields,
                ack: Some(ack),
            })
            .map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    self.queue_full
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    AuditUnavailable::QueueFull
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    AuditUnavailable::Failed("audit writer stopped".into())
                }
            })?;
        match wait.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(AuditUnavailable::Failed(e)),
            Err(_) => Err(AuditUnavailable::Failed("audit writer stopped".into())),
        }
    }

    /// Records an event without waiting. For events about requests that are
    /// already being refused (denials), where there is nothing left to
    /// fail-close; a full queue drops the event and counts it.
    pub fn record_nowait(&self, mut fields: Fields) {
        fields.ts_us = Timestamp::now().as_u64();
        if self.tx.try_send(Event { fields, ack: None }).is_err() {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn head(&self) -> (u64, String) {
        let h = self.head.lock().unwrap_or_else(|e| e.into_inner());
        (h.seq, to_hex(&h.hash))
    }

    /// Every record in this log so far, in order (for verification and export).
    pub fn records(&self) -> Result<Vec<AuditRecord>> {
        read_chain(&self.store)
    }

    pub fn pod(&self) -> &str {
        &self.pod
    }

    pub fn queue_full_count(&self) -> u64 {
        self.queue_full.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn writer_loop(
    store: Arc<RocksKv>,
    mut rx: tokio::sync::mpsc::Receiver<Event>,
    head: Arc<Mutex<Head>>,
    chain: String,
) {
    while let Some(first) = rx.blocking_recv() {
        let mut events = vec![first];
        while events.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(e) => events.push(e),
                Err(_) => break,
            }
        }

        let (mut seq, mut prev) = {
            let h = head.lock().unwrap_or_else(|e| e.into_inner());
            (h.seq, h.hash)
        };
        let mut batch = WriteBatch::default();
        let mut records = Vec::with_capacity(events.len());
        let result = (|| -> Result<()> {
            let audit_cf = store.cf_handle(cf::CF_AUDIT)?;
            for ev in &events {
                seq += 1;
                let rec = AuditRecord::build(&chain, seq, &prev, ev.fields.clone());
                prev = rec.compute_hash();
                batch.put_cf(&audit_cf, seq_key(seq), rec.encode());
                records.push(rec);
            }
            Ok(())
        })()
        .and_then(|()| store.write(batch));

        match &result {
            Ok(()) => {
                let mut h = head.lock().unwrap_or_else(|e| e.into_inner());
                h.seq = seq;
                h.hash = prev;
                drop(h);
                for rec in &records {
                    // One JSON line per record for a SIEM to collect.
                    tracing::info!(target: "audit", record = %serde_json::to_string(rec).unwrap_or_default());
                }
            }
            Err(e) => tracing::error!(error = %e, "audit write failed; events were not recorded"),
        }
        let verdict = result.map_err(|e| e.to_string());
        for ev in events {
            if let Some(ack) = ev.ack {
                let _ = ack.send(verdict.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(n: u64) -> Fields {
        Fields {
            ts_us: n,
            principal: "p".into(),
            rpc: "AddEdge".into(),
            request_id: format!("r{n}"),
            src: n,
            dst: n + 1,
            affected_count: 3,
            outcome: "committed".into(),
            commit_seq: n,
        }
    }

    fn chain_of(n: u64) -> Vec<AuditRecord> {
        let mut prev = GENESIS;
        (1..=n)
            .map(|i| {
                let r = AuditRecord::build("mutations", i, &prev, fields(i));
                prev = r.compute_hash();
                r
            })
            .collect()
    }

    #[test]
    fn an_untouched_chain_verifies_and_reports_its_head() {
        let chain = chain_of(10);
        let head = chain.last().unwrap().hash.clone();
        let s = verify_chain(chain).unwrap();
        assert_eq!((s.records, s.head_seq), (10, 10));
        assert_eq!(s.head_hash, head);
        assert_eq!(verify_chain(Vec::new()).unwrap().records, 0);
    }

    #[test]
    fn changing_a_single_byte_of_any_record_is_detected_at_that_record() {
        for victim in [0usize, 4, 9] {
            let mut chain = chain_of(10);
            chain[victim].principal.push('x');
            let err = verify_chain(chain).unwrap_err();
            assert_eq!(err.at_seq, victim as u64 + 1, "victim {victim}");
        }
    }

    #[test]
    fn deleting_inserting_or_reordering_records_is_detected() {
        let mut deleted = chain_of(6);
        deleted.remove(2);
        assert!(verify_chain(deleted).is_err());

        let mut reordered = chain_of(6);
        reordered.swap(1, 2);
        assert!(verify_chain(reordered).is_err());

        let mut forged = chain_of(6);
        let mut extra = forged[2].clone();
        extra.seq = 7;
        forged.push(extra);
        assert!(verify_chain(forged).is_err());
    }

    #[test]
    fn rewriting_a_record_and_its_hash_still_breaks_the_next_link() {
        let mut chain = chain_of(5);
        chain[1].outcome = "tampered".into();
        chain[1].hash = to_hex(&chain[1].compute_hash());
        let err = verify_chain(chain).unwrap_err();
        assert_eq!(err.at_seq, 3, "the following record no longer links");
    }
}
