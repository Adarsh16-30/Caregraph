//! Audit-chain anchoring and audit health metrics.
//!
//! A hash chain proves its records were not edited in place, but whoever controls the
//! disk could still truncate it and re-grow a different tail. Anchoring defeats that:
//! every interval the head of each chain, `(pod, chain, seq, head hash)`, is written
//! somewhere the pod cannot rewrite. This module produces the anchors; it does not
//! upload them.
//!
//! Each anchor is
//! * logged as an `audit_anchor` line (for a SIEM that keeps logs write-once), and
//! * appended as one JSON line to `anchors.jsonl` in the audit directory, which the
//!   chart's uploader sidecar ships to object storage with object lock.
//!
//! Metrics: `audit_chain_sequence{chain}`, `audit_queue_full_total` (reads refused
//! because the audit queue was full — the fail-closed path), `audit_dropped_total`
//! (denial events dropped on a full queue), `audit_last_anchor_timestamp_seconds`.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use prometheus::{IntCounter, IntGauge, IntGaugeVec, Opts, Registry};

use crate::security::audit::AuditLog;

/// File, inside the audit directory, that anchors are appended to.
pub const ANCHOR_FILE: &str = "anchors.jsonl";

pub struct AuditMetrics {
    sequence: IntGaugeVec,
    queue_full: IntCounter,
    dropped: IntCounter,
    last_anchor: IntGauge,
}

impl AuditMetrics {
    pub fn register(registry: &Registry) -> prometheus::Result<Arc<Self>> {
        let sequence = IntGaugeVec::new(
            Opts::new(
                "audit_chain_sequence",
                "Sequence number of the head of an audit chain",
            ),
            &["chain"],
        )?;
        let queue_full = IntCounter::new(
            "audit_queue_full_total",
            "Reads refused because the audit queue was full",
        )?;
        let dropped = IntCounter::new(
            "audit_dropped_total",
            "Denial events dropped because the audit queue was full",
        )?;
        let last_anchor = IntGauge::new(
            "audit_last_anchor_timestamp_seconds",
            "Unix time the audit chain heads were last anchored",
        )?;
        registry.register(Box::new(sequence.clone()))?;
        registry.register(Box::new(queue_full.clone()))?;
        registry.register(Box::new(dropped.clone()))?;
        registry.register(Box::new(last_anchor.clone()))?;
        Ok(Arc::new(AuditMetrics {
            sequence,
            queue_full,
            dropped,
            last_anchor,
        }))
    }
}

/// One line of `anchors.jsonl`.
pub fn anchor_line(pod: &str, chain: &str, seq: u64, head: &str, unix_secs: u64) -> String {
    serde_json::json!({
        "pod": pod,
        "chain": chain,
        "seq": seq,
        "head": head,
        "unix_secs": unix_secs,
    })
    .to_string()
}

fn append(path: &std::path::Path, lines: &[String]) -> std::io::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    for l in lines {
        writeln!(f, "{l}")?;
    }
    f.sync_all()
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Refreshes the audit metrics every few seconds and anchors both chain heads every
/// `interval`. `mutation_head` reads the mutation chain, which lives in the main
/// database. The first anchor is taken immediately, so a fresh pod is anchored at once.
pub fn spawn(
    log: Arc<AuditLog>,
    mutation_head: Arc<dyn Fn() -> (u64, String) + Send + Sync>,
    dir: PathBuf,
    metrics: Arc<AuditMetrics>,
    interval: Duration,
) {
    tokio::spawn(async move {
        let file = dir.join(ANCHOR_FILE);
        let (mut seen_full, mut seen_dropped) = (0u64, 0u64);
        let mut last_anchor: Option<std::time::Instant> = None;
        loop {
            let (reads_seq, reads_head) = log.head();
            let (mut_seq, mut_head) = mutation_head();
            metrics
                .sequence
                .with_label_values(&["reads"])
                .set(reads_seq as i64);
            metrics
                .sequence
                .with_label_values(&["mutations"])
                .set(mut_seq as i64);
            let full = log.queue_full_count();
            let dropped = log.dropped_count();
            metrics.queue_full.inc_by(full.saturating_sub(seen_full));
            metrics.dropped.inc_by(dropped.saturating_sub(seen_dropped));
            (seen_full, seen_dropped) = (full, dropped);

            if last_anchor.map_or(true, |t| t.elapsed() >= interval) {
                let now = unix_now();
                let pod = log.pod().to_string();
                for (chain, seq, head) in [
                    ("reads", reads_seq, &reads_head),
                    ("mutations", mut_seq, &mut_head),
                ] {
                    tracing::info!(target: "audit", pod = %pod, chain, seq, head = %head, "audit_anchor");
                }
                let lines = [
                    anchor_line(&pod, "reads", reads_seq, &reads_head, now),
                    anchor_line(&pod, "mutations", mut_seq, &mut_head, now),
                ];
                match append(&file, &lines) {
                    Ok(()) => {
                        metrics.last_anchor.set(now as i64);
                        last_anchor = Some(std::time::Instant::now());
                    }
                    Err(e) => {
                        // Not fatal to serving, but the alert on a stale anchor fires.
                        tracing::error!(error = %e, file = %file.display(), "writing the audit anchor failed");
                        last_anchor = Some(std::time::Instant::now());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_anchor_line_is_one_json_object_with_the_chain_and_head() {
        let line = anchor_line("writer-0", "reads", 7, "ab12", 1_700_000_000);
        assert!(!line.contains('\n'));
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["pod"], "writer-0");
        assert_eq!(v["chain"], "reads");
        assert_eq!(v["seq"], 7);
        assert_eq!(v["head"], "ab12");
    }

    #[test]
    fn anchors_are_appended_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join(ANCHOR_FILE);
        append(&f, &["one".into()]).unwrap();
        append(&f, &["two".into(), "three".into()]).unwrap();
        assert_eq!(std::fs::read_to_string(&f).unwrap(), "one\ntwo\nthree\n");
    }
}
