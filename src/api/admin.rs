//! The administrative gRPC service (`proto/caregraph_admin.proto`).
//!
//! These operations need the live database handle, so they are RPCs on the
//! running server. Each requires the `admin` role, is recorded in the per-pod
//! audit log *before* it runs (and refused if it cannot be recorded), and does
//! its blocking work on tokio's blocking pool, never on an async worker.
//!
//! Backups are one at a time: a second request while one is running is
//! `ABORTED`, not queued, because a backup holds the disk busy and queueing them
//! would only stack up the I/O.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prometheus::{IntCounter, IntGauge, Registry};
use tonic::{Request, Response, Status};

use crate::api::{internal, CareGraphApi};
use crate::replication::epoch::{stage_epoch, EpochHistory};
use crate::replication::Role;
use crate::security::audit::{self, Fields as AuditFields};
use crate::security::Rpc;
use crate::server::RpcClass;
use crate::storage::backup::{self, BackupManager};
use crate::storage::KvStore;

pub mod proto {
    tonic::include_proto!("caregraph.admin.v1");
}

use proto::admin_service_server::AdminService;
use proto::{
    AuditVerification, BackupInfo, CheckpointInfo, CreateBackupRequest, CreateCheckpointRequest,
    DemoteRequest, DemoteResponse, FollowerInfo, ListBackupsRequest, ListBackupsResponse,
    PromoteRequest, PromoteResponse, StatusRequest, StatusResponse, VerifyAuditRequest,
};

/// Backup configuration and state shared by the admin RPCs.
pub struct AdminOps {
    backups: Option<BackupManager>,
    checkpoint_dir: Option<PathBuf>,
    running: AtomicBool,
    last_success: IntGauge,
    failures: IntCounter,
}

impl AdminOps {
    /// `backups` and `checkpoint_dir` are `None` when no backup directory is
    /// configured; the RPCs that need them then answer `FAILED_PRECONDITION`.
    pub fn new(
        backups: Option<BackupManager>,
        checkpoint_dir: Option<PathBuf>,
        registry: &Registry,
    ) -> prometheus::Result<Arc<Self>> {
        let last_success = IntGauge::new(
            "backup_last_success_timestamp",
            "Unix time of the newest backup (0 if none)",
        )?;
        let failures = IntCounter::new("backup_failures_total", "Backups that failed")?;
        registry.register(Box::new(last_success.clone()))?;
        registry.register(Box::new(failures.clone()))?;
        if let Some(newest) = backups.as_ref().and_then(|b| b.list().pop()) {
            last_success.set(newest.created_unix);
        }
        Ok(Arc::new(AdminOps {
            backups,
            checkpoint_dir,
            running: AtomicBool::new(false),
            last_success,
            failures,
        }))
    }

    /// Takes a backup, keeping the success timestamp and failure count current.
    fn create_backup(
        &self,
        store: &crate::storage::RocksKv,
    ) -> crate::error::Result<backup::BackupInfo> {
        let manager = self.backups.as_ref().ok_or_else(|| {
            crate::error::CareGraphError::Io(std::io::Error::other("backups are not configured"))
        })?;
        match manager.create(store) {
            Ok(info) => {
                self.last_success.set(info.created_unix);
                Ok(info)
            }
            Err(e) => {
                self.failures.inc();
                Err(e)
            }
        }
    }

    fn backups(&self) -> Result<&BackupManager, Status> {
        self.backups.as_ref().ok_or_else(|| {
            Status::failed_precondition("no backup directory is configured on this server")
        })
    }

    /// Claims the single backup slot; released when the guard drops.
    fn begin(&self) -> Result<RunningGuard<'_>, Status> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(Status::aborted(
                "an admin backup operation is already running",
            ));
        }
        Ok(RunningGuard(&self.running))
    }
}

struct RunningGuard<'a>(&'a AtomicBool);

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn info_msg(i: backup::BackupInfo) -> BackupInfo {
    BackupInfo {
        backup_id: i.backup_id,
        created_unix: i.created_unix,
        size_bytes: i.size_bytes,
        num_files: i.num_files,
        sequence: i.sequence,
    }
}

impl CareGraphApi {
    fn admin_ops(&self) -> Result<&Arc<AdminOps>, Status> {
        self.inner
            .admin
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("administration is not enabled"))
    }

    /// Admission plus the pre-action audit record every admin call needs.
    async fn admit_admin<T>(&self, request: &Request<T>, action: &str) -> Result<(), Status> {
        self.admit(request, Rpc::Admin, RpcClass::Write)?;
        self.audit_durable(AuditFields {
            principal: Self::principal(request).to_string(),
            rpc: format!("Admin.{action}"),
            outcome: "requested".to_string(),
            ..AuditFields::default()
        })
        .await
    }
}

#[tonic::async_trait]
impl AdminService for CareGraphApi {
    async fn create_backup(
        &self,
        request: Request<CreateBackupRequest>,
    ) -> Result<Response<BackupInfo>, Status> {
        self.admit_admin(&request, "CreateBackup").await?;
        let ops = self.admin_ops()?.clone();
        ops.backups()?;
        let _slot = ops.begin()?;
        let inner = self.inner.clone();
        let run = ops.clone();
        let info = tokio::task::spawn_blocking(move || run.create_backup(&inner.store))
            .await
            .map_err(|e| Status::internal(format!("backup task failed: {e}")))?
            .map_err(internal)?;
        tracing::info!(
            backup_id = info.backup_id,
            sequence = info.sequence,
            "backup created"
        );
        Ok(Response::new(info_msg(info)))
    }

    async fn list_backups(
        &self,
        request: Request<ListBackupsRequest>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        self.admit_admin(&request, "ListBackups").await?;
        let ops = self.admin_ops()?.clone();
        let backups = ops.backups()?.list();
        Ok(Response::new(ListBackupsResponse {
            backups: backups.into_iter().map(info_msg).collect(),
        }))
    }

    async fn create_checkpoint(
        &self,
        request: Request<CreateCheckpointRequest>,
    ) -> Result<Response<CheckpointInfo>, Status> {
        self.admit_admin(&request, "CreateCheckpoint").await?;
        let name = request.into_inner().name;
        if !backup::valid_checkpoint_name(&name) {
            return Err(Status::invalid_argument(
                "checkpoint name must be 1-64 characters of letters, digits, '.', '_' or '-'",
            ));
        }
        let ops = self.admin_ops()?.clone();
        let dir = ops.checkpoint_dir.clone().ok_or_else(|| {
            Status::failed_precondition("no backup directory is configured on this server")
        })?;
        let _slot = ops.begin()?;
        let inner = self.inner.clone();
        let target = dir.join(&name);
        let path = target.display().to_string();
        let result = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)?;
            backup::create_checkpoint(&inner.store, &target)
        })
        .await
        .map_err(|e| Status::internal(format!("checkpoint task failed: {e}")))?;
        let sequence = match result {
            Ok(s) => s,
            Err(e) if target_exists(&e) => {
                return Err(Status::already_exists("a checkpoint with that name exists"))
            }
            Err(e) => return Err(internal(e)),
        };
        Ok(Response::new(CheckpointInfo {
            name,
            path,
            sequence,
        }))
    }

    async fn verify_audit(
        &self,
        request: Request<VerifyAuditRequest>,
    ) -> Result<Response<AuditVerification>, Status> {
        self.admit_admin(&request, "VerifyAudit").await?;
        let inner = self.inner.clone();
        let verdict = tokio::task::spawn_blocking(move || audit::verify_store(&inner.store))
            .await
            .map_err(|e| Status::internal(format!("audit verification task failed: {e}")))?
            .map_err(internal)?;
        Ok(Response::new(match verdict {
            Ok(summary) => AuditVerification {
                intact: true,
                records: summary.records,
                head: summary.head_hash,
                first_bad_seq: 0,
                detail: String::new(),
            },
            Err(e) => AuditVerification {
                intact: false,
                records: 0,
                head: String::new(),
                first_bad_seq: e.at_seq,
                detail: e.reason,
            },
        }))
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        self.admit_admin(&request, "Status").await?;
        let ops = self.admin_ops()?;
        let backups = ops.backups.as_ref().map(|b| b.list()).unwrap_or_default();
        let repl = self.replication();
        let (audit_records, audit_head) = self.inner.mutation_audit.head();
        Ok(Response::new(StatusResponse {
            latest_sequence: self.inner.store.raw().latest_sequence_number(),
            hwm_us: self.inner.sequencer.high_water_mark(),
            audit_records,
            audit_head,
            backups: backups.len() as u32,
            last_backup_unix: backups.last().map(|b| b.created_unix).unwrap_or(0),
            backups_configured: ops.backups.is_some(),
            role: repl.role().name().to_string(),
            epoch: repl.epoch(),
            leader_sequence: repl.leader_seq(),
            lag: repl.lag(),
            needs_bootstrap: repl.needs_bootstrap(),
            ready_for_reads: repl.ready_for_reads(),
            followers: repl
                .followers()
                .into_iter()
                .map(|(id, acked_sequence)| FollowerInfo { id, acked_sequence })
                .collect(),
        }))
    }

    async fn demote(
        &self,
        request: Request<DemoteRequest>,
    ) -> Result<Response<DemoteResponse>, Status> {
        self.admit_admin(&request, "Demote").await?;
        let timeout = wait_from_ms(request.into_inner().timeout_ms);
        let repl = self.replication().clone();
        match repl.role() {
            // Calling it again is how an interrupted demotion is finished.
            Role::Leader | Role::Demoted => {}
            other => {
                return Err(Status::failed_precondition(format!(
                    "only a leader can be demoted; this node is {}",
                    other.name()
                )))
            }
        }
        repl.demote();
        // New writes are refused from here; wait for the ones in flight so the
        // sequence number below is final.
        if !self.quiesce_writes(timeout).await {
            return Err(Status::deadline_exceeded(
                "in-flight writes did not finish in time; call Demote again",
            ));
        }
        let final_sequence = self.store().latest_sequence();
        // The stream stays up so the standby can receive everything up to it.
        let caught_up = wait_for_follower(&repl, final_sequence, timeout).await;
        tracing::warn!(final_sequence, caught_up, "leader demoted");
        Ok(Response::new(DemoteResponse {
            final_sequence,
            epoch: repl.epoch(),
            follower_caught_up: caught_up,
        }))
    }

    async fn promote(
        &self,
        request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        self.admit_admin(&request, "Promote").await?;
        let req = request.into_inner();
        let repl = self.replication().clone();
        if repl.role() != Role::Follower {
            return Err(Status::failed_precondition(format!(
                "only a follower can be promoted; this node is {}",
                repl.role().name()
            )));
        }
        if repl.needs_bootstrap() {
            return Err(Status::failed_precondition(
                "this follower must be re-bootstrapped from a checkpoint and cannot be promoted",
            ));
        }
        if req.wait_for_sequence > 0
            && !repl
                .wait_applied(req.wait_for_sequence, wait_from_ms(req.wait_timeout_ms))
                .await
            && !req.force
        {
            return Err(Status::deadline_exceeded(format!(
                "this follower has applied up to {} but the old leader committed {}; wait for it                  to catch up, or pass force to accept losing the difference",
                self.store().latest_sequence(),
                req.wait_for_sequence
            )));
        }

        // No batch may be half-applied when leadership changes hands.
        let _applying = repl.apply_lock().lock().await;
        if repl.role() != Role::Follower {
            return Err(Status::aborted("the role changed while promoting"));
        }
        let known = repl.epoch();
        let epoch = self
            .on_store(move |s| -> crate::error::Result<u64> {
                let epoch = EpochHistory::read(s)?.current().max(known) + 1;
                let mut batch = rocksdb::WriteBatch::default();
                stage_epoch(&mut batch, s, epoch, s.latest_sequence() + 1)?;
                s.write(batch)?;
                Ok(epoch)
            })
            .await
            .map_err(|e| Status::internal(format!("promotion task failed: {e}")))?
            .map_err(internal)?;
        self.reload_write_state().map_err(internal)?;
        repl.become_leader(epoch);
        tracing::warn!(
            epoch,
            sequence = self.store().latest_sequence(),
            "promoted to leader"
        );
        Ok(Response::new(PromoteResponse {
            epoch,
            sequence: self.store().latest_sequence(),
        }))
    }
}

fn wait_from_ms(ms: u32) -> std::time::Duration {
    std::time::Duration::from_millis(if ms == 0 { 30_000 } else { u64::from(ms) })
}

/// Waits until some follower has confirmed `sequence`, or `timeout`.
async fn wait_for_follower(
    repl: &crate::replication::ReplicationState,
    sequence: u64,
    timeout: std::time::Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if repl.followers().iter().any(|(_, acked)| *acked >= sequence) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn target_exists(e: &crate::error::CareGraphError) -> bool {
    e.to_string().contains("already exists")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_backup_operation_runs_at_a_time_and_the_slot_is_released() {
        let ops = AdminOps::new(None, None, &Registry::new()).unwrap();
        let first = ops.begin().unwrap();
        assert_eq!(ops.begin().err().unwrap().code(), tonic::Code::Aborted);
        drop(first);
        assert!(
            ops.begin().is_ok(),
            "the slot is free again after the guard drops"
        );
    }
}
