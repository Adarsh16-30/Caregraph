//! The follower's end of the replication stream.
//!
//! The loop connects to the leader, asks for everything after the last sequence
//! number it holds, and applies each batch **strictly in order**, asserting after
//! each one that the database's sequence number advanced exactly as it did on the
//! leader (`RocksKv::apply_replicated`). Anything else means this database no
//! longer matches the leader's history; the follower then stops, reports
//! not-ready, and waits to be re-bootstrapped from a checkpoint. Guessing past a
//! mismatch would turn one divergence into silent corruption.
//!
//! Transient trouble (the leader restarting, a network break) is retried with
//! exponential backoff. Acknowledgements are sent by a separate task that always
//! reports the latest applied sequence, so a burst of batches costs one
//! acknowledgement round trip, not one per batch.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::Request;

use crate::api::CareGraphApi;
use crate::error::CareGraphError;
use crate::replication::epoch::EpochHistory;
use crate::replication::proto::replication_service_client::ReplicationServiceClient;
use crate::replication::proto::{AckRequest, ReplicateRequest};
use crate::replication::Role;

/// How the follower reaches and authenticates to the leader.
#[derive(Debug, Clone, Default)]
pub struct FollowerOptions {
    /// e.g. `https://caregraph-write:50051`
    pub leader_addr: String,
    /// CA that signed the leader's certificate.
    pub ca: Option<PathBuf>,
    /// This follower's client certificate; its identity needs the `replicator`
    /// role on the leader.
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub server_name: Option<String>,
    /// Bearer token, when the leader requires the second factor.
    pub token: Option<String>,
}

enum Ended {
    /// Reconnect after a pause.
    Retry(String),
    /// This database cannot continue; stop until re-bootstrapped.
    NeedsBootstrap(String),
    /// Leadership changed hands under us (this node was promoted), or shutdown.
    Stop,
}

async fn connect(opts: &FollowerOptions) -> Result<Channel> {
    let mut endpoint = Channel::from_shared(opts.leader_addr.clone())
        .context("invalid leader address")?
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if opts.leader_addr.starts_with("https://") {
        // Read on every connect so a rotated certificate is picked up.
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = &opts.ca {
            tls = tls.ca_certificate(Certificate::from_pem(std::fs::read(ca)?));
        }
        if let (Some(cert), Some(key)) = (&opts.cert, &opts.key) {
            tls = tls.identity(Identity::from_pem(
                std::fs::read(cert)?,
                std::fs::read(key)?,
            ));
        }
        if let Some(name) = &opts.server_name {
            tls = tls.domain_name(name.clone());
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    Ok(endpoint.connect().await?)
}

fn bearer(token: &Option<String>) -> Option<MetadataValue<tonic::metadata::Ascii>> {
    token
        .as_ref()
        .and_then(|t| format!("Bearer {t}").parse().ok())
}

/// Runs until this node stops being a follower, the follower must be
/// re-bootstrapped, or `stop` fires.
pub async fn run(api: CareGraphApi, opts: FollowerOptions, mut stop: watch::Receiver<bool>) {
    let repl = api.replication().clone();
    let mut backoff = Duration::from_millis(200);
    loop {
        if *stop.borrow() || repl.role() != Role::Follower {
            return;
        }
        let received_before = repl.messages_received();
        let ended = tokio::select! {
            ended = run_once(&api, &opts) => ended,
            _ = stop.changed() => Ended::Stop,
        };
        repl.set_connected(false);
        match ended {
            Ended::Stop => return,
            Ended::NeedsBootstrap(why) => {
                repl.mark_needs_bootstrap(&why);
                return;
            }
            Ended::Retry(why) => {
                tracing::warn!(reason = %why, retry_in_ms = backoff.as_millis() as u64, "replication stream ended");
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = stop.changed() => return,
                }
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
        // A connection that delivered anything resets the pause.
        if repl.messages_received() > received_before {
            backoff = Duration::from_millis(200);
        }
    }
}

// tonic's interceptor signature fixes the (large) error type.
#[allow(clippy::result_large_err)]
async fn run_once(api: &CareGraphApi, opts: &FollowerOptions) -> Ended {
    let repl = api.replication().clone();

    let (from_seq, own_epoch, cfs) = match api
        .on_store(|s| {
            Ok::<_, CareGraphError>((
                s.latest_sequence() + 1,
                EpochHistory::read(s)?.current(),
                s.cf_ids()?.clone(),
            ))
        })
        .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Ended::Retry(format!("reading local state: {e}")),
        Err(e) => return Ended::Retry(format!("reading local state: {e}")),
    };

    let channel = match connect(opts).await {
        Ok(c) => c,
        Err(e) => return Ended::Retry(format!("connecting to the leader: {e:#}")),
    };
    let token = bearer(&opts.token);
    let interceptor = move |mut req: Request<()>| -> Result<Request<()>, tonic::Status> {
        if let Some(t) = &token {
            req.metadata_mut().insert("authorization", t.clone());
        }
        Ok(req)
    };
    let mut client =
        ReplicationServiceClient::with_interceptor(channel.clone(), interceptor.clone())
            .max_decoding_message_size(crate::replication::MAX_BATCH_MESSAGE_BYTES);
    let mut acker = ReplicationServiceClient::with_interceptor(channel, interceptor);

    let follower_id = repl.config().replica_id.clone();
    let mut stream = match client
        .replicate(ReplicateRequest {
            follower_id: follower_id.clone(),
            from_seq,
            follower_epoch: own_epoch,
            column_families: cfs.into_iter().collect(),
        })
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => return classify(status),
    };
    repl.set_connected(true);
    tracing::info!(
        from_seq,
        epoch = own_epoch,
        "replication stream established"
    );

    // Acknowledgements: always the newest applied sequence, coalesced.
    let (applied_tx, mut applied_rx) = watch::channel(0u64);
    let ack_task = tokio::spawn({
        let follower_id = follower_id.clone();
        let repl = repl.clone();
        async move {
            while applied_rx.changed().await.is_ok() {
                let applied_seq = *applied_rx.borrow_and_update();
                let sent = acker
                    .ack(AckRequest {
                        follower_id: follower_id.clone(),
                        applied_seq,
                        epoch: repl.epoch(),
                    })
                    .await;
                if let Err(status) = sent {
                    tracing::debug!(error = %status, "acknowledgement failed");
                }
            }
        }
    });
    // Tell the leader where we are right away: a follower that is already
    // caught up should count as a synchronous standby before any new write.
    applied_tx.send_replace(from_seq - 1);

    let ended = loop {
        let msg = match stream.message().await {
            Ok(Some(m)) => m,
            Ok(None) => break Ended::Retry("the leader closed the stream".into()),
            Err(status) => break classify(status),
        };
        repl.note_leader_seq(msg.leader_seq);
        repl.touch_connection();
        if msg.count == 0 {
            continue; // heartbeat
        }
        if msg.epoch < own_epoch {
            break Ended::Retry(format!(
                "the leader's epoch {} is older than this follower's {own_epoch}",
                msg.epoch
            ));
        }
        let _applying = repl.apply_lock().lock().await;
        if repl.role() != Role::Follower {
            break Ended::Stop;
        }
        let (seq, count) = (msg.seq, msg.count);
        let data = msg.data;
        let applied = api
            .on_store(move |s| s.apply_replicated(seq, count, &data))
            .await;
        match applied {
            Ok(Ok(())) => {
                let after = seq + u64::from(count) - 1;
                repl.note_epoch(msg.epoch);
                repl.note_batch_applied(after, msg.leader_seq);
                applied_tx.send_replace(after);
            }
            Ok(Err(CareGraphError::Diverged(why))) => break Ended::NeedsBootstrap(why),
            Ok(Err(e)) => break Ended::Retry(format!("applying a batch failed: {e}")),
            Err(e) => break Ended::Retry(format!("applying a batch failed: {e}")),
        }
    };
    ack_task.abort();
    ended
}

fn classify(status: tonic::Status) -> Ended {
    let msg = status.message().to_string();
    if status.code() == tonic::Code::FailedPrecondition {
        if msg.starts_with("needs_bootstrap") || msg.starts_with("schema_mismatch") {
            return Ended::NeedsBootstrap(msg);
        }
        if msg.starts_with("stale_leader") {
            return Ended::Retry(format!("the node we are following is stale: {msg}"));
        }
    }
    Ended::Retry(format!("{}: {msg}", status.code()))
}
