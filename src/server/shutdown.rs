//! Signal handling and the bounded drain sequence.
//!
//! Order of events on SIGTERM / Ctrl-C:
//! 1. readiness flips to not-ready (`Health::begin_draining`), so the load
//!    balancer stops routing here;
//! 2. the pre-stop delay elapses, letting that propagate;
//! 3. the gRPC server stops accepting and finishes in-flight requests;
//! 4. if that takes longer than the drain deadline the server task is aborted
//!    rather than waited on forever (tonic's own graceful shutdown has no bound).
//!
//! The caller then waits for in-flight database work and flushes the WAL
//! (`CareGraphApi::quiesce`/`flush`) before dropping the store.

use std::future::Future;
use std::pin::Pin;

use crate::server::{Health, ServerLimits};

/// Resolves when the process is asked to stop.
pub async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The server stopped on its own before any signal arrived.
    ServerExited,
    /// All in-flight requests finished within the deadline.
    Clean,
    /// The deadline passed; remaining requests were abandoned.
    DeadlineExceeded,
}

pub type ShutdownSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Runs `serve` (handed the shutdown future it must stop on) until `signal`
/// resolves, then performs the drain sequence described in the module docs.
pub async fn run_until_shutdown<Fut, Sig>(
    serve: impl FnOnce(ShutdownSignal) -> Fut,
    signal: Sig,
    health: &Health,
    limits: &ServerLimits,
) -> anyhow::Result<DrainOutcome>
where
    Fut: Future<Output = Result<(), tonic::transport::Error>> + Send + 'static,
    Sig: Future<Output = ()>,
{
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(Box::pin(async move {
        let _ = stop_rx.await;
    })));
    tokio::pin!(server);

    tokio::select! {
        joined = &mut server => {
            return match joined {
                Ok(Ok(())) => Ok(DrainOutcome::ServerExited),
                Ok(Err(e)) => Err(anyhow::Error::new(e).context("gRPC server exited")),
                Err(e) => Err(anyhow::anyhow!("gRPC server task failed: {e}")),
            };
        }
        _ = signal => {}
    }

    health.begin_draining();
    tracing::info!(
        prestop_delay_secs = limits.prestop_delay.as_secs(),
        drain_deadline_secs = limits.drain_deadline.as_secs(),
        "shutdown requested: not ready, draining"
    );
    tokio::time::sleep(limits.prestop_delay).await;
    let _ = stop_tx.send(());

    match tokio::time::timeout(limits.drain_deadline, &mut server).await {
        Ok(Ok(Ok(()))) => Ok(DrainOutcome::Clean),
        Ok(Ok(Err(e))) => Err(anyhow::Error::new(e).context("gRPC server failed while draining")),
        Ok(Err(e)) => Err(anyhow::anyhow!("gRPC server task failed: {e}")),
        Err(_) => {
            server.abort();
            tracing::warn!("drain deadline exceeded; abandoning in-flight requests");
            Ok(DrainOutcome::DeadlineExceeded)
        }
    }
}
