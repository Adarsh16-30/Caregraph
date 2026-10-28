//! Rust side of the boundary to `ml/embedding_server.py` (PRD 2.3's "direct
//! Rust<->Python boundary", Phase 4's actual deliverable of it).
//!
//! # Why a subprocess and not PyO3
//!
//! PyO3 is what the PRD names. It was tried first, against this machine's
//! real Python 3.14 install, and rejected for a reproducible reason rather
//! than a guess: `pyo3 0.24`'s build script refuses CPython 3.14 outright
//! ("the configured Python interpreter version (3.14) is newer than PyO3's
//! maximum supported version (3.13)"). Its own documented escape hatch,
//! `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1` under the stable ABI, does build —
//! but fails at the first `import torch`, because `_ctypes` and torch's own
//! native extensions are not abi3-limited, and CPython's own ABI-mismatch
//! guard refuses to load a non-limited extension under the compatibility
//! shim. That is a version-support gap in the crate, verified by building and
//! running it, not a configuration problem to route around.
//!
//! A long-lived worker process is the alternative that stays real: an actual
//! trained PyTorch Geometric model runs an actual forward pass (Rule 3), the
//! process just lives across the process boundary instead of in it. Workers
//! are spawned once and kept alive, so PyTorch's import cost is paid once per
//! worker, not once per mutation.
//!
//! # Pool, supervision, protocol
//!
//! [`EmbeddingModel`] is a facade over a supervised pool of N worker processes
//! (`worker::pool`): per-request deadlines, kill-and-respawn on timeout or
//! death, a circuit breaker, checkpoint SHA-256 re-verified on every spawn, and
//! a binary wire protocol (`worker::protocol`) instead of JSON text. Its public
//! API is unchanged, so every caller (`associative`, `staged_incremental`,
//! `attribution`, the benchmarks and tests) is untouched.

use std::time::Duration;

use serde::Deserialize;

use crate::embedding::manifest::ModelManifest;
use crate::embedding::worker::pool::{Expected, PoolConfig, PoolStats, WorkerPool};
use crate::embedding::worker::protocol;
use crate::error::{CareGraphError, Result};

/// Row indices and IG step/candidate-count parameters for one
/// [`EmbeddingModel::forward_with_attribution`] call. `targets` are indices
/// into the same local row space as `target_indices`/`edge_index` — the
/// caller (`src/embedding/attribution.rs`) is responsible for having built
/// `edge_index_before` from the *same* node ordering as `edge_index`, so a
/// row index means the same node on both sides.
pub struct AttributionRequest<'a> {
    pub targets: &'a [usize],
    pub edge_index_before: &'a [Vec<u32>],
    pub steps: u32,
    pub top_k: usize,
}

/// One target node's integrated-gradients result, exactly as
/// `embedding_server.py::_attribute_one_target` returns it — row indices, not
/// yet mapped back to `NodeId`s (that mapping needs the caller's `local` map,
/// which this module has no visibility into).
#[derive(Debug, Deserialize)]
pub struct RawAttributionEntry {
    pub target_index: usize,
    pub delta_norm: f32,
    pub baseline_delta: f32,
    pub completeness_residual: f32,
    pub edges_total: usize,
    /// `[src_row, dst_row, value]`, ranked by `|value|` descending, already
    /// truncated to the request's `top_k` by the worker.
    pub edges: Vec<(u32, u32, f32)>,
    pub rest_sum: f32,
    pub rest_count: usize,
}

/// A supervised pool of `embedding_server.py` workers for one deployed model.
pub struct EmbeddingModel {
    pool: WorkerPool,
    pub model_id: String,
    /// `<ml_dir>/deployed/<model_id>/dataset_manifest.json`, typed and
    /// cross-checked on every worker spawn against what the worker
    /// self-reports about the checkpoint it actually loaded.
    pub manifest: ModelManifest,
    /// Architecture the worker self-reported from `model.pt` at handshake
    /// time, kept on the record for auditability.
    pub checkpoint_architecture: Option<String>,
}

impl EmbeddingModel {
    /// Spawn with the pool settings from the environment (see
    /// [`PoolConfig::from_env`]); one worker unless `CAREGRAPH_WORKERS` says
    /// otherwise.
    pub fn spawn(model_id: &str) -> Result<Self> {
        Self::spawn_with(model_id, PoolConfig::from_env())
    }

    pub fn spawn_with(model_id: &str, cfg: PoolConfig) -> Result<Self> {
        let model_dir = crate::ml_dir().join("deployed").join(model_id);
        if !model_dir.join("model.pt").exists() {
            return Err(CareGraphError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "{} has no model.pt; train one first with ml/train_graphsage.py",
                    model_dir.display()
                ),
            )));
        }
        // A missing or malformed manifest is a hard error, never a default.
        let manifest = ModelManifest::load(model_id)?;
        let python = std::env::var("CAREGRAPH_PYTHON").unwrap_or_else(|_| "python".to_string());
        let (pool, handshake) = WorkerPool::spawn(
            model_dir,
            crate::ml_dir().join("embedding_server.py"),
            python,
            Expected {
                model_id: model_id.to_string(),
                architecture: manifest.architecture.clone(),
                is_associative: manifest.is_associative,
                model_sha256: manifest.model_sha256.clone(),
            },
            cfg,
        )?;
        Ok(EmbeddingModel {
            pool,
            model_id: model_id.to_string(),
            manifest,
            checkpoint_architecture: handshake.architecture,
        })
    }

    /// OS pid of the first live worker, for callers that manage it from
    /// outside (e.g. `tests/fault_injection`, which kills the process tree).
    pub fn worker_pid(&self) -> Option<u32> {
        self.pool.worker_pids().first().copied()
    }

    pub fn worker_pids(&self) -> Vec<u32> {
        self.pool.worker_pids()
    }

    pub fn stats(&self) -> PoolStats {
        self.pool.stats()
    }

    pub fn forward_timeout(&self) -> Duration {
        self.pool.forward_timeout()
    }

    /// One real forward pass over `(node_features, edge_index)`, returning the
    /// rows at `target_indices`. This is the Rule 3 boundary: everything past
    /// this call is the trained model's own math, not a stand-in for it.
    pub fn forward(
        &self,
        node_features: &[Vec<f32>],
        edge_index: &[Vec<u32>],
        target_indices: &[usize],
    ) -> Result<Vec<Vec<f32>>> {
        self.forward_with_deadline(
            node_features,
            edge_index,
            target_indices,
            self.pool.forward_timeout(),
        )
    }

    /// [`Self::forward`] with an explicit deadline instead of the pool's
    /// configured forward timeout. A request still running at the deadline is
    /// abandoned and its worker killed and respawned.
    pub fn forward_with_deadline(
        &self,
        node_features: &[Vec<f32>],
        edge_index: &[Vec<u32>],
        target_indices: &[usize],
        deadline: Duration,
    ) -> Result<Vec<Vec<f32>>> {
        let payload = protocol::encode_forward(node_features, edge_index, target_indices, None)?;
        let reply = self.pool.submit(payload, deadline)?;
        let (embeddings, _) = protocol::decode_forward_ok(&reply)?;
        Ok(embeddings)
    }

    /// A forward pass exactly like [`Self::forward`], plus an integrated-
    /// gradients edge attribution for each row in `attribution.targets`. A
    /// target whose embedding did not change is simply absent from the result
    /// (see `embedding_server.py::_attribute_one_target`), not an error.
    pub fn forward_with_attribution(
        &self,
        node_features: &[Vec<f32>],
        edge_index: &[Vec<u32>],
        target_indices: &[usize],
        attribution: AttributionRequest<'_>,
    ) -> Result<(Vec<Vec<f32>>, Vec<RawAttributionEntry>)> {
        let payload = protocol::encode_forward(
            node_features,
            edge_index,
            target_indices,
            Some(&attribution),
        )?;
        let reply = self.pool.submit(payload, self.pool.attribution_timeout())?;
        let (embeddings, attr) = protocol::decode_forward_ok(&reply)?;
        let attr = attr.ok_or_else(|| {
            CareGraphError::Io(std::io::Error::other(
                "embedding worker returned embeddings but no attribution for an attribution request",
            ))
        })?;
        Ok((embeddings, attr))
    }
}
