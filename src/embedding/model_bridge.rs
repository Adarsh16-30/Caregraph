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
//! process just lives across the process boundary instead of in it. Spawned
//! once and kept alive, so PyTorch's import cost is paid once per CareGraph
//! process, not once per mutation.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::embedding::manifest::ModelManifest;
use crate::error::{CareGraphError, Result};

#[derive(Serialize)]
struct ForwardRequest<'a> {
    node_features: &'a [Vec<f32>],
    edge_index: &'a [Vec<u32>],
    target_indices: &'a [usize],
    #[serde(skip_serializing_if = "Option::is_none")]
    attribution: Option<AttributionRequestWire<'a>>,
}

/// Wire shape of an attribution request — purely additive to the base
/// forward-pass request (Phase 9, Feature 1): a caller that never sets this
/// gets exactly the pre-Phase-9 protocol back, unchanged.
#[derive(Serialize)]
struct AttributionRequestWire<'a> {
    targets: &'a [usize],
    edge_index_before: &'a [Vec<u32>],
    steps: u32,
    top_k: usize,
}

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

#[derive(Deserialize)]
struct ForwardResponse {
    embeddings: Option<Vec<Vec<f32>>>,
    #[serde(default)]
    attribution: Option<Vec<RawAttributionEntry>>,
    error: Option<String>,
}

/// What `embedding_server.py` self-reports about the checkpoint it actually
/// loaded, read from `model.pt` itself rather than trusted from the caller —
/// the independent half of the Phase 9 manifest cross-check in
/// [`EmbeddingModel::spawn`]. Before this existed, Rust never learned which
/// architecture the worker actually got; a `model_id`/`ModelKind` mismatch was
/// a documented but unchecked footgun (see `tests/fault_injection`'s own doc
/// comment on the subject).
#[derive(Debug, Clone, Deserialize)]
struct ReadyHandshake {
    ready: bool,
    #[serde(default)]
    architecture: Option<String>,
    #[serde(default)]
    is_associative: Option<bool>,
}

/// A running `embedding_server.py`, one model directory per instance.
///
/// `Mutex`-guarded because stdin/stdout are one conversation: a forward pass
/// is a single write-then-read-one-line round trip, and interleaving two
/// callers' requests on the same pipe would hand one of them the other's
/// response.
pub struct EmbeddingModel {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    stdout: Mutex<BufReader<ChildStdout>>,
    pub model_id: String,
    /// `ml/deployed/<model_id>/dataset_manifest.json`, typed and cross-checked
    /// at spawn time against what the worker process self-reports about the
    /// checkpoint it actually loaded.
    pub manifest: ModelManifest,
    /// Architecture the worker self-reported from `model.pt` at handshake
    /// time. `None` only if a deployed worker predates the handshake
    /// extension and never reported one — the manifest's own `architecture`
    /// field is what dispatch actually relies on; this is corroborating
    /// evidence, kept on the record for auditability.
    pub checkpoint_architecture: Option<String>,
}

impl EmbeddingModel {
    /// Spawn the worker for the model deployed at `ml/deployed/<model_id>/`.
    pub fn spawn(model_id: &str) -> Result<Self> {
        let model_dir = Path::new("ml/deployed").join(model_id);
        if !model_dir.join("model.pt").exists() {
            return Err(CareGraphError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "{} has no model.pt; train one first with ml/train_graphsage.py",
                    model_dir.display()
                ),
            )));
        }

        let python = std::env::var("CAREGRAPH_PYTHON").unwrap_or_else(|_| "python".to_string());
        let mut child = Command::new(&python)
            .arg("ml/embedding_server.py")
            .arg(&model_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(CareGraphError::Io)?;

        let stdin = child.stdin.take().expect("piped stdin");
        let mut stdout = BufReader::new(child.stdout.take().expect("piped stdout"));

        // Block until the worker reports it has loaded the model, so the first
        // real request is never the one paying PyTorch's import cost.
        let mut ready_line = String::new();
        stdout
            .read_line(&mut ready_line)
            .map_err(CareGraphError::Io)?;
        let handshake: ReadyHandshake =
            serde_json::from_str(ready_line.trim()).map_err(CareGraphError::MalformedValue)?;
        if !handshake.ready {
            return Err(CareGraphError::Io(std::io::Error::other(format!(
                "embedding_server.py did not report ready: {ready_line}"
            ))));
        }

        // Phase 9: load the manifest, then cross-check it against what the
        // worker just self-reported about the checkpoint it loaded from
        // model.pt. A missing manifest, or a manifest that disagrees with the
        // running process, is a hard error — the whole point of introspected
        // dispatch (Feature 4) is that atomic_commit.rs trusts this flag
        // instead of a per-ModelKind match, so it must not be silently wrong.
        let manifest = ModelManifest::load(model_id)?;
        let checkpoint_architecture = handshake.architecture.clone();
        if let Some(arch) = &handshake.architecture {
            if arch != &manifest.architecture {
                return Err(CareGraphError::Io(std::io::Error::other(format!(
                    "{model_id}: dataset_manifest.json declares architecture {:?}, \
                     but the running worker loaded model.pt as {arch:?}",
                    manifest.architecture
                ))));
            }
        }
        if let Some(assoc) = handshake.is_associative {
            if assoc != manifest.is_associative {
                return Err(CareGraphError::Io(std::io::Error::other(format!(
                    "{model_id}: dataset_manifest.json says is_associative={}, \
                     but the running worker's checkpoint self-reports {assoc}",
                    manifest.is_associative
                ))));
            }
        }

        Ok(EmbeddingModel {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            stdout: Mutex::new(stdout),
            model_id: model_id.to_string(),
            manifest,
            checkpoint_architecture,
        })
    }

    /// The spawned worker's OS process id, for callers that need to manage
    /// it from outside — e.g. `tests/fault_injection`, which kills a whole
    /// process tree and needs this process's own child to clean up
    /// precisely rather than by a broad `python.exe` name match.
    pub fn worker_pid(&self) -> Option<u32> {
        self.child.lock().ok().map(|c| c.id())
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
        let request = ForwardRequest {
            node_features,
            edge_index,
            target_indices,
            attribution: None,
        };
        let response = self.round_trip(&request)?;
        response.embeddings.ok_or_else(|| {
            CareGraphError::Io(std::io::Error::other(
                "embedding_server.py returned neither embeddings nor an error",
            ))
        })
    }

    /// A forward pass exactly like [`Self::forward`], plus an integrated-
    /// gradients edge attribution for each row in `attribution.targets`
    /// (Phase 9, Feature 1). Returns full embeddings for `target_indices` as
    /// always, plus one attribution entry per row the worker could compute a
    /// well-defined direction for (see
    /// `embedding_server.py::_attribute_one_target`'s delta_norm guard — a
    /// target whose embedding did not change is simply absent, not an error).
    pub fn forward_with_attribution(
        &self,
        node_features: &[Vec<f32>],
        edge_index: &[Vec<u32>],
        target_indices: &[usize],
        attribution: AttributionRequest<'_>,
    ) -> Result<(Vec<Vec<f32>>, Vec<RawAttributionEntry>)> {
        let request = ForwardRequest {
            node_features,
            edge_index,
            target_indices,
            attribution: Some(AttributionRequestWire {
                targets: attribution.targets,
                edge_index_before: attribution.edge_index_before,
                steps: attribution.steps,
                top_k: attribution.top_k,
            }),
        };
        let response = self.round_trip(&request)?;
        let embeddings = response.embeddings.ok_or_else(|| {
            CareGraphError::Io(std::io::Error::other(
                "embedding_server.py returned neither embeddings nor an error",
            ))
        })?;
        let attribution = response.attribution.ok_or_else(|| {
            CareGraphError::Io(std::io::Error::other(
                "embedding_server.py returned embeddings but no attribution for an attribution request",
            ))
        })?;
        Ok((embeddings, attribution))
    }

    /// Write one request line, read one response line, parse it, and surface
    /// a worker-reported error as `Err` — the round trip [`Self::forward`]
    /// and [`Self::forward_with_attribution`] both need, identically.
    fn round_trip(&self, request: &ForwardRequest<'_>) -> Result<ForwardResponse> {
        let line = serde_json::to_string(request).map_err(CareGraphError::MalformedValue)?;

        let mut stdin = self.stdin.lock().unwrap_or_else(|e| e.into_inner());
        writeln!(stdin, "{line}").map_err(CareGraphError::Io)?;
        stdin.flush().map_err(CareGraphError::Io)?;
        drop(stdin);

        let mut response_line = String::new();
        {
            let mut stdout = self.stdout.lock().unwrap_or_else(|e| e.into_inner());
            stdout
                .read_line(&mut response_line)
                .map_err(CareGraphError::Io)?;
        }
        if response_line.is_empty() {
            return Err(CareGraphError::Io(std::io::Error::other(
                "embedding_server.py closed its stdout — the worker process died",
            )));
        }

        let response: ForwardResponse =
            serde_json::from_str(response_line.trim()).map_err(CareGraphError::MalformedValue)?;
        if let Some(err) = response.error {
            return Err(CareGraphError::Io(std::io::Error::other(format!(
                "embedding_server.py: {err}"
            ))));
        }
        Ok(response)
    }
}

impl Drop for EmbeddingModel {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
        }
    }
}
