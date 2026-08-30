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
use serde_json::json;

use crate::error::{CareGraphError, Result};

#[derive(Serialize)]
struct ForwardRequest<'a> {
    node_features: &'a [Vec<f32>],
    edge_index: &'a [Vec<u32>],
    target_indices: &'a [usize],
}

#[derive(Deserialize)]
struct ForwardResponse {
    embeddings: Option<Vec<Vec<f32>>>,
    error: Option<String>,
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
        let ready: serde_json::Value =
            serde_json::from_str(ready_line.trim()).map_err(CareGraphError::MalformedValue)?;
        if ready.get("ready") != Some(&serde_json::Value::Bool(true)) {
            return Err(CareGraphError::Io(std::io::Error::other(format!(
                "embedding_server.py did not report ready: {ready_line}"
            ))));
        }

        Ok(EmbeddingModel {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            stdout: Mutex::new(stdout),
            model_id: model_id.to_string(),
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
        };
        let line = serde_json::to_string(&request).map_err(CareGraphError::MalformedValue)?;

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
        response.embeddings.ok_or_else(|| {
            CareGraphError::Io(std::io::Error::other(
                "embedding_server.py returned neither embeddings nor an error",
            ))
        })
    }
}

impl Drop for EmbeddingModel {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
        }
    }
}

/// Written to `dataset_manifest.json` by the trainer; read back here so the
/// deployed manifest and the running worker can be checked against each other
/// (Rule 3's manifest requirement, from the serving side).
pub fn manifest_json(model_id: &str) -> serde_json::Value {
    let path = Path::new("ml/deployed")
        .join(model_id)
        .join("dataset_manifest.json");
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| json!({}))
}
