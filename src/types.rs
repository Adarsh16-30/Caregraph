//! Core value types shared by every layer of CareGraph.
//!
//! The byte-level representations here are load-bearing: `src/temporal/keys.rs`
//! (PRD Section 9.1, provided verbatim) depends on `NodeId::to_be_bytes`,
//! `EdgeType::to_be_bytes`, and `Timestamp::as_u64` existing with exactly these
//! signatures. Changing a width here changes the on-disk key format.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable identifier for a node in the clinical graph.
///
/// Big-endian encoded in keys so that RocksDB's lexicographic byte ordering
/// matches numeric ordering, which is what makes prefix scans coherent.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl NodeId {
    pub const WIDTH: usize = 8;

    #[inline]
    pub fn to_be_bytes(self) -> [u8; Self::WIDTH] {
        self.0.to_be_bytes()
    }

    #[inline]
    pub fn from_be_bytes(bytes: [u8; Self::WIDTH]) -> Self {
        NodeId(u64::from_be_bytes(bytes))
    }

    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Relationship kinds in the IDPIP/UKPDS-derived care-pathway graph (PRD 2.6).
///
/// Discriminants are part of the on-disk key format and must never be
/// renumbered — only appended to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u16)]
pub enum EdgeType {
    DiagnosedWith = 1,
    PrescribedMedication = 2,
    UnderwentProcedure = 3,
    TreatedByProvider = 4,
    HasLabResult = 5,
    HasEncounter = 6,
}

impl EdgeType {
    pub const WIDTH: usize = 2;

    #[inline]
    pub fn to_be_bytes(self) -> [u8; Self::WIDTH] {
        (self as u16).to_be_bytes()
    }

    pub fn from_u16(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(EdgeType::DiagnosedWith),
            2 => Some(EdgeType::PrescribedMedication),
            3 => Some(EdgeType::UnderwentProcedure),
            4 => Some(EdgeType::TreatedByProvider),
            5 => Some(EdgeType::HasLabResult),
            6 => Some(EdgeType::HasEncounter),
            _ => None,
        }
    }
}

/// A point on the version timeline: microseconds since the Unix epoch.
///
/// Microsecond resolution keeps a full 20-year UKPDS follow-up (PRD 6.1)
/// well inside u64 while still separating mutations that land in the same
/// millisecond during a high-throughput ingest replay.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const WIDTH: usize = 8;

    /// The largest representable timestamp — inverts to all-zero bytes, so it
    /// is the correct seek target for "the newest version of anything".
    pub const MAX: Timestamp = Timestamp(u64::MAX);

    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn now() -> Self {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before the Unix epoch")
            .as_micros();
        Timestamp(micros as u64)
    }

    pub fn from_unix_micros(micros: u64) -> Self {
        Timestamp(micros)
    }

    pub fn from_unix_seconds(secs: u64) -> Self {
        Timestamp(secs.saturating_mul(1_000_000))
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({}us)", self.0)
    }
}

/// Which GNN produced an embedding (PRD 2.3 / Section 4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ModelKind {
    GraphSAGE = 1,
    GCN = 2,
    GAT = 3,
}

impl ModelKind {
    /// Whether this model's neighbourhood aggregation is associative, which is
    /// what decides between the `IncrementalAggregator` and `GATUpdatePath`
    /// branches of the pipeline (PRD 4.1).
    pub fn is_associative(self) -> bool {
        matches!(self, ModelKind::GraphSAGE | ModelKind::GCN)
    }
}

/// Which code path actually produced an embedding update.
///
/// Persisted alongside every embedding so that a `Fallback` can never be
/// silently reported as incremental (Rule 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ComputationPath {
    Associative = 1,
    GatConstrained = 2,
    Fallback = 3,
}

impl ComputationPath {
    pub fn as_str(self) -> &'static str {
        match self {
            ComputationPath::Associative => "associative",
            ComputationPath::GatConstrained => "gat_constrained",
            ComputationPath::Fallback => "fallback",
        }
    }
}

/// A versioned node embedding as stored in `CF_EMBEDDINGS` (PRD 3.3).
///
/// The stored value carries its provenance — model identity and computation
/// path — so Rule 3 and Rule 7 are auditable directly from the database rather
/// than from application logs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Embedding {
    pub vector: Vec<f32>,
    /// Identifies the exact trained model, matching the `model_id` recorded in
    /// the embedding store's `dataset_manifest.json` (Rule 3).
    pub model_id: String,
    pub model_kind: ModelKind,
    pub computation_path: ComputationPath,
}

impl Embedding {
    pub fn new(
        vector: Vec<f32>,
        model_id: impl Into<String>,
        model_kind: ModelKind,
        computation_path: ComputationPath,
    ) -> Self {
        Embedding {
            vector,
            model_id: model_id.into(),
            model_kind,
            computation_path,
        }
    }

    pub fn dim(&self) -> usize {
        self.vector.len()
    }

    /// Value encoding for `CF_EMBEDDINGS`. Called by `atomic_commit`
    /// (PRD Section 9.2).
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Embedding is infallibly serializable")
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
