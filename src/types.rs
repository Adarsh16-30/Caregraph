//! Core value types shared by every layer of CareGraph.
//!
//! The byte-level representations here are load-bearing: `src/temporal/keys.rs`
//! depends on `NodeId::to_be_bytes`,
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

/// Relationship kinds in the IDPIP/UKPDS-derived care-pathway graph.
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

    /// Every edge type, in discriminant order. Traversal uses this when a
    /// request does not name specific types.
    pub const ALL: [EdgeType; 6] = [
        EdgeType::DiagnosedWith,
        EdgeType::PrescribedMedication,
        EdgeType::UnderwentProcedure,
        EdgeType::TreatedByProvider,
        EdgeType::HasLabResult,
        EdgeType::HasEncounter,
    ];

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
/// Microsecond resolution keeps a full 20-year UKPDS follow-up
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

/// Which GNN produced an embedding.
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
    /// branches of the pipeline.
    pub fn is_associative(self) -> bool {
        matches!(self, ModelKind::GraphSAGE | ModelKind::GCN)
    }
}

/// Which code path actually produced an embedding update.
///
/// Persisted alongside every embedding so that a `Fallback` can never be
/// silently reported as incremental. `#[repr(u8)]` discriminants are
/// part of this on-disk format and must never be renumbered — only appended
/// to, exactly like [`EdgeType`]'s own discriminants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ComputationPath {
    Associative = 1,
    GatConstrained = 2,
    Fallback = 3,
    /// Phase 9: a manifest-declared non-associative aggregation that isn't
    /// GAT specifically. `GatConstrained` is kept as its own discriminant
    /// (rather than folded into this one) so every embedding stored before
    /// Phase 9 keeps its exact original meaning; this variant only ever
    /// applies to a model deployed after the manifest-driven dispatch in
    /// `atomic_commit.rs` decided its architecture wasn't `"GAT"`.
    NonAssociative = 4,
}

impl ComputationPath {
    pub fn as_str(self) -> &'static str {
        match self {
            ComputationPath::Associative => "associative",
            ComputationPath::GatConstrained => "gat_constrained",
            ComputationPath::Fallback => "fallback",
            ComputationPath::NonAssociative => "non_associative",
        }
    }
}

/// A versioned node embedding as stored in `CF_EMBEDDINGS`.
///
/// The stored value carries its provenance — model identity and computation
/// path — so model provenance and fallback behaviour are auditable directly from the database rather
/// than from application logs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Embedding {
    pub vector: Vec<f32>,
    /// Identifies the exact trained model, matching the `model_id` recorded in
    /// the embedding store's `dataset_manifest.json`.
    pub model_id: String,
    pub model_kind: ModelKind,
    pub computation_path: ComputationPath,
}

/// Leading byte of the binary `CF_EMBEDDINGS` value format (see
/// [`Embedding::serialize`]).
const EMBEDDING_FORMAT_V2: u8 = 0x01;

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

    /// Value encoding for `CF_EMBEDDINGS`. Called by `atomic_commit`.
    ///
    /// Binary format v2: `0x01 | model_kind u8 | computation_path u8 |
    /// model_id_len u16 LE | model_id | dim u32 LE | dim × f32 LE`. A JSON
    /// document always starts with `{`, so the leading tag byte tells the two
    /// formats apart and [`deserialize`](Self::deserialize) reads both — a
    /// database written before v2 needs no migration. Decoding v2 is a copy;
    /// decoding the JSON form is a float parser, and the similarity scan
    /// decodes every node's embedding.
    pub fn serialize(&self) -> Vec<u8> {
        let id = self.model_id.as_bytes();
        let mut out = Vec::with_capacity(1 + 2 + 2 + id.len() + 4 + self.vector.len() * 4);
        out.push(EMBEDDING_FORMAT_V2);
        out.push(self.model_kind as u8);
        out.push(self.computation_path as u8);
        out.extend_from_slice(&(id.len() as u16).to_le_bytes());
        out.extend_from_slice(id);
        out.extend_from_slice(&(self.vector.len() as u32).to_le_bytes());
        for v in &self.vector {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        use serde::de::Error as _;
        if bytes.first() != Some(&EMBEDDING_FORMAT_V2) {
            return serde_json::from_slice(bytes);
        }
        let bad = |what: &str| serde_json::Error::custom(format!("embedding v2: {what}"));
        let body = &bytes[1..];
        let [kind, path, l0, l1, rest @ ..] = body else {
            return Err(bad("truncated header"));
        };
        let id_len = u16::from_le_bytes([*l0, *l1]) as usize;
        if rest.len() < id_len + 4 {
            return Err(bad("truncated model id"));
        }
        let (id, rest) = rest.split_at(id_len);
        let model_id = std::str::from_utf8(id)
            .map_err(|_| bad("model id is not utf-8"))?
            .to_string();
        let dim = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        let payload = &rest[4..];
        if payload.len() != dim * 4 {
            return Err(bad("vector length does not match dim"));
        }
        let vector = payload
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        let model_kind = match kind {
            1 => ModelKind::GraphSAGE,
            2 => ModelKind::GCN,
            3 => ModelKind::GAT,
            _ => return Err(bad("unknown model kind")),
        };
        let computation_path = match path {
            1 => ComputationPath::Associative,
            2 => ComputationPath::GatConstrained,
            3 => ComputationPath::Fallback,
            4 => ComputationPath::NonAssociative,
            _ => return Err(bad("unknown computation path")),
        };
        Ok(Embedding {
            vector,
            model_id,
            model_kind,
            computation_path,
        })
    }
}

#[cfg(test)]
mod embedding_format_tests {
    use super::*;

    fn sample() -> Embedding {
        Embedding::new(
            vec![0.0, -1.5, 3.25e-7, f32::MAX, f32::MIN_POSITIVE, 1.0 / 3.0],
            "diabetes130_graphsage",
            ModelKind::GAT,
            ComputationPath::GatConstrained,
        )
    }

    #[test]
    fn binary_v2_round_trips_bit_exactly() {
        let e = sample();
        let bytes = e.serialize();
        assert_eq!(bytes[0], EMBEDDING_FORMAT_V2);
        let back = Embedding::deserialize(&bytes).unwrap();
        assert_eq!(back, e);
        for (a, b) in back.vector.iter().zip(&e.vector) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn values_written_as_json_before_v2_still_decode() {
        let e = sample();
        let legacy = serde_json::to_vec(&e).unwrap();
        assert_eq!(legacy[0], b'{');
        assert_eq!(Embedding::deserialize(&legacy).unwrap(), e);
    }

    #[test]
    fn truncated_or_inconsistent_v2_values_are_errors_not_panics() {
        let bytes = sample().serialize();
        for cut in 1..bytes.len() {
            assert!(
                Embedding::deserialize(&bytes[..cut]).is_err(),
                "a v2 value cut to {cut} bytes must not decode"
            );
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(Embedding::deserialize(&extra).is_err());
    }

    #[test]
    fn v2_is_smaller_than_json() {
        let e = Embedding::new(
            (0..64).map(|i| i as f32 * 0.123_456_79).collect(),
            "m",
            ModelKind::GraphSAGE,
            ComputationPath::Associative,
        );
        assert!(e.serialize().len() < serde_json::to_vec(&e).unwrap().len() / 2);
    }
}
