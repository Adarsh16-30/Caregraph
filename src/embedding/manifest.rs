//! Typed reader for `ml/deployed/<model_id>/dataset_manifest.json` (Phase 9).
//!
//! Replaces `model_bridge::manifest_json`, which was dead code (zero call
//! sites), returned an untyped `serde_json::Value`, and silently degraded to
//! `{}` on any read or parse failure. The manifest already carries the exact
//! fact Feature 4 needs — `ml/train_graphsage.py` and `ml/train_gat.py` both
//! write a literal `"is_associative"` boolean — so this module's job is to
//! read it typed and treat its absence as a hard error, not a default.

use std::path::Path;

use serde::Deserialize;

use crate::error::{CareGraphError, Result};

/// `ml/deployed/<model_id>/dataset_manifest.json`, typed. Extra JSON fields
/// (`dataset`, `epochs`, `trained_at`, ...) are ignored, not rejected — this
/// reader only needs the fields that govern dispatch.
#[derive(Debug, Clone, Deserialize)]
pub struct ModelManifest {
    pub model_id: String,
    pub architecture: String,
    pub aggregation: String,
    pub is_associative: bool,
    pub layers: u32,
    pub embedding_dim: u32,
    #[serde(default)]
    pub attention_heads: Option<u32>,
    pub model_sha256: String,
}

impl ModelManifest {
    /// Load and parse the manifest for `model_id`. A missing file or a file
    /// that fails to parse is a hard error at spawn time — Rule 7 is about
    /// silent fallback in the embedding-update path, and a dispatch decision
    /// built on a manifest that quietly defaulted to `{}` would be exactly
    /// that kind of silent fallback one layer up.
    pub fn load(model_id: &str) -> Result<Self> {
        let path = Path::new("ml/deployed")
            .join(model_id)
            .join("dataset_manifest.json");
        let raw = std::fs::read_to_string(&path).map_err(|e| {
            CareGraphError::Io(std::io::Error::new(
                e.kind(),
                format!("reading {}: {e}", path.display()),
            ))
        })?;
        let manifest: ModelManifest = serde_json::from_str(&raw).map_err(|e| {
            CareGraphError::Io(std::io::Error::other(format!("{}: {e}", path.display())))
        })?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_deployed_manifests_parse_and_disagree_on_associativity() {
        // Exercises the real files on disk rather than a fabricated fixture —
        // if either deployed manifest's shape drifts, this test is the first
        // thing to notice.
        let sage = ModelManifest::load("diabetes130_graphsage").expect("graphsage manifest");
        assert!(sage.is_associative);
        assert_eq!(sage.aggregation, "mean");
        assert_eq!(sage.attention_heads, None);

        let gat = ModelManifest::load("diabetes130_gat").expect("gat manifest");
        assert!(!gat.is_associative);
        assert_eq!(gat.attention_heads, Some(4));
    }

    #[test]
    fn a_nonexistent_model_id_is_a_hard_error_not_an_empty_default() {
        assert!(ModelManifest::load("does_not_exist_______").is_err());
    }
}
