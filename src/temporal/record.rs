//! Versioned value envelopes stored in the temporal column families.
//!
//! # Deletions are tombstones, not deletes
//!
//! A `RemoveEdge` must never issue a RocksDB `delete`. Removing the key would
//! erase the very history that point-in-time reconstruction reads: after a hard
//! delete, `as_of(T)` for a `T` *before* the removal would report the edge as
//! absent, which is wrong — the edge did exist then.
//!
//! Instead a removal appends a new version carrying `deleted: true` at the
//! removal's timestamp. Reconstruction at `T` finds the newest version at or
//! before `T` and reports absence only if that version is a tombstone. History
//! before the removal stays intact and queryable.
//!
//! The same reasoning applies to node retraction and to corrected records: the
//! timeline is append-only, and nothing is ever rewritten in place.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;

/// Default for `deleted` when absent from stored JSON.
fn not_deleted() -> bool {
    false
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// One version of a node, as stored in `CF_NODES`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeValue {
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub properties: Value,
    /// True if this version retracts the node.
    #[serde(default = "not_deleted", skip_serializing_if = "is_false")]
    pub deleted: bool,
}

impl NodeValue {
    pub fn new(node_type: impl Into<String>, properties: Value) -> Self {
        NodeValue {
            node_type: node_type.into(),
            properties,
            deleted: false,
        }
    }

    /// A tombstone retracting a node. `node_type` is retained so that a reader
    /// can tell *what* was retracted without walking further back.
    pub fn tombstone(node_type: impl Into<String>) -> Self {
        NodeValue {
            node_type: node_type.into(),
            properties: Value::Null,
            deleted: true,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("NodeValue is infallibly serializable")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// One version of an edge, as stored in `CF_EDGES` and `CF_REVERSE`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgeValue {
    #[serde(default)]
    pub properties: Value,
    /// True if this version removes the edge.
    #[serde(default = "not_deleted", skip_serializing_if = "is_false")]
    pub deleted: bool,
}

impl EdgeValue {
    pub fn new(properties: Value) -> Self {
        EdgeValue {
            properties,
            deleted: false,
        }
    }

    pub fn tombstone() -> Self {
        EdgeValue {
            properties: Value::Null,
            deleted: true,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("EdgeValue is infallibly serializable")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_live_value_omits_the_deleted_flag_on_the_wire() {
        let encoded = EdgeValue::new(json!({"severity": "stage-3"})).encode();
        let text = String::from_utf8(encoded).unwrap();
        assert!(
            !text.contains("deleted"),
            "live versions are the common case and should not pay for the flag"
        );
    }

    #[test]
    fn a_tombstone_round_trips() {
        let decoded = EdgeValue::decode(&EdgeValue::tombstone().encode()).unwrap();
        assert!(decoded.deleted);
    }

    #[test]
    fn a_value_written_without_the_flag_decodes_as_live() {
        // Traces predating the envelope carry a bare properties object.
        let decoded = EdgeValue::decode(br#"{"properties":{"dose":"500mg"}}"#).unwrap();
        assert!(!decoded.deleted);
        assert_eq!(decoded.properties, json!({"dose": "500mg"}));
    }

    #[test]
    fn node_values_round_trip_with_their_type() {
        let value = NodeValue::new("Patient", json!({"sex": "F"}));
        assert_eq!(NodeValue::decode(&value.encode()).unwrap(), value);
    }
}
