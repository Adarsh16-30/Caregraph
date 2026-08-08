//! Full care-graph snapshot reconstruction (PRD Phase 3).
//!
//! Answers the question the whole system exists for: *what did this patient's
//! care graph look like at time T?* — reading directly from the versioned
//! column families, with no recompute (PRD 3.2, step 10).
//!
//! A snapshot is the patient's own state at `as_of`, plus every live edge from
//! them at `as_of`, plus the state of each node those edges point at. It is
//! deliberately one hop: the clinical question is "what was true of this
//! patient", not "what was the whole cohort doing". Wider views go through
//! [`Traverser`](crate::graph::traversal::Traverser).

use std::collections::BTreeMap;

use crate::error::Result;
use crate::graph::limits::TraversalLimits;
use crate::graph::traversal::Truncation;
use crate::storage::KvStore;
use crate::temporal::index::{Edge, Node, TemporalIndex};
use crate::types::{EdgeType, NodeId, Timestamp};

/// One edge from the subject, together with whatever the node on the far end
/// looked like at the same instant.
#[derive(Clone, Debug)]
pub struct RelatedEntity {
    pub edge: Edge,
    /// The target node's state at `as_of`.
    ///
    /// `None` means the edge was live but the target had no version at or
    /// before `as_of`. On a well-formed load that should not happen — a
    /// reference node is written before anything points at it — so `None` is
    /// surfaced rather than silently dropped: it indicates the loader wrote an
    /// edge before its target, and hiding it would hide a data bug.
    pub node: Option<Node>,
}

/// A patient's care graph as it stood at a single timestamp.
#[derive(Clone, Debug)]
pub struct CareGraphSnapshot {
    /// The patient (or other subject) whose graph this is.
    pub subject: Node,
    pub as_of: Timestamp,
    pub related: Vec<RelatedEntity>,
    pub truncation: Truncation,
}

impl CareGraphSnapshot {
    /// Related entities grouped by relationship, which is how a clinician reads
    /// it: conditions together, medications together.
    pub fn by_edge_type(&self) -> BTreeMap<EdgeType, Vec<&RelatedEntity>> {
        let mut grouped: BTreeMap<EdgeType, Vec<&RelatedEntity>> = BTreeMap::new();
        for entity in &self.related {
            grouped.entry(entity.edge.edge_type).or_default().push(entity);
        }
        grouped
    }

    /// Targets of a single relationship type.
    pub fn targets_of(&self, edge_type: EdgeType) -> impl Iterator<Item = NodeId> + '_ {
        self.related
            .iter()
            .filter(move |e| e.edge.edge_type == edge_type)
            .map(|e| e.edge.dst)
    }

    /// Related entities whose target node could not be resolved. Empty on a
    /// well-formed graph.
    pub fn unresolved_targets(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.related
            .iter()
            .filter(|e| e.node.is_none())
            .map(|e| e.edge.dst)
    }
}

/// Reconstructs point-in-time care-graph snapshots.
pub struct SnapshotReader<'a, S: KvStore + ?Sized> {
    index: TemporalIndex<'a, S>,
    limits: TraversalLimits,
}

impl<'a, S: KvStore + ?Sized> SnapshotReader<'a, S> {
    pub fn new(store: &'a S, limits: TraversalLimits) -> Self {
        SnapshotReader {
            index: TemporalIndex::new(store),
            limits,
        }
    }

    /// The subject's full care graph at `as_of`, across every edge type.
    ///
    /// Returns `None` if the subject did not exist at `as_of` — either because
    /// they had not been enrolled yet, or because their record was retracted at
    /// or before that time.
    pub fn snapshot(
        &self,
        subject: NodeId,
        as_of: Timestamp,
    ) -> Result<Option<CareGraphSnapshot>> {
        self.snapshot_of_types(subject, as_of, &EdgeType::ALL)
    }

    /// As [`snapshot`](Self::snapshot), restricted to certain relationships.
    pub fn snapshot_of_types(
        &self,
        subject: NodeId,
        as_of: Timestamp,
        edge_types: &[EdgeType],
    ) -> Result<Option<CareGraphSnapshot>> {
        let Some(node) = self.index.node_as_of(subject, as_of)? else {
            return Ok(None);
        };

        let mut related = Vec::new();
        let mut truncation = Truncation::default();
        let budget = self.limits.max_result_edges;

        for &edge_type in edge_types {
            let mut edges = self.index.edges_as_of(subject, edge_type, as_of)?;

            if edges.len() > self.limits.max_neighbors_per_node {
                truncation.fanout_capped_nodes += 1;
                truncation.fanout_dropped_neighbors +=
                    edges.len() - self.limits.max_neighbors_per_node;
                edges.truncate(self.limits.max_neighbors_per_node);
            }

            for edge in edges {
                if related.len() >= budget {
                    truncation.hit_edge_limit = true;
                    break;
                }
                // Resolved at the same instant as the edge, so the snapshot is
                // internally consistent: a medication's label is the label it
                // had then, not the one it has now.
                let target = self.index.node_as_of(edge.dst, as_of)?;
                related.push(RelatedEntity { edge, node: target });
            }

            if truncation.hit_edge_limit {
                break;
            }
        }

        Ok(Some(CareGraphSnapshot {
            subject: node,
            as_of,
            related,
            truncation,
        }))
    }
}
