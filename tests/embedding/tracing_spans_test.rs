//! A write emits one span per pipeline stage, nested under the request's span, and
//! those spans carry no identifiers (production-hardening P9).
//!
//! Runs the real pipeline against the real model and a real RocksDB, with a capturing
//! subscriber installed for the one call.

use std::sync::{Arc, Mutex};

use caregraph::config::TsPolicy;
use caregraph::embedding::caps::CapController;
use caregraph::embedding::metrics::EmbeddingMetrics;
use caregraph::embedding::pipeline::{run_mutation_pipeline, AuditContext};
use caregraph::embedding::sequencer::Sequencer;
use caregraph::embedding::state::GraphMutation;
use caregraph::embedding::worker::pool::PoolConfig;
use caregraph::embedding::EmbeddingModel;
use caregraph::security::audit::MutationAudit;
use caregraph::storage::{KvStore, RocksKv};
use caregraph::temporal::record::{EdgeValue, NodeValue};
use caregraph::temporal::TemporalWriter;
use caregraph::types::{EdgeType, ModelKind, NodeId, Timestamp};
use rocksdb::WriteBatch;
use serde_json::json;
use tempfile::TempDir;
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::{Id, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[derive(Debug, Clone)]
struct Recorded {
    name: &'static str,
    parent: Option<&'static str>,
    fields: Vec<(String, String)>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Recorded>>>);

struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span exists");
        let mut fields = Fields(Vec::new());
        attrs.record(&mut fields);
        self.0.lock().unwrap().push(Recorded {
            name: span.name(),
            parent: span.parent().map(|p| p.name()),
            fields: fields.0,
        });
    }
}

#[test]
fn a_write_emits_a_span_per_stage_under_the_request_and_no_identifiers() {
    // Ids and a principal that would be easy to spot if they leaked into a span.
    const SRC: u64 = 7_777_001;
    const DST: u64 = 7_777_002;
    const PRINCIPAL: &str = "spiffe://caregraph/secret-principal";

    let model = EmbeddingModel::spawn_with(
        "diabetes130_graphsage",
        PoolConfig {
            workers: 1,
            torch_threads: 1,
            ..PoolConfig::default()
        },
    )
    .unwrap();
    let dir = TempDir::new().unwrap();
    let store = RocksKv::open(dir.path().join("db")).unwrap();
    {
        let writer = TemporalWriter::new(&store).unwrap();
        let mut batch = WriteBatch::default();
        writer.put_node(
            &mut batch,
            NodeId(SRC),
            Timestamp(0),
            &NodeValue::new("Patient", json!({})),
        );
        writer.put_node(
            &mut batch,
            NodeId(DST),
            Timestamp(0),
            &NodeValue::new("Condition", json!({})),
        );
        drop(writer);
        store.write(batch).unwrap();
    }
    let registry = prometheus::Registry::new();
    let metrics = EmbeddingMetrics::new(&registry).unwrap();
    let caps = Mutex::new(CapController::pinned(512, 1_500));
    let sequencer = Sequencer::open(&store, TsPolicy::Strict).unwrap();
    let audit = MutationAudit::open(&store).unwrap();

    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        tracing::info_span!("rpc", rpc.method = "AddEdge").in_scope(|| {
            run_mutation_pipeline(
                GraphMutation::AddEdge {
                    src: NodeId(SRC),
                    dst: NodeId(DST),
                    edge_type: EdgeType::DiagnosedWith,
                    ts: Timestamp(0),
                },
                &EdgeValue::new(json!({"note": "free text that must not appear"})),
                ModelKind::GraphSAGE,
                &store,
                &model,
                &metrics,
                &caps,
                &sequencer,
                None,
                false,
                Some(AuditContext {
                    chain: &audit,
                    principal: PRINCIPAL,
                    rpc: "AddEdge",
                }),
            )
            .unwrap();
        });
    });

    let spans = capture.0.lock().unwrap().clone();
    let names: Vec<&str> = spans.iter().map(|s| s.name).collect();
    for stage in [
        "admission",
        "resolve",
        "build_input",
        "forward",
        "commit_wait",
        "write",
        "audit",
    ] {
        let found: Vec<_> = spans.iter().filter(|s| s.name == stage).collect();
        assert!(!found.is_empty(), "no `{stage}` span; got {names:?}");
        assert!(
            found.iter().all(|s| s.parent == Some("rpc")),
            "`{stage}` must nest under the request span: {found:?}"
        );
    }
    let forward = spans.iter().find(|s| s.name == "forward").unwrap();
    assert!(
        forward
            .fields
            .iter()
            .any(|(k, v)| k == "model" && v.contains("diabetes130_graphsage")),
        "the forward span names its model: {forward:?}"
    );

    // Nothing that identifies a patient, a caller or a payload is in any span.
    let everything = format!("{spans:?}");
    for secret in [
        SRC.to_string(),
        DST.to_string(),
        PRINCIPAL.to_string(),
        "free text".to_string(),
    ] {
        assert!(
            !everything.contains(&secret),
            "{secret:?} leaked into a span: {everything}"
        );
    }
}
