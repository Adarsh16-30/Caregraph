//! Logging and tracing setup, and W3C trace-context propagation.
//!
//! Logs go to stdout (JSON in production). Traces are exported over OTLP/gRPC when
//! `CAREGRAPH_OTEL_ENDPOINT` (or the standard `OTEL_EXPORTER_OTLP_ENDPOINT`) is set;
//! otherwise nothing is exported and the spans cost almost nothing.
//!
//! **No PHI in spans.** Span names are fixed stage names and the only attributes are
//! the RPC method, a model name, a worker index and counts. Node ids, properties,
//! embeddings and principals never go into a span, the same rule as for log lines
//! and metric labels.
//!
//! A caller's `traceparent` header is honoured: the server's span becomes a child of
//! the caller's, and that sampling decision is followed. Root traces (no caller
//! context) are sampled at `CAREGRAPH_OTEL_SAMPLE_RATIO`.

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::config::Config;

/// Flushes and shuts the trace exporter down when dropped (end of `main`).
pub struct Guard {
    #[cfg(feature = "otel")]
    provider: Option<opentelemetry_sdk::trace::TracerProvider>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some(provider) = self.provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("shutting down the trace exporter: {e}");
            }
        }
    }
}

/// Installs the global subscriber. Must run inside the tokio runtime (the exporter
/// batches on it). A trace exporter that cannot be built is logged and skipped:
/// observability must never stop the server from starting.
pub fn init(cfg: &Config) -> Guard {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "caregraph=info".into());
    let fmt = if cfg.log_json {
        tracing_subscriber::fmt::layer().json().boxed()
    } else {
        tracing_subscriber::fmt::layer().boxed()
    };
    let registry = tracing_subscriber::registry().with(filter).with(fmt);

    #[cfg(feature = "otel")]
    if let Some(endpoint) = &cfg.otel_endpoint {
        match build_provider(endpoint, &cfg.pod_name, cfg.otel_sample_ratio) {
            Ok(provider) => {
                use opentelemetry::trace::TracerProvider as _;
                let tracer = provider.tracer("caregraph");
                opentelemetry::global::set_tracer_provider(provider.clone());
                opentelemetry::global::set_text_map_propagator(
                    opentelemetry_sdk::propagation::TraceContextPropagator::new(),
                );
                registry
                    .with(tracing_opentelemetry::layer().with_tracer(tracer))
                    .init();
                tracing::info!(%endpoint, sample_ratio = cfg.otel_sample_ratio, "exporting traces over OTLP");
                return Guard {
                    provider: Some(provider),
                };
            }
            Err(e) => {
                registry.init();
                tracing::warn!(error = %e, %endpoint, "trace export disabled: the exporter could not be built");
                return Guard { provider: None };
            }
        }
    }

    registry.init();
    #[cfg(not(feature = "otel"))]
    if cfg.otel_endpoint.is_some() {
        tracing::warn!("an OTLP endpoint is configured but this build has no `otel` feature");
    }
    Guard {
        #[cfg(feature = "otel")]
        provider: None,
    }
}

#[cfg(feature = "otel")]
fn build_provider(
    endpoint: &str,
    pod: &str,
    ratio: f64,
) -> Result<opentelemetry_sdk::trace::TracerProvider, Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::{Sampler, TracerProvider};

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    Ok(TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            ratio,
        ))))
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            KeyValue::new("service.name", "caregraph"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("service.instance.id", pod.to_string()),
        ]))
        .build())
}

/// Makes `span` a child of the trace the caller sent in `traceparent` /
/// `tracestate`, if there is one.
#[cfg(feature = "otel")]
pub fn adopt_remote_parent(span: &tracing::Span, headers: &tonic::codegen::http::HeaderMap) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    struct Headers<'a>(&'a tonic::codegen::http::HeaderMap);
    impl opentelemetry::propagation::Extractor for Headers<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }
        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }
    let parent = opentelemetry::global::get_text_map_propagator(|p| p.extract(&Headers(headers)));
    span.set_parent(parent);
}

#[cfg(not(feature = "otel"))]
pub fn adopt_remote_parent(_span: &tracing::Span, _headers: &tonic::codegen::http::HeaderMap) {}
