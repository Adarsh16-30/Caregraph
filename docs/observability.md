# Observability: metrics, alerts, dashboards, traces, logs

## Metrics

Every pod serves Prometheus metrics on the HTTP port (`:9100/metrics`) beside the
probes (`/healthz`, `/readyz`, `/startupz`) and `/role`. The series, by area:

| Area | Series |
|---|---|
| Requests | `rpc_requests_total{rpc,code}`, `rpc_duration_seconds{rpc}` (from a tower layer around every gRPC service; the code is the response's `grpc-status`, so an error after a stream has started is not seen) |
| Writes | `mutations_total`, `mutation_latency_seconds`, `embedding_update_latency_seconds{computation_path}`, `commit_wait_seconds`, `mutation_conflict_retries_total`, `incremental_fallback_total` |
| Deferral and repair | `embedding_updates_deferred_total`, `repair_pending`, `repairs_completed_total`, `repair_failures_total`, `repair_batch_seconds` |
| Reads | `point_in_time_query_seconds`, `traversal_latency_seconds`, `similarity_delta_seconds` |
| Workers | `workers_configured`, `worker_healthy`, `breaker_open`, `worker_restarts_total`, `worker_timeouts_total`, `worker_failures_total` (all `{model}`) |
| Replication | `replication_epoch`, `replication_applied_sequence`, `replication_lag_sequences`, `replication_connected_followers`, `replication_sync_degraded`, `replication_needs_bootstrap`, `replication_batches_applied_total`, `replication_degraded_writes_total`, `replication_ack_timeouts_total`, `replication_ack_wait_seconds` |
| Security and audit | `tls_cert_expiry_seconds`, `tls_reloads`, `tls_reload_failures`, `audit_chain_sequence{chain}`, `audit_queue_full_total`, `audit_dropped_total`, `audit_last_anchor_timestamp_seconds` |
| Backup | `backup_last_success_timestamp`, `backup_failures_total` |
| Memory | `memory_budget_bytes`, `memory_planned_bytes{component}`, `process_resident_memory_bytes`, `worker_resident_memory_bytes{model}`, `tree_resident_memory_bytes` |

Disk usage comes from the kubelet (`kubelet_volume_stats_*`), not from CareGraph.

**No identifiers in metrics.** Labels are RPC names, status codes, model names, chain
names and component names. Never a node id, a principal or a patient.

## Alerts

`observability/prometheus/alert-rules.yaml` (30 alerts in seven groups: availability,
latency, workers, replication, storage, security, backup). The chart installs the same
file as a `PrometheusRule` (`metrics.prometheusRule.enabled`). Each alert has a
severity (`page` or `ticket`) and links the runbook that says what to do.

`scripts/check_alert_rules.py` (CI) checks that every metric an alert, recording rule
or dashboard query uses is one the server registers or a kubelet series on its
allowlist, that every alert's runbook exists, and that the chart's copy equals the
source. A renamed metric therefore fails the build instead of leaving an alert that can
never fire.

The thresholds are starting points taken from the measurements in
`docs/benchmark_report.md` on one machine and one graph. Tune them on your traffic.
What each one is meant to catch:

| Alert | Fires when |
|---|---|
| `CareGraphDown` | a pod is not being scraped for 2 minutes |
| `CareGraphErrorRatioHigh` | over 2% of requests end `INTERNAL`/`UNAVAILABLE`/`DATA_LOSS`/`UNKNOWN` for 10 minutes (back-pressure and client errors do not count) |
| `CareGraphReadLatencyHigh` / `WriteLatencyHigh` | read p99 > 250 ms / write p99 > 2 s for 10 minutes |
| `CareGraphWorkerRestarting`, `BreakerOpen`, `WorkersUnhealthy` | embedding workers are failing; writes to that model fail fast while the breaker is open |
| `CareGraphReplicationLag`, `ReplicaNeedsBootstrap`, `SyncReplicationDegraded`, `NoStandbyConnected`, `EpochDisagreement` | the standby or a replica is behind, gone or confused; **degraded** means writes are acknowledged without a standby (RPO > 0) |
| `CareGraphDiskFilling`, `DiskAlmostFull`, `DiskFillsInFourHours` | a volume is over 80% / 92% or will fill within four hours |
| `CareGraphCertExpiresSoon` / `VerySoon`, `CertReloadFailing` | a serving certificate has under 21 / 3 days left, or a rotation failed to load |
| `CareGraphReadAuditSaturated`, `AuditEventsDropped`, `AuditAnchorStale`, `AccessDenialsSpiking` | reads are being refused because audit cannot keep up; denial events were dropped; the chain has not been anchored for 10 minutes; probing |
| `CareGraphBackupStale`, `BackupFailing` | no successful backup in 26 hours, or a backup failed |

## Dashboards

`observability/grafana/dashboards/`, bound to the provisioned datasource
`caregraph-prometheus`:

* `caregraph_mutations_workers.json` — write rate and latency, commit wait, conflicts,
  fallbacks, worker health, breaker, request rate and latency by method.
* `caregraph_replication.json` — lag, epoch, followers, synchronous-replication state and
  its latency cost.
* `caregraph_security_audit.json` — certificate lifetime, audit chains and anchoring,
  denials, backup age, memory against the budget, volume use.
* `caregraph_section1.json` — the original query-path targets.

The chart ships them as ConfigMaps for the Grafana sidecar
(`metrics.grafanaDashboards.enabled`). Every query is checked by
`check_alert_rules.py`, and `scripts/check_rules.sh` (Rule 9) fails on any placeholder.

## Traces

Set `CAREGRAPH_OTEL_ENDPOINT` (or the standard `OTEL_EXPORTER_OTLP_ENDPOINT`) to an
OTLP/gRPC collector, for example `http://otel-collector:4317`, and
`CAREGRAPH_OTEL_SAMPLE_RATIO` (default 0.05) for the share of new traces kept. Without
an endpoint nothing is exported and spans are almost free. A build without the `otel`
Cargo feature has no exporter at all.

A request's trace:

```
rpc (method)
├─ admission            idempotency check, ticket in the commit order
├─ resolve              the affected subgraph
├─ build_input          features and edges for the model
├─ forward (model)      the Python worker
├─ commit_wait          waiting for earlier timestamps to commit
├─ write                the single atomic batch (and the standby's confirmation)
└─ audit                staging the audit record into the batch
```

A caller's W3C `traceparent` is honoured: the server's span becomes a child of it and
its sampling decision is followed, so a trace started in a client continues through
CareGraph. Verified by `tests/embedding/tracing_spans_test.rs` (with the real model:
every stage span exists and nests under the request) and `tests/tracing_export`
(a stand-in OTLP/gRPC collector receives the exported span, which carries the
caller's trace id and has the caller's span as its parent, and a resource naming the
service and pod). The cost of tracing on request latency has not been measured.

**What spans never carry.** Node ids, properties, embeddings, principals and payloads.
The only attributes are the RPC method and, on `forward`, the model name. The embedding
test fails if a node id, the caller's identity or a property value appears anywhere in
a span.

## Logs

JSON on stdout in production (`CAREGRAPH_LOG_JSON`), one object per line, filtered by
`RUST_LOG` (default `caregraph=info`). Worker stderr is re-emitted as JSON tagged with
model and worker. The `audit` target carries audit records and the periodic
`audit_anchor` lines for a SIEM, and is the one deliberate place that names nodes;
protect it like the audit database. Internal errors returned to clients are a generic
message plus a reference id; the detail is in the log under that id.
