# Runbook: embedding workers restarting, breaker open, writes failing or slow

Applies to the alerts `CareGraphWorkerRestarting`, `CareGraphBreakerOpen`,
`CareGraphWorkersUnhealthy`, `CareGraphErrorRatioHigh`, `CareGraphLoadShedding`,
`CareGraphWriteLatencyHigh`, `CareGraphReadLatencyHigh`, `CareGraphCommitWaitHigh`,
`CareGraphConflictRetriesHigh` and `CareGraphIncrementalFallbackHigh`.

Every write runs a forward pass in a Python worker (one or more per model). A pool
supervises them: a worker that dies or exceeds its deadline is killed and respawned;
after `CAREGRAPH_BREAKER_FAILURES` (default 5) failures inside 60 s the model's
**breaker opens** for `CAREGRAPH_BREAKER_COOLDOWN_SECS` (default 15) and writes to that
model fail fast with `UNAVAILABLE`, and `/readyz` goes false so the pod leaves the
Service. Reads never use the workers and keep working.

## 1. What is failing

```
kubectl -n NS get pods -L caregraph.io/role -l app.kubernetes.io/component=writer
kubectl -n NS logs POD -c caregraph --since=15m | grep -E 'worker|breaker|forward'
kubectl -n NS describe pod POD | grep -A6 'Last State'
```

| What you see | Meaning | Go to |
|---|---|---|
| `Last State: Terminated, Reason: OOMKilled` | The container hit its memory limit. | 2 |
| `worker died` / `exceeded N s; the worker was killed` in the log | A worker crashed or a forward pass was too slow. | 3 |
| `model checksum` / `manifest` / `handshake` errors at spawn | The model on disk is not the one the manifest describes. | 4 |
| `breaker OPEN`, no crashes before it | A burst of timeouts. | 3 |
| Workers healthy, `commit_wait_seconds` high | One slow write is holding the ordered commit. | 5 |
| Workers healthy, `RESOURCE_EXHAUSTED` rising | Real overload. | 6 |

## 2. Out of memory

The writer's budget is `CAREGRAPH_MEMORY_MB` for the **whole process tree**; the
container limit must sit above it (`docs/memory.md`). Measured on the reference graph:
about 733 MB idle and 968 MB after load with both models, about 330 MB per worker.

```
kubectl -n NS top pod POD --containers
```

- If `tree_resident_memory_bytes` is close to `memory_budget_bytes`, raise
  `writer.memoryMB` and the container limit together, or serve one model
  (`writer.models.gat=false`: about 370 MB less).
- If the container limit is below the tree's real size, raise the limit. A worker
  grows by about 70 MB under write load (not enforced by the budget).
- A larger `CAREGRAPH_WORKERS` multiplies the worker cost; do not raise it without
  raising memory.

## 3. Workers dying or timing out

1. Look at the worker's stderr in the pod log: it is re-emitted as JSON lines tagged
   with the model and worker.
2. CPU starvation looks like timeouts: check `kubectl top`. The chart sets
   `CAREGRAPH_TORCH_THREADS=1`; `workers x threads` must not exceed the CPU limit (the
   server refuses to start otherwise).
3. A forward pass that legitimately takes longer than `CAREGRAPH_FORWARD_TIMEOUT_SECS`
   (default 120) is killed. A hub node with a very large neighbourhood is the usual
   cause; the receptive-field caps bound it, and `incremental_fallback_total` shows how
   often a full recompute was needed.
4. If workers die immediately after spawn, the breaker will keep reopening. Restart
   the pod only after fixing the cause: a restart reloads the model, it does not fix a
   bad model or too little memory.

While the breaker is open, mutations return `UNAVAILABLE`; clients that retry with a
`request_id` are safe to retry. Edges committed while a forward pass failed are stored
with a stale embedding and listed by `caregraph-admin repair-status`. Nothing re-embeds
them automatically (an automatic repair is not implemented); run `repair-status` after
an incident to see how many there are.

## 4. Model integrity failure

Each spawn verifies `sha256(model.pt)` against the manifest and cross-checks the
architecture the worker reports. A mismatch means the image contains a model that
does not match its manifest. Do not work around it (there is no override on purpose):
roll back to the previous image, then rebuild the image from a clean checkout.

## 5. Writes queueing behind one slow write

Writes commit in timestamp order, so one slow forward pass delays the ones after it
(`commit_wait_seconds`). `explain` requests are the slowest by two to three orders of
magnitude and run one at a time; if `explain` is not needed, set
`CAREGRAPH_EXPLAIN_ENABLED=false`. A high `mutation_conflict_retries_total` ratio
means concurrent writes keep touching the same hub nodes; it costs a second compute
for the affected writes and is not an error.

## 6. Overload

`RESOURCE_EXHAUSTED` means a limit did its job: the write or read pool is full, or a
principal exceeded its rate. Raise `CAREGRAPH_MAX_CONCURRENT_WRITES` only if the CPU
has room; writes saturate near the worker pool's throughput (about 16 writes/s per
GraphSAGE worker on the reference machine), and more concurrency only lengthens the
queue. To add write capacity add workers *and* CPU and memory; to add read capacity
add read replicas.

## After it is fixed

Confirm `worker_healthy` equals `workers_configured`, `breaker_open` is 0 and
`/readyz` is true; send one write and one read. If the pod was the leader and stayed
down, see `failover.md` before it comes back as a second writer.
