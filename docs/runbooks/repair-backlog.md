# Runbook: embeddings waiting for repair

Applies to the alerts `CareGraphRepairBacklog` and `CareGraphRepairFailing`.

A node's embedding can be left behind when a write commits, and each such node has a
durable marker in `CF_REPAIR`, written in the same atomic batch as the write. There are
two reasons, and both are consumed by the **repair worker**, a background thread in the
writer that recomputes the embedding as of the marker's timestamp and writes it in its
own atomic batch:

* **Hub deferral** (`CAREGRAPH_DEFER_HUB_DEGREE`). A write that touches a node with more
  neighbours than this does not recompute that node or, if it is an endpoint, its
  neighbours. That is the expensive part of a write on a shared reference node. The
  `embedding_updates_deferred_total` counter counts them. This is by design, not a fault.
* **A model failure.** The forward pass failed on this input, the write still committed
  (`CAREGRAPH_COMMIT_WITHOUT_EMBEDDING` or a model error), and `incremental_fallback_total`
  went up. Those nodes are stale until repaired.

While a node waits, a point-in-time read as of the write returns its previous embedding.
The marker is how you know that is stale. On a running server the `repair_pending` gauge is
the live count. `caregraph-admin repair-status --db PATH` lists the markers still unresolved
(count and first nodes), but it opens the database directly, so run it against a stopped
server or a checkpoint, not a live one.

The worker only runs on a node that accepts writes (standalone or leader). It rebuilds its
queue from `CF_REPAIR` when it starts and when a standby is promoted, keeps only the newest
marker per node (a burst of writes on one hub is repaired once), and yields to client
writes except that it takes a turn at least every 2 seconds.

## 1. Which alert

| Alert | Meaning |
|---|---|
| `CareGraphRepairBacklog` | `repair_pending` has stayed above 20,000 for 15 minutes. |
| `CareGraphRepairFailing` | 5 or more repair batches failed in 15 minutes. |

```
kubectl -n NS logs POD -c caregraph --since=15m | grep -i repair
kubectl -n NS exec POD -c caregraph -- wget -qO- localhost:9100/metrics | grep -E '^repair|^embedding_updates_deferred'
```

Look at `repair_pending`, `repairs_completed_total` (rate) and `repair_batch_seconds`.

## 2. Backlog growing, no failures

Repairs are running but writes mark nodes faster than they are refreshed.

* Check it is the leader: `caregraph-admin remote status` shows the role. A
  follower never repairs. After a promotion the new leader rebuilds the queue within one
  interval.
* Check the repair rate against the deferral rate:
  `rate(repairs_completed_total[5m])` against `rate(embedding_updates_deferred_total[5m])`.
  Coalescing means the first can be much smaller than the second and still keep up.
* Lower the work per write: raise `CAREGRAPH_DEFER_HUB_DEGREE` (fewer nodes count as hubs),
  or lower the write rate to hubs. Raise `CAREGRAPH_REPAIR_BATCH` to give each pass more
  nodes. Repair competes with writes for the Python workers, so more workers
  (`CAREGRAPH_WORKERS`) help both.
* If nobody reads point-in-time embeddings for these nodes, a standing backlog is
  harmless; it only costs memory (on the order of 100 bytes a node).

## 3. Failures

`repair_failures_total` rising means a batch could not be computed. The log line says why.

| Log | Cause | Action |
|---|---|---|
| `repair: model not deployed` | A marker names a model this pod does not serve. | Deploy the model, or set `CAREGRAPH_GRAPHSAGE_MODEL` / `CAREGRAPH_GAT_MODEL` correctly. |
| `repair batch failed` with a worker or deadline error | The embedding workers are unhealthy. | `docs/runbooks/worker-crashloop.md`. |
| `repair batch failed` with a storage error | Disk or database trouble. | `docs/runbooks/disk-full.md`. |

If the embedding service itself is unavailable (workers restarting, breaker open, deadline),
the nodes stay queued, no attempt is spent, and the worker pauses 2 seconds between passes,
so an outage of any length strands nothing. Any other failure is retried up to 5 times and
the node is then dropped from the in-memory queue. Its marker stays, and the worker re-scans
`CF_REPAIR` every 30 minutes (and at start-up and on promotion), so it is queued again.
Nothing is lost.

## 4. Turning it off

`CAREGRAPH_REPAIR_ENABLED=false` stops the worker. It cannot be combined with
`CAREGRAPH_DEFER_HUB_DEGREE` (the server refuses to start), because deferred nodes would
never be refreshed.
