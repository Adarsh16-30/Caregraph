# Runbook: a volume is filling up

Applies to `CareGraphDiskFilling` (80%), `CareGraphDiskAlmostFull` (92%) and
`CareGraphDiskFillsInFourHours`. These alert on the pod's persistent volume
(`kubelet_volume_stats_*`), which holds the database, its write-ahead log, the audit
database and (on the leader) the checkpoint scratch space.

A full volume stops writes: RocksDB cannot append, the commit fails, and the pod
reports not ready. Act at 80%, not 100%.

## 1. What is using it

```
kubectl -n NS exec POD -c caregraph -- du -sh /var/lib/caregraph/db /var/lib/caregraph/audit /var/lib/caregraph/db.bootstrap
kubectl -n NS exec POD -c caregraph -- sh -c 'ls -la /var/lib/caregraph/db | sort -k5 -n | tail'
```

| Large | Cause | Action |
|---|---|---|
| `db/*.sst` | The data. | Grow the volume (3). |
| `db/*.log` (write-ahead log) | Retention for replicas: the leader keeps log files for `CAREGRAPH_WAL_TTL_SECS` (chart default 6 h) or `CAREGRAPH_WAL_SIZE_MB`. A busy leader keeps hours of writes. | Lower the TTL, or set a size cap (2). |
| `db.bootstrap/` | A checkpoint transfer was interrupted. It is deleted at the next transfer. | Safe to delete when no replica is bootstrapping. |
| `audit/` | The per-pod read audit grows with every read. | Plan capacity (4). |
| Backup volume | Old backups. | Purge (5). |

## 2. The log (leader)

A shorter retention shrinks the volume but narrows the window in which a replica that
was down can resume. A replica that falls out of the window must be rebuilt from a
checkpoint (`replica-bootstrap.md`), which is slower but correct.

- `CAREGRAPH_WAL_TTL_SECS`: how long finished log files are kept.
- `CAREGRAPH_WAL_SIZE_MB`: a hard cap on retained log size (0 = none). Set one if
  writes can burst: a burst larger than the cap pushes a lagging follower out of the
  window, and `replication_needs_bootstrap` fires.

Change it in the chart (`config.walTtlSecs`) and roll the standby first, then fail
over, then roll the old leader (`failover.md`).

## 3. Growing a volume

Most storage classes allow online expansion:

```
kubectl -n NS patch pvc data-RELEASE-writer-N -p '{"spec":{"resources":{"requests":{"storage":"100Gi"}}}}'
```

Do the standby first, then the leader. Also update `writer.persistence.size` in your
values so a rebuilt pod gets the larger volume (a StatefulSet's volume template cannot
be edited in place; new pods use the new value after you delete the StatefulSet with
`--cascade=orphan` and re-apply it, which does not touch running pods or volumes).

## 4. The audit database

It is not replicated and not rotated: it is the evidence. Do not delete it to free
space. Grow its volume, or ship it to cold storage and verify the copy before
archiving (`caregraph-admin audit-verify` checks a stopped database's mutation chain;
the per-pod read chain is verified by the anchors file the pod writes every minute,
`audit/anchors.jsonl`).

## 5. Backups

`kubectl exec` into the standby's `caregraph` container and list
`/var/lib/caregraph-backup`; remove old backups with
`caregraph-admin backup purge` (offline) or restore the volume size. Never remove the
newest verified backup.

## If it is already full

The pod is not ready and the writer cannot commit. Grow the volume first (3); a pod
that is out of space cannot be repaired from inside. Then confirm `/readyz`, send a
write, and check `caregraph-admin remote status` on the leader and the standby.
