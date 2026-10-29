# Runbook: add or rebuild a replica

Use this to add a replica, to rebuild one that reports `needs_bootstrap`, and to
rebuild a deposed leader after a failover. Background: `docs/replication.md`.

## When a follower needs rebuilding

`caregraph-admin remote status` on the follower shows `NEEDS RE-BOOTSTRAP`, the metric
`replication_needs_bootstrap` is 1, and `/readyz` is false. The server log has the
reason; the common ones:

| Reason in the log | Cause |
|---|---|
| `the log no longer reaches back to N` | The follower was down or behind longer than the leader keeps log files (`CAREGRAPH_WAL_TTL_SECS`). |
| `diverged: ...` | The follower holds writes the leader does not have (a deposed leader, or a copy of the wrong database). |
| `schema_mismatch` | The follower was built by a different version with different column families. Roll replicas first when upgrading. |
| `this database is at sequence X but the leader sent a batch starting at Y` | A batch arrived out of order; the follower stopped rather than guess. |

A follower in this state serves nothing it should not (it is not ready), and it will
not apply anything further. It does not repair itself.

## Procedure

1. **On the current leader**, take a checkpoint:

   ```
   caregraph-admin remote checkpoint replica-$(date +%Y%m%d%H%M) ...
   ```

   It is a hard-linked copy under `<backup dir>/checkpoints/<name>`; it costs almost
   no space and no time.

2. **Stop the replica** and move its old database aside (do not delete it yet).

3. **Copy the checkpoint** to the replica's database path. The copy is encrypted
   under the leader's data key, which the replica must also use
   (`CAREGRAPH_WRAPPED_DEK` and `CAREGRAPH_KEK_FILE` identical to the leader's).
   Remove any old `<db path>.role.json` on the replica so it starts with the role in
   its configuration.

4. **Start it** with `CAREGRAPH_ROLE=follower`, `CAREGRAPH_LEADER_ADDR`, and the
   replication certificate and key (`CAREGRAPH_REPLICATION_CERT`, `_KEY`, `_CA`). Its
   certificate's identity must have the `replicator` role in the leader's policy.

5. **Confirm** it catches up: `caregraph-admin remote status` on the follower shows a
   falling `lag`, then `ready_for_reads`; on the leader it lists the follower under
   "follower ... confirmed". `replication_connected_followers` is 1 per standby.

Do it promptly: the checkpoint's sequence must still be inside the leader's log when
the replica starts. If it says `needs_bootstrap` again straight away, the checkpoint
was already too old; take a fresh one.

## Rebuilding the old leader after a failover

Same procedure, using a checkpoint of the **new** leader. Before discarding the old
volume, if you need to know whether anything was acknowledged only on it, compare its
mutation audit chain (`caregraph-admin audit-verify --db ...`, then the records after
the new leader's first epoch sequence) with the new leader's.

## A new replica (scale-out)

Same procedure; nothing needs to change on the leader except the `replicator` role
for the new certificate. Read replicas that must not be promotable can run on the
reader image (no Python, no models); a standby that may be promoted needs the models
and workers a leader has.
