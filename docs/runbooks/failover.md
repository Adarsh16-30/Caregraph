# Runbook: failover (promote the standby)

Background and guarantees: `docs/replication.md`. Commands are `caregraph-admin`;
`remote` commands need the `admin` role.

## Is this the right action?

| Situation | Action |
|---|---|
| Leader is up but you want to move it (maintenance, node drain) | Planned failover below |
| Leader is down, volume is gone or the node is unreachable | Failover with `--old-leader-down` |
| Leader is slow or erroring but alive | Fix or restart it first. Failover costs a rebuild of the old leader. |
| A follower is behind or `needs_bootstrap` | `replica-bootstrap.md`, not failover |

Check the standby is fit to take over:

```
caregraph-admin remote status --addr https://standby:50051 ...
```

It should show `role follower`, a small `lag`, no `NEEDS RE-BOOTSTRAP`, and
`ready_for_reads`. A standby that needs a bootstrap cannot be promoted.

## Planned failover (old leader reachable)

```
caregraph-admin remote failover \
    --addr https://old-leader:50051 --new-addr https://standby:50051 \
    --ca ca.pem --cert admin.pem --key admin-key.pem
```

What it does: demotes the old leader (writes stop; in-flight ones finish; it reports
its final sequence and waits for the standby to confirm it), then promotes the
standby with that sequence. It prints the new epoch and sequence.

If the demotion times out ("in-flight writes did not finish"), the old leader is
already refusing new writes; run `demote` again (it is repeatable), then `promote`
with `--wait-for-sequence` set to the final sequence it reports.

## Failover when the old leader is gone

First make sure it cannot come back and take writes: stop the pod, detach its
volume, or cut it off from clients. CareGraph has no quorum; it cannot stop an
unreachable-but-running leader by itself.

```
caregraph-admin remote failover --old-leader-down \
    --addr https://old-leader:50051 --new-addr https://standby:50051 ...
```

With synchronous replication every write the old leader acknowledged is on the
standby. A write it had committed but not yet acknowledged may be lost; clients that
sent a `request_id` will find it absent when they retry and will re-apply it.

## Afterwards

1. **Move the write traffic.** In Kubernetes the `caregraph-write` Service selects
   `role=leader`: relabel the new leader's pod `role=leader` and the old one
   `role=follower` (or remove it), so no client keeps writing to a fenced node.
2. **Verify** on the new leader: `caregraph-admin remote status` shows `role leader`,
   the epoch increased by one, and `remote verify-audit` reports an intact chain.
   Send one write and check `replication_sync_degraded` (it will be 1 until a standby
   is attached again).
3. **Rebuild the old leader** as a follower of the new one
   (`replica-bootstrap.md`, from a checkpoint of the new leader). Its database may
   hold writes the new leader never saw; do not reuse it. Keep its volume until you
   are sure nothing was acknowledged only there (compare its audit chain against the
   new leader's).
4. Until a standby is attached, writes run asynchronously: alert fires on
   `replication_sync_degraded`. Restoring redundancy is the next priority.

## If something looks wrong

| Symptom | Meaning |
|---|---|
| `only a follower can be promoted` | You targeted the leader, or a node that was already promoted. |
| `this follower must be re-bootstrapped ... cannot be promoted` | It diverged or fell out of the log. Pick another standby, or bootstrap one first. |
| `applied up to X but the old leader committed Y` | The standby has not caught up. Wait, or pass `--force` to accept losing the difference. |
| Old leader still takes writes | It was not demoted and nothing in the new epoch has contacted it. Stop it now; its writes since the promotion are not replicated. |
| `stale_leader: ... this node has been fenced` in a log | The old leader learned of the new epoch and fenced itself. Expected after a promotion. |
