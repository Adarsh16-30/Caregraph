# Runbook: audit alerts and suspected tampering

Applies to `CareGraphAuditAnchorStale`, `CareGraphReadAuditSaturated`,
`CareGraphAuditEventsDropped` and `CareGraphAccessDenialsSpiking`, and to any time
`caregraph-admin remote verify-audit` or `audit-verify` reports a broken chain.

Two hash chains exist:

| Chain | Holds | Where | Replicated |
|---|---|---|---|
| mutations | every committed write: who, what, when, outcome | in the main database, in the same batch as the write | yes (byte-identical on the standby) |
| reads | every read, denial and admin action, per pod | the pod's own audit database (`CAREGRAPH_AUDIT_DIR`) | no |

Each record contains the hash of the one before it. Editing a record in place breaks
every record after it. **Truncating the end of a chain and writing a different tail
does not break it**: that is what the *anchors* are for. Every 60 s each pod writes
`(pod, chain, seq, head hash)` to the log (`audit_anchor` lines) and appends it to
`audit/anchors.jsonl`; the uploader ships that to storage the pod cannot rewrite.
A chain whose head is *earlier* than a recorded anchor was truncated.

## `CareGraphAuditAnchorStale`

No anchor for 10 minutes: the audit directory is unwritable or the anchoring task
stopped. Anchors are what make tampering detectable, so a gap is itself a gap in
evidence.

```
kubectl -n NS logs POD -c caregraph | grep -E 'audit_anchor|writing the audit anchor'
kubectl -n NS exec POD -c caregraph -- ls -la /var/lib/caregraph/audit
```

A full or read-only volume (`disk-full.md`) is the usual cause. Fix it and confirm
`audit_last_anchor_timestamp_seconds` advances. Note the interval of the gap in your
incident record.

## `CareGraphReadAuditSaturated`

The audit queue was full, so reads were **refused** (`UNAVAILABLE`) instead of served
unaudited. This is by design: no read goes unrecorded. Causes: the audit volume is slow
(each batch is fsynced), or read traffic exceeds what one pod's audit writer can
group-commit. Reduce load on the pod, add read replicas (each audits its own reads), or
move the audit volume to faster storage. It does not indicate an attack by itself.

## `CareGraphAuditEventsDropped`

Denial events (requests already refused) were dropped on a full queue. Treat a rising
count together with `CareGraphAccessDenialsSpiking` as probing: the dropped events were
denials, the access itself was refused.

## `CareGraphAccessDenialsSpiking`

More than one `PERMISSION_DENIED` or `UNAUTHENTICATED` request per second for ten
minutes. Find who:

```
caregraph-admin audit-verify ...        # on a stopped copy, or read the audit_log JSON lines
kubectl -n NS logs POD -c caregraph | grep '"target":"audit"' | grep denied | tail
```

The records carry the principal (the certificate identity), the RPC and the time, not
the data. A misconfigured client with a valid certificate is far more common than an
attacker, and an expired certificate shows as handshake failures, not denials. If it is
hostile, remove the principal from the RBAC policy (reloaded in seconds) and revoke its
certificate (`cert-rotation.md`).

## A chain fails verification

```
caregraph-admin remote verify-audit --addr https://POD...:50051 ...
```

reports the sequence number where the chain breaks. Do not restart, repair or delete
anything yet.

1. **Preserve.** Take a checkpoint of the pod (`caregraph-admin remote checkpoint
   evidence-YYYYMMDD`) and copy the audit directory; snapshot the volumes.
2. **Compare the two copies.** The mutation chain is replicated: verify it on the
   other writer. If the leader's chain is broken and the standby's is intact, the
   standby's copy is the reliable one for the records the break covers.
3. **Compare with the anchors.** The uploaded anchors are the reference. The break's
   sequence and the head hashes around it tell you whether records were edited (chain
   broken in the middle) or removed (head earlier than an anchor).
4. **Contain.** Fail over to the intact side if it is the leader that is suspect
   (`failover.md`); rebuild the suspect node afterwards from the intact one
   (`replica-bootstrap.md`). Rotate the data key if the volume itself may have been read
   (`key-rotation.md`) and revoke the certificates the host could have held.
5. **Record.** A broken chain on a pod that was only ever written by CareGraph points at
   the storage layer or an operator with volume access, not at the application.

## What the audit does not prove

It records that a principal read a given subject, not what was returned. It cannot say a
write that was never attempted did not happen, and it is only as trustworthy as the
anchors' storage: if an attacker can rewrite the anchor bucket too, nothing detects the
truncation. Configure that bucket with object lock.
