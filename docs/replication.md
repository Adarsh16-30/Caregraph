# Replication and failover

CareGraph scales reads and survives the loss of a node with **one writer and any
number of read replicas**, by shipping RocksDB's write-ahead log. There is no
sharding and no consensus protocol: a single leader orders every write, followers
copy its log, and moving leadership is an operator action.

## What is replicated

Each commit is one atomic RocksDB write batch: the edge, its reverse, the affected
nodes' new embeddings, the provenance records, the idempotency record, the commit
high-water mark and the audit-chain record. The leader streams those batches to
followers exactly as written, and a follower applies them in order. After every
batch it checks that its database's sequence number advanced by exactly the
batch's operation count, as it did on the leader. A follower is therefore not
"eventually similar" to the leader: it is the same database, byte for byte, as of
a sequence number (`replication_test::a_follower_converges_to_the_leaders_exact_state_and_sequence`,
and `tests/embedding/replication_end_to_end_test.rs` for real mutations).

If anything does not line up (a batch out of order, a different column-family
layout, a sequence that did not advance as expected) the follower **stops and
reports `needs_bootstrap`** instead of guessing. One divergence must not become
silent corruption.

## Roles

| Role | Accepts writes | Serves the stream | Applies the stream | Takes read traffic |
|---|---|---|---|---|
| `standalone` (default) | yes | no | no | yes |
| `leader` | yes | yes | no | yes |
| `follower` | no | no | yes | when connected and within the lag limit |
| `demoted` | no | yes, until the standby has caught up | no | no |
| `fenced` | no | no | no | no |

`CAREGRAPH_ROLE` selects the starting role. The role is persisted beside the
database (`<db path>.role.json`), and a persisted role overrides the configured
one, so a promoted standby is still the leader after a restart and a deposed leader
cannot come back as one. A `demoted` or `fenced` node must be rebuilt from a
checkpoint of the current leader before it rejoins (its database may hold writes
the new leader never saw).

## Synchronous replication

With `CAREGRAPH_REPLICATION_MIN_ACKS=1` (the default for a leader) a write is
acknowledged to its client only after a standby has durably applied it. While a
standby is healthy that gives RPO = 0 for acknowledged writes.

- **No standby connected, or none within `CAREGRAPH_REPLICATION_SYNC_LAG`
  sequence numbers of the leader:** the write proceeds without waiting
  (asynchronous). This is counted (`replication_degraded_writes_total`) and
  exported (`replication_sync_degraded`); alert on it. The alternative, refusing
  every write while the standby is down, turns a standby outage into a full outage.
- **An eligible standby does not confirm within `CAREGRAPH_REPLICATION_ACK_TIMEOUT_MS`,
  or disappears while the leader waits:** the write is durable on the leader, but
  its replication is unconfirmed. The client gets `DEADLINE_EXCEEDED` ("outcome
  unconfirmed") and should retry **with the same `request_id`**, which is
  idempotent, so the retry cannot apply it twice. Clients that do not send a
  `request_id` cannot retry safely and should treat this error as an unknown
  outcome.
- `CAREGRAPH_REPLICATION_MIN_ACKS=0` is asynchronous replication: lowest write
  latency, and acknowledged writes can be lost if the leader is lost before its
  standby has them.

## Read replicas

A follower serves `Traverse`, `Snapshot`, `SimilarCarePathways` and
`SimilarityDelta` from its own copy and refuses mutations
(`FAILED_PRECONDITION`, naming the leader as the place to write). It is eventually
consistent. For read-your-writes, pass the `db_sequence` from the mutation
response as `min_sequence` on the read: the replica waits up to two seconds for it
and otherwise answers `UNAVAILABLE`, so the client can retry or read from the
leader.

`/readyz` on a follower is false while it is disconnected, more than
`CAREGRAPH_REPLICATION_READY_LAG` sequence numbers behind, or needs a bootstrap, so
a Service in front of replicas stops sending it traffic.

## Bootstrapping a replica

A new follower starts from a copy of the leader's database, then catches up from the
log.

**Over the network (the normal way).** `caregraph-admin bootstrap --leader-addr ...
--dest <db path>` calls the leader's `FetchCheckpoint` stream: the leader makes a
hard-linked checkpoint in a scratch directory on its own volume
(`CAREGRAPH_BOOTSTRAP_DIR`, default `<db path>.bootstrap`) and streams a header
(sequence number, epoch, file list), every file's bytes in order, and a footer. The
client writes into `<dest>.partial`, checks each file against the header and the footer
against what arrived, syncs, and renames it to `<dest>` only then; a transfer that fails
leaves nothing at `<dest>`, and a directory that already holds data is refused. One
transfer runs at a time (others get `RESOURCE_EXHAUSTED` and retry). Only a leader serves
it, to the `replicator` role. The files are streamed as they are on disk (still
encrypted under the leader's data key). In Kubernetes an init container runs this
(`docs/kubernetes.md`).

**By hand.**

1. On the leader: `caregraph-admin remote checkpoint <name>` (a consistent,
   hard-linked copy under `<backup dir>/checkpoints/<name>`).
2. Copy that directory to the new node's volume as its database.
3. Start the node with `CAREGRAPH_ROLE=follower`, `CAREGRAPH_LEADER_ADDR`, and, with
   TLS, `CAREGRAPH_REPLICATION_CERT` / `_KEY` / `_CA` (the certificate's identity needs
   the `replicator` role in the leader's policy).

The follower resumes from the checkpoint's sequence number. It has to be started
before the leader's log is rotated away (see retention below), otherwise it is told
`needs_bootstrap` and the procedure is repeated from a newer checkpoint.

**The follower's files are the leader's files, so it uses the leader's data key.**
Re-encrypting a database under a different key copies it logically, which gives it
different sequence numbers, and a follower with different sequence numbers cannot
apply the leader's batches. Replicas therefore share one data key (and the wrapped
key and key-encryption key that go with it); rotating it means building a new
database under a new key and bootstrapping every replica from it.

## Log retention

The leader keeps flushed log files for `CAREGRAPH_WAL_TTL_SECS` (default 6 hours
when replicating) and optionally `CAREGRAPH_WAL_SIZE_MB`. A follower that was down
longer than that, or fell behind further, is told `needs_bootstrap`. Size the window
to your longest tolerated replica outage and watch disk usage.

## Failover

Leadership moves by operator command, in the order **demote the old leader, then
promote the standby**:

```
caregraph-admin remote failover --addr https://old-leader:50051 \
    --new-addr https://standby:50051 --ca ca.pem --cert admin.pem --key admin-key.pem
```

1. **Demote** (on the old leader) stops it accepting writes, waits for in-flight
   writes to finish, so its last sequence number is final, and keeps streaming until
   the standby has everything up to it. It reports that final sequence.
2. **Promote** (on the standby) waits until it has applied that sequence, starts a
   new **epoch** (a record of the new leader's first sequence number, written into
   the replicated history) and begins accepting writes. Its commit high-water mark
   and audit-chain head are re-read from the database first, so strict timestamp
   ordering and the audit chain continue unbroken.
3. **Outside CareGraph:** move the write Service to the new leader (relabel the pod
   in Kubernetes; see the runbook).
4. Rebuild the old leader from a checkpoint of the new one before it rejoins.

If the old leader cannot be reached, confirm it is down or its volume is detached
and use `--old-leader-down`: the standby is promoted without waiting. With
synchronous replication every write the old leader *acknowledged* is already on the
standby; a write it had committed but not yet acknowledged may be lost.

### Fencing

An **epoch** is a number that increases by one at each promotion; each epoch's first
sequence number is stored in the replicated history. Two rules use it:

- A leader that learns of a newer epoch (a follower or ack from it reaches the old
  leader) **fences itself**: it refuses writes and the stream from then on.
- A node whose history forked from the leader's (it holds sequence numbers at or
  beyond the point where the next epoch began, or is ahead of the leader) is refused
  with `needs_bootstrap: diverged`, so a deposed leader that kept writing cannot be
  grafted back onto the new history.

**What this does not do.** There is no quorum or leader lease. If the old leader is
alive, reachable by clients and unaware, it keeps accepting writes until something
in the newer epoch contacts it or the operator demotes it. Those writes are not
replicated, and they are discarded when the node is rebuilt. Fencing makes the
split visible and stops it spreading; it does not make it impossible. Preventing it
means making sure the old leader is stopped or cut off from clients before the
standby is promoted (the `failover` command demotes it first for exactly that
reason, and `--old-leader-down` is an assertion by the operator that it is gone).

## Security

The stream carries decrypted batches, so it must run over mutual TLS in production
and is available only to principals with the `replicator` role. Treat that role as
highly privileged: besides reading every write, a compromised replicator identity can
fence the leader by claiming a newer epoch. Replication calls
are not rate limited (the acknowledgement follows every batch). The `admin` role
cannot stream the log and the `replicator` role cannot call the admin service.

## Metrics

| Metric | Where | Meaning |
|---|---|---|
| `replication_epoch` | all | leadership epoch this node knows |
| `replication_applied_sequence`, `replication_lag_sequences` | follower | progress and distance behind the leader |
| `replication_batches_applied_total` | follower | |
| `replication_needs_bootstrap` | follower | 1 when it must be rebuilt |
| `replication_connected_followers` | leader | followers currently streaming |
| `replication_ack_wait_seconds` | leader | how long writes waited for a standby |
| `replication_sync_degraded`, `replication_degraded_writes_total` | leader | writes that went without a synchronous standby |
| `replication_ack_timeouts_total` | leader | writes whose confirmation did not arrive in time |

## Not covered

- Automatic failover and leader election.
- Quorum writes across more than one standby (`MIN_ACKS` counts any N eligible
  standbys; there is no consensus).
- Per-replica data keys (see bootstrapping).
- Cascading replication (a follower serving the stream to another follower).
