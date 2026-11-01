# Design: automatic failover

**Status: proposed. Nothing in this document is implemented.** Failover today is an operator
action (`docs/replication.md`, `docs/runbooks/failover.md`). This is the design for making
it automatic without giving up the safety properties the manual path already has, and without
a rewrite into a consensus database.

## The problem

A leader dies at 03:00. Today writes stop until a human runs `caregraph-admin remote failover`.
The manual path is safe because a person decides that the old leader is really gone. An
automatic path has to make that decision itself, and the two ways to get it wrong are both bad:

* **Promote too eagerly** and a slow-but-alive leader and the new one both accept writes
  (split brain). Acknowledged writes on the old leader's side are lost when it is rebuilt.
* **Promote too reluctantly** and an outage lasts as long as it does today.

A two-node system cannot settle this alone: when the standby stops hearing from the leader it
cannot tell "leader dead" from "I am cut off". It needs a third party that both sides can ask.

## What we already have, and what is missing

| Already there | Role in this design |
|---|---|
| Epochs: each promotion starts a new epoch, recorded in the replicated history | The **fencing token**. A node that sees a newer epoch fences itself; a follower refuses a leader on an older epoch. |
| `Promote` / `Demote` RPCs, persisted roles, `needs_bootstrap: diverged` | The mechanics of a role change. Promotion by a standby is already safe and tested. |
| Synchronous replication (`MIN_ACKS=1`): a write is acknowledged only after a standby has applied it | Makes an *acknowledged* write survive the loss of the leader (RPO 0 while the standby is in sync). |
| Role-label sidecar and `-write` Service | Moves client traffic to whoever holds the leader role. |
| **Missing: a decision-maker** that is not one of the two nodes, and a rule that makes the old leader stop on its own. | This design. |

## Decision: a lease held in the Kubernetes API, not Raft

Options considered:

1. **Raft (or Paxos) inside the database**, replacing the WAL shipping. Gives automatic failover
   and quorum writes. Rewrites the replication layer, the commit path's durability story and the
   whole test suite, and needs three voting nodes. The cost is out of proportion to the gap.
2. **An external consensus service** (etcd, Consul, ZooKeeper) used only for a lock. Right idea,
   but adds a system to run. On Kubernetes the API server already is one: it is backed by etcd,
   and `coordination.k8s.io/v1` `Lease` objects exist for exactly this.
3. **A Lease in the Kubernetes API** (chosen). The same pattern controllers use. No new
   component, and a quorum store we already depend on. It sits behind a small trait
   (`LeaseBackend`), so a deployment outside Kubernetes can supply another (etcd, a cloud
   provider's lock) without touching the protocol.

What this buys and costs: the consensus is the API server's, not ours. **If a leader cannot reach
the lease store it stops accepting writes** (it chooses consistency over availability). That is
deliberate and is documented as a trade-off below.

## The protocol

One `Lease` object per cluster, `holder` = the leader's id, annotated with its `epoch`.
Two durations: `lease_duration` (default 15 s) and `renew_interval` (default 5 s).

### The leader

* Renews the lease every `renew_interval` (a compare-and-swap on the object's
  `resourceVersion`). It measures time on its own **monotonic clock**, starting from when it
  *sent* the last successful renewal, never from the lease's own timestamp.
* **A leader may commit writes only while `now < last_renewal_start + lease_duration - margin`**
  (`margin` default 3 s, for clock rate error and scheduling delay). Past that it refuses
  writes with the existing `replaced as leader` / fenced error and stops renewing.
  It does not demote itself for good: if the lease still names it and the epoch has not moved
  (an API server blip), the next successful renewal lets it resume. If anyone else holds the
  lease, or a newer epoch exists, it fences itself permanently as it does today.

### The standby

* Watches the lease. If the holder has not changed it for `lease_duration` as measured on the
  standby's own monotonic clock from when it *observed* the last change, the lease is expired.
* An expired lease makes the standby a candidate, but only if all of these hold:
  * its replication is healthy (`!needs_bootstrap`, no divergence);
  * its lag is at most `auto_failover.max_lag_sequences` (default 0 in synchronous mode, so
    it can only win with everything it was sent, a bounded number otherwise);
  * it was in sync when the leader was last seen (see "What it does not guarantee").
* The candidate acquires the lease with a compare-and-swap. Exactly one standby wins; the
  others see the conflict and go back to watching. The winner then runs the existing
  `Promote` (new epoch, epoch record in the replicated history), and the role-label sidecar
  moves the `-write` Service.

### Why this is safe

* **Two writers at once** need the old leader to still believe it holds the lease after a
  standby has acquired it. The standby waits a full `lease_duration` from the last renewal it
  saw; the leader stops `margin` earlier than that, by its own clock. The two are safe as long
  as the clocks' *rates* differ by less than `margin / lease_duration` (20% here). Neither
  needs the clocks to agree on the time.
* **A paused leader** (a VM freeze, a long GC or scheduling stall) can wake up inside the
  window, believing it still holds the lease, and commit once before it notices. The lease
  cannot prevent that. The epoch does: in synchronous mode such a write is acknowledged only
  after a standby applies it, and every standby is on the new epoch and refuses it, so it is
  never acknowledged (`DEADLINE_EXCEEDED`, outcome unconfirmed, retried with the same
  `request_id` against the new leader).
* **Promotion loses nothing that was acknowledged**, in synchronous mode, because the winner is
  a standby the leader was waiting on.

## What it does not guarantee

State these in the runbook; do not let them be discovered in an incident.

1. **Writes pause for up to `lease_duration` + promotion time** after a leader dies: about
   15 s + 1-2 s with the defaults. This is the RTO. It can be shortened, at the cost of
   more false failovers.
2. **A lease-store outage stops writes**, even with a healthy leader and standby, once the
   leader's last renewal ages out (15 s). Reads continue.
3. **The no-loss guarantee holds only in synchronous mode.** If the standby was out of sync
   (the `replication_degraded_writes_total` case), writes acknowledged in that window exist
   only on the leader. The protocol records whether the leader was in sync in the lease's
   annotations at each renewal, and a candidate refuses to promote automatically if the last
   renewal it saw said "degraded", unless the operator has set
   `auto_failover.allow_degraded_promotion` (default false). The alternative is an outage
   until a person looks; the default chooses that.
4. **Still one writer.** This is availability for the single-writer design, not scale-out.
5. **Clock rate drift beyond the margin**, or a paused process that sits past the epoch check
   in asynchronous mode, can still produce a short split. Fencing makes it visible and stops
   it spreading; it cannot make it impossible without a quorum write path.

## Changes

| Where | Change |
|---|---|
| `src/replication/lease.rs` (new) | `LeaseBackend` trait; `LeaseKeeper` (leader renewal and the write-validity clock); `Elector` (standby watch and candidacy). Pure state machines over an injected clock and backend, so they are testable without Kubernetes. |
| `src/replication/state.rs` | `writable()` also checks `lease_valid()` when auto-failover is on. |
| `src/kube_label.rs` | Already speaks to the Kubernetes API in-cluster; reuse its client for `KubeLease`. |
| Helm chart | RBAC for `leases` (get/list/watch/create/update) on the one object; values and the production profile. |
| Config | `CAREGRAPH_AUTO_FAILOVER` (default **off**), `..._LEASE_DURATION_MS`, `..._RENEW_INTERVAL_MS`, `..._MARGIN_MS`, `..._MAX_LAG`, `..._ALLOW_DEGRADED`. |
| Metrics, alerts | `lease_held`, `lease_renew_failures_total`, `lease_remaining_seconds`, `auto_failovers_total`, `auto_failover_refused_total{reason}`; alerts for a leader without a lease and for a refused promotion. |
| Docs | `docs/replication.md` (remove it from "Not covered"), `docs/runbooks/failover.md` (what automatic failover did and how to take over by hand), this file's guarantees section. |

Manual failover stays and works with automatic failover on: an operator `Demote` makes the
leader release the lease explicitly (holder cleared) so the standby does not wait out the
expiry.

## How it will be tested

The protocol is the risk, not the plumbing, so the plan is built around evidence of safety:

1. **Deterministic simulation** of the two state machines against a fake lease store and a fake
   clock, with injected delays, drops, pauses and clock-rate skew within and beyond the margin.
   Invariants checked on every step of thousands of seeded runs: *never two nodes for which
   `writable()` is true at overlapping real times* (within the stated drift bound), *an epoch
   never decreases*, *every acknowledged write in synchronous mode is on the node that becomes
   leader*. Runs that exceed the drift bound are expected to violate the first and must be the
   only ones that do.
2. **Unit tests** for each rule above (expiry measured from observation, margin, the
   degraded-refusal, CAS loser, resume after a blip).
3. **Fault-injection tests** in `tests/fault_injection` on real processes: kill the leader,
   freeze it (SIGSTOP) past the lease and resume it, cut it from the lease store only, run two
   standbys, restart everything. Each ends with the existing audit-chain and exact-state checks.
4. **The kind smoke test** gains a step: delete the leader pod and require writes to resume
   through the Service on their own, within the stated RTO.

## Rollout

Off by default. First step is a build that runs the elector in **observe-only** mode: it
computes what it would do and exports the metrics and a log line, without acting, so the
timings and false-positive rate can be seen on a real cluster before it is allowed to promote.

## Estimate

About 2-3 weeks of focused work for one engineer, most of it the simulation and the
fault-injection tests: the state machines are small, and being sure of them is the job.
