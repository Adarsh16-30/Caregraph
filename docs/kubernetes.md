# Running CareGraph on Kubernetes

The Helm chart is `infrastructure/helm/caregraph`. This page says what it deploys,
how to install it, and what it does and does not guarantee. Operating procedures are
in `docs/runbooks/`; metrics, alerts and tracing are in `docs/observability.md`.

## What it deploys

```
                clients
          writes │      │ reads
                 ▼      ▼
   Service <rel>-write   Service <rel>-read
   (role=leader pod)     (read replicas)
                 │               │
   ┌─────────────┴──────────┐    │      StatefulSet <rel>-reader (HPA, no Python)
   │ StatefulSet <rel>-writer│    │      one PVC per pod, built from a checkpoint
   │  writer-0   writer-1    │    │      of the leader, follows the leader's log
   │  leader ──► standby     │◄───┘
   │  (synchronous)          │
   └─────────────────────────┘
   Service <rel>-standby (the backup job's target) · <rel>-headless (pod DNS)
```

| Object | What it is |
|---|---|
| `StatefulSet <rel>-writer` | Exactly two pods: a leader and a **synchronous standby**. Never autoscaled. Each has a data volume, a backup volume and a sidecar that publishes its role as a pod label. |
| `StatefulSet <rel>-reader` | Read replicas on the reader image (no Python, no models). Scale with the HPA. They answer reads only and cannot be promoted. |
| Services | `-write` selects the pod labelled `caregraph.io/role=leader` (ready pods only); `-standby` selects the follower writer; `-read` the readers; `-headless` gives every pod a stable DNS name. |
| `CronJob <rel>-backup` | Calls `CreateBackup` on the standby, so the leader pays nothing. An optional uploader sidecar ships backups and audit anchors off the pod. |
| PodDisruptionBudgets | One writer may be drained at a time, never both. |
| NetworkPolicy | Ingress only: gRPC from the release's own pods and the clients you list; metrics from the scraper you list. |
| PodMonitor, PrometheusRule, dashboards | Optional; need the Prometheus Operator / the Grafana sidecar. |
| ExternalSecrets | Optional; build the TLS, key and token Secrets from your secret manager. |

Every container runs as UID 10001 with a read-only root filesystem, all capabilities
dropped, no privilege escalation and the runtime-default seccomp profile; writable
space is the data volume and two `emptyDir`s (`/tmp`, `/run/caregraph`). Probes use
the HTTP port (`/startupz`, `/readyz`, `/healthz`) because the gRPC port requires a
client certificate that kubelet probes cannot present.

## How roles work (read this before operating it)

The server, not Kubernetes, decides who the leader is. It keeps its role (standalone,
leader, follower, demoted, fenced) and the leadership epoch on its own volume; a
failover changes them. The **role-label sidecar** asks the local server for its role
(`GET /role` on the metrics port) every five seconds and sets the pod label
`caregraph.io/role` through the Kubernetes API (it may patch only the writer pods'
own objects). The `-write` Service follows that label. So:

* a failover moves write traffic by itself within a few seconds, with no pod
  relabelling by a person;
* a leader pod that is recreated comes back with the role it persisted, not the role
  its ordinal suggests;
* a deposed leader is never ready and never selected.

**Starting an empty pod** is the one dangerous moment, because guessing wrong
creates a second leader. The `init-role` init container (`caregraph-admin init-pod`)
applies a fixed rule:

| Volume | Another writer reports `leader` | Result |
|---|---|---|
| has a database | (any) | keep it; the persisted role decides |
| empty | yes | build the database from that leader's checkpoint and start as a follower |
| empty | no, and this is `writer-0` **and** `writer.bootstrapLeader=true` and no live peer exists | start as the first leader of a new cluster |
| empty | no, anything else | **wait** (and eventually fail with the reason) |

`writer.bootstrapLeader` therefore means "this is a new cluster". Set it for the
first install and turn it off afterwards; with it off, a rebuilt `writer-0` can only
join an existing leader.

A replica is built from the leader with `FetchCheckpoint` (mutual TLS, `replicator`
role): a hard-linked checkpoint streamed file by file, verified (every file whole,
footer present) and moved into place atomically. A transfer that fails leaves nothing
behind. Only one transfer runs at a time; others are told to retry.

## Installing

Development (plaintext, one shared key; for trying it out):

```
helm install cg infrastructure/helm/caregraph --set auth.apiKey=$(openssl rand -hex 24)
```

Production needs, and the chart enforces, mutual TLS, an encryption key, an RBAC
policy, synchronous writes and a non-legacy timestamp policy:

1. **Secrets** (or `externalSecrets`):
   * `caregraph-tls`: `tls.crt`, `tls.key`, `ca.crt`. The certificate carries the
     `<rel>-write` Service names as SANs and both `serverAuth` and `clientAuth`
     usages, because replicas present it to the leader as their client certificate.
   * `caregraph-admin-tls`: the backup job's client certificate (`admin` role).
   * `caregraph-keys`: `kek` and `wrapped-dek` (`caregraph-admin kek new`, see
     `docs/runbooks/key-rotation.md`). All pods share one data key.
   * `caregraph-tokens`, `caregraph-replication-token`, `caregraph-admin-token` if
     you use the second factor.
2. **Policy**: `rbac.principals` maps certificate identities to roles; the serving
   certificate's identity needs `replicator`, the backup job's `admin`.
3. **Install** with `values-production.yaml`, once with `--set writer.bootstrapLeader=true`,
   then upgrade with it `false` (see above).
4. **Load the data.** A new cluster is empty. Nodes enter only through the bulk loader
   (`caregraph-load`), which writes a stopped database: scale the writer to one pod
   (`kubectl scale`), run `caregraph-load --db /var/lib/caregraph/db` in a one-off Pod
   that mounts the leader's volume, then scale back. The standby builds itself from the
   leader's checkpoint afterwards.

`helm lint` and `helm template` with each values file, kubeconform on the output and a
kind smoke test run in CI (`.github/workflows/k8s.yml`).

## Sizing

Starting values, from the measurements in `docs/memory.md` (one machine, one graph;
yours will differ, so watch `tree_resident_memory_bytes` against
`memory_budget_bytes`):

| | CPU request/limit | Memory request/limit | `CAREGRAPH_MEMORY_MB` |
|---|---|---|---|
| writer (both models) | 1 / 2 | 1.5 / 2 GiB | 1500 |
| reader | 0.5 / 1 | 512 / 768 MiB | 512 |

A writer's memory is dominated by its Python workers (about 330-450 MB each); disabling
a model you do not serve (`writer.models.gat=false`) removes one. The container limit
must stay above `CAREGRAPH_MEMORY_MB`.

## What this does not give you

* **Automatic failover.** Promotion is an operator action (`docs/runbooks/failover.md`),
  by design: with two nodes and no quorum, automatic promotion risks two leaders.
* **Protection from a second leader if the old one is alive and unreachable.** The
  runbook says to stop it first; the server cannot fence a node it cannot reach.
* **Write scale-out.** One leader; capacity grows with its CPU and workers. Reads
  scale with replicas.
* **Uploading backups.** The chart produces encrypted backups on the standby's volume and
  has a slot for your uploader (image, command, credentials). It ships none, and
  `backup_last_success_timestamp` measures that a backup was *made*, not that it was
  uploaded.
* **A validated multi-zone story.** The writers prefer different nodes; zone placement
  and storage-class behaviour are yours to set (`writer.affinity`).
* **A local run of the images and the cluster.** What was checked on the development
  machine: `helm lint` and `helm template` with all three values files, kubeconform
  (Kubernetes 1.30 schemas) on every rendered object, the chart's refusal of each unsafe
  production setting, `promtool check rules` on the alert rules, and every Rust test the
  chart depends on (checkpoint bootstrap, start decision, label sidecar against a stand-in
  API server, OTLP export). What was **not** run there: building the two images and the
  kind smoke test (`scripts/k8s_smoke.sh`). The image build was attempted twice; the
  release build of RocksDB and the dependencies took the 16 GB laptop (with about 10 GB in
  use by other programs) below 600 MB free, the first attempt crashed Docker's VM, and the
  second was stopped to protect the machine. Both run in CI
  (`.github/workflows/security.yml` builds and scans the images, `k8s.yml` runs the smoke
  test) on a runner with the memory for it. Until that has run green, treat the Dockerfile
  and the in-cluster behaviour as untested end to end.
