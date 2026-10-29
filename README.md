# CareGraph

A temporally-versioned graph database with incrementally-maintained GNN embeddings.

Graph mutations and their resulting embedding updates commit atomically inside a
single RocksDB `WriteBatch`, so both graph structure and embeddings are queryable
at any historical point in time. Embeddings are a first-class versioned field,
not a batch-computed side artifact.

### Contents

- [Features](#features)
- [Getting started](#getting-started)
  - [One-command setup](#one-command-setup)
  - [Configuration](#configuration)
  - [Loading the clinical graph](#loading-the-clinical-graph)
  - [Live demo](#live-demo)
- [Architecture](#architecture)
  - [Component diagram](#component-diagram)
  - [Mutation lifecycle](#mutation-lifecycle)
  - [Column families](#column-families)
  - [Deletions are tombstones](#deletions-are-tombstones)
- [The ten non-negotiable rules](#the-ten-non-negotiable-rules)
- [Repository layout](#repository-layout)
- [License](#license)

---

## Features

All numbers below are measured on the real 174,298-node / 515,117-edge
Diabetes 130 clinical graph — see `docs/benchmark_report.md` for full
methodology, baselines, and disclosed limitations.

- **Temporally-versioned graph storage** — every node/edge mutation is
  appended, never overwritten. Point-in-time reads (`as_of`) are a single
  RocksDB seek thanks to bit-inverted timestamp keys — no reverse iterator,
  no secondary index (see [Column families](#column-families)).
- **Incrementally-maintained GNN embeddings** — GraphSAGE and GAT embeddings
  update in the same atomic commit as the structural mutation that changed
  them, not in a separate batch job.
- **Atomic mutation + embedding commit** — the structural edge, its
  embedding, and its provenance record land in one RocksDB `WriteBatch`.
  Verified by killing the committing process at a randomised point across
  100 iterations on both dispatch paths: zero non-atomic states observed.
- **Architecture-agnostic dispatch** — each deployed model's manifest states
  whether its aggregation is associative (fast in-place patch, e.g.
  GraphSAGE's mean) or not (staged incremental recompute, e.g. GAT's
  attention). The engine reads that instead of hardcoding per-architecture
  logic, and cross-checks it against the running checkpoint at spawn time.
- **Self-tuning incremental-update boundary** — a discrete cap controller
  hill-climbs the affected-subgraph size limits against a live p95 latency
  target, recording every adjustment as versioned metadata alongside the
  mutation it affected.
- **Explainable delta attribution** — optional (`explain: true`)
  integrated-gradients attribution reports which edges drove an embedding
  change and by how much, committed atomically with the embedding itself so
  it stays queryable at any point in time rather than just logged in passing.
- **Point-in-time similarity queries** — `similar_care_pathways` for a
  single-timestamp nearest-neighbor search, plus `SimilarityDelta` for "how
  much did X's similarity to Y change between two timestamps."
- **Bounded graph traversal & snapshot reconstruction** — k-hop neighborhood
  queries and full-graph state reconstruction at any historical timestamp.
- **Real encryption at rest + mutual TLS** — AES-256 via a from-scratch
  RocksDB encryption shim, and mTLS on the gRPC listener; both opt-in via
  environment variables and fail-loud (never silently downgraded) when
  misconfigured.
- **Observability** — Prometheus metrics and a Grafana dashboard for the
  query and mutation paths, including per-dispatch-path latency breakdowns.
- **Benchmarked against real baselines** — a three-way harness runs the same
  trace against CareGraph, Neo4j + GDS, and TerminusDB.

### Dispatch, self-tuning, and attribution at a glance

```mermaid
graph LR
    M["Mutation\n(AddEdge / RemoveEdge)"] --> D

    D{"Manifest-driven\ndispatch"} -->|"is_associative=true"| G["GraphSAGE path\n(associative)"]
    D -->|"is_associative=false"| A["GAT path\n(staged incremental)"]

    G --> CC["Self-tuning cap controller\ndiscrete rung hill-climb, p95-driven"]
    A --> CC

    CC --> CM[("CF_COMMIT_META\ndispatch + caps + attribution")]

    E["explain=true?"] -->|yes| IG["Integrated-gradients\nedge attribution"]
    IG --> CM

    Q["SimilarityDelta RPC"] -.->|"reads two timestamps"| EMB[("CF_EMBEDDINGS")]
```

| Feature | What it does | Measured result |
|---------|--------------|------------------|
| Manifest-driven dispatch | Reads each model's `is_associative`/`architecture` from its manifest instead of a hardcoded `match`; cross-checks the running checkpoint at spawn time | Closes a real, previously-unchecked model/manifest mismatch footgun |
| Self-tuning cap controller | Discrete 5-rung hill-climb over `(fanout, max_expanded_nodes)`, driven by a live p95, recorded as versioned metadata | 1.46x p95 reduction (1165.53ms → 799.55ms) — real but modest, see `docs/benchmark_report.md` §2.7 |
| Integrated-gradients attribution | Per-request opt-in (`explain: true`) edge attribution, committed atomically with the embedding it explains | Completeness identity verified with zero failures on both architectures; real overhead is 100-300x an early estimate at full-graph scale, see `docs/benchmark_report.md` §2.6 |
| `SimilarityDelta` RPC | "How much did patient X's similarity to Y change between T1 and T2?" — a two-timestamp query, not a single snapshot | p50 0.056ms / p95 0.065ms on the full graph |

## Getting started

### One-command setup

```bash
docker compose -f infrastructure/docker-compose/dev-stack.yml up --build   # CareGraph only
```

This starts CareGraph alone, which is all a development session needs and keeps the
stack small. Add `--profile observability` for Prometheus and Grafana, and
`--profile baselines` for the two systems that Rule 4 requires benchmarks to run
against, Neo4j Community + GDS and TerminusDB; each service has a memory ceiling you
can set in `.env` (see [docs/memory.md](docs/memory.md)). No environment variables
are required.

To work outside Docker you need Rust, and a C++ toolchain for RocksDB. See
[docs/TOOLCHAIN.md](docs/TOOLCHAIN.md) for platform-specific setup.

```bash
cargo build
cargo test --lib              # unit tests
cargo test --test integration # against a real on-disk RocksDB
bash scripts/check_rules.sh   # Section 0 rule enforcement
bash scripts/run_demo.sh      # live end-to-end demo, see below
```

### Configuration

Every `caregraph*` binary loads a `.env` file from the current directory (or
the nearest parent) on startup, via the `dotenvy` crate — a real environment
variable you've already set always takes precedence over the file. Create
one at `caregraph/.env`:

```bash
cat > .env <<EOF
CAREGRAPH_API_KEY=$(openssl rand -hex 32)
EOF
```

| Variable | Why it's required |
|----------|--------------------|
| `CAREGRAPH_API_KEY` | Bearer token checked on every gRPC call by the auth interceptor. Unset means the server **refuses to start** rather than serving with auth silently disabled (Rule 2). |

Everything else — listener addresses, which model to load, encryption,
mTLS, attribution step count — has a working default and only needs to be
touched for non-default behavior. Settings are typed and validated in
`src/config.rs`; layering is defaults → optional TOML (`--config file.toml`) →
`CAREGRAPH_*` env vars, and any secret can be supplied as `<NAME>_FILE`
(a path, e.g. a Kubernetes Secret mount). `caregraph --print-config` shows the
effective configuration with secrets redacted.

**Production mode.** `CAREGRAPH_ENV=production` makes the server refuse to
start unless mTLS, encryption at rest, fsync'd writes (`CAREGRAPH_SYNC_WRITES`,
default on in production), a non-legacy timestamp policy and an RBAC policy
are all configured, and switches logs to JSON. Development mode keeps today's
permissive defaults.

**Throughput and ordering knobs.** `CAREGRAPH_WORKERS` (embedding worker
processes per model, default 1; 2 is a good production value),
`CAREGRAPH_TORCH_THREADS` (intra-op threads per worker; workers x threads must
not exceed the CPU count), `CAREGRAPH_TS_POLICY` (`strict` | `server_assigned` |
`legacy`; `strict` is the production default and the only policy under which
more than one worker is safe), `CAREGRAPH_MAX_CONCURRENT_READS`/`_WRITES`
(bulkheads; excess requests are refused with `RESOURCE_EXHAUSTED`), and
`CAREGRAPH_COMMIT_WITHOUT_EMBEDDING` (default off: if the embedding workers are
unavailable a mutation is rejected with a retryable `UNAVAILABLE` rather than
committed with stale embeddings).

**Limits, probes and shutdown.** Requests are bounded before they reach
storage: `properties_json` at most 64 KiB and 32 levels deep
(`CAREGRAPH_MAX_PROPERTIES_BYTES`/`_DEPTH`), client timestamps at most five
minutes ahead of the server clock (`CAREGRAPH_MAX_FUTURE_SKEW_SECS`),
`SimilarCarePathways` at most 1000 results, request messages at most 1 MiB
(`CAREGRAPH_MAX_REQUEST_BYTES`), and a whole-request deadline
(`CAREGRAPH_REQUEST_TIMEOUT_SECS`). `explain` is on in development and off in
production unless `CAREGRAPH_EXPLAIN_ENABLED=true`, and runs one at a time.
`CAREGRAPH_RATE_LIMIT_RPS` and `_BURST` enable a per-caller token bucket (reads
and writes budgeted separately; refusals carry a `retry-after-ms` trailer). The
metrics port also serves `/healthz` (process and database), `/readyz` (models
healthy, breaker closed, not draining) and `/startupz`. On SIGTERM the server
reports not-ready, waits `CAREGRAPH_PRESTOP_DELAY_SECS`, finishes in-flight
requests for at most `CAREGRAPH_DRAIN_DEADLINE_SECS`, waits for database work
to finish, flushes the WAL and exits.

**Security.** Production mode requires mutual TLS 1.3 (certificates reload without a
restart), a role policy (`CAREGRAPH_RBAC_POLICY`: who is a reader, writer, admin or
replicator, keyed by the client certificate's name), an encryption key, and an audit
directory. Optional hashed bearer tokens add a second factor tied to the same
identity. Every committed mutation writes a hash-chained audit record in the same
atomic batch; every read is recorded durably before it is served and is refused if it
cannot be. Encryption at rest can use a wrapped data key so the key-encryption key
rotates without re-encrypting the database. See [docs/security.md](docs/security.md).

**Backup and operations.** With `CAREGRAPH_BACKUP_DIR` set, an admin service
(role `admin`) takes incremental encrypted backups, hard-link checkpoints and verifies
the audit chain on the running server. `caregraph-admin` is the command line for it
and for the offline tasks: verifying and restoring backups (a restore never overwrites
a database and a backup directory refuses the wrong key), re-encrypting under a new
data key, rotating the key-encryption key, generating tokens, and listing nodes with
stale embeddings. See [docs/runbooks/](docs/runbooks/).

**Memory.** `CAREGRAPH_MEMORY_MB` is one budget for the whole server, the Rust process and
its Python embedding workers together: the workers are reserved first (about 330 MB
resident each, one per deployed model), and the rest is divided between a RocksDB cache
shared by every database in the process (memtables included, with a hard limit), the
node-type cache and the replication tail. A budget the workers cannot fit in is refused at
startup with what to change; `caregraph --print-config` shows the plan; `/metrics` exports
the plan and what the process tree actually holds. Not loading a model you do not serve
saves the most. Build and test memory is bounded too: linking, not compiling, was what
used it, and a full rebuild of every test target now grows system memory by 0.8 GB
instead of about 6 GB. Measurements and limits: [docs/memory.md](docs/memory.md).

**Replication.** `CAREGRAPH_ROLE=leader` plus `follower` nodes give read replicas and a
standby by shipping the write-ahead log: a follower applies the leader's batches in order
and is byte-for-byte the leader's database as of a sequence number, or stops and says it
needs rebuilding. Writes are acknowledged only after a standby has them (synchronous, with
a counted fall back to asynchronous if none is connected), reads on a replica can wait for
a sequence number to read their own writes, and `caregraph-admin remote failover` moves
leadership under an epoch number that fences the old leader. There is no automatic
failover and no quorum; see [docs/replication.md](docs/replication.md) for exactly what
is and is not guaranteed.

**Container images.** `docker build --target runtime-reader` (no Python) or
`--target runtime-writer` (Python + CPU torch + deployed models, the default)
from `infrastructure/docker-compose/Dockerfile`. One variable worth setting in `.env` even though it
has a default: `CAREGRAPH_PYTHON`, if the Python that has `torch` +
`torch_geometric` installed isn't the one named `python` on your `PATH`.

### Loading the clinical graph

The PRD names the IDPIP UKPDS-derived clinical graph (5,102 T2DM patients,
20-year follow-up) as the evaluation dataset; that source isn't reachable in
this environment, so every trace, benchmark, and demo in this repository
actually runs on the **Diabetes 130-US Hospitals** dataset (UCI id 296)
instead — a real, cited, public substitute, not synthetic data. Use this
path to reproduce anything in `docs/benchmark_report.md` or
`scripts/run_demo.sh`:

```bash
python data/diabetes130_loader.py \
    --csv data/raw/diabetic_data.csv \
    --out benchmarks/traces/diabetes130_full.jsonl

cargo run --release --bin caregraph-load -- \
    --trace benchmarks/traces/diabetes130_full.jsonl \
    --db data/db/diabetes130
```

`data/diabetes130_loader.py`'s own module doc lists exactly what is derived
rather than read verbatim from the source file (encounter dates, provider
identity) — read it before citing a number from this data (Rule 6).

**The loader has no synthetic mode.** It exits non-zero if it cannot read the
real source file (Rule 6). A benchmark measured on invented data is not a
measurement.

### Live demo

```bash
bash scripts/run_demo.sh
```

One command, no manual steps, safe to re-run. It seeds a fresh database from
a real slice of the Diabetes 130 trace, then replays that slice's final three
patient encounters live over gRPC instead of through the bulk loader — each
one committing its structural edge and its GraphSAGE embedding update
atomically (Rule 5) — and walks through bounded traversal, a before/after
point-in-time snapshot across those live mutations, and point-in-time
similarity search against the embeddings they just produced. See
[docs/api_reference.md](docs/api_reference.md) for what each RPC does, and
`src/bin/demo_client.rs` for the real client code the script drives.

## Architecture

Six layers, each reachable only through its defined interface.

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1. Storage | `src/storage/` | RocksDB, column families, WAL durability |
| 2. Temporal indexing | `src/temporal/` | Versioned key encoding, point-in-time scans |
| 3. Graph semantics | `src/graph/` | Bounded traversal, snapshot reconstruction |
| 4. Incremental embedding | `src/embedding/` | Affected-subgraph resolution, atomic commit |
| 5. Query & API | `src/api/` | gRPC service, auth, result limits |
| 6. Observability | `observability/` | Prometheus, Grafana, benchmark harness |

### Component diagram

```mermaid
graph TB
    Client["gRPC client\n(src/bin/demo_client.rs)"]

    subgraph L5 ["5. Query & API — src/api/"]
        API["CareGraphApi\nauth · result limits"]
        DIFF["similarity_delta / similar_care_pathways"]
    end

    subgraph L4 ["4. Incremental embedding — src/embedding/"]
        RES["Resolver\naffected-subgraph resolution"]
        MANI["Manifest-driven dispatch\n(associative vs. staged)"]
        CAPS["Self-tuning cap controller"]
        ATTR["Integrated-gradients attribution\n(opt-in: explain=true)"]
        AC["AtomicCommitter"]
    end

    subgraph L3 ["3. Graph semantics — src/graph/"]
        TRAV["Bounded traversal · snapshot reconstruction"]
    end

    subgraph L2 ["2. Temporal indexing — src/temporal/"]
        IDX["Versioned key encoding · as_of() scans"]
    end

    subgraph L1 ["1. Storage — src/storage/"]
        DB[("RocksDB\n5 column families, AES-256 at rest")]
    end

    subgraph L6 ["6. Observability — observability/"]
        PROM["Prometheus /metrics"]
        GRAF["Grafana dashboards"]
    end

    PY["ml/embedding_server.py\nGraphSAGE + GAT (torch_geometric)"]

    Client -- gRPC + mTLS --> API
    API --> DIFF
    API --> RES
    API --> TRAV
    RES --> MANI
    MANI <-- "forward pass" --> PY
    MANI --> CAPS
    MANI --> ATTR
    ATTR <-- "attribution request" --> PY
    MANI --> AC
    TRAV --> IDX
    DIFF --> IDX
    AC --> IDX
    IDX --> DB
    API -. metrics .-> PROM
    PROM --> GRAF
```

### Mutation lifecycle

What actually happens inside one `AddEdge`/`RemoveEdge` call — the path that
Rule 5 (atomicity) and Rule 7 (no silent fallback) are enforced against.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as "gRPC API (src/api)"
    participant R as Resolver
    participant D as "Dispatch (manifest-driven)"
    participant P as embedding_server.py
    participant K as CapController
    participant W as AtomicCommitter

    C->>A: AddEdge(src, dst, ts, model, explain?)
    A->>R: resolve affected subgraph
    R-->>A: nodes, edges, truncation flags
    A->>D: model.manifest.is_associative?
    alt associative (GraphSAGE)
        D->>P: forward(pre-patch) once, patch in place, forward(post-patch) once
    else non-associative (GAT)
        D->>P: staged incremental forward passes (RIPPLE++-style operator decoupling)
    end
    P-->>D: new embeddings
    D->>K: observe(dispatch-only duration)
    K-->>D: next fanout/expansion caps (recorded as versioned metadata)
    opt explain = true
        D->>P: integrated-gradients attribution request
        P-->>D: top-k edge attributions + completeness residual
    end
    D->>W: embeddings, dispatch decision, effective caps, attribution
    W->>W: single WriteBatch: edge + embedding + CF_COMMIT_META
    Note over W: all three writes commit together or not at all —<br/>verified by killing the process mid-commit (Rule 5)
    W-->>A: MutationResponse
    A-->>C: response
```

### Column families

| CF | Key | Value |
|----|-----|-------|
| `CF_EDGES` | `[src_id \| edge_type \| ts_desc \| dst_id]` | edge properties |
| `CF_REVERSE` | same, src/dst swapped | edge properties |
| `CF_NODES` | `[node_id \| ts_desc]` | node properties |
| `CF_EMBEDDINGS` | `[node_id \| ts_desc]` | vector + model_id + computation_path |
| `CF_COMMIT_META` | `[node_id \| ts_desc]` | dispatch decision + effective caps + attribution |
| `CF_REPAIR` | `[node_id \| ts_desc]` | durable stale-embedding marker (written when a forward pass fails) |

Timestamps are stored bit-inverted, so a *newer* version produces a *smaller*
byte sequence and sorts first. A point-in-time read is therefore a single
forward seek — no reverse iterator, no secondary index. That is the mechanism
behind O(log n) point-in-time retrieval:

```
CF_EMBEDDINGS, node 42, three versions written at T1 < T2 < T3
(ts stored as !T — bitwise NOT — so byte order sorts newest first)

  key: [42 | !T3]  ─┐  smallest byte sequence → seek() lands here first
  key: [42 | !T2]   ├─ RocksDB forward-iteration order
  key: [42 | !T1]  ─┘  largest byte sequence  → sorts last

  as_of(node=42, T=T2.5):
      seek(key = [42 | !T2.5])
        └──▶ first key ≥ target is [42 | !T2]   (one seek, no scan back)
```

Note that edge keys order by timestamp *before* `dst_id`, so within one
adjacency list the versions of different destinations are interleaved in time.
That is what makes a time-windowed scan ("every change to this patient's
diagnoses between T1 and T2") a single contiguous key range. The trade-off is
that reconstructing a full adjacency list at a timestamp costs a walk over the
list's version history rather than a seek per edge — see the module docs on
`src/temporal/index.rs`.

### Deletions are tombstones

A removal appends a version marked `deleted`, never a RocksDB `delete`.
Erasing the key would erase the history that point-in-time reconstruction
reads, making `as_of(T)` for a `T` before the removal wrongly report that the
edge never existed. The timeline is append-only.

## The ten non-negotiable rules

`scripts/check_rules.sh` continuously enforces ten invariants — real storage
(no mocks), real trained models (no random vectors), atomic commits, no
silent fallback to full recompute, real encryption, no placeholder
dashboards, and benchmark-cited claims among them. Every rule reports
`PASS`, `FAIL`, or `PENDING`; `PENDING` means the feature it covers hasn't
been built yet in a from-scratch build.

```bash
bash scripts/check_rules.sh            # report everything
bash scripts/check_rules.sh --phase 3  # gate: phase-3 rules must be live
bash scripts/check_rules.sh --rule 5   # one rule
```

`PENDING` is deliberately loud and never silent. Rule 10 has been retired for
this project and always reports `RETIRED`; rules 1-9 remain fully enforced.

## Repository layout

```
src/            Rust core — storage, temporal, graph, embedding, api
proto/          gRPC schema
data/           Diabetes 130 loader and clinical graph schema
ml/             GNN training and incremental-update reference
benchmarks/     Baseline harness and mutation traces
observability/  Prometheus rules, Grafana provisioning
infrastructure/ Docker Compose dev stack
scripts/        check_rules.sh, run_demo.sh
tests/          integration/, unit/, fault_injection/
docs/           Design notes, benchmark reports, API reference, runbooks
```

## License

Apache-2.0. Clinical data is **not** covered by this license and is never
committed to this repository.
