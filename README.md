# CareGraph

A temporally-versioned graph database with incrementally-maintained GNN embeddings.

Graph mutations and their resulting embedding updates commit atomically inside a
single RocksDB `WriteBatch`, so both graph structure and embeddings are queryable
at any historical point in time. Embeddings are a first-class versioned field,
not a batch-computed side artifact.

**Status: Phase 1 of 8 (Infrastructure Foundation).** See
[Build status](#build-status) for exactly what does and does not exist yet.

---

## One-command setup

```bash
docker compose -f infrastructure/docker-compose/dev-stack.yml up --build
```

This starts CareGraph alongside the two baseline systems that Rule 4 requires
benchmarks to run against — Neo4j Community + GDS, and TerminusDB — plus
Prometheus and Grafana.

To work outside Docker you need Rust, and a C++ toolchain for RocksDB. See
[docs/TOOLCHAIN.md](docs/TOOLCHAIN.md) for platform-specific setup.

```bash
cargo build
cargo test --lib              # unit tests
cargo test --test integration # against a real on-disk RocksDB
bash scripts/check_rules.sh   # Section 0 rule enforcement
```

## Loading the clinical graph

CareGraph evaluates on the real IDPIP UKPDS-derived clinical graph (5,102 T2DM
patients, 20-year follow-up). Loading is two steps: export a mutation trace from
IDPIP's TimescaleDB, then apply it.

```bash
export IDPIP_DATABASE_URL='postgresql://user@host:5432/idpip'
python data/idpip_ukpds_loader.py --limit-patients 100 \
    --out benchmarks/traces/ukpds_smoke_100.jsonl

cargo run --bin caregraph-load -- \
    --trace benchmarks/traces/ukpds_smoke_100.jsonl \
    --db data/db/caregraph
```

The intermediate trace exists so the byte-identical input can be replayed into
CareGraph, Neo4j+GDS, and TerminusDB — which is what makes the comparison
apples-to-apples.

**There is no synthetic mode.** The loader exits non-zero if it cannot reach a
real clinical source (Rule 6). A benchmark measured on invented data is not a
measurement.

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

### Column families

| CF | Key | Value |
|----|-----|-------|
| `CF_EDGES` | `[src_id \| edge_type \| ts_desc \| dst_id]` | edge properties |
| `CF_REVERSE` | same, src/dst swapped | edge properties |
| `CF_NODES` | `[node_id \| ts_desc]` | node properties |
| `CF_EMBEDDINGS` | `[node_id \| ts_desc]` | vector + model_id + computation_path |

Timestamps are stored bit-inverted, so a *newer* version produces a *smaller*
byte sequence and sorts first. A point-in-time read is therefore a single
forward seek — no reverse iterator, no secondary index. That is the mechanism
behind O(log n) point-in-time retrieval.

## The ten non-negotiable rules

`scripts/check_rules.sh` enforces PRD Section 0. Every rule reports `PASS`,
`FAIL`, or `PENDING`; `PENDING` means the phase that introduces the rule's
subject matter has not started.

```bash
bash scripts/check_rules.sh            # report everything
bash scripts/check_rules.sh --phase 3  # gate: phase-3 rules must be live
bash scripts/check_rules.sh --rule 5   # one rule
```

`PENDING` is deliberately loud and never silent. At a phase gate, `--phase N`
upgrades any rule that should be live by phase N into a hard failure — so a rule
cannot be quietly outrun by the build.

## Build status

Later phases are **absent, not stubbed**. A stub that compiles is
indistinguishable from an implementation to anything except a reader, which is
the failure mode Section 0 exists to prevent.

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Infrastructure, KV abstraction, column families | code complete, **not yet compiled** |
| 2 | Temporal indexing, `as_of()` reads | key encoding done; read API pending |
| 3 | Graph semantics, traversal, first Neo4j benchmark | not started |
| 4 | GraphSAGE/GCN incremental embeddings | not started |
| 5 | GAT incremental path, atomic commit, fault injection | not started |
| 6 | gRPC API, three-way benchmark harness | not started |
| 7 | Encryption at rest, mTLS, live dashboards | not started |
| 8 | Demo, patent hooks, paper draft | not started |

### Known gaps

1. **Nothing has been compiled.** Written on a machine without a Rust toolchain.
   Expect dependency-version drift on first `cargo build` — in particular the
   RocksDB crate (see [docs/TOOLCHAIN.md](docs/TOOLCHAIN.md)).
2. **No clinical data access.** The 100-patient Phase 1 smoke-test dataset
   requires `IDPIP_DATABASE_URL`. Until that is available, Phase 1's data
   deliverable is incomplete — and is reported as incomplete rather than filled
   in with generated records.

## Repository layout

```
src/            Rust core — storage, temporal, (later) graph/embedding/api
proto/          gRPC schema (Phase 6)
data/           UKPDS loader and clinical graph schema
ml/             GNN training and incremental-update reference (Phase 4)
benchmarks/     Baseline harness and mutation traces (Phase 3)
observability/  Prometheus rules, Grafana provisioning
infrastructure/ Docker Compose dev stack
scripts/        check_rules.sh, run_demo.sh
tests/          integration/, unit/, fault_injection/
docs/           Design notes, benchmark reports, patent hooks
```

## License

Apache-2.0. Clinical data is **not** covered by this license and is never
committed to this repository.
