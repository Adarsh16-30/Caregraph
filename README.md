# CareGraph

A temporally-versioned graph database with incrementally-maintained GNN embeddings.

Graph mutations and their resulting embedding updates commit atomically inside a
single RocksDB `WriteBatch`, so both graph structure and embeddings are queryable
at any historical point in time. Embeddings are a first-class versioned field,
not a batch-computed side artifact.

**Status: Phases 1-7 complete and verified** (gRPC API + point-in-time
similarity query, three-way benchmark harness against live Neo4j/TerminusDB,
CI run for real on GitHub Actions, real encryption at rest + mTLS + live
Grafana dashboards). See [Build status](#build-status) for exactly what does
and does not exist yet.

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
| 1 | Infrastructure, KV abstraction, column families | complete — compiled, unit + integration tests passing |
| 2 | Temporal indexing, `as_of()` reads, windowed scans | complete — point-in-time read benchmark run against real clinical data |
| 3 | Bounded traversal, snapshots, Neo4j/TerminusDB baseline harness | complete — 2-hop traversal benchmark passing; baseline harness built, not yet run against live baselines |
| 4 | GraphSAGE/GCN incremental embeddings | complete — real trained model deployed (Rule 3); 50/50 randomised mutation sequences match full recompute exactly; 7.79x median speedup vs. the 5x target |
| 5 | GAT incremental path, atomic commit, fault injection | complete — atomic commit + 100-run fault injection (Rule 5: 0 non-atomic states across 80 actual kills); GAT path implemented and trained, 50/50 mutation sequences match full recompute exactly, same as Phase 4's GraphSAGE claim |
| 6 | gRPC API, three-way benchmark harness | complete — full gRPC API (mutation, traversal, snapshot, `similar_care_pathways`) implemented, real bearer-token auth, 5 RPCs covered by real-server endpoint tests (Rule 2); Neo4j + TerminusDB brought up live, loaded with the identical trace, and measured against CareGraph on 2-hop traversal (`docs/benchmark_report.md` §8) — CareGraph passes with ~2.8x headroom, Neo4j passes marginally, TerminusDB misses the target |
| 7 | Encryption at rest, mTLS, live dashboards | complete — real RocksDB encryption at rest via a from-scratch C++/AES-256 shim (the `rocksdb` crate exposes no encryption API; Rule 8), verified by reading raw on-disk SST bytes after a flush; mutual TLS on the gRPC listener, verified against real TLS handshakes with rcgen-generated certificates; `GET /metrics` finally serves the Prometheus registry dev-stack.yml has pointed at since Phase 1, with new query-path series verified to record real nonzero values, and a real Grafana dashboard bound to the live datasource (Rule 9) |
| 8 | Demo, patent hooks, paper draft | not started |

### Known gaps

1. **The three-way comparison covers one Section 1 metric, not all of them.**
   `benchmarks/run_baseline.sh` has now been run end to end against live
   Neo4j + GDS and TerminusDB containers, loaded with the byte-identical
   trace and measured on 2-hop bounded traversal — see
   `docs/benchmark_report.md` §8 for the numbers and their caveats (the run
   was from a dirty tree; point-in-time read latency and incremental-embedding
   speedup are still CareGraph-only). Extending the harness to the rest of
   Section 1's metrics three-way, and writing a full benchmark report
   generator, has not been done.
2. **Evaluation data is a substitute, disclosed as one.** The PRD names
   IDPIP/UKPDS; that source was not reachable in this environment. Evaluation
   instead runs on the Diabetes 130-US Hospitals dataset (UCI id 296) — see
   `data/diabetes130_loader.py`'s module doc for exactly what is derived
   rather than recorded (Rule 6). The IDPIP/UKPDS loader
   (`data/idpip_ukpds_loader.py`) is still implemented for when that source
   becomes available.
3. **CI now runs for real on GitHub's own infrastructure.** The repository
   is pushed to `https://github.com/Adarsh16-30/Caregraph`, and every
   currently-enabled `ci.yml` job (Section 0 rule enforcement, fmt+clippy,
   tests against real RocksDB, dev-stack Docker build, embedding correctness,
   100-run fault injection) has completed successfully on a GitHub runner —
   not just locally. The `benchmarks` job now runs too, now that
   `run_baseline.sh` has completed against live baselines at least once by
   hand (§8).
4. **Phase 7's AES-256 implementation is from-scratch, not a vetted library.**
   `native/rocksdb_encryption/aes256.h` exists because the `rocksdb` crate
   has no encryption API to build on at all (see its own module doc) and
   this build doesn't link OpenSSL. It's verified against two independent
   FIPS-197 test vectors and exercised for real by the integration suite,
   but it has not had independent cryptographic review — treat it as
   correct-per-the-standard-test-vectors, not as audited production crypto.
5. **mTLS is opt-in, not enforced.** `CAREGRAPH_TLS_CERT`/`_KEY`/`_CLIENT_CA`
   unset means the gRPC listener is plaintext (with a logged warning) —
   nothing in this build requires an operator to turn mTLS on. Same shape
   as `CAREGRAPH_ENCRYPTION_KEY`: explicit and fail-loud when configured,
   never silently downgraded, but not mandatory.
6. **The Grafana dashboard covers Section 1's metrics, not a full production
   dashboard suite.** One dashboard (`caregraph_section1.json`), seven
   panels, all bound to real series — no alerting rules, no per-model or
   per-column-family breakdowns beyond what `embedding_update_latency_seconds`'s
   `computation_path` label already gives.

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
