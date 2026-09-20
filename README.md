# CareGraph

A temporally-versioned graph database with incrementally-maintained GNN embeddings.

Graph mutations and their resulting embedding updates commit atomically inside a
single RocksDB `WriteBatch`, so both graph structure and embeddings are queryable
at any historical point in time. Embeddings are a first-class versioned field,
not a batch-computed side artifact.

**Status: Phases 1-9 complete and verified** (gRPC API + point-in-time
similarity query and similarity *delta*, three-way benchmark harness against
live Neo4j/TerminusDB, CI run for real on GitHub Actions, real encryption at
rest + mTLS + live Grafana dashboards, a real end-to-end live demo, versioned
explainable attribution, a self-tuning incremental-update boundary, and
architecture-agnostic dispatch). See [Build status](#build-status) for
exactly what does and does not exist yet — several phases leave real,
disclosed gaps rather than a smoothed-over "done".

Patent-strategy and prior-art documents (`docs/patent_hooks.md`,
`docs/novelty_analysis.md`, `docs/paper_draft.md`) are maintained privately
and are **not committed to this public repository** — see
[Known gaps](#known-gaps) #14 for what that means for `scripts/check_rules.sh`
Rule 10 on a fresh clone.

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
bash scripts/run_demo.sh      # Phase 8: live end-to-end demo, see below
```

## Configuration

`docker compose` sets all of these for you. Running `caregraph` (the
`src/main.rs` binary) directly needs at least `CAREGRAPH_API_KEY`; everything
else has a safe default.

| Variable | Required? | Default | Purpose |
|----------|-----------|---------|---------|
| `CAREGRAPH_API_KEY` | **Yes** | *(none — refuses to start)* | Bearer token the gRPC auth interceptor checks on every RPC. An unset key is a hard error, never "auth disabled" (Rule 2). Generate with `openssl rand -hex 32`. |
| `CAREGRAPH_DB_PATH` | No | `data/db/caregraph` | On-disk RocksDB directory. |
| `CAREGRAPH_GRPC_ADDR` | No | `0.0.0.0:50051` | gRPC listener address. |
| `CAREGRAPH_METRICS_ADDR` | No | `0.0.0.0:9100` | `GET /metrics` (Prometheus) listener address. |
| `CAREGRAPH_GRAPHSAGE_MODEL` | No | `diabetes130_graphsage` | Model id under `ml/deployed/` to serve for the associative dispatch path. |
| `CAREGRAPH_GAT_MODEL` | No | `diabetes130_gat` | Model id under `ml/deployed/` to serve for the staged (non-associative) dispatch path. |
| `CAREGRAPH_PYTHON` | No | `python` | Interpreter used to spawn `ml/embedding_server.py`. Point this at a venv's `python` if `torch`/`torch_geometric` aren't on the system interpreter. |
| `CAREGRAPH_ENCRYPTION_KEY` | No (recommended) | *(unencrypted, with a logged warning)* | 64 hex characters (32-byte AES-256 key) enabling encryption at rest (Rule 8). Set-but-malformed is refused outright, never silently downgraded. Generate with `openssl rand -hex 32`. |
| `CAREGRAPH_TLS_CERT` / `CAREGRAPH_TLS_KEY` / `CAREGRAPH_TLS_CLIENT_CA` | No | *(plaintext gRPC)* | PEM file paths enabling mutual TLS. Setting `CAREGRAPH_TLS_CERT` requires the other two. |
| `CAREGRAPH_ATTRIBUTION_STEPS` | No | `16` | Integrated-gradients quadrature step count for Phase 9's `explain: true` attribution path. |
| `CAREGRAPH_HARDWARE` | No (benchmarks only) | *(none)* | Free-text hardware description recorded into each `caregraph-bench-*` binary's output JSON for provenance (Rule 10). |
| `IDPIP_DATABASE_URL` | No (only for the IDPIP loader) | *(none — loader exits non-zero)* | PostgreSQL/TimescaleDB connection string for `data/idpip_ukpds_loader.py`. Not needed for the Diabetes 130 path this repo actually runs on. |

## Loading the clinical graph

The PRD names the IDPIP UKPDS-derived clinical graph (5,102 T2DM patients,
20-year follow-up) as the evaluation dataset; that source is not reachable in
this environment (Known gap #2), so every trace, benchmark, and demo in this
repository actually runs on the **Diabetes 130-US Hospitals** dataset (UCI
id 296) instead — a real, cited, public substitute, not synthetic data. Use
this path to reproduce anything in `docs/benchmark_report.md` or
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

If IDPIP's TimescaleDB source becomes reachable, `data/idpip_ukpds_loader.py`
implements the loader the PRD actually names, producing a byte-identical
trace format so the same `caregraph-load` command and the same three-way
Neo4j/TerminusDB comparison apply unchanged:

```bash
export IDPIP_DATABASE_URL='postgresql://user@host:5432/idpip'
python data/idpip_ukpds_loader.py --limit-patients 100 \
    --out benchmarks/traces/ukpds_smoke_100.jsonl

cargo run --release --bin caregraph-load -- \
    --trace benchmarks/traces/ukpds_smoke_100.jsonl \
    --db data/db/caregraph
```

**Neither loader has a synthetic mode.** Both exit non-zero if they cannot
reach their real source (Rule 6). A benchmark measured on invented data is
not a measurement.

## Live demo

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

### Column families

| CF | Key | Value |
|----|-----|-------|
| `CF_EDGES` | `[src_id \| edge_type \| ts_desc \| dst_id]` | edge properties |
| `CF_REVERSE` | same, src/dst swapped | edge properties |
| `CF_NODES` | `[node_id \| ts_desc]` | node properties |
| `CF_EMBEDDINGS` | `[node_id \| ts_desc]` | vector + model_id + computation_path |
| `CF_COMMIT_META` | `[node_id \| ts_desc]` | dispatch decision + effective caps + attribution (Phase 9) |

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
cannot be quietly outrun by the build. Rule 10 additionally has a narrow
`EXCLUDED` outcome, used only when `docs/patent_hooks.md` is absent because it
was deliberately kept out of this public repository — see
[Known gaps](#known-gaps) #14.

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
| 5 | GAT incremental path, atomic commit, fault injection | complete — atomic commit + 100-run fault injection against **both** `AtomicCommitter` dispatch arms (GraphSAGE: 49 actual kills, 0 non-atomic states; GAT: 78 actual kills, 0 non-atomic states — Rule 5); GAT path implemented and trained, 50/50 mutation sequences match full recompute exactly, 8.1x median incremental speedup (p95 latency misses the 100ms target on the full graph, same as Phase 4's GraphSAGE finding — see `docs/benchmark_report.md` §2.3-§2.4) |
| 6 | gRPC API, three-way benchmark harness | complete — full gRPC API (mutation, traversal, snapshot, `similar_care_pathways`) implemented, real bearer-token auth, 5 RPCs covered by real-server endpoint tests (Rule 2); Neo4j + TerminusDB brought up live, loaded with the identical trace, and measured against CareGraph on 2-hop traversal (`docs/benchmark_report.md` §8) — CareGraph passes with ~2.8x headroom, Neo4j passes marginally, TerminusDB misses the target |
| 7 | Encryption at rest, mTLS, live dashboards | complete — real RocksDB encryption at rest via a from-scratch C++/AES-256 shim (the `rocksdb` crate exposes no encryption API; Rule 8), verified by reading raw on-disk SST bytes after a flush; mutual TLS on the gRPC listener, verified against real TLS handshakes with rcgen-generated certificates; `GET /metrics` finally serves the Prometheus registry dev-stack.yml has pointed at since Phase 1, with new query-path series verified to record real nonzero values, and a real Grafana dashboard bound to the live datasource (Rule 9) |
| 8 | Demo, patent hooks, paper draft | complete for what an agent in this repository can do — `scripts/run_demo.sh` runs a real end-to-end demo (live mutation, traversal, snapshot, similarity) start to finish with no manual steps; `docs/patent_hooks.md` states five benchmark-cited claims (Rule 10) plus a real, newly-run Rule 5 fault-injection result; `docs/novelty_analysis.md` gives the per-claim prior-art comparison; `docs/paper_draft.md` is a CIDR/ICDE/SIGMOD/VLDB-shaped draft citing the same real numbers. The university IDF-B filing and the Palantir/Pinterest/LinkedIn patent-literature cross-check are explicitly **not done** — see Known gaps below and `docs/novelty_analysis.md` §4 |
| 9 | Explainable attribution, self-tuning caps, similarity delta, architecture-agnostic dispatch | complete — integrated-gradients edge attribution committed atomically with the embedding it explains (`CF_COMMIT_META`), verified via a real completeness identity on both deployed architectures with zero failures (`tests/embedding/attribution_completeness_test.rs`); a discrete self-tuning cap controller measuring a real 1.46x p95 reduction (`docs/benchmark_report.md` §2.7); a point-in-time similarity delta RPC (`SimilarityDelta`, Rule 2-covered); manifest-driven dispatch replacing the hardcoded `ModelKind` match, closing a real, previously-unchecked model/manifest mismatch footgun. Two severe, disclosed findings: attribution overhead on the real graph is 100-300x larger than an initial small-subgraph estimate (tens of seconds per request — §2.6), and one real GAT mutation's completeness residual reached 22.35%, over 3x the correctness suite's own synthetic-fixture tolerance. Rule 5 re-verification that the new three-way write (edge + embedding + `CF_COMMIT_META`) survives a mid-commit kill was run against both dispatch arms — see Known gaps below for the exact kill counts |

### Known gaps

1. **The three-way *baseline comparison* covers one Section 1 metric, not
   all of them.** `benchmarks/run_baseline.sh` has been run end to end
   against live Neo4j + GDS and TerminusDB containers, loaded with the
   byte-identical trace and measured on 2-hop bounded traversal — see
   `docs/benchmark_report.md` §8 for the numbers and their caveats (the run
   was from a dirty tree). Point-in-time read latency, incremental-embedding
   speedup, and sustained ingestion throughput are all now measured
   (`docs/benchmark_report.md` §2), but only CareGraph-side — extending
   `run_baseline.sh` to run all of Section 1's metrics three-way against
   Neo4j and TerminusDB, and writing a full benchmark report generator,
   has not been done. Two of the CareGraph-only numbers are themselves a
   **miss** against their Section 1 target, not just an incomplete
   comparison: incremental embedding update p95 latency is ~15x over
   target for both GraphSAGE and GAT on the full 174,298-node graph — see
   `docs/benchmark_report.md` §2.3-§2.4 for the honest number and why the
   miss is scale-dependent, not a defect in the incremental path.
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
7. **The IDF-B disclosure and the Palantir/Pinterest/LinkedIn prior-art
   cross-check are not done.** `docs/patent_hooks.md` and
   `docs/novelty_analysis.md` are real inputs to that process — five
   benchmark-cited claims and a per-claim comparison against the systems
   the PRD names — but actually filing through VIT's IDF-B process and
   searching the patent literature itself (not just competing systems'
   public behavior) needs a human decision this repository cannot make on
   its own. See `docs/novelty_analysis.md` §4.
8. **`docs/paper_draft.md` is a draft, not a submission**, and Rule 10's
   automated `[benchmark: file]` check (`scripts/check_rules.sh`) only
   greps `docs/patent_hooks.md` — the paper draft follows the same citation
   convention voluntarily, but nothing enforces it there yet. See the
   paper draft's own §7 for what turning it into an actual submission would
   still need.
9. **`scripts/run_demo.sh`'s server teardown needed a real fix mid-Phase-8.**
   Killing the demo server via bash's own `$!` PID silently failed under
   Git Bash / MSYS on Windows — `$!` is an MSYS-internal PID, not the real
   Windows PID `taskkill` needs, so the first version of the cleanup left
   the server (and its Python embedding-model child) running after the
   script exited. Fixed by resolving the real PID through MSYS `ps`'s own
   WINPID column first; re-run and confirmed via `Get-Process` that nothing
   was left behind afterward.
10. **Two Section 2/10 stack entries are unused, not substituted.** ONNX
    Runtime (§2.3) never appears anywhere in this build — `ml/embedding_server.py`
    fills its role instead (see `docs/benchmark_report.md` §7.1). Section 10's
    `ml/embedding/gat_incremental.py` and `ml/ripple_plus_reference/` paths
    don't exist either: the GAT incremental logic lives in
    `src/embedding/gat_incremental.rs`, and there is no vendored RIPPLE++
    checkout, only its operator-decoupling technique reimplemented directly.
    Creating stub files at those two paths to match the directory listing
    would itself be placeholder content — not done, disclosed here and in
    `docs/benchmark_report.md` §7.1 instead. (Phase 9 renamed
    `src/embedding/gat_incremental.rs` to `src/embedding/staged_incremental.rs`
    and generalized it beyond GAT specifically — see Phase 9's row above.)
11. **Attribution overhead, measured on the real graph, is severe — plan
    around tens of seconds per request, not milliseconds.** An early
    estimate based on a small synthetic subgraph projected 160-550 ms; the
    real number on the full 174,298-node graph is **100-300x larger**
    (GraphSAGE median 23.6s/p95 41.8s; GAT median 78.1s/p95 97.9s — see
    `docs/benchmark_report.md` §2.6). This is exactly why attribution is a
    per-request opt-in (`explain: bool`) and not a default. A separate,
    real production mutation pushed GAT's completeness residual to 22.35%
    — over 3x the 6% worst case measured on the correctness suite's own
    synthetic fixture — real evidence that extreme-degree hubs need more
    than the default 16 integration steps to converge tightly, not a
    fabricated number smoothed into the correctness test's tolerance.
12. **The cap controller's measured win is real but modest.** 1.46x p95
    reduction (1165.53ms → 799.55ms), not a fix for Known Gap #1's ~15x
    miss — see `docs/benchmark_report.md` §2.7. Both the pinned and
    adaptive arms hit the same 63.3% `expansion_capped_rate` on the sampled
    mutations, disclosed rather than left implicit: the latency win did not
    come from truncating less often.
13. **Phase 9's Rule 5 fault-injection re-verification is done: the new
    three-way write (edge, embedding, and `CF_COMMIT_META`) still commits
    atomically.** 100 iterations per dispatch arm: GraphSAGE 57 actual
    kills (56 fully committed, 44 fully uncommitted), GAT 94 actual kills
    (6 fully committed, 94 fully uncommitted) — **0 non-atomic states in
    either arm**. GAT's near-total 94/100 kill rate exercised the race far
    more aggressively than GraphSAGE's 57/100, for the same reason Phase
    5's own log noted: attention-weighted aggregation takes measurably
    longer per commit, widening the window a kill can land inside. See
    `benchmarks/results/gate/phase9_fault_injection.log` and
    `phase9_fault_injection_gat.log` for the full breakdown and provenance.
14. **Patent-strategy and prior-art documents are deliberately not part of
    this public repository.** `docs/patent_hooks.md`, `docs/novelty_analysis.md`,
    and `docs/paper_draft.md` are maintained privately instead of being
    committed. They existed in this repository's history from Phase 8
    onward and were removed by request, including from prior commits —
    `git log` on a fresh clone will not surface them. This has one concrete,
    disclosed effect: `scripts/check_rules.sh`'s Rule 10 depends on
    `docs/patent_hooks.md` to check that every quantitative claim carries a
    `[benchmark: file]` citation, and a fresh clone has no such file to
    check. Rather than let that surface as a bare, unexplained PENDING/FAIL,
    CI sets `CAREGRAPH_PATENT_DOCS_EXCLUDED=1`, which makes `check_rules.sh`
    report a distinct, explicit `EXCLUDED` outcome for Rule 10 naming exactly
    why — never counted as a `PASS`. Anyone running `check_rules.sh` locally
    with the real files present on disk (as this project's own author does)
    still gets the genuine citation check, unaffected.

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
docs/           Design notes, benchmark reports, API reference
                (patent_hooks.md / novelty_analysis.md / paper_draft.md are
                kept privately, not in this repository — see Known gaps #14)
```

## License

Apache-2.0. Clinical data is **not** covered by this license and is never
committed to this repository.
