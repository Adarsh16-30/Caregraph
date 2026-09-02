# CareGraph Patent Disclosure Hooks

Five technical claims, each traceable to a specific benchmark run per Rule
10 — commit hash, timestamp, and raw-results file next to every
quantitative statement, never a projected or assumed number. This document
is the input to the university IDF-B invention-disclosure process (Phase 8);
it is not itself that filing.

**This is a draft for that process, not a substitute for it.** Actually
submitting through VIT's IDF-B process and cross-checking against the
Palantir, Pinterest, and LinkedIn prior-art families named in the PRD's
Phase 8 tasks requires a human decision (what to file, when, with which
inventors listed) that this repository cannot make on its own — see
`docs/novelty_analysis.md` §4 for what a real cross-check would need and
why it isn't attempted here.

---

## Claim 1 — Atomic Versioned Mutation-Plus-Embedding Transaction

**What is claimed:** A durable, ACID commit of a graph structural mutation
and its incrementally-computed GNN embedding update as a single unit —
implemented as one RocksDB `WriteBatch` per mutation
(`src/embedding/atomic_commit.rs`, PRD §9.2). No system named in the PRD's
own prior-art scan (TerminusDB, NeutronRT, RIPPLE++) combines durable ACID
versioning with incremental embedding maintenance in the same transaction.

**Measured evidence:**

- 100 fault-injection iterations raced a randomised kill against the worker's own commit call, against `AtomicCommitter`'s GraphSAGE dispatch arm — 49 of the 100 actually landed a kill (the rest finished naturally before the jitter window elapsed), leaving 59 fully-committed and 41 fully-uncommitted outcomes and 0 non-atomic states: every reopened database showed either both the structural edge and both endpoints' embeddings, or neither. [benchmark: benchmarks/results/gate/phase5_fault_injection.log]
- The same 100-iteration suite, run separately against `AtomicCommitter`'s other dispatch arm (`gat_incremental_update`, Claim 4) — 78 of 100 landed a kill, 26 fully-committed, 74 fully-uncommitted, 0 non-atomic states. This arm is a materially different function, not just a different weight file, and was never exercised under a kill before this pass. [benchmark: benchmarks/results/gate/phase5_fault_injection_gat.log]
- The commit path used in that test is the same `atomic_commit.rs` code the
  live gRPC `AddEdge`/`RemoveEdge` RPCs call — not a test-only stand-in
  (`src/api/mod.rs::run_mutation`).

**Prior measurement context (informational, not this claim's own
evidence):** the incremental-embedding claim this transaction wraps
(Claim 3) measures 7.79x [benchmark: benchmarks/results/gate/phase4_incremental_speedup.json]
median speedup over full recompute — Claim 1 is about the commit being
atomic regardless of that speedup, not about the speedup itself.

---

## Claim 2 — Temporal Key Encoding for O(log n) Point-in-Time Embedding Retrieval

**What is claimed:** A descending-timestamp column-family key layout
(`src/temporal/keys.rs::encode_edge_key`/`as_of_prefix`, PRD §9.1) — bits of
the timestamp are inverted so the newest version of a node, edge, or
embedding sorts first in a forward RocksDB prefix scan. A point-in-time read
is therefore one seek, with no secondary index and no reverse iterator.

**Measured evidence:**

- Point-in-time node read: p50 0.013 ms, **p95 0.192 ms**, p99 0.258 ms. [benchmark: benchmarks/results/gate/phase2_pit_latency.json]
- Point-in-time edge read: p50 0.029 ms, **p95 0.240 ms**, p99 0.318 ms. [benchmark: benchmarks/results/gate/phase2_pit_latency.json]
- Both clear the PRD's target by roughly **100x** at p95. [benchmark: benchmarks/results/gate/phase2_pit_latency.json] Measured
  on 1,000 queries against the 174,298-node Diabetes 130 graph
  (`docs/benchmark_report.md` §2.1), at a graph size where the margin is
  large enough that the target is not a binding constraint. This is the
  same key encoding `CF_EMBEDDINGS` uses for versioned embedding storage
  (`src/temporal/keys.rs::encode_embedding_key`); the measured reads above
  are node/edge reads through the identical scan primitive, since embedding
  point-in-time reads were not separately load-tested this session (see
  `docs/benchmark_report.md` §2.1's own note that embedding PIT latency is
  not measured).

---

## Claim 3 — Durability-Integrated Incremental Runtime Embedding Computation (RTEC)

**What is claimed:** Bridging NeutronRT/RIPPLE++-style operator-decoupled
incremental GNN inference into a transactional, crash-consistent storage
engine (`src/embedding/resolver.rs`, `src/embedding/associative.rs`),
rather than treating incremental embeddings as an ephemeral GPU/CPU cache
that is lost or must be rebuilt on restart.

**Measured evidence:**

- 30 real `add_edge` mutations sampled across the full 174,298-node / 515,117-edge graph: incremental median 820.27 ms vs. full-recompute median 6210.12 ms — **7.79x** median speedup, comfortably above target. [benchmark: benchmarks/results/gate/phase4_incremental_speedup.json]
- `incremental_fallback_total` was 0 across all 30 real samples and all 400
  correctness-test mutations (Rule 7 — a silent full-recompute fallback
  would be a build failure, not just a slow path) — the same source cited
  above records this alongside the speedup figures.
- Every one of those 30 mutations committed through the same
  `atomic_commit.rs` path Claim 1's fault-injection evidence exercises —
  the incremental result is never observable in a partially-written state.
- This required fixing two real, measured bugs before the number above was
  trustworthy: a resolver that missed second-ring neighbours (silently
  corrupting affected embeddings by ~1e-2, four orders of magnitude looser
  than the 6e-7 floating-point noise floor after the fix) and an
  unbounded ring-two receptive field (49,165 nodes fetched for a 521-node
  affected set on one representative mutation before `max_expanded_nodes`
  was added) — see `docs/benchmark_report.md` §7.4–§7.7 for the full,
  undisclosed-nothing account of both.

---

## Claim 4 — Staged Incremental Update Path for Constrained Aggregations (GAT)

**What is claimed:** A correct, durable incremental update path for
attention-based GNNs (`src/embedding/gat_incremental.rs`), whose
non-associative attention aggregation means a mutation's effect cannot be
summed as a delta the way GraphSAGE/GCN's aggregation can — current
incremental-GNN literature flags this case as an open problem (PRD §6.2).

**Measured evidence:**

- `tests/embedding/gat_correctness_test.rs` — the GAT counterpart to Claim
  3's correctness gate — shows the incremental GAT path exactly matches a
  full-graph recompute (same float32 tolerance discipline as the
  GraphSAGE test) on 50 randomised mutation sequences. Run with
  `cargo test --test embedding`; not a timed benchmark
  [benchmark: N/A — correctness test, see docs/benchmark_report.md §7.4 for
  why exactness, not a percentage, is the relevant claim here].
- 30 real `add_edge` mutations sampled from the same 174,298-node full clinical graph Claim 3 uses, timed through `gat_incremental_update` instead of the associative path: incremental median 517.36 ms vs. full-recompute median 8209.36 ms — **8.1x** median speedup, above Rule 7's 5x bar. [benchmark: benchmarks/results/gate/phase5_gat_incremental_speedup.json]
- `incremental_fallback_total` stayed at zero across every one of those sampled mutations — the exactness claim above is unaffected by the disclosed gap below. [benchmark: benchmarks/results/gate/phase5_gat_incremental_speedup.json]

**Disclosed gap:** the PRD's own Phase 5 success criterion — p95 GAT incremental latency under 100ms — is measured here for the first time and is **not met**: p95 1531.04 ms on this graph size, roughly 15x over target. [benchmark: benchmarks/results/gate/phase5_gat_incremental_speedup.json] Same scale-dependent
shape as Claim 3's own GraphSAGE miss (see `docs/benchmark_report.md`
§2.3-§2.4) — this is a genuine miss on this graph size, not a fallback or
a measurement gap.

---

## Claim 5 — Native Point-in-Time Embedding Similarity as a Query Primitive

**What is claimed:** Care-pathway similarity search evaluated `as of` a
historical timestamp, not only against current state
(`src/api/similarity.rs::similar_care_pathways`) — a single versioned
`CF_EMBEDDINGS` scan at a chosen `as_of`, ranked by cosine similarity, with
no separate recompute step and no export to an external vector index.

**Measured evidence:**

- The RPC is exercised end to end by a real spawned server and a real
  generated gRPC client in `tests/integration/api_endpoint_test.rs`
  (Rule 2 — response content varies with `node_id`/`as_of`/`top_k`, never a
  fixed shape) and again live in `scripts/run_demo.sh` /
  `src/bin/demo_client.rs`, which calls it against embeddings a live
  mutation just committed moments earlier in the same run.
- `point_in_time_query_seconds` (`src/api/metrics.rs`) is a live Prometheus
  histogram wired into this exact code path (Phase 7) and was verified to
  record real nonzero values from a live gRPC call before being trusted —
  see the Phase 7 completion record in `README.md`'s Build status table.

**Disclosed gap:** no p95/p99 latency percentile has been measured for
`similar_care_pathways` specifically — Section 1 names point-in-time *node
and edge* read latency (Claim 2's evidence) and 2-hop traversal latency
(§2.2 of the benchmark report) as its two measured point-in-time/bounded
targets; similarity latency was not one of the metrics `run_baseline.sh`
was extended to cover in Phase 6 (see `docs/benchmark_report.md` §8.3's own
list of what is and is not yet three-way, or CareGraph-only). A number is
not asserted here in its absence.

---

## Comparative standing (supporting context, not a sixth claim)

The three-way benchmark (Rule 4, `docs/benchmark_report.md` §8) measured CareGraph **~3.7x** faster than Neo4j Community + GDS and **~11.9x** faster than TerminusDB at p95 2-hop bounded traversal, on the identical byte-for-byte-loaded graph and query set, in one uncooled run on one machine (commit `cc3a62c`, run id `20260831T063511Z`). [benchmark: benchmarks/results/run_20260831T063511Z.manifest.json]

Per-system raw results for that run:
[benchmark: benchmarks/results/traversal_2hop_caregraph_1788159953.json]
[benchmark: benchmarks/results/traversal_2hop_neo4j_1788160089.json]
[benchmark: benchmarks/results/traversal_2hop_terminusdb_1788160424.json]
Read `docs/benchmark_report.md` §8's own caveats (dirty tree at load time,
single run, one of Section 1's several target metrics) before citing this
figure outside this repository.
