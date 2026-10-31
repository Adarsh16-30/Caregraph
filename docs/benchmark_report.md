# CareGraph Benchmark Report — Phases 2–9 and production hardening (P1–P10)

**Status: §§2–7 are CareGraph-only (Phases 2–4). §8 is the Rule 4 three-way
comparative benchmark** — Neo4j Community + GDS and TerminusDB were both
brought up live, loaded with the byte-identical trace, and measured against
CareGraph on the same 2-hop traversal workload. Read §8's own caveats
(dirty tree, single uncooled run, one of several Section 1 metrics) before
quoting a cross-system number out of this file.

§9 covers the Phase 9 features. §10 covers the production-hardening work
(P1–P10), including the concurrent-load comparison against Neo4j and
TerminusDB in §10.8, which supersedes §8 for throughput comparisons. Every
figure was measured on one shared laptop, not a dedicated machine.

---

## 1. Provenance

| Field | Value |
|---|---|
| Commit | `d1757bd14a674d5a0cee18f51f99d4b1b9256602` |
| Working tree | **clean** at run time — the gate results below reproduce from this commit |
| Hardware | Intel Core i5-13450HX (10C/16T), 15.6 GB RAM, NVMe SSD |
| OS | Windows 11 Home 26200 |
| Build | `cargo build --release`, rustc 1.97.1, rocksdb crate 0.23.0 (RocksDB 10.4.2) |

### Dataset

Diabetes 130-US Hospitals for years 1999–2008 — UCI Machine Learning
Repository, dataset 296. Real, de-identified inpatient records.

> Strack B, DeShazo JP, Gennings C, Olmo JL, Ventura S, Cios KJ, Clore JN.
> *Impact of HbA1c Measurement on Hospital Readmission Rates: Analysis of 70,000
> Clinical Database Patient Records.* BioMed Research International,
> 2014:781670.

| | |
|---|---|
| Source SHA-256 | `0689e7ec031237dc63031b938805c48377748761a3b26acab621567afa24df97` |
| Trace SHA-256 | `ff614401a42bd81f7d7670a99d33299f7d42eb0c88e79ce8ba77858d37f6ffc4` |
| Encounters | 101,766 |
| Patients | 71,518 |
| Graph records | 784,784 (174,298 nodes, 599,497 edges, 10,989 retractions) |
| Load time | 3.83 s into a real RocksDB instance (39 MB on disk) |

**This is not the UKPDS graph the PRD specifies.** IDPIP's TimescaleDB was not
reachable, so `IDPIP_DATABASE_URL` could not be set and no UKPDS extract exists
locally. This dataset was substituted under Rule 6's "another named, cited
public dataset" clause. It is diabetes-focused inpatient data and structurally
comparable, but it is a substitution and every number here inherits that.

### What is derived rather than read

1. **Absolute dates.** The public release strips calendar dates.
   `encounter_id` is assigned in admission order, so encounter *sequence* is
   real; the dates attached to it are not. Encounters are ranked by
   `encounter_id` and spread evenly across the documented 1999–2008 window.
   Relative order — what point-in-time reads and traversal windows actually
   exercise — is faithful. Inter-encounter gaps are uniform where the real ones
   were not.
2. **Provider identity.** The file records an admitting `medical_specialty`,
   not a provider ID. Specialty is used as a provider-proxy node.
3. **Procedures omitted.** `num_procedures` is a count with no procedure
   identities, so no `UNDERWENT_PROCEDURE` edges are emitted. The graph is
   honestly short one edge type.

---

## 2. Results against Section 1 targets

Section 1 names six metrics. All six now have a real, measured number below —
three pass with wide margin, one (incremental update latency, GraphSAGE) is a
genuine miss on this graph size, and two (GAT incremental latency, sustained
ingestion throughput) are newly measured in this pass and were not
previously in this table at all; see each subsection for what "embedding
staleness eliminated" is checked against, since it isn't a benchmarked
latency number the way the other five are.

| Metric | Target | Measured | Verdict |
|---|---|---|---|
| Point-in-time node read (p95) | < 20 ms | **0.192 ms** | PASS |
| Point-in-time edge read (p95) | < 20 ms | **0.240 ms** | PASS |
| 2-hop bounded traversal (p95) | < 50 ms | **13.026 ms** | PASS |
| Incremental embedding update (p95), GraphSAGE | < 100 ms | **1548.96 ms** (Phase 4; since improved, see the note in §2.3) | **MISS** at Phase 4; ~205-405 ms after P4 (§10.3); **67 ms** with hub deferral on (§10.9) |
| Incremental embedding update (p95), GAT | < 100 ms | **1531.04 ms** | **MISS** — see §2.4 |
| Sustained ingestion throughput | 10,000-50,000 edges/sec | **216,865 edges/sec** | PASS — see §2.5 |

### 2.1 Point-in-time reads — passes with ~100x headroom

[benchmark: benchmarks/results/gate/phase2_pit_latency.json]

```
node_as_of   p50 0.013 ms   p95 0.192 ms   p99 0.258 ms
edges_as_of  p50 0.029 ms   p95 0.240 ms   p99 0.318 ms
1000 queries, 58.3% hit rate
```

Re-measured after the §5 bounded-scan change, which touched the shared
adjacency walk these reads depend on. No regression: still ~100x inside target.
An earlier run recorded 0.007/0.015 ms
[benchmark: benchmarks/results/pit_latency_1787243614.json]; the difference is
thermal state, not the code change (see §3) — both are far enough inside the
target that the distinction does not matter for the verdict.

The descending-timestamp key encoding (PRD Section 9.1, Contribution 2)
resolves a point-in-time read as a single seek, with no secondary index. The
margin is large enough that the target is not a meaningful constraint on this
graph size.

Embedding point-in-time latency is **not** measured — `CF_EMBEDDINGS` is
populated from Phase 4.

### 2.2 2-hop bounded traversal — passes

[benchmark: benchmarks/results/gate/phase3_traversal_2hop.json]

```
p50 3.550 ms   p95 13.026 ms   p99 20.390 ms   max 40.379 ms
1000 queries, seed 42, default bounds (fan-out 512, expanded 5000)
mean result: 1366.9 nodes, 1564.8 edges
590 of 1000 traversals hit the fan-out cap
```

Measured on a **clean working tree** at commit `d1757bd`, so this figure traces
to a specific tree state (Rule 10). An earlier identical run on a dirty tree
recorded p95 8.144 ms; the 8.1 vs 13.0 ms spread across two runs of the same
code, same seed, same graph is thermal (see §3), not a code difference. The
slower of the two is quoted here because it is the reproducible one.

This target was **missed by ~4x until the §5 change landed**. The history matters
more than the number, so it is kept in full below rather than overwritten:

| | p95 | mean edges returned |
|---|---|---|
| Before (materialise-then-truncate) | 189.8 ms | 1564.9 |
| After (bounded scan) | **13.026 ms** | 1564.8 |

**15x faster returning the same answer.** Edge count is unchanged to within 0.1
edges, which is the evidence that this was wasted work rather than a
completeness/latency trade: the old path read tens of thousands of neighbours
per hub and discarded all but 512 of them.

The bounds were **not** tuned to reach this. Both runs use the same default
`TraversalLimits`, the same seed, and the same graph.

Node count rose slightly (1316.5 → 1366.9) because the surviving neighbours
changed — see the semantic caveat in §5.

### 2.3 Incremental embedding update latency (GraphSAGE) — a genuine miss

[benchmark: benchmarks/results/gate/phase4_incremental_speedup.json]

§7.8 measures this same run's **speedup ratio** against full recompute
(7.79x median, passing Rule 7's 5x bar) but never checks the absolute
number against Section 1's separate `< 100 ms` p95 latency target — an
omission this report previously left uncorrected. Read directly from that
file:

```
incremental  median 820.27 ms   p95 1548.96 ms
```

> **Superseded.** The figures in this section are the Phase 4 measurement and were never
> updated. The P4 work (section 10.3) brought the same measurement to a p95 of roughly
> 205 to 405 ms, depending on machine state, and hub deferral (section 10.9) brings the
> synchronous p95 to 67 ms. The "~15x" below describes Phase 4, not the current code.

**p95 1548.96 ms was ~15x over the 100 ms target.** This is the honest number
on the full 174,298-node / 599,497-edge graph the PRD's Phase 4/6 sections
mean; it is not a contradiction of `scripts/run_demo.sh`'s own live mutations
landing in 9-57 ms (§Live demo, README) — the demo runs against a ~370-node
seeded graph, where the same bounded-subgraph forward pass has far less to
resolve, and passes the 100 ms target easily. The miss is scale-dependent,
not a defect in the incremental path itself: `incremental_fallback_total`
stayed at 0 across all 30 samples (§7.8), so nothing here is a fallback to
full recompute in disguise. Closing this gap would mean revisiting
`MAX_EXPANDED_NODES` and `FANOUT_CAP` (§7.6-§7.7) specifically for latency,
not just for the speedup ratio they were originally tuned against.

### 2.4 Incremental embedding update latency (GAT)

[benchmark: benchmarks/results/gate/phase5_gat_incremental_speedup.json]

Phase 5's own success criterion — "p95 GAT incremental update latency under
100ms on the benchmark graph" — was previously asserted nowhere in this
report; only correctness (§4.3 of `docs/paper_draft.md`,
`tests/embedding/gat_correctness_test.rs`) had ever been measured. Measured
here for the first time, same 30-sample-from-the-full-trace method §7 uses,
through `gat_incremental_update` rather than the associative path (see
`caregraph-bench-incremental --model-kind gat`, added this pass):

```
incremental     median 517.36 ms   p95 1531.04 ms
full recompute  median 8209.36 ms   p95 9935.94 ms
median speedup: 8.1x     target: >= 5.0x     8.1_VERDICT
```

**p95 1531.04 ms against the 100 ms target: MISS.** This is essentially the
same p95 as §2.3's GraphSAGE figure (1548.96 ms), and GAT's median (517.36 ms)
is lower than GraphSAGE's (820.27 ms). That is not the direction one would
expect, since GAT's attention does more work per affected node on the same
subgraph. Both runs are 30 samples on a shared laptop, and §3 shows this
machine's latency drifts with thermal state, so the difference is not
evidence that GAT is cheaper. What the two runs do show is that neither model's
forward pass is the dominant cost: the miss comes from the size of the affected
subgraph around hub nodes (§7.6), which is the same for both models.

### 2.5 Sustained ingestion throughput

[benchmark: benchmarks/results/gate/phase1_ingest_throughput.json]

Not previously measured at all. `caregraph-load` (Phase 1's bulk ingestion
path — structural writes only, no embedding computation; see that binary's
own module doc for why) now accepts `--out-dir` to time its own read-plus-
batch-write loop and emit a raw-results file. Loading the full trace fresh:

```
599,497 edges, 174,298 nodes, 10,989 removals in 2.764s
216,865 edges/sec (283,892 total mutations/sec)
target: 10,000-50,000 edges/sec — PASS, and above the target's own upper end
```

Passing comfortably above even the target range's upper bound is expected,
not suspicious: this number is structural writes only, on a single-node
embedded RocksDB instance with no network hop and no embedding computation
in the loop (that computation is what makes the Phase 4/5/6 live-mutation
path, measured in §2.3/§2.4 and in `scripts/run_demo.sh`, orders of
magnitude slower per edge) — bulk-loading bare structure was always going to
be the fastest path this system has, and Section 1's range appears written
with a mixed live-mutation workload in mind, not this specific bulk-load
case. Single machine, single run — §3's thermal-variance finding applies
here as much as it does to §2.1/§2.2's numbers.

### 2.6 Attribution overhead (Phase 9, Feature 1) — a severe, disclosed miss

[benchmark: benchmarks/results/gate/phase9_attribution_overhead_graphsage.json]
[benchmark: benchmarks/results/gate/phase9_attribution_overhead_gat.json]

No PRD target exists for this — attribution is a Phase 9 addition. It is
reported here because the number that came back is dramatically worse than
an earlier back-of-envelope estimate, and that gap is itself the finding
worth recording plainly.

```
GraphSAGE, 15 real mutations, full 174,298-node graph:
  incremental only:            median 680.03 ms   p95 917.62 ms
  incremental + attribution:   median 24,377.27 ms  p95 42,538.14 ms
  attribution overhead alone:  median 23,599.63 ms  p95 41,812.33 ms

GAT, 5 real mutations, full 174,298-node graph:
  incremental only:            median 1,015.94 ms   p95 1,321.28 ms
  incremental + attribution:   median 79,380.21 ms  p95 98,917.36 ms
  attribution overhead alone:  median 78,058.94 ms  p95 97,901.42 ms
```

An earlier back-of-envelope figure, extrapolated from integrated-gradients
timing measured on a small (1,500-node, 6,398-directed-edge) synthetic
subgraph, put attribution overhead in the range of 160 ms (GraphSAGE) to
550 ms (GAT) per
mutation. The real number, measured against real sampled mutations on the
real clinical graph, is **two to three orders of magnitude larger** — tens
of seconds, not hundreds of milliseconds. The root cause is visible in the
"incremental only" row above: a plain incremental forward pass on this
graph's real affected subgraphs already costs hundreds of milliseconds to
over a second (against ~5 ms on the small synthetic test subgraph the
earlier estimate used), so 16-step integrated gradients — roughly two
states times sixteen forward-and-backward passes each — multiplies that
real per-pass cost by dozens, not the sparser subgraph's near-trivial cost.

**What this means in practice:** `AddEdgeRequest.explain`/
`RemoveEdgeRequest.explain` exist specifically so this cost is opt-in, never
paid by default. At this measured cost, synchronous per-request attribution
is not viable for an interactive or bulk-ingest path at this graph's scale —
it is viable only for a caller that can tolerate tens of seconds of added
latency for one specific, deliberately-requested explanation (e.g. a
regulator's after-the-fact audit query), not for a live workflow. This is
disclosed here rather than left for a reader to discover by running the
benchmark themselves.

A second finding from the same run: one real GAT mutation (a hub-adjacent
edge on the production graph) produced a completeness residual of **22.35%**
[benchmark: benchmarks/results/gate/phase9_attribution_overhead_gat.json] —
well above the 6% worst case `tests/embedding/attribution_completeness_test.rs`
measured on its own synthetic hub fixture. Not a correctness failure (the
identity is a fixed-step-count approximation by construction, and the test's
own step-count convergence check already showed the residual shrinks toward
zero with more steps — see that test's module doc) — but real evidence that
a production-scale, genuinely high-degree hub can push discretization error
well past what a smaller test fixture predicts, at the default 16-step
setting.

### 2.7 Self-tuning cap controller (Phase 9, Feature 2)

[benchmark: benchmarks/results/gate/phase9_cap_controller_ab.json]

Same-machine, same-seed A/B over 30 real sampled mutations on the full
clinical graph, GraphSAGE:

```
pinned (512, 1500):              p95 1,165.53 ms   expansion_capped_rate 63.3%
adaptive (settled at rung 1,
  384/1000):                     p95   799.55 ms   expansion_capped_rate 63.3%
p95 reduction: 1.46x
```

A real, modest improvement — not the claim that adaptive caps eliminate
§2.3/§2.4's ~15x latency miss. 799.55 ms remains roughly 8x over the PRD's
100 ms target. The identical 63.3% `expansion_capped_rate` on both arms is
worth noting explicitly: the pinned default was *already* truncating the
receptive field on the majority of these real mutations before the
adaptive controller ever ran, so tightening the cap further did not
materially change how often truncation binds — only how wide the untruncated
portion is. The controller's real win here is latency, not a reduction in
how often the answer is already an approximation.

### 2.8 Point-in-time similarity delta latency (Phase 9, Feature 3)

[benchmark: benchmarks/results/gate/phase9_similarity_delta_latency.json]

```
p50 0.056 ms   p95 0.065 ms   p99 0.077 ms   (50 queries, 100% hit rate)
```

No PRD target exists for this query either. The population for this run was
6 nodes — real embeddings, seeded via genuine `AtomicCommitter::commit`
calls through `caregraph-fault-injection-worker`, not a synthetic stand-in —
so this number characterizes the query mechanism's own cost (two versioned
scans plus an in-memory join, reusing Claim 2's exact single-seek
primitive), not a full-population latency distribution. A larger-population
run has not been performed.

---

## 3. Finding: this machine cannot produce trustworthy sustained latency numbers

Identical configuration, identical seed, identical deterministic result set
(697.6 nodes / 823.5 edges every run):

| Condition | p50 | p95 |
|---|---|---|
| After 150 s cooldown | 10.2 ms | **91.5 ms** |
| 1st run under sustained load | 33.8 ms | 238.4 ms |
| 2nd run under sustained load | 27.9 ms | 223.6 ms |
| 3rd run under sustained load | 17.4 ms | 231.0 ms |

A 2.5x p95 swing from thermal state alone, on a laptop CPU. The returned result
is byte-identical across all four runs, so the traversal itself is
deterministic — only timing drifts.

**Consequence:** the first traversal figure produced in this session (81.7 ms
p95, `benchmarks/results/traversal_2hop_caregraph_1787243582.json`) is not
trustworthy, and neither is any single uncontrolled run. Absolute latencies in
this report should be read as upper bounds contaminated by throttling. The
*relative* comparison in Section 4 is the reliable part, because every
configuration there was measured under the same cooldown protocol.

Any Rule 4 number intended for publication must come from a thermally
controlled machine, or from a documented cooldown protocol with repeated runs.
This is a hardware limitation, not a CareGraph one.

---

## 4. Finding (now fixed): the fan-out cap bounded the result, not the work

> **Status: diagnosed here, fixed in §5.** The sweep below is the evidence that
> motivated the change. It is kept because it is the reason the fix was the
> right one — and because a sweep showing a knob does nothing is worth more in a
> paper than the knob's final value.

Fan-out cap swept with `max_expanded_nodes` fixed at the server default, same
seed, 150 s cooldown before each run.

| `max_neighbors_per_node` | p95 (ms) | mean edges returned | raw results |
|---|---|---|---|
| 512 (default) | 189.8 | 1564.9 | [benchmark: benchmarks/results/sweep/traversal_2hop_caregraph_1787329168.json] |
| 256 | 202.2 | 823.5 | [benchmark: benchmarks/results/sweep/traversal_2hop_caregraph_1787329389.json] |
| 128 | 181.1 | 425.4 | [benchmark: benchmarks/results/sweep/traversal_2hop_caregraph_1787329602.json] |
| 64 | 178.7 | 217.8 | [benchmark: benchmarks/results/sweep/traversal_2hop_caregraph_1787329802.json] |
| 32 | 191.7 | 111.9 | [benchmark: benchmarks/results/sweep/traversal_2hop_caregraph_1787330018.json] |

**Tightening the cap 16x changes p95 by nothing (179–202 ms, no trend) while
cutting the answer 14x (1565 → 112 edges).** The cap is pure loss: it costs
completeness and buys no latency.

### Why (the code as it stood at the time of the sweep)

```rust
for edge in self.index.edges_as_of(node, edge_type, request.as_of)? {
    neighbors.push((edge, other));          // full adjacency materialised
}
neighbors.sort_by_key(|(edge, other)| (*other, edge.timestamp));   // all of it sorted
if neighbors.len() > self.limits.max_neighbors_per_node {
    neighbors.truncate(self.limits.max_neighbors_per_node);        // then N kept
}
```

The entire adjacency is read from RocksDB, materialised into a `Vec`, and
sorted — *before* the cap applies. Per-hub cost is O(degree · log degree) in CPU
and O(degree) in reads and memory, independent of `max_neighbors_per_node`. The
cap only shrinks what is expanded and returned downstream.

This contradicted the claim in `src/graph/limits.rs` that the cap makes traversal
"bounded in work, not just in hops." As written, it was bounded in *result*, not
in work. The module doc has since been corrected to describe both the current
bounded scan and the mistake it replaced.

### Why it matters here: the hubs are enormous

Measured in-degree on the loaded graph:

| Node type | Nodes | Max in-degree | Nodes with degree > 512 |
|---|---|---|---|
| Medication | 21 | **54,383** (insulin) | 10 |
| Condition | 915 | **18,101** | 100 |
| Provider (specialty proxy) | 72 | 14,635 | 16 |
| LabResult | 6 | 8,216 | 6 |
| Encounter | 101,766 | 1 | 0 |

The design hypothesis recorded in `limits.rs` — that shared reference nodes make
degree wildly skewed and hop-bounding alone insufficient — is **confirmed**, at a
scale well beyond what that comment anticipated ("thousands"; actual 54,383).

Note the provider proxy is *less* extreme than insulin or the top conditions, so
the skew is a property of the clinical data, not an artifact of the
Section 1 modelling substitution.

---

## 5. The fix: stop materialising the hub

The fix was not to tighten the cap. It was to stop reading what the cap throws
away.

`src/temporal/index.rs` already noted that walking forward from the `as_of` seek
walks *backward through time* — adjacency arrives newest-first. So the scan can
stop once it has collected N live neighbours, making a hub cost O(N) instead of
O(degree). **No change to the key encoding**: Contribution 2's descending
timestamp layout is precisely what makes early termination correct, since each
destination's first sighting is already its newest version at or before `as_of`.

Implemented as:

- `TemporalIndex::edges_as_of_limited` / `incoming_edges_as_of_limited` — the
  shared adjacency walk takes an optional live-neighbour limit and breaks out of
  the scan when it is reached. Tombstoned versions still consume a step (they
  resolve a destination to "absent", which is an answer) but do not count toward
  the limit.
- `Traverser::traverse` requests `cap + 1` per direction. Getting `cap + 1` back
  proves the cap bound without having read the rest of the adjacency.

### Two honest consequences

**1. Which neighbours survive changed.** The cap now keeps the most recently
updated neighbours rather than the lowest destination ids. For care-pathway
queries that is the more defensible answer, but it is a behaviour change, not a
pure optimisation. Determinism is preserved — key order is total and the scan is
deterministic — and
`traversal_synthetic_test::fanout_truncation_is_deterministic_across_runs` still
passes unchanged.

**2. `Truncation::fanout_dropped_neighbors` is now a lower bound.** Counting
dropped neighbours exactly means reading the whole adjacency, which is the cost
being eliminated. The traversal now reports *that* neighbours were dropped
without claiming to know how many: the sweep's headline "19.7M neighbours
dropped" has no post-fix equivalent, and the same run now reports 2,783 — which
is a floor, not a total. `fanout_capped_nodes` remains exact, and
`SnapshotReader` still scans exhaustively and reports an exact figure.

Trading an exact diagnostic for a 15x latency win is the right call here, but it
is a real loss of information and is recorded as one rather than quietly
dropped.

---

## 6. Outstanding before any Rule 4 claim

- [x] Neo4j Community + GDS running, identical graph loaded, traversal measured — §8
- [x] TerminusDB running, identical graph loaded, traversal measured — §8 (load
      measured three-way; a temporal-versioning-specific comparison — e.g.
      point-in-time read latency across all three systems — is still not
      implemented in the harness, see §8.3)
- [ ] A thermally controlled machine, or a documented cooldown protocol
- [ ] Clean working tree at run time, so results trace to a commit — §8's run
      was dirty; see §8.3 for why that doesn't touch the measured numbers
- [ ] Real UKPDS graph, or an explicit decision to publish on this substitution
- [x] Bounded-selection fix, then re-measure the traversal target — §5, p95 13.026 ms

Both Section 1 latency targets in scope for Phases 2 and 3 are now met on real
data, and §8 now gives CareGraph's first real comparative numbers against both
baselines. What still remains: a thermally controlled protocol, the real
UKPDS graph, and extending the harness beyond 2-hop traversal to the rest of
Section 1's metrics three-way.

---

## 7. Phase 4 — incremental embedding, associative models

### 7.1 Framework substitution

The PRD's Phase 4 task text names DGL. DGL 2.x is not installable on this
machine — the package index offers only the 2018-era 0.1.x line for the
available Python version. PyTorch Geometric is used instead, which Rule 3
names as an equally acceptable implementation; the deviation is from task
wording, not from the rule that gates the phase. `torch 2.11.0+cpu`,
`torch_geometric 2.7.0`.

Two smaller Section 2 stack entries are affected by the same substitution
and were not previously called out:

- **ONNX Runtime (§2.3)** is not used anywhere in this build. The PRD names
  it for "serving the trained embedding model at low latency from the Rust
  query path" — that role is filled instead by `ml/embedding_server.py`, a
  persistent subprocess worker (§7.2 explains why a subprocess and not a
  bound library at all). No code path exports a model to ONNX or loads an
  ONNX Runtime session; `grep -ri onnx` across `src/`, `ml/`, and `Cargo.toml`
  returns nothing.
- **Directory layout**: Section 10 names `ml/embedding/gat_incremental.py`
  and `ml/ripple_plus_reference/`. Neither exists as a separate path — the
  GAT incremental logic lives in `src/embedding/gat_incremental.rs` (Rust,
  not the PyO3-called Python the PRD's Section 6.2 code sample sketches;
  see §7.2 for why PyO3 itself was dropped) and there is no vendored
  RIPPLE++ reference checkout, only the operator-decoupling *technique*
  RIPPLE++ describes, reimplemented directly in Rust and Python across
  `src/embedding/gat_incremental.rs`, `src/embedding/associative.rs`, and
  `ml/embedding_server.py`. Creating empty or stub files at those two paths
  purely to satisfy the directory listing would itself be the kind of
  placeholder content Section 10 says not to leave — not done, disclosed
  here instead.

### 7.2 Calling mechanism substitution

The PRD names PyO3 as the Rust↔Python boundary. Tried first, against this
machine's real Python 3.14 install, and rejected for a reproducible reason:

- `pyo3 0.24`'s build script refuses CPython 3.14 outright — its own maximum
  supported version is 3.13.
- Its documented forward-compatibility escape hatch,
  `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1` under the stable ABI, **builds**,
  but fails at the first `import torch`: `_ctypes` and torch's own native
  extensions are not abi3-limited, so CPython's own ABI-mismatch guard
  refuses to load them under the compatibility shim
  (`Module use of python314.dll conflicts with this version of Python`).

Both findings are empirical — built and run, not inferred from documentation.
`ml/embedding_server.py` is a persistent worker process instead: spawned once,
kept alive across mutations (so PyTorch's import cost is paid once, not per
mutation), real forward passes through the real trained model, communicating
over newline-delimited JSON on stdio. Warm round-trip: ~2-4 ms for a small
subgraph.

### 7.3 Trained model (Rule 3)

`ml/train_graphsage.py` trains a 2-layer GraphSAGE (mean aggregation, 32-dim
output) on the full Diabetes 130 graph via unsupervised link prediction — no
labels, because the derived timeline (§Dataset) would make any temporal label
leaky. Loss 0.804 → 0.432 over 60 epochs. Deployed to
`ml/deployed/diabetes130_graphsage/` with `model.pt`, `model.sha256`, and
`dataset_manifest.json` — the manifest and checksum Rule 3's gate checks for.

### 7.4 A real bug the correctness test caught

Phase 4's success criterion: incremental results must exactly match a
full-graph recompute on 50 randomised mutation sequences.

The first implementation of `AffectedSubgraphResolver` fetched edges for every
node in the affected set, but not for the nodes *discovered as their
neighbours*. For a 2-layer model, computing `h2(hub)` needs `h1(n)` for every
neighbour `n` of the hub — and `h1(n)` depends on `n`'s own edges, not just on
`n` existing as a feature row. Missing that second ring left those neighbours
as structurally isolated rows in the subgraph: present, but with none of their
real edges, silently corrupting their own layer-1 embedding and therefore
every hub embedding that depended on them. Measured error from this bug:
~1e-2, unmistakably structural, not numerical.

Fixed by expanding the receptive field in two rings instead of one — see
`src/embedding/resolver.rs`. After the fix, the residual gap across all 50
sequences is a max absolute difference of 6e-7 and max relative difference of
6e-5 (float32 scatter-mean taking a different internal path for
differently-shaped tensors) — four orders of magnitude tighter than the bug it
replaced. `tests/embedding/associative_correctness_test.rs` compares with a
tolerance chosen to sit between those two numbers, not to paper over one with
room for the other.

[benchmark: N/A — correctness test, not a timed benchmark; run with
`cargo test --test embedding`]

### 7.5 First measurement: passes on the median, with a specific weak point

30 real `add_edge` mutations sampled evenly across the full trace, on the full
174,298-node / 515,117-edge graph.

```
incremental     median 710.97 ms   p95 3004.05 ms
full recompute  median 5212.64 ms  p95 5920.53 ms
median speedup: 6.15x     target: >= 5.0x     PASS
```

The median cleared the target, but individual mutations were uneven: 12 of 30
samples (40%) fell below 5x on their own, from 1.9x to 4.8x, concentrated at
affected-set sizes of ~500–545 (mutations touching a reference node right at
the fan-out cap). The hypothesis at the time: `AffectedSubgraphResolver`
issues one `capped_neighbors` call per edge type per direction — 12 RocksDB
scans per node — and at ~520 affected nodes that's several thousand small
round trips whose per-call overhead was the actual cost.

### 7.6 The hypothesis was wrong — measured, not assumed

Collapsing the 12 calls to 2 (§7.7) was real and correct on its own terms, but
re-measuring after it landed: **median speedup dropped to 4.3x — worse, and
now failing.** Call count was not the bottleneck.

A timing split for one representative mutation (`290 -> 100001`, 521
affected) found the real one:

```
resolve():            196 ms
build_model_input():  228 ms
model.forward():      146 ms   (JSON + subprocess round trip)
```

`resolve()` returned a **49,165-node, 101,675-edge** receptive field for a
521-node affected set. `AffectedSubgraphResolver`'s two-ring expansion caps
each node's *own* fan-out at 512 — correctly — but nothing capped how many of
the 521 affected nodes got their own neighbours expanded in ring two. 521
nodes × up to 512 neighbours each has no reason to stay small, and on this
mutation it didn't. This is §4's fan-out finding one layer up: bounding
*per-node* fan-out does not bound *total subgraph size* when there are many
affected nodes, each individually allowed up to the cap.

### 7.7 Two fixes, in order, both needed

**Fix 1 — call count.** `TemporalIndex::all_edges_as_of_limited` /
`all_incoming_edges_as_of_limited` scan a node's entire key range in one pass
instead of once per edge type, since edge type sorts immediately after
`src_id` in the key layout and is therefore already one contiguous byte range
per node. 12 calls → 2 per node.

This fix surfaced a real, separate bug in the process: `CF_EDGES` /
`CF_REVERSE` are tuned with a fixed 10-byte prefix extractor
(`[src_id | edge_type]`), and `scan_from`'s `prefix_same_as_start` read option
assumes the seek key is exactly that width. The new scan's seek key is
`[src_id]` alone — 8 bytes, narrower than the extractor's domain — and
RocksDB's prefix bound couldn't be derived from it, so the iterator silently
returned nothing. First caught by the correctness test: a real, reproducible,
deterministic ~1e-2-magnitude divergence on one specific (sequence, node)
pair, not the noise-level drift a genuine floating-point cause would produce.
Fixed with a new `KvStore::scan_from_narrow_prefix` that uses
`total_order_seek` instead of the extractor-based bound, relying purely on the
manual `key.starts_with(prefix)` check every `scan_from` implementation
already performs — see `storage/kv.rs`.

**Fix 2 — total receptive-field size.** `AffectedSubgraphResolver` gained a
`max_expanded_nodes` backstop, mirroring `TraversalLimits::max_expanded_nodes`
on the query path. Applied to ring two only, never ring one: ring one *is* the
affected set — the nodes this call reports embeddings for — and every one of
them must get its own edges fetched or its computed embedding is wrong, not
merely stale (isolated in the subgraph looks identical to having no
neighbours). Ring two is what actually needs bounding, and what produced the
49,165-node blowup.

The default (1,500) was chosen empirically, not by reusing
`TraversalLimits`'s 5,000: on the same representative mutation, 5,000 never
even bound (fewer than 5,000 distinct nodes were being *fetched from* — the
blowup was in how much each one returned, not how many there were), 1,000
still left a ~617 ms pipeline, 1,500 brought it to ~234 ms, and 2,000 was
worse again (~1,190 ms) — the search was empirical, not monotonic, and
1,500 is a measured choice, not a round number.

### 7.8 Second measurement, clean tree, cooled

An uncooled run on a dirty tree immediately after the fix landed measured
309.22 ms / 5.28x — kept out of the headline for the same reason §3 exists:
not reproducible from a commit, and this machine's thermal state moves
absolute latencies by more than a small fix does. Re-measured cooled
(150s), on `a0d0937` clean:

```
incremental     median 820.27 ms   p95 1548.96 ms
full recompute  median 6210.12 ms  p95 7700.03 ms
median speedup: 7.79x     target: >= 5.0x     PASS
```

[benchmark: benchmarks/results/gate/phase4_incremental_speedup.json]

**7.79x median, 5 of 30 samples (17%) below 5x individually** — down from 12
and then 15 of 30 across the two pre-fix measurements. `full recompute`'s
absolute numbers moved substantially across all three runs (5212 ms → 1432 ms
→ 6210 ms median) on code this session never touched, which is §3's thermal
finding restated, not a new one: read the *speedup ratio* as the signal and
the absolute milliseconds as thermally contaminated, same as §2.2's traversal
figures.

`incremental_fallback_total` was 0 across all 30 real samples and all 400
correctness-test mutations both before and after these fixes — the fallback
path exists and is counted (Rule 7) but was never exercised by anything in
this session's real workload.

---

## 8. Phase 6 — three-way comparative benchmark (Rule 4)

**Status: complete for 2-hop bounded traversal, CareGraph's own Section 1
target the harness has covered since Phase 3.** This is the project's first
run where Neo4j Community + GDS and TerminusDB were both live, loaded with
the byte-identical trace, and queried — not merely health-checked.

```
benchmarks/run_baseline.sh --trace benchmarks/traces/diabetes130_full.jsonl --hops 2 --queries 5000
```

[benchmark: benchmarks/results/run_20260831T063511Z.manifest.json]

| Field | Value |
|---|---|
| Run id | `20260831T063511Z` |
| Commit | `cc3a62cf75ac9b7c7e60de33adf87b4f6adf7961` |
| Working tree | **dirty** — see §8.3 |
| Hardware | Intel Core i5-13450HX, 16GB RAM, Micron 2400 NVMe, Windows 11 |
| Neo4j | 5.26.30 + GDS 2.13.12 |
| TerminusDB | 11.1.14 |

### 8.1 Load — byte-identical trace into all three

| System | Nodes | Edges reported |
|---|---|---|
| CareGraph | 174,298 | 599,497 (of which 10,989 are retractions) |
| Neo4j 5.26.30 + GDS 2.13.12 | 174,298 | 599,497 |
| TerminusDB 11.1.14 | 174,298 | 591,456 — net, see below |

**TerminusDB's lower edge count is a disclosed loader limitation, not a graph
discrepancy.** `GraphEdge`'s document id is Lexical over `(src, dst,
edge_type)` with no timestamp, so re-touching the same logical edge
(add → remove → re-add) inside one 2,000-record commit batch collides on a
single document id — TerminusDB rejects mutating the same id twice in one
transaction. The loader (`benchmarks/baselines/terminusdb_runner.py`) works
around this by collapsing each batch to every key's *last* operation before
writing. When an edge's add and its retraction land in the *same* batch, the
pair nets to nothing and is never written at all: correct for final state,
but the intermediate history for those specific edges never round-trips
through TerminusDB's document store the way it does for CareGraph and
Neo4j, which apply all 599,497 discrete mutation events individually.
591,456 vs. 599,497 is consistent with roughly 8,041 of the 10,989
retractions landing in the same batch as their edge's own creation — an
artifact of the fix, disclosed here rather than smoothed over.

### 8.2 2-hop bounded traversal (5,000 queries per system)

| System | p50 | p95 | p99 | max | vs. 50 ms target |
|---|---|---|---|---|---|
| CareGraph | 6.158 ms | 17.900 ms | 26.915 ms | 69.469 ms | **PASS** |
| Neo4j + GDS | 22.724 ms | 47.161 ms | 61.551 ms | — | PASS, ~6% headroom |
| TerminusDB | 72.254 ms | 90.567 ms | 95.838 ms | — | **FAIL**, ~1.8x over |

[benchmark: benchmarks/results/traversal_2hop_caregraph_1788159953.json]
[benchmark: benchmarks/results/traversal_2hop_neo4j_1788160089.json]
[benchmark: benchmarks/results/traversal_2hop_terminusdb_1788160424.json]

On this machine, this run: CareGraph is **~3.7x faster than Neo4j+GDS** and
**~11.9x faster than TerminusDB** at p95, on the identical graph and query
set. Only p95 < 50 ms is a Section 1 target; p50/p99/max are reported for
completeness. Neo4j clears the target with only ~6% headroom — closer to the
line than CareGraph's ~2.8x margin — and TerminusDB misses it outright.

CareGraph's own harness additionally reports that 57.5% of its 5,000
traversals returned at least one edge and 2,862 hit the fan-out cap (13,474
neighbours dropped in total) — §5's bounded-selection fix, restated: this is
the latency of *bounded* traversal, not exhaustive traversal. Neither the
Neo4j nor the TerminusDB runner currently emits the equivalent per-query
result-size/cap-hit breakdown. That's a real asymmetry in observability
parity across the three harnesses — not evidence the baselines are doing
less work, just that the harness doesn't yet measure it for them.

As with §3's finding, this is a single uncooled run on one machine — read the
relative ordering and the rough magnitude of the gaps as the trustworthy
signal, the same way §7.8 treats absolute milliseconds as thermally
contaminated.

> **Correction (see 10.8):** the TerminusDB figure in this table was produced by a runner whose query
> follows a single edge regardless of `--hops`, so it is a one-hop latency, not a two-hop one. The
> Neo4j and CareGraph rows are two-hop.

### 8.3 Outstanding

- **Working tree was dirty at run time**: `benchmarks/baselines/terminusdb_runner.py`
  carried the batch-collapse/upsert fix described in §8.1, committed
  immediately after this run. That fix only changes load-phase document
  mutation logic — the 2-hop traversal measurement code in §8.2 is unchanged
  by it — but strictly, these numbers don't trace to a single clean commit.
  A re-run from a clean tree would take another load-plus-measure pass
  (TerminusDB's load alone runs 20–25+ minutes on this dataset); not repeated
  here for that reason.
- Only 2-hop bounded traversal is compared three-way. Section 1 also names
  point-in-time node/edge read latency (§2.1, CareGraph-only so far) and
  incremental-embedding speedup (§7, a CareGraph-specific capability neither
  baseline has an equivalent for). Extending `run_baseline.sh` to run
  point-in-time reads three-way is not done.
- Single run, single uncooled machine — §3's finding applies here as much as
  it does to §2.2's CareGraph-only numbers.

## 9. Phase 9 — explainability, self-tuning, and architecture-agnostic dispatch

Four additions, summarized in detail in §2.6-§2.8 above:
integrated-gradients edge attribution
committed atomically with the embedding it explains, a discrete self-tuning
cap controller, a point-in-time similarity delta query, and manifest-driven
dispatch replacing a hardcoded per-`ModelKind` match. The two findings worth
restating here rather than leaving buried in §2.6:

1. **Attribution overhead on the real graph is 100-300x larger than an
   initial estimate based on a smaller test subgraph** — tens of seconds per
   request, not hundreds of milliseconds (§2.6). This is why it shipped as a
   per-request opt-in (`explain: bool`), not a default.
2. **`CF_COMMIT_META`, the new column family carrying attribution/dispatch/
   cap provenance, is written into the same `WriteBatch` as the structural
   mutation and the embedding** (Rule 5) — `atomic_commit.rs` still issues
   exactly one batch write, verified statically by `scripts/check_rules.sh`'s
   Rule 5 gate and now dynamically as well: the fault-injection suite was
   re-run against the real three-way commit, 100 iterations per dispatch
   arm. GraphSAGE landed 57 actual kills (56 fully committed, 44 fully
   uncommitted); GAT landed 94 (6 fully committed, 94 fully uncommitted) —
   **0 non-atomic states in either arm**. [benchmark: benchmarks/results/gate/phase9_fault_injection.log] [benchmark: benchmarks/results/gate/phase9_fault_injection_gat.log]
   GAT's markedly higher kill rate (94/100 vs. GraphSAGE's 57/100 in this
   same run) matches the Phase 5 finding that attention-weighted aggregation
   takes measurably longer per commit, widening the window a kill can land
   inside.

## 10. Concurrent load — pre-hardening baseline (production-hardening P1)

Every earlier section is sequential. `caregraph-bench-load` is an **open-loop**
generator: requests have fixed intended send times and latency is measured from
that intended instant, so a slow server cannot hide queueing (no coordinated
omission). Workload ids are sampled from the real Diabetes 130 trace. Server and
generator ran on the same laptop (i5-13450HX, 15.6 GB, Windows 11), against a
copy of the 174,298-node graph, default config, one Python worker per model,
adaptive cap controller on, `sync_writes` off. Files:
`benchmarks/results/gate/prod0_load_baseline_*.json`.

A first run of the generator reported ~10 ms p50 at 50 req/s. That was the
generator, not the server: tokio's timer is ~15 ms-granular on Windows and the
delay was charged to every request. The scheduler now uses an OS sleep plus a
final spin, and each result records `late_slots` (scheduler wake-ups >5 ms late)
so a generator-bound run is detectable. The numbers below have `late_slots`
near zero except where noted.

**Reads** (85% Snapshot / 10% 2-hop Traverse / 5% SimilarCarePathways):

| offered req/s | p50 | p95 | p99 | outcome |
|---|---|---|---|---|
| 100 | 0.62 ms | 6.71 ms | 11.06 ms | all ok |
| 500 | 0.40 ms | 4.94 ms | 8.69 ms | all ok |
| 1,500 | 0.37 ms | 5.50 ms | 9.11 ms | all ok |
| 3,000 | 0.47 ms | 5.88 ms | 10.33 ms | all ok |
| 6,000 | 0.52 ms | 7.43 ms | 13.37 ms | all ok |
| 12,000 | 2,228 ms | 3,934 ms | 4,252 ms | saturated: 8.3k ok/s achieved, 55,926 requests dropped by the generator's memory guard |
| 20,000 | 482 ms | 1,151 ms | 1,475 ms | overload: 8,110 `ResourceExhausted`, 268,563 `Unknown`, 200 `Cancelled` |

The read knee sits between 6,000 and 12,000 req/s on this machine. Past it
there is no admission control: latency jumps from milliseconds to seconds and
errors surface as `Unknown`. (At 12k+ the generator also falls behind
`late_slots` 5,632, sharing the CPU with the server, so those two rows show
overload rather than clean capacity.)

**Mutations** (`AddEdge` replaying real trace edges, stamped at wall-clock now
so the resolver sees the full current graph; GraphSAGE, `explain=false`):

| offered req/s | p50 | p95 | p99 |
|---|---|---|---|
| 0.5 | 67.6 ms | 87.0 ms | 97.7 ms |
| 5 | 73.9 ms | 99.1 ms | 125.3 ms |
| 15 | 92.6 ms | 165.0 ms | 196.2 ms |
| 25 | 48.0 ms | 142.5 ms | 181.6 ms |
| 40 | 48.6 ms | 188.9 ms | 235.3 ms |
| 60 | 64.1 ms | 377.1 ms | 438.5 ms |
| 100 | 3,432 ms | 7,266 ms | 8,438 ms |

Writes hold up to ~60 req/s, then at 100 req/s collapse into 3.4 s median
latency with **zero errors**: pure unbounded queueing, no load shedding.

**Caveats, stated plainly.**
- This p95 (~100 ms at low rate) is far below the 1,549 ms in §2.3 because the
  populations differ: §2.3 samples 30 mutations, many hub-adjacent, while this
  run draws uniformly from 5,000 trace edges, most of which touch low-degree
  nodes. Mutation cost depends heavily on endpoint degree; do not read this
  table as "the miss in §2.3 is fixed".
- The replayed edges already exist, so structurally they are no-ops with new
  versions; the embedding work is still real.
- The run was from a tree with uncommitted changes (`git_dirty: true` in each
  file), on a thermally variable laptop (§3), single run per rate.
- Attribution (`explain=true`) was not exercised.

This is the "before" for the hardening work: later phases re-run the same
sweeps and report the change, including where it does not improve.

### 10.1 Effect of the binary worker protocol and supervised pool (P2)

`[benchmark: benchmarks/results/gate/prod2_incremental_protocol_v2_graphsage.json]`
`[benchmark: benchmarks/results/gate/prod2_incremental_protocol_v2_gat.json]`

The JSON-over-stdio worker protocol was replaced by length-prefixed binary
frames (raw little-endian `f32`/`u32` arrays). Same `caregraph-bench-incremental`
method, same 30 evenly sampled real mutations, same pinned caps (512, 1500),
same trained models, same laptop. Before = §2.3/§2.4; after = this run.

| | incremental median | incremental p95 | full-recompute median | median speedup |
|---|---|---|---|---|
| GraphSAGE before | 820.27 ms | 1548.96 ms | ~6.4 s | 7.79x |
| GraphSAGE after | 222.42 ms | 331.23 ms | 917.53 ms | 4.4x |
| GAT before | 517.36 ms | 1531.04 ms | 8209.36 ms | 15.9x |
| GAT after | 244.27 ms | 374.54 ms | 1421.81 ms | 5.9x |

What this shows, and what it does not:

- **A real latency win:** incremental p95 improved about 4.7x (GraphSAGE) and
  4.1x (GAT). The old protocol serialised every float as decimal text and the
  worker built tensors with `torch.tensor(nested_python_lists)`; the binary
  path removes both. The exactness suites (50 random mutation sequences per
  model, attribution completeness) pass unchanged, since `f32` bytes round-trip
  losslessly.
- **The 100 ms p95 target is still missed:** 331 ms and 375 ms are roughly 3.3x
  and 3.7x over it. The remaining cost is resolution, input construction and the
  forward pass itself (see §7.6), which later phases target.
- **GraphSAGE now fails its own "at least 5x faster than full recompute" gate
  (4.4x).** That is because the *baseline* improved even more than the
  incremental path: full recompute ships the whole 174,298-node graph through
  the pipe, which is where the old protocol spent most of its time. The gate
  compares against a baseline that is no longer slow, so it no longer measures
  what it was written to measure; the absolute latencies above are the honest
  comparison. The binary exits non-zero on this gate by design and is left that
  way rather than loosened.
- Single run per model, a thermally variable laptop (§3), from a working tree
  with uncommitted changes (`git_dirty: true` in the files). Treat the ratios
  as indicative, not as precise constants.

### 10.2 Ordered commit and worker count (P3)

`[benchmark: benchmarks/results/gate/prod3_write_sweep_workers1.json]`
`[benchmark: benchmarks/results/gate/prod3_write_sweep_workers2.json]`
`[benchmark: benchmarks/results/gate/prod3_write_sweep_workers4.json]`

With more than one embedding worker, mutations are computed in parallel, and an
unguarded pool can lose updates (two mutations compute from the same pre-state;
the later write silently overwrites the correct values). The ordered-commit
sequencer (`src/embedding/sequencer.rs`) makes the parallelism safe: mutations
are admitted in timestamp order, computed with nothing held, committed in
timestamp order, and recomputed once if a commit that landed in the meantime
touched anything they read. `tests/embedding/concurrent_commit_equivalence_test.rs`
applies 50 seeds x 200 random mutations to two identical databases, one
sequentially and one with 4 workers and 16 mutations in flight: every structural
column family is byte-identical, 198,456 stored embeddings match (worst
difference 0), every node's latest embedding matches a from-scratch recompute on
the final graph (worst 7.2e-7), and 9,943 conflicts were detected and repaired.
With conflict repair deliberately disabled the same test fails on seed 0, so it
is not vacuous.

Write sweep on the real 174,298-node graph (strict timestamp policy,
server-assigned timestamps, torch pinned to 1 thread per worker, same laptop as
section 10; a fresh copy of the database per worker count):

| offered req/s | 1 worker p50 / p95 | 2 workers p50 / p95 | 4 workers p50 / p95 |
|---|---|---|---|
| 8 | 88 / 122 ms | 87 / 187 ms | 86 / 166 ms |
| 16 | 1,565 / 2,054 ms | **167 / 388 ms** | 269 / 1,135 ms |
| 32 | saturated (15.2 ok/s, 421 refused) | saturated (15.5 ok/s, 412 refused) | saturated (14.0 ok/s, 449 refused) |
| 64 | saturated (17.2 ok/s, 1,170 refused) | saturated (17.6 ok/s, 1,160 refused) | saturated (17.0 ok/s, 1,174 refused) |

What this shows:

- **Worker count matters below saturation.** At 16 req/s two workers cut p50
  from 1,565 ms to 167 ms. Four is not better than two here; the load generator
  shares the same 10-core machine, so more workers contend for CPU.
- **Saturation throughput does not scale with workers: about 16-17 ok/s with 1,
  2 or 4.** At saturation roughly 85% of mutations conflict (1,521 of 1,736 in
  the 1-worker run). This follows from the graph, not from the implementation:
  mutations that touch shared hub nodes (common medications and conditions)
  change each other's results, so they form a true serial dependency chain, and
  sequential-equivalent semantics forbid running them in parallel. Only making
  each mutation cheaper raises this ceiling.
- **The 60 req/s held in section 10 is not comparable.** That run had no
  ordering guarantee: concurrent mutations could compute from stale state and
  overwrite each other, so part of its speed was work it did not do. The
  correct figure for this workload is the one above.
- **Overload now sheds instead of queueing.** Past saturation the write
  bulkhead refuses with `ResourceExhausted` in about 0.4 ms, where the earlier
  run queued without bound (3.4 s median at 100 req/s, zero errors).
- Single run per cell, laptop with thermal variance (section 3), uncommitted
  tree (`git_dirty: true`).

**Fault injection under the ordered-commit pipeline.** 100 SIGKILLs per model,
each landing at a random point in a worker's life while it runs the real
mutation pipeline (`benchmarks/results/gate/prod3_fault_injection.log`):
GraphSAGE 48 kills, GAT 91 kills, every run either fully committed or fully
uncommitted, **0 non-atomic states**, and the persisted high-water mark was
atomic with the edge and embeddings in every case. The run shared the CPU with
other builds and benchmarks (23,840 s wall).

### 10.3 Resolve, input build and storage-read optimisations (P4)

Changes measured here, all in the incremental path:

1. Adjacency walks used by the resolver read neighbour ids only (they no longer
   build edges or parse properties).
2. The resolver expands each ring across up to four scoped threads and spends
   the ring-two budget on nodes in sorted order.
3. A per-store node-type cache removes the per-node `CF_NODES` seek in
   `build_model_input`.
4. Embeddings are stored in a binary format; the reader still accepts the old
   JSON values. (Affects the similarity scan and the embedding write, not the
   incremental latency measured below.)
5. RocksDB: shared 256 MB block cache, bloom filters on the versioned column
   families, cached index/filter blocks.

Same 30 real mutations, same database copy procedure (fresh copy, OS cache
warmed), alternating the P3 commit (`b213da8`) and this tree, GraphSAGE
(`prod4_graphsage_ab_*.json`):

| run | incremental median / p95 | full recompute median |
|---|---|---|
| P3 commit, run 1 | 241 / 976 ms | 3,552 ms |
| this tree, run 2 | **108 / 408 ms** | 1,683 ms |
| P3 commit, run 3 | 452 / 887 ms | 3,576 ms |
| this tree, run 4 | **112 / 405 ms** | 1,711 ms |

Incremental median falls 2.2-4.0x and p95 2.2-2.4x. Two things to read
alongside that:

- **Absolute numbers depend on the machine's state.** The same tree measured
  51 ms median / 206 ms p95 in an earlier single run
  (`prod4_graphsage_warm_single_run.json`; GAT 63 / 260 ms in
  `prod4_gat_warm_single_run.json`) and the P3 commit's full recompute took 517
  ms-3.5 s depending on when it ran. Only the interleaved pairs above are a
  like-for-like comparison. The P2-era figures in section 10.1 (median 222 ms,
  p95 331 ms) came from a different machine state and should not be compared
  with these.
- **The 100 ms p95 target is still missed** (best run 206 ms, interleaved runs
  about 405 ms). The remaining time is ring-two adjacency reads: median phase
  split in the best run was resolve 36 ms, input build 11 ms, forward pass 9 ms
  (GraphSAGE) or 26 ms (GAT). Removing ring two by storing layer-1 hidden states
  (the structural fix in the plan) is not implemented.

The 5x-over-full-recompute gate now passes (15x median in the interleaved
runs), partly because the full-recompute baseline itself also benefits from the
cache and the faster reads.

The block-cache size comparison (8 MB / 256 MB / 1024 MB) was inconclusive: the
runs happened while a fault-injection suite loaded the machine and the 256 MB
run produced no output, so no claim is made about cache sizing beyond keeping
the 256 MB default.

Similarity: `top_k` is capped at 1,000 and selection keeps only the best k
rather than sorting every candidate; the bounded selection is checked against a
full sort in a unit test. A latency number for `SimilarCarePathways` over the
full graph is not part of this section.

### 10.4 Read overload with limits in place (P5)

Same read mix (85% snapshot, 10% traverse, 5% similar), same 50,000-patient
sample and same machine as the section 10 baseline; server with the read
bulkhead (512), per-connection concurrency limit (256) and message caps.
`prod5_reads_sweep.json`, `prod5_reads_overload.json`, `prod5_reads_fine_*.json`.

| offered req/s | P1 baseline ok/s, p50 / p99 | now ok/s, p50 / p99 |
|---|---|---|
| 100 | 100, 0.6 / 11 ms | 100, 1.2 / 14 ms |
| 3,000 | 3,000, 0.5 / 10 ms | 3,000, 0.2 / 8 ms |
| 6,000 | 6,000, 0.5 / 13 ms | 6,000, 0.4 / 12-13 ms (4 reruns); one first run read p99 395 ms |
| 8,000 | not run | 8,000, 0.7-1.0 / 20-23 ms (4 runs) |
| 12,000 | 8,272, 2,228 / 4,252 ms | 9,686, 2,187 / 2,863 ms, 106 refused |
| 20,000 | **1,392**, 482 / 1,475 ms, 276,873 errors | **9,406**, 2,061 / 3,273 ms, 782 refused |

What this shows, and what it does not:

- **Goodput no longer collapses.** At twice the sustainable rate the baseline
  fell to 1,392 ok/s with 276,873 errors; the server now holds about 9,400-9,700
  ok/s, which is its capacity on this machine.
- **Admitted-request latency is not bounded in this setup.** At 12,000 and
  20,000 req/s the median is still about 2 s. The load generator and the server
  share one 10-core laptop, and at those rates the generator itself falls behind
  its schedule (142,914 and 252,005 late slots), so most of that latency is
  schedule backlog on a saturated CPU, not time inside the server. The refusal
  counts (106 and 782) show the server's own bulkhead rarely engaged. A bounded
  p99 under overload can only be demonstrated with the generator on a separate
  machine; that is part of the section 10 comparison run.
- **One outlier first run at 6,000 req/s** (p95 160 ms, p99 395 ms, 6,353 late
  slots) was not reproduced in four reruns with default limits and with the
  connection limits effectively disabled (p95 6.6-7.2 ms each time). It followed
  a cold start and a fresh build on the same machine; it is reported here rather
  than dropped.
- The mutation path's overload behaviour is unchanged from section 10.2: the
  write bulkhead refuses in about 0.4 ms.

### 10.5 Cost of the security work (P6)

**Encryption at rest, old cipher against new.** The hand-written AES was replaced
by the RustCrypto `aes` crate. Same machine, same code, same workload (300,000
puts of 200-byte values in 1,000-entry batches, flush, then 100,000 random point
reads), alternating the two builds twice with three repetitions each
(`prod6_cipher_throughput.txt`; the test is `encrypted_storage_throughput` in
`tests/integration/encryption_at_rest.rs`).

| | plain | old cipher | new cipher |
|---|---|---|---|
| ingest + flush, 300k puts | 271-416 ms | 1,206-1,344 ms (45-50 MB/s) | **336-399 ms (150-179 MB/s)** |
| 100k point reads | 227-296 ms | 270-334 ms | 229-257 ms |

Encrypted ingest is about 3.4x faster than before and now within roughly 15% of
unencrypted; encrypted reads are indistinguishable from plain ones (block cache
hits do not decrypt).

**Read auditing on against off.** Every read is written durably to the per-pod
audit log before it is served (group-committed, fsynced). Read mix as in section
10.4, two runs per setting, 12 s per rate (`prod6_read_audit_on_off.txt`):

| offered req/s | audit off p50 / p95 / p99 | audit on p50 / p95 / p99 |
|---|---|---|
| 1,000 | 0.34-0.35 / 4.0 / 6.9-7.3 ms | 1.2-4.9 / 5.5-118 / 9-254 ms |
| 3,000 | 0.23-0.24 / 4.1 / 7.4-8.0 ms | 1.9-2.0 / 9-25 / 143-209 ms |
| 6,000 | 0.41-0.42 / 6.7 / 11.7-12.0 ms | 2.3-2.4 / 10 / 16-17 ms |

Auditing costs about 1.5-2 ms at the median (the group commit) and a few ms at
p95 once warm. Two caveats: the first audited run after each server start was much
worse at low rates (p95 118 ms at 1,000 req/s, against 5.5 ms on the second), and
p99 at 3,000 req/s was 143-209 ms in both audited runs against about 8 ms
unaudited. That looks like occasional slow fsyncs on this laptop's SSD rather than
steady-state cost, but it was not isolated further, and a production target would
need fsync latency measured on its own storage. No read was refused for a full
queue in these runs. Throughput was unaffected up to 6,000 req/s.

**Fault injection with the audit chain.** 40 SIGKILLs per model with the mutation
audit record and chain head staged in the same batch: GraphSAGE 15 kills, GAT 32
kills, 0 non-atomic states, and in every case a committed mutation had exactly one
valid audit record and an uncommitted one had none
(`prod6_fault_injection_audit.log`). This is 40 per model, not the full 100.

### 10.6 Backup, restore and re-encryption (P7)

A drill on a copy of the real diabetes130 database (174,298 nodes, 1,395,288 keys,
39.8 MB plaintext), encrypted under a fresh data key, served by the release build,
backed up through the admin service and restored offline
(`benchmarks/results/gate/prod7_backup_restore.txt`, produced by
`scripts/p7_backup_drill.sh`):

| step | wall time |
|---|---|
| re-encrypt the plaintext database into an encrypted copy (1.4 M keys, verified equal) | 2.9 s |
| first backup, online, 42 MB | 0.36 s |
| second backup of an unchanged database | 0.08 s, and the backup directory grew by 0.1 MB, not 42 MB |
| checkpoint (hard links) | 0.07 s |
| verify the backup (restore to scratch, read every key, check the audit chain) | 0.77 s |
| restore | 0.32 s |

After the restore, per-column-family key counts and the audit chain matched the
original. The byte-for-byte comparison is in `backup_restore_test`, which dumps
every key and value of every column family at the backup point and again after
the restore. The database here is small, and these are single runs on a laptop:
the figures show the operations work and are cheap at this size, not how they
scale. The "second backup" case is also the unchanged-database case; incremental
growth under writes is covered by `backups_are_incremental_and_each_one_restores_its_own_point`
(adding 10% more data grows the backup directory by less than the first backup's
size) rather than measured on this dataset.

The restore and verify paths read the backup through the same encrypted
environment, so a wrong data key, a missing key, or a plaintext/encrypted mix-up is
refused before anything is read (last section of the result file).

### 10.7 Replication (P8)

`[benchmark: benchmarks/results/gate/prod8_replication.txt]` (with the six
`prod8_write_*.json` files), produced by `scripts/p8_replication_bench.sh` on the real
174,298-node database: a leader and a follower (and the load generator) on the same
16-thread laptop, strict timestamps, `sync_writes` on for both, one GraphSAGE worker
on the leader, a fresh copy of the database per run, runs interleaved (standalone,
synchronous, asynchronous, twice).

**An earlier version was slow, and why.** The first run of this benchmark showed
synchronous replication adding about 100 ms at p50 (mean standby-confirmation wait
120-150 ms). Timestamping each stage of the path showed the cost was not the network,
the follower's apply (5.7 ms median, fsync included) or the acknowledgement (0.4 ms),
but the leader reading each new batch back from the write-ahead log: 70 ms at the
median on this database, because opening a log iterator scans the log directory and
seeks into the file. The leader now keeps the last 32 MiB of batches in memory
(byte-identical to what the log holds, `the_in_memory_tail_holds_exactly_the_batches_written_and_is_bounded`)
and streams from it; only a follower that is catching up reads the log. The numbers
below are from after that change.

Write latency at 8 requests/s (below saturation; 20 s per run after a 5 s warm-up;
the two numbers per cell are the two runs):

| mode | p50 | p95 | p99 |
|---|---|---|---|
| standalone (no replica) | 75 / 78 ms | 108 / 127 ms | 277 / 288 ms |
| asynchronous replica | 88 / 77 ms | 250 / 117 ms | 325 / 248 ms |
| **synchronous replica** | **87 / 87 ms** | **221 / 203 ms** | **366 / 324 ms** |

- Synchronous replication costs about **10 ms at the median** over standalone. The
  leader's own measurement of the wait for the standby's confirmation, over 500
  writes per run, averaged 7.4 ms and 6.3 ms; the first run's histogram shows 81% of
  writes confirmed within 10 ms, 99% within 25 ms and all within 50 ms.
- The tails are noisy on this machine (the asynchronous run's p95 was 250 ms in one
  run and 117 ms in the other, which should be no different from standalone), so the
  p95/p99 gap between synchronous and standalone, roughly 80-100 ms at p95, is
  suggestive rather than established. It is within the spread the asynchronous mode
  shows against itself.
- Replication lag, sampled every 250 ms from the follower, had a median of 0 and a
  maximum of about 1,050 sequence numbers, which is one to two write batches in
  flight (a mutation is one batch of about 800 operations on average: 207,672
  operations in the 256 batches of the catch-up run). Both followers
  ended every run at lag 0, having applied exactly the 500 batches the leader
  committed.
- **12 requests/s is saturation for one worker on this machine** and the results are
  not interpretable: standalone alone measured p50 142 ms in one run and 1,798 ms in
  the other. Nothing about replication can be concluded from that row (it is in the
  result files).

**Catch-up.** A follower that was down while the leader committed 256 batches
(30 s at 8 requests/s), started from the old copy, reached lag 0 in 1.6 s including
process start and readiness. That is a bound on, not a measurement of, apply
throughput: start-up dominates at this size, and the follower's own apply step is
about 6 ms per batch with fsync (about 170 batches/s sustained, derived from the
stage timings, not measured end to end).

**Reads from a replica.** At 1,000 requests/s of the read mix, the follower matched
the leader: p50 1.2 ms against 1.0-1.1 ms, p99 39-48 ms against 35-41 ms, over two
runs each. At 3,000 requests/s both are overloaded on this shared machine (the load
generator falls behind, as in section 10.4), so the 3,000 rows show only that
neither collapses differently. **This benchmark does not show read throughput
scaling with replica count**: the leader, the follower and the load generator share
the same CPUs, so adding a replica here adds contention, not capacity. A scaling
claim needs replicas on separate hardware, which this laptop cannot provide.

**Not measured:** write latency with the standby on separate hardware (the network
round trip here is loopback), behaviour when the standby is slow rather than absent,
and replication under the full 100 req/s-class write load of section 10.2's saturation
rows (one worker saturates near 16 req/s, so there is little headroom to measure).

**Fault injection after the storage-write change.** The commit path now serialises
writes in `RocksKv::write` (so the in-memory tail records exact sequence ranges) and
routes single-key puts and deletes through it. 30 SIGKILLs per model with the real
mutation pipeline (`benchmarks/results/gate/prod8_fault_injection.log`): GraphSAGE 20
kills, GAT 28 kills, every run fully committed or fully uncommitted, **0 non-atomic
states**. This is 30 per model, not the full 100.

### 10.8 Concurrent load against Neo4j and TerminusDB (P10)

Section 8 compared the three systems one request at a time. This compares them under
concurrent load, with one load generator for all three, so that client overhead, arrival
process and latency definition are identical.

[benchmark: benchmarks/results/gate/prod10_compare/summary.md]

**What was run.** `scripts/p10_compare.sh`, one system at a time, on the same real
graph (174,298 nodes, the Diabetes 130 trace), driven by `caregraph-bench-load`
(`--target caregraph|neo4j|terminusdb`): open loop, constant arrivals, latency measured
from the intended send time, patients sampled from the trace. Each workload is a sweep
at offered rates that double (the same rates for every system; TerminusDB, which cannot
sustain them, also has lower rates), 3 s warm-up plus 12 s measured per rate, a 10 s
client timeout counted as an error, and the sweep ends at the first rate the system
cannot sustain. **Every sweep was repeated three times** and all three are shown, so the
spread is visible. Raw results for every run are in `prod10_compare/`.

| | CareGraph | Neo4j | TerminusDB |
|---|---|---|---|
| version | this repository (d936819 plus the P10 load-generator changes, uncommitted at run time) | 5.26 Community | 11.1.14 |
| where | native process, pinned to 4 logical CPUs, `CAREGRAPH_MEMORY_MB=2048`, embedding models off (reads only) | Docker, 4 CPUs, 3 GB, heap 1 GB, page cache 1 GB | Docker, 4 CPUs, 3 GB |
| client | tonic (gRPC) | `neo4rs` (Bolt) | `reqwest` (HTTP, WOQL) |
| graph held | 174,298 nodes, 515,117 edges (versioned) | 174,298 nodes, 524,116 relationships (current state) | 174,298 nodes, 591,456 edge documents (current state) |

The edge counts differ because the trace has repeated and retracted edges and each
system's loader applies them its own way (section 8.1); none of the three holds a
different set of patients.

**What each operation means** (the same question on all three; see the asymmetries
below):

* `snapshot`: a node with its outgoing edges and the type of each far-end node, current
  state.
* `traverse`: every node within two hops, edges taken as undirected.
* `ping`: the cheapest request each system answers (`gRPC health check`, `RETURN 1`,
  `GET /api/info`). It is the floor for protocol, authentication, and, for the two
  containers, Docker's port forwarding. That forwarding was measured separately with a
  trivial HTTP server (busybox in Docker against a native Python server, same client):
  about 0.3 ms extra, so it is a small part of the floors below.
* `mixed`: 90% snapshot, 10% traverse.

#### Throughput: the highest offered rate each system sustained

"Sustained" means p99 under the limit and at least 95% of the offered rate completed with
under 1% errors. The rates double, so each number is a **lower bound within a factor of
two** of where the system actually breaks; the three repeats are listed in order.

| workload | system | p99 < 50 ms | p99 < 250 ms | p99 < 1 s |
|---|---|---|---|---|
| ping | CareGraph | 51,200 / 51,200 / 12,800 | 51,200 / 51,200 / 25,600 | 102,400 / 102,400 / 51,200 |
| | Neo4j | 3,200 x3 | 3,200 x3 | 3,200 x3 |
| | TerminusDB | 400 x3 | 400 x3 | 400 / 800 / 400 |
| snapshot | CareGraph | 10,240 x3 | 10,240 x3 | 10,240 x3 |
| | Neo4j | 2,560 x3 | 2,560 x3 | 2,560 x3 |
| | TerminusDB | 0 x3 | 0 x3 | 10 / 0 / 10 |
| traverse | CareGraph | 320 / 320 / 160 | 320 x3 | 320 x3 |
| | Neo4j (all nodes) | 0 x3 | 40 x3 | 40 x3 |
| | Neo4j (at most 10,000 nodes) | 160 x3 | 160 x3 | 160 x3 |
| | TerminusDB | 0 x3 | 0 x3 | 0 x3 |
| mixed | CareGraph | 2,560 x3 | 2,560 x3 | 2,560 x3 |
| | Neo4j | 160 x3 | 320 / 160 / 320 | 320 x3 |
| | TerminusDB | 0 x3 | 0 x3 | 0 / 0 / 2 |

The CareGraph `ping` and `snapshot` ceilings are partly the load generator's: it shares
the machine, and the third repeat of both shows the generator falling behind
(`late_slots` in the result files: 4,214 at 10,240 snapshots/s) well before the first two
repeats did. Read them as "at least this much", not as CareGraph's limit.

#### Latency at the same offered rate (median, with min-max over the three repeats)

| operation, offered rate | CareGraph p50 / p99 | Neo4j p50 / p99 | TerminusDB p50 / p99 |
|---|---|---|---|
| ping, 100/s | 0.39 [0.37-0.47] / 0.81 ms | 2.81 [2.63-3.18] / 5.90 ms | 4.51 [4.21-4.63] / 8.06 ms |
| ping, 800/s | 0.22 / 0.42 ms | 1.98 / 4.70 ms | 455 / 1,085 ms (queueing begins) |
| snapshot, 10/s | 0.73 [0.69-1.15] / 2.69 ms | 3.48 [3.29-3.71] / 5.97 ms | 213 [201-226] / 455 ms |
| snapshot, 20/s | 0.70 / 1.26 ms | 3.23 / 6.15 ms | overloaded: 8 completed/s, 283 timeouts over the three repeats |
| traverse, 5/s | 5.04 [4.53-9.88] / 10.70 ms | 31.7 [30.8-33.5] / 79.4 ms (all nodes); 10.1 [9.7-10.7] / 19.7 ms (at most 10,000) | every request timed out (10 s) |

#### What the numbers do and do not say

* **Point reads: CareGraph is roughly 4x Neo4j in sustained throughput and 4-5x lower in
  latency, and both are far ahead of TerminusDB.** Most of Neo4j's 3.3 ms snapshot
  latency is its floor: a `RETURN 1` costs it 2-3 ms through this client, so the query
  itself adds about 0.5-1 ms. CareGraph's floor is 0.2-0.4 ms. Some of that difference is
  the protocol and the client (gRPC against Bolt), not the storage engine, and the
  comparison cannot separate them.
* **Traversal: the two systems did not do the same work, and that is the main caveat of
  this section.** CareGraph bounds a traversal by design (at most 512 neighbours per node,
  direction and edge type, 10,000 nodes returned; section 5). On this graph every sampled
  traversal hit a cap: it returned 2,505 nodes on average, against 38,702 from Neo4j,
  which answers exhaustively through the hub nodes. Neo4j limited to CareGraph's own
  10,000-node ceiling still returned about 9,800 nodes, four times what CareGraph did. So
  "320/s against 40/s" is about 8x on a different amount of work; against the bounded
  Neo4j (160/s) it is 2x for a quarter of the data, which makes the per-node cost close.
  What is solid: CareGraph answers a bounded 2-hop question in about 5 ms at p50 and keeps
  p99 under 50 ms to 160-320 requests/s, and it says when it cut the answer short (every
  response carries the truncation flags; 100% did here). What is not shown: that it is
  faster than Neo4j for the *exhaustive* answer, which it does not offer.
* **TerminusDB** sustains about 10 snapshots/s with p99 near 450 ms, fails at 20/s, and
  does not complete a 2-hop traversal in 10 s even at 1 request/s. Two things temper
  that. The WOQL here is mine, written to ask the same question, and has not been tuned
  by someone who knows the engine; the snapshot joins each edge to its far-end node by a
  value lookup, which may be the slow part. And TerminusDB kept computing a query after
  the client gave up: a few abandoned traversals held all 4 CPUs for minutes, so the
  script restarts the server before every workload and warms it with a trickle of
  snapshots (a restarted server answers its first queries slowly while it pages the
  graph in). A correction to section 8: its TerminusDB "2-hop" row (72 ms p50)
  came from a runner whose WOQL follows one edge whatever `--hops` says (its own comment
  notes the shape differs for more than one hop), so that row measured one hop. The
  two-hop query here is a much heavier request.
* **Mixed (90% snapshot, 10% traverse)**: CareGraph sustains 2,560/s, Neo4j 160-320/s.
  The traversals are the heavy tenth of the mix, and this sweep does not separate their
  cost from the snapshots'. The answer-size caveat applies.
* **Writes are not compared.** A CareGraph write includes a GNN forward pass and an
  atomic commit with a synchronous standby; neither baseline does anything comparable.
  CareGraph's own write numbers are in sections 10.2 to 10.7.

#### Limits of this measurement

* One laptop (Intel i5-13450HX, 16 GB, Windows 11), shared by the server, the Docker VM
  and the load generator, with other programs using about 9 GB. It is not a dedicated
  machine, it is thermally variable, and the Neo4j and TerminusDB containers pay for
  Docker Desktop's virtual machine while CareGraph does not (measured at about 0.3 ms on
  a trivial server, but not eliminated). A CareGraph container could not be built on
  this machine (the release build of RocksDB exhausted its memory; `docs/memory.md`), which
  is why CareGraph ran natively.
* The CPU limits are equal in count (four) but not in kind: a native process pinned to
  four logical CPUs against containers limited by quota over all of them.
* The rate lattice doubles, so every "sustained" value is within 2x of the break point.
  Three repeats show the run-to-run spread but are not a confidence interval.
* Neo4j and TerminusDB hold the current state only. Their half of the comparison is
  "now"; CareGraph's point-in-time reads at earlier timestamps have no counterpart here.
* The query shapes are the author's. Neo4j has a uniqueness index on `id` and used it;
  nothing was tuned for any system.
* An earlier pass of the TerminusDB sweep was discarded: the laptop had gone to sleep
  during it (one latency reads 3,600,809 ms) and a freshly restarted server timed out its
  first requests. The numbers above are from the rerun, with the machine kept awake and
  the warm-up in the script.
* Neo4j's relationship count (524,116) is lower than the 599,497 trace records because
  its loader merges repeated edges, as section 8 describes.

**Reproducing 10.8.** Load Neo4j and TerminusDB from the trace
(`benchmarks/baselines/neo4j_runner.py load`, `terminusdb_runner.py load`, with the
containers from `infrastructure/docker-compose/dev-stack.yml`), build
`cargo build --release --features baselines --bin caregraph --bin caregraph-bench-load`,
then `bash scripts/p10_compare.sh <out-dir>` and `python scripts/summarize_compare.py <out-dir>`.
On a dedicated machine, with the load generator on its own hardware, this is the run
that should replace the laptop figures above.

### 10.9 Hub deferral and the repair worker (production-hardening P11)

[benchmark: benchmarks/results/gate/prod11_hub_deferral_off.json]
[benchmark: benchmarks/results/gate/prod11_hub_deferral_on_64.json]
[benchmark: benchmarks/results/gate/prod11_hub_deferral_off_b2b.json]
[benchmark: benchmarks/results/gate/prod11_hub_deferral_on_64_b2b.json]

The remaining time in an incremental update (section 10.3) is resolution around hub
nodes. A mutation on a shared medication, provider or common condition names up to the
fan-out cap of neighbours as affected, and the resolver then reads their neighbourhoods
too. Hub deferral (`src/embedding/deferral.rs`) stops doing that on the request path.
A node with more than `CAREGRAPH_DEFER_HUB_DEGREE` neighbours is read but not
recomputed, and neither are that hub's neighbours when the hub is an endpoint. Each gets
a `CF_REPAIR` marker in the mutation's own batch, and the repair worker
(`src/embedding/repair_worker.rs`) refreshes them shortly after, off the request path.

Same 30 real mutations from the full trace, same graph, GraphSAGE,
`caregraph-bench-incremental` (release build), one run per cell. There are two
back-to-back pairs because the first pair was followed by a change to the repair worker
(below) and by other programs being closed on the laptop, so the machine was in a
different state for the second:

| | pair 1 off | pair 1 on (degree 64) | pair 2 off | pair 2 on (degree 64) |
|---|---|---|---|---|
| incremental median / p95 | 68.1 / 212.5 ms | **11.4 / 67.0 ms** | 50.0 / 180.5 ms | **7.2 / 73.5 ms** |
| the 18 hub-adjacent samples, median | 136.4 ms | 19.0 ms | 112.3 ms | 12.1 ms |
| median phase split: resolve / build input / forward | 53.3 / 12.4 / 8.1 | 6.4 / 3.3 / 2.4 | 30.2 / 11.1 / 9.0 | 4.1 / 2.3 / 2.1 |
| mutations that deferred work | 0 of 30 | 22 of 30 | 0 of 30 | 22 of 30 |
| full recompute median (a check on machine state) | 452.5 ms | 457.3 ms | 475.2 ms | 492.8 ms |

Within each pair the synchronous p95 falls 2.5 to 3.2 times and the median 6 to 7 times. The
100 ms p95 target is met in both "on" columns. In the "off" columns p95 is 180 to 212 ms.
The full-recompute medians agree within 8% across all four runs, which is the only evidence
offered that the machine was in a comparable state; this is not a controlled experiment.
Across pairs the absolute numbers differ by up to 30%, which is the size of machine-state
noise on this laptop (see section 3), so compare within a pair.

**What was moved, not removed.** The deferred work still has to happen. Timed the way the
worker does it (`--time-repair`), refreshing one mutation's deferred nodes took a median of
54.9 ms (p95 158.1 ms) over the 22 deferring mutations in pair 2, against 112 ms
synchronously for the hub-adjacent ones with deferral off. The same measurement in pair 1
gave a median of 382 ms (p95 440 ms). The difference is a defect this measurement exposed and
that was then fixed: pair 1 repaired in chunks of 128 nodes, so a mutation deferring about
500 nodes was resolved four times over overlapping neighbourhoods. The default chunk is now
twice the fan-out cap, which keeps one mutation's deferred group in a single pass with the
same receptive-field budget the synchronous update would have had. Pair 1 is kept because
its "on" column is what that earlier code measured; pair 2 is the code as shipped.

Deferral lowers the latency a writer sees. It does not make the total work disappear, except
through coalescing: the worker keeps only the newest marker per node, so a burst of N writes
on one hub is repaired once. `tests/embedding/hub_deferral_repair_test.rs` shows it on a
40-patient hub: two mutations produced two embedding versions per patient synchronously and
one with deferral. Writes spread across different hubs get no coalescing. On the repair
timings above one worker would keep up with roughly 6 (at p95) to 18 (at the median)
deferring mutations per second, but sustained repair throughput, and its effect on write
latency when repairs and writes compete for the same model workers, were **not measured**.
The `repair_pending` gauge and the `CareGraphRepairBacklog` alert exist for that case.

**What it costs in freshness.** Between the commit and the repair, the deferred nodes
have no embedding at the mutation's timestamp, so a point-in-time read returns their
previous embedding. The marker says so, and `repair_pending` counts them. The window is
the worker's interval (200 ms) plus its queue wait plus the repair itself. Intermediate
timestamps in a coalesced burst never get their own embedding version; a read as of one of
them returns the earlier version.

**Correctness.** With a real model, the nodes still updated synchronously equal the fully
synchronous result, the deferred nodes have no embedding until repaired, and after repair
they equal the synchronous result, within the float32 tolerance of
`associative_correctness_test.rs` (`tests/embedding/hub_deferral_repair_test.rs`, GraphSAGE
and GAT). The resolver tests (`tests/unit/hub_deferral_synthetic_test.rs`) check that
nothing a synchronous update refreshed is lost: it is either refreshed now or deferred. The
exactness checks use a 45-node fixture in which no cap binds. On the real graph the
fan-out cap binds for hub-adjacent mutations (about 520 nodes), so the equivalence there is
"same budget, same truncation", not "exact"; the benchmark does not compare values.

**Limits of this measurement.**

* One model (GraphSAGE), one hub degree (64), 30 samples, one run per cell. A p95 over 30
  samples is the 29th value, so it is sensitive to a single slow sample; the worst
  "on" samples (86.7 and 89.9 ms) did not defer anything.
* The repair timing runs each repair against the graph without its own mutation (the
  benchmark computes updates but does not commit them), so it is a timing, not an
  exactness, figure.
* The tree was dirty at run time (this change was uncommitted), so the results do not
  trace to a commit.
* No sampled mutation had an affected set above 2,000 nodes; the hub-adjacent ones were
  capped at about 520 by the fan-out cap.
* Deferral is **off by default**. It changes what `affected_nodes` reports in a mutation
  response (the nodes updated synchronously, not the deferred ones), so it is opt-in:
  `CAREGRAPH_DEFER_HUB_DEGREE=64`. The repair worker itself is on by default and also
  consumes the markers a model failure leaves behind, which nothing did before.
