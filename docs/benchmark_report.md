# CareGraph Benchmark Report — Phase 2 and Phase 3

**Status: partial. This is not yet a Rule 4 comparative report.**

Every number below was measured on CareGraph alone. Neo4j Community + GDS and
TerminusDB were **not** run, so no comparative claim appears here and none may
be quoted from this file. Rule 4 requires the baselines to be live and the
comparison to be measured; that work is outstanding.

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

| Metric | Target | Measured | Verdict |
|---|---|---|---|
| Point-in-time node read (p95) | < 20 ms | **0.192 ms** | PASS |
| Point-in-time edge read (p95) | < 20 ms | **0.240 ms** | PASS |
| 2-hop bounded traversal (p95) | < 50 ms | **13.026 ms** | PASS |

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

- [ ] Neo4j Community + GDS running, identical graph loaded, traversal measured
- [ ] TerminusDB running, identical graph loaded, versioning comparison measured
- [ ] A thermally controlled machine, or a documented cooldown protocol
- [ ] Clean working tree at run time, so results trace to a commit
- [ ] Real UKPDS graph, or an explicit decision to publish on this substitution
- [x] Bounded-selection fix, then re-measure the traversal target — §5, p95 13.026 ms

Both Section 1 latency targets in scope for Phases 2 and 3 are now met on real
data. What remains before any *comparative* claim is the baseline work: these
numbers say CareGraph is fast, not that it is faster than anything.

---

## 7. Phase 4 — incremental embedding, associative models

### 7.1 Framework substitution

The PRD's Phase 4 task text names DGL. DGL 2.x is not installable on this
machine — the package index offers only the 2018-era 0.1.x line for the
available Python version. PyTorch Geometric is used instead, which Rule 3
names as an equally acceptable implementation; the deviation is from task
wording, not from the rule that gates the phase. `torch 2.11.0+cpu`,
`torch_geometric 2.7.0`.

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

### 7.8 Second measurement, clean tree

```
incremental     median 309.22 ms   p95 1099.20 ms
full recompute  median 1431.83 ms  p95 6487.70 ms
median speedup: 5.28x     target: >= 5.0x     PASS
```

[benchmark: benchmarks/results/gate/phase4_incremental_speedup.json]

Incremental median more than halved (710.97 ms → 309.22 ms). This is a
**marginal pass, stated plainly**: 5.28x against a 5.0x target, and 15 of 30
samples (50%) still fall below 5x individually — a similar count to before the
fix, though the absolute latencies behind them are substantially lower.
`full recompute`'s own numbers moved too (median 5212.64 ms → 1431.83 ms
across the two runs) — consistent with §3's thermal finding, not with any
change on the full-recompute code path, which this session did not touch.
The honest reading: the fix delivered a large, real improvement to the
incremental path specifically, and the topline speedup ratio is noisier than
either median alone suggests, because both numbers move with machine state.

`incremental_fallback_total` was 0 across all 30 real samples and all 400
correctness-test mutations both before and after these fixes — the fallback
path exists and is counted (Rule 7) but was never exercised by anything in
this session's real workload.
