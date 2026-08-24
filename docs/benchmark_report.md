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
| Commit | `dfd1fd85c291d025bde1358d7d9167885f4b2753` |
| Working tree | **dirty** at run time (5 files) — results are not reproducible from the commit alone |
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
| 2-hop bounded traversal (p95) | < 50 ms | **8.144 ms** | PASS |

### 2.1 Point-in-time reads — passes with ~100x headroom

[benchmark: benchmarks/results/pit_latency_1787570007.json]

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

[benchmark: benchmarks/results/traversal_2hop_caregraph_1787569774.json]

```
p50 1.998 ms   p95 8.144 ms   p99 12.704 ms   max 22.633 ms
1000 queries, seed 42, default bounds (fan-out 512, expanded 5000)
mean result: 1366.9 nodes, 1564.8 edges
590 of 1000 traversals hit the fan-out cap
```

This target was **missed by ~4x until the §5 change landed**. The history matters
more than the number, so it is kept in full below rather than overwritten:

| | p95 | mean edges returned |
|---|---|---|
| Before (materialise-then-truncate) | 189.8 ms | 1564.9 |
| After (bounded scan) | **8.144 ms** | 1564.8 |

**23x faster returning the same answer.** Edge count is unchanged to within 0.1
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

Trading an exact diagnostic for a 23x latency win is the right call here, but it
is a real loss of information and is recorded as one rather than quietly
dropped.

---

## 6. Outstanding before any Rule 4 claim

- [ ] Neo4j Community + GDS running, identical graph loaded, traversal measured
- [ ] TerminusDB running, identical graph loaded, versioning comparison measured
- [ ] A thermally controlled machine, or a documented cooldown protocol
- [ ] Clean working tree at run time, so results trace to a commit
- [ ] Real UKPDS graph, or an explicit decision to publish on this substitution
- [x] Bounded-selection fix, then re-measure the traversal target — §5, p95 8.144 ms

Both Section 1 latency targets in scope for Phases 2 and 3 are now met on real
data. What remains before any *comparative* claim is the baseline work: these
numbers say CareGraph is fast, not that it is faster than anything.
