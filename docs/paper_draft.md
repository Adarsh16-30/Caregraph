# CareGraph: A Temporally-Versioned Graph Database with Incrementally-Maintained GNN Embeddings

**Status:** working draft, positioned for a database-systems venue (CIDR /
ICDE / SIGMOD / VLDB per the PRD's Phase 8 target). Not submitted anywhere.
Every quantitative claim below carries a `[benchmark: <file>]` citation by
the same convention `docs/patent_hooks.md` uses, even though
`scripts/check_rules.sh`'s Rule 10 check currently only greps
`docs/patent_hooks.md` itself, not this file — see §7 "Limitations and
disclosed gaps" for that and other honesty notes a reviewer would want
surfaced up front rather than found later.

## Abstract

Applications built on evolving graphs increasingly rely on GNN-derived node
embeddings for similarity search, risk scoring, and prediction, but treat
the embedding as an external, offline artifact: recomputed on a schedule,
written back later, with no durable record of what it was at an earlier
time. We present CareGraph, a durable, versioned graph database in which
GNN node embeddings are a first-class, incrementally-maintained field. A
graph mutation and its corresponding embedding update commit atomically
within a single storage-engine transaction, so both structure and
embeddings are queryable at any historical instant with no separate
recompute step. We describe the system's temporal key encoding, its
staged incremental-update pipeline for both associative (GraphSAGE/GCN)
and non-associative (GAT) aggregation, and its durability guarantee under
process failure, and evaluate it against Neo4j+GDS and TerminusDB on a
real clinical dataset.

## 1. Introduction

Two structural failures motivate this work. First, **embedding
staleness**: a new diagnosis does not affect a risk-scoring embedding
until the next scheduled batch job runs, because the embedding pipeline
and the transactional store are separate systems with no shared commit
boundary. Second, **no historical embedding record**: point-in-time
questions such as "what did this patient's care-pathway embedding look
like the day before their readmission" are unanswerable once an
embedding has been overwritten by a later batch run, because nothing
versions the embedding itself.

CareGraph's approach is to make the embedding a versioned column-family
value with the same durability and point-in-time-query properties as the
graph structure it is derived from, and to update it incrementally inside
the same atomic transaction as the structural mutation that motivated the
update. This paper's contributions map directly to the five claims in
`docs/patent_hooks.md`:

1. Atomic versioned mutation-plus-embedding transaction (§3, §5.1).
2. Temporal key encoding for O(log n) point-in-time retrieval (§2, §5.2).
3. Durability-integrated incremental runtime embedding computation (§4, §5.3).
4. A staged incremental update path for GAT's non-associative aggregation (§4.3).
5. Point-in-time embedding similarity as a native query primitive (§2.3).

## 2. Storage design

### 2.1 Column families

Four RocksDB column families: `CF_EDGES` (forward adjacency), `CF_REVERSE`
(reverse adjacency, for incoming-edge queries), `CF_NODES` (versioned node
state), `CF_EMBEDDINGS` (versioned embedding vectors). Each key embeds a
timestamp with its bits inverted, so the newest version of any entity
sorts first in a forward RocksDB prefix scan (`src/temporal/keys.rs`). A
point-in-time read is therefore a single seek — no reverse iterator, no
secondary index.

### 2.2 Deletions are tombstones

A removal appends a version marked `deleted` rather than issuing a RocksDB
`delete`. Erasing the key would erase the history point-in-time
reconstruction depends on, making `as_of(T)` for a `T` before the removal
wrongly report the edge never existed. The timeline is append-only.

### 2.3 Point-in-time similarity as a query primitive

`similar_care_pathways(node_id, as_of, top_k)` (`src/api/similarity.rs`)
reads the query node's `CF_EMBEDDINGS` entry at `as_of` and ranks every
other node with an embedding at that same instant by cosine similarity —
one versioned scan, no export to an external index, no separate recompute.
This is the mechanism Claim 5 names.

## 3. Atomic mutation-plus-embedding commit

Every structural mutation and its resulting embedding update are staged
into a single `WriteBatch` and written with one `db.write(batch)` call
(`src/embedding/atomic_commit.rs`). After a crash at any point before that
call, neither write is visible; after a crash at any point during or
after it, both are — there is no code path that can observe one without
the other.

**Evidence.** 100 fault-injection iterations killed the committing worker
process at a randomised point racing its own commit call; 37 kills
actually landed inside the race window, and across all 100 iterations
(committed, uncommitted, or killed) 0 non-atomic states were observed —
every reopened database showed either the structural edge with both
endpoints' embeddings, or neither.
[benchmark: benchmarks/results/gate/phase5_fault_injection.log]

## 4. Incremental embedding computation

### 4.1 Affected-subgraph resolution

A mutation's two endpoints and their direct neighbours form the affected
set; a two-layer model additionally needs each affected node's own
neighbours' features (the "ring two" expansion,
`src/embedding/resolver.rs`) — a single-ring expansion silently corrupts
downstream embeddings by leaving ring-two nodes as edge-less feature rows.
This was a real, measured bug in an earlier version of this resolver
(structural error ~1e-2, four orders of magnitude above the ~6e-7 floating
point noise floor of the fixed version) before its correctness test caught
it — see `docs/benchmark_report.md` §7.4 for the full account, kept rather
than silently corrected out of the record.

### 4.2 Associative models (GraphSAGE, GCN)

For GraphSAGE/GCN's associative mean aggregation, the update is
operator-decoupled (following the NeutronRT/RIPPLE++ line of incremental
GNN inference) — only the affected receptive field is recomputed, not the
whole graph.

**Evidence.** 30 real `add_edge` mutations sampled across a 174,298-node /
515,117-edge clinical graph: incremental median 820.27 ms vs. full-recompute
median 6210.12 ms — **7.79x** median speedup against a 5.0x target, with
`incremental_fallback_total` at 0 across all 30 samples and 400
correctness-test mutations both before and after the resolver fix above.
[benchmark: benchmarks/results/gate/phase4_incremental_speedup.json]

Bounding the receptive field's *total size* (not just each node's own
fan-out) was necessary to keep this fast: an unbounded ring-two expansion
was observed to pull a 49,165-node subgraph for one 521-node affected set
before a `max_expanded_nodes` backstop was added — see
`docs/benchmark_report.md` §7.6-§7.7.

### 4.3 GAT's non-associative case

GAT's attention coefficients depend jointly on all of a node's neighbours,
so a mutation's effect cannot be summed as a delta the way GraphSAGE/GCN's
aggregation can — current incremental-GNN literature flags this as an open
problem. `src/embedding/gat_incremental.rs` recomputes attention-weighted
aggregation for the affected neighbourhood only, without a full forward
pass. Correctness (exact match against full recompute, float32 tolerance)
is verified on 50 randomised mutation sequences
(`tests/embedding/gat_correctness_test.rs`); a GAT-specific
incremental-vs-recompute *speedup* number has not been measured this
session (see §7).

## 5. Evaluation

### 5.1 Setup

Diabetes 130-US Hospitals (UCI dataset 296) substitutes for the PRD's
named IDPIP/UKPDS source, which was not reachable in this environment
(disclosed, cited public dataset, per Rule 6's substitution clause — see
`docs/benchmark_report.md` §1 and README's Known gaps). 174,298 nodes,
599,497 edges (10,989 retractions), loaded identically into CareGraph,
Neo4j 5.26.30 + GDS 2.13.12, and TerminusDB 11.1.14.

### 5.2 Point-in-time reads (Claim 2)

Point-in-time node read: p50 0.013 ms, p95 **0.192 ms**, p99 0.258 ms.
Point-in-time edge read: p50 0.029 ms, p95 **0.240 ms**, p99 0.318 ms. Both
roughly **100x** inside the PRD's target on this graph size.
[benchmark: benchmarks/results/gate/phase2_pit_latency.json]

### 5.3 Comparative traversal (Rule 4, Claim 1/3's supporting context)

2-hop bounded traversal, 5,000 queries per system, identical graph and
query set, one uncooled run on one machine:

| System | p95 | vs. 50 ms target |
|---|---|---|
| CareGraph | 17.900 ms | **PASS**, ~2.8x headroom |
| Neo4j + GDS | 47.161 ms | PASS, ~6% headroom |
| TerminusDB | 90.567 ms | **FAIL**, ~1.8x over |

CareGraph measured **~3.7x** faster than Neo4j+GDS and **~11.9x** faster
than TerminusDB at p95 in this run.
[benchmark: benchmarks/results/traversal_2hop_caregraph_1788159953.json]
[benchmark: benchmarks/results/traversal_2hop_neo4j_1788160089.json]
[benchmark: benchmarks/results/traversal_2hop_terminusdb_1788160424.json]

This is a single run on one uncooled laptop CPU — a repeated,
thermally-controlled measurement swung p95 by up to 2.5x on identical,
deterministic query results in a separate finding this session made
(`docs/benchmark_report.md` §3). Read the ordering and rough magnitude as
the trustworthy signal, not the absolute millisecond values, and see §7 and
`docs/benchmark_report.md` §8.3 for what this comparison does not yet
cover (point-in-time reads and incremental-embedding speedup are still
CareGraph-only; TerminusDB's edge count differs from the other two systems'
for a disclosed loader reason, not a graph discrepancy).

## 6. Related work

**TerminusDB** provides native document versioning but no GNN-embedding
concept at the storage layer. **Neo4j + GDS** computes graph embeddings
(FastRP, GraphSAGE) as a named, separately-invoked algorithm run against a
graph projection — not a field committed inside the same transaction as
the mutation that motivated recomputing it, and Neo4j Community has no
native point-in-time query primitive (temporal versioning there is an
application-level modeling pattern, not a storage feature). **NeutronRT**
and **RIPPLE++** are the incremental-GNN-inference research line this
project's update math is built on directly (disclosed, not obscured); both
are described as runtime/serving-layer techniques living in GPU/CPU
memory, not carried across a durability boundary the way CareGraph's
`AtomicCommitter` does. See `docs/novelty_analysis.md` for the full,
per-claim comparison and its own disclosed limits.

## 7. Limitations and disclosed gaps

- **Rule 10's automated check does not yet cover this file.**
  `scripts/check_rules.sh`'s `rule_10` greps `docs/patent_hooks.md` only.
  Every citation above follows that document's own convention voluntarily;
  extending the check to include this file (or generalizing it to any file
  under `docs/`) has not been done.
- **Single-machine, largely single-run measurements.** Nearly every number
  in §5 comes from one uncooled laptop CPU, sometimes one run. §5.3's own
  citation of the thermal-variance finding applies to every other number in
  this section too.
- **GAT has no incremental-speedup number**, only a correctness result
  (§4.3) — the PRD's own "up to ~100ms" GAT latency figure is a design
  target stated in the build brief, not something measured and repeated
  here as a result.
- **Similarity search has no latency benchmark.** `similar_care_pathways`
  is exercised functionally (real client, real varying responses) but was
  not one of the metrics extended to a percentile measurement this
  session — see `docs/patent_hooks.md` Claim 5's own disclosed gap.
- **This is a draft, not a submission.** Turning this into an actual
  CIDR/ICDE/SIGMOD/VLDB submission needs related-work coverage well beyond
  the three systems named in the PRD (a real literature search, not this
  document's desk comparison), a threats-to-validity section addressing
  the single-machine measurement issue directly, and a human author's
  judgment about venue fit and framing — none of which this repository is
  positioned to do on its own.
