# CareGraph — Novelty Analysis

Supporting analysis for `docs/patent_hooks.md`. Where that document states
*what* is claimed and cites the measurement backing it, this document states
*why* each claim is not already covered by a named prior-art system, and is
explicit about the limits of that comparison.

## 1. Method

This is a **desk review against publicly documented behavior** of the
systems the PRD names as the relevant prior art (TerminusDB, Neo4j + GDS,
and the NeutronRT/RIPPLE++ research line CareGraph's incremental-embedding
math is built on) — reading their own documentation, papers, and source
where available, not a survey of the patent literature itself. It is a
necessary input to a real novelty search, not a substitute for one: a
proper freedom-to-operate or patentability opinion needs a professional
prior-art search (patent databases, not just competing systems' docs) that
this repository is not positioned to run. See §4.

## 2. Per-claim comparison

### Claim 1 — Atomic versioned mutation-plus-embedding transaction

| System | Durable ACID graph mutations? | Embeddings as a versioned, transactionally-committed field? |
|---|---|---|
| Neo4j + GDS | Yes (its own transaction log) | No — GDS embeddings are computed by a separate named graph projection/algorithm run, exported as a result, not committed inside the same transaction as the mutation that motivated recomputing them. |
| TerminusDB | Yes (its own versioned document commits) | No embedding concept at the storage layer at all — TerminusDB versions documents, not GNN-derived vectors. |
| NeutronRT / RIPPLE++ (research) | Not applicable — both are described as runtime/in-memory incremental-inference systems, not durable stores. | Incremental, but the update lives in the inference runtime's own state, not inside a WAL-backed transaction that survives a process kill. |

CareGraph's distinguishing move is specifically the **combination**: the
same `WriteBatch` that durably persists the structural mutation also
persists the embedding update, verified in this repository by killing the
process mid-commit (Claim 1's fault-injection evidence) — a test that is
meaningless for a system where the embedding update was never going to be
part of the same durability boundary as the mutation in the first place.

### Claim 2 — Descending-timestamp key encoding for O(log n) point-in-time embedding retrieval

Neo4j has no native point-in-time query primitive at all in Community
edition — temporal versioning is an application-level pattern (extra nodes/
relationships modeling valid-time), not a storage-engine feature, so a
point-in-time read there costs whatever that modeling pattern costs, not a
single seek. TerminusDB *does* version documents natively (its core
selling point), but versions commits, not a per-embedding-key seek — asking
"what was this specific vector at time T" still means resolving through
its commit graph, not a direct RocksDB-style prefix scan. CareGraph's
specific mechanism — invert the timestamp's bits so the newest version
sorts first in a forward scan, avoiding both a secondary index and a
reverse iterator — is a storage-layer technique, not a claim that no other
system can answer "what was X at time T" by some means.

### Claim 3 — Durability-integrated incremental RTEC

NeutronRT and RIPPLE++ (2026 reference implementation, PRD §2.3) are the
actual source of the incremental-update math CareGraph's
`AffectedSubgraphResolver`/`associative.rs` are built on — this is
disclosed, not obscured (PRD §4.1, §6.2). Their own published framing
positions the incremental computation as a runtime/serving-layer
optimization: fast, but living in GPU/CPU memory, rebuilt from the graph on
restart. CareGraph's contribution is not the incremental math itself but
carrying it across the durability boundary — the same 7.79x-measured
incremental path is provably resumable from a crash without recomputing
from scratch, which is not a property either reference implementation's
own published description claims for itself.

### Claim 4 — Staged incremental update path for GAT

The PRD states plainly that GAT's non-associative attention aggregation is
"explicitly flagged as an open problem in current incremental-GNN
literature" (§6.2) — this claim's novelty rests on that literature gap,
not on a specific competing system already solving it. This repository has
not independently verified that literature claim beyond what the PRD
states; a real disclosure should re-verify it against current papers at
filing time, not rely on the PRD's 2026 framing being still accurate.

### Claim 5 — Native point-in-time embedding similarity

Vector-similarity search is now common (Neo4j has vector indexes; dedicated
vector databases exist as their own category), but evaluated against
*current* embeddings — "most similar right now," not "most similar as of a
chosen historical instant using exactly the embedding that was live then."
CareGraph's `similar_care_pathways` reads a versioned scan at a caller-
supplied `as_of`, which is the same point-in-time mechanism Claim 2
describes, applied to similarity ranking specifically. The novelty is in
the *combination* of versioned storage with similarity search, not in
either half alone.

## 3. What would weaken these claims

Disclosed here rather than left for an examiner to find:

- **Claim 2's mechanism (descending/inverted timestamp keys for
  newest-first scans) is a known LSM-tree technique**, not invented by this
  project — RocksDB itself and other timeseries-oriented stores use
  timestamp-ordered keys for similar reasons. The claim as scoped is the
  *application* of this technique to embedding retrieval specifically, not
  the key-inversion technique in the abstract.
- **None of these five claims have been checked against the patent
  literature itself** — only against competing systems' public behavior
  (§1). A real prior-art search could surface an existing patent covering
  any of these combinations that this desk review would not find.
- **The three-way benchmark backing Claim 1/3's "why this matters"
  framing is a single uncooled run on one laptop** (`docs/benchmark_report.md`
  §8.3) — sufficient to support the ordering and rough magnitude of the
  comparison, not to support a specific number appearing in a patent
  claim's own text.

## 4. What "checked against Palantir, Pinterest, and LinkedIn prior-art families" actually requires

The PRD's Phase 8 task list names three companies' patent families as a
specific cross-check to run before filing. This repository cannot perform
that check: it requires searching issued patents and applications (USPTO
full-text search, Google Patents, or a paid tool) for filings assigned to
those three companies (and their known graph/ML infrastructure teams)
touching graph storage, incremental graph embeddings, or versioned graph
databases — reading claim language, not just marketing material — which is
a task for the named inventor and/or the university's technology-transfer
office, not something to fabricate a result for here. Treat this section,
and `docs/patent_hooks.md`'s own header note, as the disclosure that this
step is outstanding, not as evidence it was done.
