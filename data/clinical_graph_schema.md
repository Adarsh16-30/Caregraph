# CareGraph Clinical Graph Schema

The graph schema (PRD 2.6, 6.1) was written for the IDPIP UKPDS pipeline: 5,102
type-2 diabetes patients with 20-year follow-up. That source is not reachable
from this repository, so the graph actually built and benchmarked here comes from
the public Diabetes 130-US Hospitals dataset through `data/diabetes130_loader.py`,
using the **node types and ID namespaces below**. Which properties that loader
populates, derives or leaves absent is stated in its own module documentation; the
"Source table" column below is the PRD's UKPDS naming and does not describe the
Diabetes 130 columns.

**What the built graph actually contains.** Of the seven node types below, the
Diabetes 130 graph has six: `Patient`, `Condition`, `Medication`, `Provider`
(admitting specialty, used as a provider proxy), `LabResult` and `Encounter`.
There are **no `Procedure` nodes and no `UNDERWENT_PROCEDURE` edges**, because the
source file carries only a procedure count. The shared reference types are small
(915 conditions, 21 medications, 72 providers, 6 lab results), so each is a very
high-degree hub; that is the main driver of the incremental-update latency miss
in `docs/benchmark_report.md` §2.3.

Nothing in this schema is invented. Where a field is absent from the source, the
property is simply absent from the node rather than being filled with a
placeholder (Rule 6).

## Node types

Node IDs are assigned by the loader from a stable, deterministic namespace so
that reloading the same source produces the same graph.

| Type | ID range | PRD source table (UKPDS) | Key properties |
|------|----------|--------------|----------------|
| `Patient` | `1 .. 99_999` | `ukpds.patients` | `sex`, `birth_year`, `enrollment_date`, `treatment_arm` |
| `Condition` | `100_000 .. 199_999` | `ukpds.conditions` (ICD-coded) | `icd10_code`, `label`, `category` |
| `Medication` | `200_000 .. 299_999` | `ukpds.medications` | `atc_code`, `label`, `drug_class` |
| `Procedure` | `300_000 .. 399_999` | `ukpds.procedures` | `code`, `label` |
| `Provider` | `400_000 .. 499_999` | `ukpds.providers` | `specialty`, `site_id` |
| `LabResult` | `500_000 .. 599_999` | `ukpds.lab_results` | `loinc_code`, `analyte`, `unit` |
| `Encounter` | `600_000 .. 699_999` | `ukpds.encounters` | `encounter_type`, `setting` |

`Condition`, `Medication`, `Procedure`, and `LabResult` are **shared reference
nodes** — one `Condition` node for Stage 3 CKD, referenced by every patient
diagnosed with it. This is what gives the graph the connectivity that makes
care-pathway similarity meaningful; a per-patient copy would leave the graph a
disjoint union of stars with nothing for a GNN to aggregate over.

## Edge types

Discriminants are fixed in `src/types.rs::EdgeType` and are part of the on-disk
key format. They may be appended to but never renumbered.

| Edge | Discriminant | Direction | Timestamp semantics |
|------|-------------|-----------|---------------------|
| `DIAGNOSED_WITH` | 1 | Patient → Condition | date of diagnosis |
| `PRESCRIBED_MEDICATION` | 2 | Patient → Medication | prescription start date |
| `UNDERWENT_PROCEDURE` | 3 | Patient → Procedure | procedure date |
| `TREATED_BY_PROVIDER` | 4 | Patient → Provider | first encounter with that provider |
| `HAS_LAB_RESULT` | 5 | Patient → LabResult | specimen collection date |
| `HAS_ENCOUNTER` | 6 | Patient → Encounter | encounter date |

Every edge is written to both `CF_EDGES` and `CF_REVERSE`, so incoming traversal
("which patients are on metformin?") is a prefix scan rather than a full scan.

## Temporal semantics

The edge timestamp is the **clinical event time**, not the ingestion time. This
matters for the central query the system exists to answer — *"what did this
patient's care-pathway embedding look like the day before their readmission?"* —
which is only meaningful if the version timeline reflects when care actually
happened.

A retracted or corrected record is written as a new version at the correction's
timestamp. History is never rewritten in place; that is what makes point-in-time
reconstruction faithful.

Timestamps are microseconds since the Unix epoch (`types::Timestamp`). UKPDS
records dates at day resolution; the loader maps a date to midnight UTC and
resolves same-day ordering using the source table's sequence column, so that two
events on one day retain their recorded order rather than colliding.

## Value encoding

Node and edge properties are JSON objects. Patient-identifying fields are listed
explicitly here because Phase 7 encrypts exactly this set at rest (Rule 8):

- `Patient.birth_year`
- `Patient.enrollment_date`
- `Patient.site_id`
- any `Encounter` property

Reference-node properties (ICD codes, drug labels) are not patient-identifying
and are stored in the clear so that they remain scannable.

## Provenance

The loader records, for every run, the source database, the query used, the row
counts, and a content hash of the emitted trace. That record is what lets a
benchmark number be traced back to the exact graph it was measured on (Rule 10).
