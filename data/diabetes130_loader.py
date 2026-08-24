#!/usr/bin/env python3
"""Load the Diabetes 130-US Hospitals clinical graph into a CareGraph trace.

Reads the real, de-identified inpatient records published by the UCI Machine
Learning Repository and emits the same timestamped JSONL mutation trace format
that `caregraph-load` applies to RocksDB — byte-compatible with the trace from
`idpip_ukpds_loader.py`, so the Rule 4 comparison against Neo4j and TerminusDB
replays identical input into all three systems.

Dataset (Rule 6 — named, cited public dataset):
    Strack B, DeShazo JP, Gennings C, et al. "Impact of HbA1c Measurement on
    Hospital Readmission Rates: Analysis of 70,000 Clinical Database Patient
    Records." BioMed Research International, 2014. Article ID 781670.
    https://archive.ics.uci.edu/dataset/296/

    101,766 encounters, 71,518 distinct patients, 130 US hospitals, 1999-2008.
    Every clinical fact below — diagnosis, medication, lab result, admitting
    specialty, encounter disposition — is read from that file unmodified. This
    script has no synthetic mode and will not fabricate clinical values.

WHAT IS DERIVED RATHER THAN READ (disclose this in any benchmark report):

  1. Absolute dates. The public release strips calendar dates for
     de-identification. `encounter_id` is assigned in admission order, so the
     *sequence* of encounters is real; the dates attached to that sequence are
     not. Encounters are ranked by `encounter_id` and spread evenly across the
     dataset's documented 1999-2008 window. Relative order — which is what
     point-in-time reads and traversal windows actually exercise — is faithful.
     The inter-encounter gaps are uniform where the real ones were not.

  2. Provider identity. The file records an admitting `medical_specialty`, not
     a provider ID. Specialty is used as a provider-proxy node, so every patient
     admitted under "InternalMedicine" shares one node. That is a modelling
     choice, not a record: it is accurate as a specialty, and it deliberately
     produces the high-degree shared reference nodes that make the traversal
     fan-out cap load-bearing.

  3. Procedures are omitted. The file carries `num_procedures` as a count with
     no procedure identities. Emitting UNDERWENT_PROCEDURE edges would require
     inventing which procedures occurred, so no such edges are produced and the
     graph is honestly short one edge type.

Usage:
    python data/diabetes130_loader.py \\
        --csv data/raw/diabetic_data.csv \\
        --out benchmarks/traces/diabetes130_full.jsonl
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

# --- ID namespaces (data/clinical_graph_schema.md) ---------------------------
#
# Source identifiers in this dataset are sparse and large (patient_nbr runs to
# ~1.9e8), so each namespace is assigned dense indices in first-seen order
# rather than being offset directly. `NAMESPACE_CEILING` is asserted at emit
# time: silently overflowing one namespace into the next would merge unrelated
# clinical entities into a single node.

NAMESPACES = {
    "patient": 1,
    "condition": 100_000,
    "medication": 200_000,
    "procedure": 300_000,
    "provider": 400_000,
    "lab_result": 500_000,
    "encounter": 600_000,
}

NAMESPACE_CEILING = {
    "patient": 100_000,
    "condition": 200_000,
    "medication": 300_000,
    "procedure": 400_000,
    "provider": 500_000,
    "lab_result": 600_000,
    "encounter": 2**63,
}

# Edge discriminants must match src/types.rs::EdgeType exactly.
EDGE_TYPES = {
    "DIAGNOSED_WITH": 1,
    "PRESCRIBED_MEDICATION": 2,
    "UNDERWENT_PROCEDURE": 3,
    "TREATED_BY_PROVIDER": 4,
    "HAS_LAB_RESULT": 5,
    "HAS_ENCOUNTER": 6,
}

# Quasi-identifying demographics; Phase 7 encrypts exactly these at rest (Rule 8).
PATIENT_IDENTIFYING = {"race", "gender", "age", "weight"}

# The 23 drug columns. Each holds No / Steady / Up / Down for that encounter.
MEDICATION_COLUMNS = (
    "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride",
    "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone",
    "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide",
    "examide", "citoglipton", "insulin", "glyburide-metformin",
    "glipizide-metformin", "glimepiride-pioglitazone", "metformin-rosiglitazone",
    "metformin-pioglitazone",
)

# Categorical lab results carried per encounter. Both are real measured results.
LAB_COLUMNS = ("max_glu_serum", "A1Cresult")

MISSING = {"?", "", "None", "NULL", "Unknown/Invalid"}

# The dataset's documented collection window (Strack et al. 2014).
WINDOW_START = dt.datetime(1999, 1, 1, tzinfo=dt.timezone.utc)
WINDOW_END = dt.datetime(2008, 12, 31, tzinfo=dt.timezone.utc)


class SourceUnavailable(RuntimeError):
    """The real clinical source could not be read. There is no fallback."""


def present(value: str | None) -> bool:
    """True when the source actually recorded something here."""
    return value is not None and value.strip() not in MISSING


class Namespace:
    """Assigns dense, deterministic node ids within one namespace."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.base = NAMESPACES[name]
        self.ceiling = NAMESPACE_CEILING[name]
        self._ids: dict[str, int] = {}

    def id_for(self, key: str) -> int:
        existing = self._ids.get(key)
        if existing is not None:
            return existing
        node_id = self.base + len(self._ids)
        if node_id >= self.ceiling:
            raise SourceUnavailable(
                f"namespace '{self.name}' overflowed into the next namespace at "
                f"{len(self._ids)} entries; node ids would collide."
            )
        self._ids[key] = node_id
        return node_id

    def __len__(self) -> int:
        return len(self._ids)

    def items(self):
        return self._ids.items()


def read_encounters(csv_path: Path) -> list[dict[str, str]]:
    """Read every encounter, ordered by admission sequence.

    `encounter_id` is assigned in admission order, so sorting by it recovers the
    real chronological sequence both globally and within each patient — the one
    temporal fact the de-identified release preserves.
    """
    if not csv_path.exists():
        raise SourceUnavailable(
            f"{csv_path} not found.\n"
            "\n"
            "Download the real dataset first:\n"
            "    curl -L -o data/raw/diabetes130.zip \\\n"
            "      https://archive.ics.uci.edu/static/public/296/"
            "diabetes+130-us+hospitals+for+years+1999-2008.zip\n"
            "    unzip -o data/raw/diabetes130.zip -d data/raw/\n"
            "\n"
            "Rule 6 prohibits substituting generated patient data, so this\n"
            "loader has no offline mode."
        )

    with csv_path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    if not rows:
        raise SourceUnavailable(
            f"{csv_path} holds no rows; refusing to emit an empty graph."
        )

    missing = {"encounter_id", "patient_nbr", "diag_1"} - set(rows[0])
    if missing:
        raise SourceUnavailable(
            f"{csv_path} is missing expected columns: {sorted(missing)}. "
            "This is not the Diabetes 130-US Hospitals file."
        )

    rows.sort(key=lambda r: int(r["encounter_id"]))
    return rows


def build_timeline(count: int) -> tuple[int, int]:
    """Return (base_micros, step_micros) spreading `count` encounters evenly.

    See the module docstring: the ordering is real, the spacing is not. Even
    spacing across the documented window keeps the derived axis simple to state
    and simple to discount when reading a benchmark number.
    """
    base = int(WINDOW_START.timestamp() * 1_000_000)
    span = int((WINDOW_END - WINDOW_START).total_seconds() * 1_000_000)
    step = max(span // max(count, 1), 1)
    return base, step


def emit(csv_path: Path, out_path: Path, limit_patients: int | None) -> dict[str, Any]:
    """Write the mutation trace and return a provenance record."""
    rows = read_encounters(csv_path)

    patients = Namespace("patient")
    conditions = Namespace("condition")
    medications = Namespace("medication")
    providers = Namespace("provider")
    labs = Namespace("lab_result")
    encounters = Namespace("encounter")

    # The time axis is derived from the whole dataset, before any subsetting, so
    # a smoke trace places an encounter at the same instant the full trace does.
    total_encounters = len(rows)
    ranked = list(enumerate(rows))

    if limit_patients is not None:
        keep: set[str] = set()
        for row in rows:
            if len(keep) >= limit_patients and row["patient_nbr"] not in keep:
                continue
            keep.add(row["patient_nbr"])
        ranked = [(rank, r) for rank, r in ranked if r["patient_nbr"] in keep]

    base_us, step_us = build_timeline(total_encounters)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    counts: dict[str, int] = {"upsert_node": 0, "remove_edge": 0}
    for name in EDGE_TYPES:
        counts[name] = 0

    # Medications a patient was on at their previous encounter, so a drug that
    # disappears from the chart is recorded as a real retraction rather than
    # silently vanishing from the graph.
    active_medications: dict[str, set[str]] = {}
    seen_patients: set[str] = set()

    with out_path.open("w", encoding="utf-8", newline="\n") as fh:

        def write(record: dict[str, Any]) -> None:
            line = json.dumps(record, separators=(",", ":"), sort_keys=True, default=str)
            fh.write(line + "\n")
            digest.update(line.encode("utf-8"))

        # Reference nodes (a condition, a drug, a specialty) are shared by many
        # patients and are written once at the window start. Re-emitting them per
        # encounter would rewrite the same key hundreds of thousands of times.
        reference_written: set[int] = set()

        def upsert_reference(node_id: int, node_type: str,
                             properties: dict[str, Any]) -> None:
            if node_id in reference_written:
                return
            reference_written.add(node_id)
            upsert_node(node_id, node_type, base_us, properties)

        def upsert_node(node_id: int, node_type: str, ts: int,
                        properties: dict[str, Any],
                        encrypted: list[str] | None = None) -> None:
            record: dict[str, Any] = {
                "op": "upsert_node",
                "node_id": node_id,
                "node_type": node_type,
                "timestamp_us": ts,
                "properties": properties,
            }
            if encrypted:
                record["encrypted_fields"] = encrypted
            write(record)
            counts["upsert_node"] += 1

        def add_edge(src: int, dst: int, kind: str, ts: int,
                     properties: dict[str, Any]) -> None:
            write({
                "op": "add_edge",
                "src": src,
                "dst": dst,
                "edge_type": EDGE_TYPES[kind],
                "edge_type_name": kind,
                "timestamp_us": ts,
                "properties": properties,
            })
            counts[kind] += 1

        for rank, row in ranked:
            # Sub-microsecond ordering within one encounter: each edge gets its
            # own timestamp so no two versions of the same adjacency collide.
            encounter_us = base_us + rank * step_us
            offset = 0

            def next_ts() -> int:
                nonlocal offset
                offset += 1
                return encounter_us + offset

            patient_key = row["patient_nbr"]
            patient_id = patients.id_for(patient_key)

            if patient_key not in seen_patients:
                seen_patients.add(patient_key)
                demographics = {
                    field: row[field]
                    for field in ("race", "gender", "age", "weight")
                    if present(row.get(field))
                }
                upsert_node(
                    patient_id, "Patient", encounter_us, demographics,
                    sorted(PATIENT_IDENTIFYING & demographics.keys()),
                )

            # --- the encounter itself ---------------------------------------
            encounter_id = encounters.id_for(row["encounter_id"])
            encounter_props = {
                field: row[field]
                for field in ("admission_type_id", "discharge_disposition_id",
                              "admission_source_id", "time_in_hospital",
                              "number_diagnoses", "num_medications",
                              "num_lab_procedures", "num_procedures",
                              "readmitted")
                if present(row.get(field))
            }
            upsert_node(encounter_id, "Encounter", encounter_us, encounter_props)
            add_edge(patient_id, encounter_id, "HAS_ENCOUNTER", next_ts(), {})

            # --- diagnoses (ICD-9, real codes) ------------------------------
            for position in ("diag_1", "diag_2", "diag_3"):
                code = row.get(position, "")
                if not present(code):
                    continue
                condition_id = conditions.id_for(code)
                upsert_reference(condition_id, "Condition", {"icd9_code": code})
                add_edge(patient_id, condition_id, "DIAGNOSED_WITH", next_ts(),
                         {"position": position,
                          "encounter_id": row["encounter_id"]})

            # --- admitting specialty as provider proxy ----------------------
            specialty = row.get("medical_specialty", "")
            if present(specialty):
                provider_id = providers.id_for(specialty)
                upsert_reference(provider_id, "Provider",
                                 {"specialty": specialty,
                                  "identity": "specialty_proxy"})
                add_edge(patient_id, provider_id, "TREATED_BY_PROVIDER",
                         next_ts(), {"encounter_id": row["encounter_id"]})

            # --- categorical lab results ------------------------------------
            for column in LAB_COLUMNS:
                result = row.get(column, "")
                if not present(result):
                    continue
                lab_id = labs.id_for(f"{column}={result}")
                upsert_reference(lab_id, "LabResult",
                                 {"assay": column, "result": result})
                add_edge(patient_id, lab_id, "HAS_LAB_RESULT", next_ts(),
                         {"encounter_id": row["encounter_id"]})

            # --- medications, with real start/stop transitions ---------------
            prescribed_now: set[str] = set()
            for column in MEDICATION_COLUMNS:
                status = row.get(column, "No")
                if status in ("Steady", "Up", "Down"):
                    prescribed_now.add(column)
                    medication_id = medications.id_for(column)
                    upsert_reference(medication_id, "Medication",
                                     {"drug": column})
                    add_edge(patient_id, medication_id, "PRESCRIBED_MEDICATION",
                             next_ts(),
                             {"status": status,
                              "encounter_id": row["encounter_id"]})

            previously = active_medications.get(patient_key, set())
            for stopped in sorted(previously - prescribed_now):
                write({
                    "op": "remove_edge",
                    "src": patient_id,
                    "dst": medications.id_for(stopped),
                    "edge_type": EDGE_TYPES["PRESCRIBED_MEDICATION"],
                    "timestamp_us": next_ts(),
                })
                counts["remove_edge"] += 1
            active_medications[patient_key] = prescribed_now

    total = sum(counts.values())
    if total == 0:
        raise SourceUnavailable("produced no records; refusing to report an empty load")

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": "Diabetes 130-US Hospitals for years 1999-2008 (UCI ML Repository, id 296)",
        "citation": (
            "Strack B, DeShazo JP, Gennings C, Olmo JL, Ventura S, Cios KJ, "
            "Clore JN. Impact of HbA1c Measurement on Hospital Readmission "
            "Rates. BioMed Research International, 2014:781670."
        ),
        "source_file": str(csv_path),
        "source_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "encounters": len(ranked),
        "encounters_in_source": total_encounters,
        "patients": len(patients),
        "distinct_conditions": len(conditions),
        "distinct_medications": len(medications),
        "distinct_providers": len(providers),
        "distinct_lab_results": len(labs),
        "record_counts": counts,
        "total_records": total,
        "timeline": {
            "basis": "encounter_id rank spread evenly across the 1999-2008 window",
            "ordering": "real — encounter_id is assigned in admission order",
            "spacing": "derived — inter-encounter gaps are not in the public release",
            "window_start": WINDOW_START.isoformat(),
            "window_end": WINDOW_END.isoformat(),
            "step_micros": step_us,
        },
        "omitted": {
            "UNDERWENT_PROCEDURE": (
                "source records num_procedures as a count with no procedure "
                "identities; emitting these edges would require inventing them"
            ),
        },
        "provider_identity": "medical_specialty used as a provider proxy",
        "trace_sha256": digest.hexdigest(),
        "trace_path": str(out_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv", type=Path, default=Path("data/raw/diabetic_data.csv"),
        help="the real diabetic_data.csv (default: data/raw/diabetic_data.csv)",
    )
    parser.add_argument(
        "--out", type=Path, required=True,
        help="destination JSONL mutation trace",
    )
    parser.add_argument(
        "--limit-patients", type=int, default=None,
        help="load only the first N patients by admission order (smoke test)",
    )
    args = parser.parse_args()

    try:
        provenance = emit(args.csv, args.out, args.limit_patients)
    except SourceUnavailable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # The provenance record is what lets a benchmark number be traced back to
    # the exact graph it was measured on (Rule 10).
    manifest = args.out.with_suffix(args.out.suffix + ".manifest.json")
    manifest.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(provenance, indent=2))
    print(f"\ntrace:    {args.out}", file=sys.stderr)
    print(f"manifest: {manifest}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
