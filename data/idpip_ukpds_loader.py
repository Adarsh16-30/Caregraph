#!/usr/bin/env python3
"""Load the IDPIP UKPDS-derived clinical graph into a CareGraph mutation trace.

Reads the real UKPDS tables from IDPIP's existing TimescaleDB instance (PRD 2.6)
and emits a timestamped JSONL mutation trace that `caregraph-load` applies to
RocksDB. The intermediate trace exists so that the same byte-identical input can
be replayed into CareGraph, Neo4j+GDS, and TerminusDB — which is what makes the
Rule 4 comparison apples-to-apples.

Rule 6: this script has no synthetic mode. If it cannot reach a real clinical
source it exits non-zero. It will not fabricate patients, conditions, or lab
values under any flag, because a benchmark measured on invented data is not a
measurement.

Usage:
    export IDPIP_DATABASE_URL='postgresql://user@host:5432/idpip'
    python data/idpip_ukpds_loader.py --limit-patients 100 \\
        --out benchmarks/traces/ukpds_smoke_100.jsonl

Requires: psycopg[binary]>=3.1
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

# --- ID namespaces (data/clinical_graph_schema.md) ---------------------------

NAMESPACES = {
    "patient": 1,
    "condition": 100_000,
    "medication": 200_000,
    "procedure": 300_000,
    "provider": 400_000,
    "lab_result": 500_000,
    "encounter": 600_000,
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

PATIENT_IDENTIFYING = {"birth_year", "enrollment_date", "site_id"}


class SourceUnavailable(RuntimeError):
    """The real clinical source could not be reached. There is no fallback."""


@dataclass(frozen=True)
class EdgeSpec:
    """One source query mapped onto one edge type."""

    edge_type: str
    target_namespace: str
    table: str
    target_id_column: str
    event_time_column: str
    property_columns: tuple[str, ...]


# Each spec reads a real UKPDS table. Column names track IDPIP's schema; if the
# upstream schema changes, this fails loudly at query time rather than silently
# producing an empty graph.
EDGE_SPECS: tuple[EdgeSpec, ...] = (
    EdgeSpec("DIAGNOSED_WITH", "condition", "ukpds.patient_conditions",
             "condition_id", "diagnosed_on", ("severity", "status")),
    EdgeSpec("PRESCRIBED_MEDICATION", "medication", "ukpds.patient_medications",
             "medication_id", "started_on", ("dose", "frequency")),
    EdgeSpec("UNDERWENT_PROCEDURE", "procedure", "ukpds.patient_procedures",
             "procedure_id", "performed_on", ("outcome",)),
    EdgeSpec("TREATED_BY_PROVIDER", "provider", "ukpds.patient_providers",
             "provider_id", "first_seen_on", ("relationship",)),
    EdgeSpec("HAS_LAB_RESULT", "lab_result", "ukpds.patient_lab_results",
             "lab_result_id", "collected_on", ("value", "unit", "abnormal_flag")),
    EdgeSpec("HAS_ENCOUNTER", "encounter", "ukpds.patient_encounters",
             "encounter_id", "occurred_on", ("disposition",)),
)


def node_id(namespace: str, source_id: int) -> int:
    """Deterministic graph ID, so reloading the same source is idempotent."""
    base = NAMESPACES[namespace]
    if source_id < 0:
        raise ValueError(f"negative source id {source_id} in namespace {namespace}")
    return base + source_id


def to_micros(value: Any, sequence: int = 0) -> int:
    """Map a source date/datetime to microseconds since the Unix epoch.

    UKPDS records dates at day resolution. Same-day events are separated by the
    source table's sequence column so their recorded order survives, rather than
    two events colliding on one timestamp.
    """
    if value is None:
        raise ValueError("event time is NULL; cannot place this record on the timeline")
    if isinstance(value, dt.datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    elif isinstance(value, dt.date):
        moment = dt.datetime(value.year, value.month, value.day, tzinfo=dt.timezone.utc)
    else:
        raise TypeError(f"unsupported event time type: {type(value)!r}")
    return int(moment.timestamp() * 1_000_000) + sequence


def connect(database_url: str | None):
    """Open a connection to the real IDPIP TimescaleDB, or fail."""
    if not database_url:
        raise SourceUnavailable(
            "IDPIP_DATABASE_URL is not set.\n"
            "\n"
            "This loader reads the real UKPDS-derived tables from IDPIP's\n"
            "TimescaleDB instance. Rule 6 prohibits substituting generated\n"
            "patient data, so there is no offline or synthetic mode.\n"
            "\n"
            "Set IDPIP_DATABASE_URL to the IDPIP instance, or obtain a UKPDS\n"
            "extract and load it into a local Postgres first."
        )
    try:
        import psycopg
    except ImportError as exc:
        raise SourceUnavailable(
            "psycopg is not installed. Install it with:\n"
            "    pip install 'psycopg[binary]>=3.1'"
        ) from exc

    try:
        return psycopg.connect(database_url, autocommit=True)
    except Exception as exc:
        raise SourceUnavailable(f"could not connect to the IDPIP database: {exc}") from exc


def fetch_patients(conn, limit: int | None) -> list[dict[str, Any]]:
    sql = """
        SELECT patient_id, sex, birth_year, enrollment_date, treatment_arm, site_id
        FROM ukpds.patients
        ORDER BY patient_id
    """
    if limit is not None:
        sql += f"\nLIMIT {int(limit)}"

    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    if not rows:
        raise SourceUnavailable(
            "ukpds.patients returned zero rows. The database is reachable but "
            "holds no clinical data; refusing to emit an empty graph."
        )
    return rows


def fetch_edges(conn, spec: EdgeSpec, patient_ids: list[int]) -> Iterator[dict[str, Any]]:
    props = "".join(f", {c}" for c in spec.property_columns)
    sql = f"""
        SELECT patient_id, {spec.target_id_column}, {spec.event_time_column}{props}
        FROM {spec.table}
        WHERE patient_id = ANY(%s)
        ORDER BY patient_id, {spec.event_time_column}
    """
    with conn.cursor() as cur:
        cur.execute(sql, (patient_ids,))
        columns = [d[0] for d in cur.description]
        for row in cur:
            yield dict(zip(columns, row))


def emit(out_path: Path, conn, limit: int | None) -> dict[str, Any]:
    """Write the mutation trace and return a provenance record."""
    patients = fetch_patients(conn, limit)
    patient_ids = [p["patient_id"] for p in patients]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    counts: dict[str, int] = {"upsert_node": 0}

    with out_path.open("w", encoding="utf-8", newline="\n") as fh:

        def write(record: dict[str, Any]) -> None:
            line = json.dumps(record, separators=(",", ":"), sort_keys=True, default=str)
            fh.write(line + "\n")
            digest.update(line.encode("utf-8"))

        for patient in patients:
            enrolled = patient.get("enrollment_date")
            properties = {
                k: v for k, v in patient.items()
                if k != "patient_id" and v is not None
            }
            write({
                "op": "upsert_node",
                "node_id": node_id("patient", patient["patient_id"]),
                "node_type": "Patient",
                "timestamp_us": to_micros(enrolled),
                "properties": properties,
                # Phase 7 encrypts exactly these fields at rest (Rule 8).
                "encrypted_fields": sorted(PATIENT_IDENTIFYING & properties.keys()),
            })
            counts["upsert_node"] += 1

        for spec in EDGE_SPECS:
            same_day: dict[tuple[int, Any], int] = {}
            emitted = 0
            for row in fetch_edges(conn, spec, patient_ids):
                pid = row["patient_id"]
                raw_time = row[spec.event_time_column]
                seq_key = (pid, raw_time)
                sequence = same_day.get(seq_key, 0)
                same_day[seq_key] = sequence + 1

                write({
                    "op": "add_edge",
                    "src": node_id("patient", pid),
                    "dst": node_id(spec.target_namespace, row[spec.target_id_column]),
                    "edge_type": EDGE_TYPES[spec.edge_type],
                    "edge_type_name": spec.edge_type,
                    "timestamp_us": to_micros(raw_time, sequence),
                    "properties": {
                        c: row[c] for c in spec.property_columns if row.get(c) is not None
                    },
                })
                emitted += 1
            counts[spec.edge_type] = emitted

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": "IDPIP TimescaleDB — UKPDS-derived clinical graph",
        "patients": len(patients),
        "record_counts": counts,
        "total_records": sum(counts.values()),
        "trace_sha256": digest.hexdigest(),
        "trace_path": str(out_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", type=Path, required=True,
        help="destination JSONL mutation trace",
    )
    parser.add_argument(
        "--limit-patients", type=int, default=None,
        help="load only the first N patients (Phase 1 smoke test uses 100)",
    )
    parser.add_argument(
        "--database-url", default=os.environ.get("IDPIP_DATABASE_URL"),
        help="IDPIP TimescaleDB URL (default: $IDPIP_DATABASE_URL)",
    )
    args = parser.parse_args()

    try:
        conn = connect(args.database_url)
    except SourceUnavailable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        provenance = emit(args.out, conn, args.limit_patients)
    except SourceUnavailable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()

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
