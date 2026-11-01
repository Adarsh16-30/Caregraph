#!/usr/bin/env python3
"""Derive ml/feature_spec.json from the trace's own value vocabularies.

Run once, review the diff, commit the result. It is not run by the trainers: the spec is a
frozen contract, and a retrain on new data must not move it. Value lists come from the
frequency of each field in the trace (the most common values, in descending order, so the
file is deterministic), never from the readmission label.

    python ml/build_feature_spec.py --trace benchmarks/traces/diabetes130_full.jsonl
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from features import NODE_TYPES, SPEC_PATH, check_spec, dimension  # noqa: E402

# ICD-9-CM chapters by the whole-number part of the code.
ICD9_CHAPTERS = [
    ["infectious", 1, 139],
    ["neoplasms", 140, 239],
    ["endocrine_metabolic", 240, 279],
    ["blood", 280, 289],
    ["mental", 290, 319],
    ["nervous_sense", 320, 389],
    ["circulatory", 390, 459],
    ["respiratory", 460, 519],
    ["digestive", 520, 579],
    ["genitourinary", 580, 629],
    ["pregnancy", 630, 679],
    ["skin", 680, 709],
    ["musculoskeletal", 710, 739],
    ["congenital", 740, 759],
    ["perinatal", 760, 779],
    ["symptoms", 780, 799],
    ["injury_poisoning", 800, 999],
]

# (node type, field, how many of the most common values get their own slot; None = all)
ONEHOTS = [
    ("Encounter", "admission_type_id", None),
    ("Encounter", "admission_source_id", 8),
    ("Encounter", "discharge_disposition_id", 10),
    ("Medication", "drug", None),
    ("Provider", "specialty", 16),
    ("LabResult", "assay", None),
    ("LabResult", "result", None),
]

# (field, scale): a value is divided by the scale and clipped to [0, 1]. Scales are the
# largest value in the data, rounded up, so nothing is clipped on this dataset.
NUMERICS = [
    ("time_in_hospital", 14),
    ("num_lab_procedures", 132),
    ("num_procedures", 6),
    ("num_medications", 81),
    ("number_diagnoses", 16),
]

FORBIDDEN = ["readmitted", "age", "gender", "race", "weight", "encrypted_fields"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--trace", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=SPEC_PATH)
    args = ap.parse_args()

    counts: dict[tuple[str, str], collections.Counter] = collections.defaultdict(collections.Counter)
    maxima: dict[str, float] = collections.defaultdict(float)
    with args.trace.open(encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            if r["op"] != "upsert_node":
                continue
            for k, v in r.get("properties", {}).items():
                counts[(r["node_type"], k)][str(v)] += 1
                if r["node_type"] == "Encounter":
                    try:
                        maxima[k] = max(maxima[k], float(v))
                    except ValueError:
                        pass

    for field, scale in NUMERICS:
        if maxima[field] > scale:
            print(f"error: {field} reaches {maxima[field]} but its scale is {scale}", file=sys.stderr)
            return 1

    blocks: list[dict] = []
    for field, scale in NUMERICS:
        blocks.append({"for": "Encounter", "kind": "numeric", "field": field, "scale": scale})
    for node_type, field, keep in ONEHOTS:
        ranked = [v for v, _ in sorted(counts[(node_type, field)].items(), key=lambda kv: (-kv[1], kv[0]))]
        blocks.append(
            {
                "for": node_type,
                "kind": "onehot",
                "field": field,
                "values": ranked if keep is None else ranked[:keep],
            }
        )
    blocks.append({"for": "Condition", "kind": "icd9_chapter", "field": "icd9_code", "chapters": ICD9_CHAPTERS})

    spec = {
        "version": 2,
        "description": (
            "Node features: the type one-hot, then per-type blocks. No label (readmitted) and no "
            "field the loader stores encrypted (age, gender, race, weight) is an input."
        ),
        "types": list(NODE_TYPES),
        "forbidden_fields": FORBIDDEN,
        "blocks": blocks,
    }
    check_spec(spec)
    spec["dimension"] = dimension(spec)
    args.out.write_text(json.dumps(spec, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {args.out}: {spec['dimension']} dimensions, {len(blocks)} blocks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
