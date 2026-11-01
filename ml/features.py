"""Node features for the embedding models: one definition, shared by everything.

The trainers, the offline evaluation and (once adopted) the Rust feature builder must give
a node exactly the same vector, or a model trained on one sees different inputs at serve
time. So there is a single definition, `ml/feature_spec.json`, and this module is the
reference reading of it. `ml/build_feature_spec.py` derives the spec from the data; the spec
is committed and frozen, so a retrain does not silently change the contract.

What a node's vector is made of: its type one-hot, then, for each block of the spec that
applies to the node's type, that block's values; blocks for other types are zeros. A block is
a scaled number, a one-hot over a fixed value list (plus an "other" slot for anything unseen),
or the ICD-9 chapter of a diagnosis code.

What it deliberately leaves out, and why (`forbidden_fields` in the spec, enforced by
`check_spec` and by a test):

* `readmitted` is the label the embeddings are evaluated against.
* `age`, `gender`, `race` and `weight` on a patient are stored as *encrypted fields* by the
  loader. Putting them into an embedding, which is stored in the clear next to the node,
  would undo that protection, so they are not inputs.

Everything used is therefore a non-sensitive field already stored unencrypted on the node.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

SPEC_PATH = Path(__file__).resolve().parent / "feature_spec.json"

# Must stay in step with ml/train_graphsage.py::NODE_TYPES and resolver.rs::NODE_TYPES.
NODE_TYPES = ("Patient", "Condition", "Medication", "Provider", "LabResult", "Encounter")


def load_spec(path: Path = SPEC_PATH) -> dict:
    spec = json.loads(path.read_text(encoding="utf-8"))
    check_spec(spec)
    return spec


def spec_sha256(path: Path = SPEC_PATH) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_spec(spec: dict) -> None:
    """Refuse a spec that uses a field it must not."""
    if tuple(spec["types"]) != NODE_TYPES:
        raise ValueError("the spec's node types do not match NODE_TYPES")
    forbidden = set(spec["forbidden_fields"])
    used = {b["field"] for b in spec["blocks"]}
    bad = sorted(used & forbidden)
    if bad:
        raise ValueError(f"the feature spec uses forbidden field(s): {bad}")


def block_width(block: dict) -> int:
    kind = block["kind"]
    if kind == "numeric":
        return 1
    if kind == "onehot":
        return len(block["values"]) + 1  # + "other"
    if kind == "icd9_chapter":
        return len(block["chapters"]) + 3  # + "V", "E", "other"
    raise ValueError(f"unknown block kind {kind!r}")


def dimension(spec: dict) -> int:
    return len(spec["types"]) + sum(block_width(b) for b in spec["blocks"])


def block_offsets(spec: dict) -> list[int]:
    off = len(spec["types"])
    out = []
    for b in spec["blocks"]:
        out.append(off)
        off += block_width(b)
    return out


def icd9_chapter_index(code: str, chapters: list[list]) -> int:
    """Index of a diagnosis code's chapter: the numbered chapters, then V, E, other."""
    code = code.strip().upper()
    n = len(chapters)
    if code.startswith("V"):
        return n
    if code.startswith("E"):
        return n + 1
    try:
        whole = int(float(code))
    except ValueError:
        return n + 2
    for i, (_name, lo, hi) in enumerate(chapters):
        if lo <= whole <= hi:
            return i
    return n + 2


def featurize(spec: dict, node_type: str, props: dict | None) -> list[float]:
    """The feature vector of one node. `props` are its properties as of the time asked."""
    props = props or {}
    types = spec["types"]
    row = [0.0] * dimension(spec)
    if node_type in types:
        row[types.index(node_type)] = 1.0
    offsets = block_offsets(spec)
    for block, off in zip(spec["blocks"], offsets):
        if block["for"] != node_type:
            continue
        value = props.get(block["field"])
        kind = block["kind"]
        if kind == "numeric":
            try:
                x = float(value)
            except (TypeError, ValueError):
                x = 0.0
            row[off] = min(max(x / block["scale"], 0.0), 1.0)
        elif kind == "onehot":
            values = block["values"]
            v = None if value is None else str(value)
            row[off + (values.index(v) if v in values else len(values))] = 1.0
        elif kind == "icd9_chapter":
            if value is not None:
                row[off + icd9_chapter_index(str(value), block["chapters"])] = 1.0
    return row
