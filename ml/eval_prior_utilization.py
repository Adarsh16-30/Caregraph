#!/usr/bin/env python3
"""Does the loader's missing prior-utilization data explain the embedding quality gap?

`data/diabetes130_loader.py` reads `number_inpatient`, `number_outpatient` and
`number_emergency` from the raw CSV into nothing: `emit()`'s `encounter_props` dict lists
nine fields to carry over, and these three are not among them, so they never reach the
trace. The literature on this exact dataset (see `docs/embedding_quality.md`'s "Attempt 2")
repeatedly names them as its strongest predictors. This script checks, without touching the
loader, the committed trace, or any deployed model, whether adding them back closes
`docs/embedding_quality.md`'s gap.

It maps each raw CSV row to the CareGraph encounter node id it already has in the existing
trace by replaying `data/diabetes130_loader.py::read_encounters`'s exact, deterministic
row order (`rows.sort(key=lambda r: int(r["encounter_id"]))`) and `Namespace`'s exact
assignment (dense, from base 600_000, in that order) — reproducing logic that already lives
there, not inventing new logic — and checks the replay against a field that *did* make it
into the trace (`number_diagnoses`) before trusting it.

Usage:
    python ml/eval_prior_utilization.py \\
        --trace benchmarks/traces/diabetes130_full.jsonl \\
        --csv data/raw/diabetic_data.csv --model diabetes130_graphsage --samples 20000
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval_readmission as base  # noqa: E402


def map_rows_to_encounter_nodes(csv_path: Path) -> list[dict[str, str]]:
    """Raw CSV rows, in the order `data/diabetes130_loader.py::read_encounters` sorts them,
    so row `i`'s encounter is node `600_000 + i` in a trace built with no `--limit-patients`.
    """
    with csv_path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    rows.sort(key=lambda r: int(r["encounter_id"]))
    return rows


def select_l2(fit_x, fit_y, val_x, val_y, grid: tuple[float, ...]) -> tuple[float, float]:
    """The l2 that scores best on a held-out validation slice, and that score. Chosen on
    data the final reported test number never sees — see the module doc for why: a sweep
    that picks l2 by looking at the test set is not a number to trust."""
    best_l2, best_auc = grid[0], -1.0
    for l2 in grid:
        s = base.fit_logistic(fit_x, fit_y, l2=l2)(val_x)
        a = base.auc(val_y, s)
        if a > best_auc:
            best_auc, best_l2 = a, l2
    return best_l2, best_auc


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--trace", type=Path, required=True)
    ap.add_argument("--csv", type=Path, required=True)
    ap.add_argument("--model", default="diabetes130_graphsage")
    ap.add_argument("--samples", type=int, default=20_000)
    ap.add_argument("--ring2", type=int, default=16)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--bootstrap", type=int, default=200)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    t0 = time.time()
    print(f"reading {args.trace} ...", flush=True)
    g = base.read_trace(args.trace)
    encounters = sorted(g.encounter_us, key=g.encounter_us.get)
    print(f"  {len(g.type):,} nodes, {len(encounters):,} encounters ({time.time() - t0:.0f}s)")

    rows = map_rows_to_encounter_nodes(args.csv)
    prior_util = {
        600_000 + i: np.array(
            [float(row["number_inpatient"]), float(row["number_outpatient"]), float(row["number_emergency"])]
        )
        for i, row in enumerate(rows)
    }
    # The replay above must put the same encounter_id's row at the same node the loader
    # did: check it against number_diagnoses, which the loader *does* carry into the trace.
    bad = 0
    for i, enc in enumerate(encounters):
        if i >= 500:
            break
        want = g.encounter_props[enc].get("number_diagnoses")
        got = prior_util.get(enc)
        if want is not None and got is None:
            bad += 1
    if bad:
        raise SystemExit(f"{bad}/500 sampled encounters have no mapped CSV row; refusing to trust the mapping")
    row_by_node = {600_000 + i: row for i, row in enumerate(rows)}
    mismatches = sum(
        1
        for enc in encounters[:500]
        if str(g.encounter_props[enc].get("number_diagnoses")) != str(row_by_node[enc]["number_diagnoses"])
    )
    if mismatches:
        raise SystemExit(f"{mismatches}/500 sampled encounters: number_diagnoses disagrees with the mapped CSV row")
    print("  verified the raw-CSV-row mapping against number_diagnoses on 500 encounters: ok")

    rng = random.Random(args.seed)
    n = min(args.samples, len(encounters))
    picked = sorted(rng.sample(range(len(encounters)), n))
    sample_enc = [encounters[i] for i in picked]
    categories = {
        f: sorted({str(p.get(f, "")) for p in g.encounter_props.values()}) for f in base.CLINICAL_CATEGORICAL
    }

    model, _ = base.load_model(Path(__file__).resolve().parent / "deployed" / args.model)
    samples = [(e, g.encounter_patient[e], g.encounter_us[e] + base.BLOCK_US) for e in sample_enc]
    feats = base.NodeFeatures(g, None)  # the deployed models: node-type one-hot only

    print(f"embedding {n:,} encounters with {args.model} ...", flush=True)
    enc_emb, pat_emb = [], []
    for i in range(0, n, 1_000):
        a, b = base.embed_batch(model, g, samples[i : i + 1_000], args.ring2, feats)
        enc_emb.append(a)
        pat_emb.append(b)
    emb = np.hstack([np.vstack(enc_emb), np.vstack(pat_emb)])

    y = np.array([1 if g.encounter_props[e].get("readmitted") == "<30" else 0 for e in sample_enc])
    clin = np.vstack([base.clinical(g.encounter_props[e], categories) for e in sample_enc])
    struct = np.vstack([base.structural(g, p, t) for _, p, t in samples])
    # /20 then clip to 1: the largest observed values are 42/76/21 (out/emergency/inpatient);
    # same scale-and-clip convention as the numeric blocks in ml/feature_spec.json.
    util = np.vstack([np.clip(prior_util[e] / 20.0, 0, 1) for e in sample_enc])
    clin2 = np.hstack([clin, util])

    cut = int(n * 0.7)
    tr, te = slice(0, cut), slice(cut, n)
    print(f"train {cut:,} / test {n - cut:,}; readmitted<30: train {y[tr].mean():.3%}, test {y[te].mean():.3%}")

    print("\nPart 1: does adding the three fields help at the harness's fixed l2=1e-3,")
    print("the same setting every other comparison on this page uses (95% bootstrap interval)?")
    sets = {
        "clinical": clin,
        "clinical2 (+ prior inpatient/outpatient/emergency)": clin2,
        "clinical + structural + embedding": np.hstack([clin, struct, emb]),
        "clinical2 + structural + embedding": np.hstack([clin2, struct, emb]),
    }
    m = n - cut
    boot = np.array([rng.choices(range(m), k=m) for _ in range(args.bootstrap)])
    y_te = y[te]
    results = {}
    for name, x in sets.items():
        s = base.fit_logistic(x[tr], y[tr])(x[te])
        a = base.auc(y_te, s)
        boot_aucs = [base.auc(y_te[idx], s[idx]) for idx in boot]
        lo, hi = np.percentile(boot_aucs, [2.5, 97.5])
        results[name] = {"auc": float(a), "auc_ci95": [float(lo), float(hi)]}
        print(f"  {name:<52} AUC {a:.3f}  [{lo:.3f}, {hi:.3f}]")

    def paired(a_name: str, b_name: str) -> dict:
        xa, xb = sets[a_name], sets[b_name]
        sa = base.fit_logistic(xa[tr], y[tr])(xa[te])
        sb = base.fit_logistic(xb[tr], y[tr])(xb[te])
        d = base.auc(y_te, sa) - base.auc(y_te, sb)
        ds = [base.auc(y_te[idx], sa[idx]) - base.auc(y_te[idx], sb[idx]) for idx in boot]
        lo, hi = np.percentile(ds, [2.5, 97.5])
        print(f"  {a_name}  vs  {b_name}: {d:+.4f}  [{lo:+.4f}, {hi:+.4f}]")
        return {"delta_auc": float(d), "ci95": [float(lo), float(hi)]}

    print("\n  paired differences:")
    diffs = {
        "clinical2_vs_clinical": paired(
            "clinical2 (+ prior inpatient/outpatient/emergency)", "clinical"
        ),
        "clinical2+structural+embedding_vs_clinical+structural+embedding": paired(
            "clinical2 + structural + embedding", "clinical + structural + embedding"
        ),
    }

    print("\nPart 2: a regularization strength that was never cross-validated is a real confound")
    print("on a feature set this size. l2 selected on a held-out validation slice the final")
    print("test number never sees (train 60% / val 10% / test 30%, time-ordered, same split origin):")
    cut_tr, cut_val = int(n * 0.6), int(n * 0.7)
    tr2, val, te2 = slice(0, cut_tr), slice(cut_tr, cut_val), slice(cut_val, n)
    grid = (1e-4, 3e-4, 1e-3, 3e-3, 1e-2, 3e-2, 1e-1, 3e-1, 1.0)
    selected = {}
    for name, X in [("clinical+structural+embedding", sets["clinical + structural + embedding"]),
                     ("clinical2+structural+embedding", sets["clinical2 + structural + embedding"])]:
        l2, val_auc = select_l2(X[tr2], y[tr2], X[val], y[val], grid)
        x_fit = np.vstack([X[tr2], X[val]])
        y_fit = np.concatenate([y[tr2], y[val]])
        s = base.fit_logistic(x_fit, y_fit, l2=l2)(X[te2])
        test_auc = base.auc(y[te2], s)
        selected[name] = {"l2": l2, "val_auc": float(val_auc), "test_auc": float(test_auc)}
        print(f"  {name:<34} selected l2={l2:<8} val AUC {val_auc:.4f}  TEST AUC {test_auc:.4f}")

    out = {
        "benchmark": "prior_utilization_probe",
        "description": (
            "Does adding number_inpatient/outpatient/emergency (present in the raw CSV, never "
            "read by data/diabetes130_loader.py) close the gap in docs/embedding_quality.md? "
            "Read-only: does not modify the loader, the trace, or any deployed model."
        ),
        "model": args.model,
        "samples": n,
        "seed": args.seed,
        "fixed_l2_1e-3": results,
        "fixed_l2_1e-3_paired_differences": diffs,
        "validation_selected_l2": selected,
    }
    if args.out:
        args.out.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
