#!/usr/bin/env python3
"""Do the deployed embeddings carry clinical signal? A causal 30-day readmission test.

The embeddings CareGraph maintains are only useful if they say something about
patients. Nothing in the repository measured that: training is unsupervised link
prediction, and the only quality number was the training loss. This script asks
the question directly, on the real Diabetes 130 trace, with the deployed model.

Task. For an encounter, predict whether the patient is readmitted within 30 days
(`readmitted == "<30"` on the Encounter node), using only what the graph knew at
the end of that encounter.

Why it is causal. Each prediction uses the graph *as of the end of the encounter's
own block of edges*. A patient's later encounters, which are what the label is
about, are not in that graph. Using the final graph instead would leak the answer
(a readmission is, structurally, another encounter node on the same patient).
Embeddings are computed by the same deployed `model.pt` through the same
`embedding_server.load_model`, over each encounter's own as-of 2-hop neighbourhood,
mirroring `resolver.rs` (live neighbours newest-first, capped).

Split. Encounters are ordered by time; the first 70% train, the last 30% test.
Metrics are on the test set only, with a bootstrap interval.

Feature sets compared:

    structural          counts of the patient's prior encounters / conditions / drugs
    clinical            the encounter's own recorded fields (stay length, #drugs, ...)
    embedding           the deployed embeddings (the encounter's and its patient's)
    clinical + embedding
    clinical + structural
    clinical + structural + embedding

It also reports how much of the embeddings' variance is explained by nothing but
the *type composition* of the patient's neighbourhood. The deployed models take a
one-hot node type as their only input feature, so that is expected to be most of
it; this measures it instead of assuming it.

Limits worth stating: the loader keeps only the encounter fields listed in
`data/diabetes130_loader.py` (not the source's prior-visit counts, which are the
strongest known predictors), the dates are derived from encounter order, and the
absolute AUCs are modest for every method. The point is the comparison.

Usage:
    python ml/eval_readmission.py --trace benchmarks/traces/diabetes130_full.jsonl \\
        --model diabetes130_graphsage --samples 20000
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from bisect import bisect_right
from pathlib import Path

import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from embedding_server import load_model  # noqa: E402

# Must match src/embedding/resolver.rs::NODE_TYPES and ml/train_graphsage.py.
NODE_TYPES = ["Patient", "Condition", "Medication", "Provider", "LabResult", "Encounter"]
TYPE_INDEX = {t: i for i, t in enumerate(NODE_TYPES)}

# The loader gives an encounter's edges timestamps encounter_us + 1, +2, ...; rows
# are at least seconds apart, so this cutoff includes this encounter's whole block
# and excludes every other encounter's.
BLOCK_US = 1_000

CLINICAL_NUMERIC = (
    "time_in_hospital",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_diagnoses",
)
CLINICAL_CATEGORICAL = ("admission_type_id", "discharge_disposition_id", "admission_source_id")


# --------------------------------------------------------------------------- trace


class Graph:
    def __init__(self) -> None:
        self.type: dict[int, str] = {}
        self.encounter_props: dict[int, dict] = {}
        self.encounter_us: dict[int, int] = {}
        # node -> [(ts, neighbour, +1 added / -1 removed)], both directions, sorted by ts
        self.adj: dict[int, list[tuple[int, int, int]]] = {}
        self.encounter_patient: dict[int, int] = {}

    def add_edge(self, src: int, dst: int, ts: int, sign: int) -> None:
        self.adj.setdefault(src, []).append((ts, dst, sign))
        self.adj.setdefault(dst, []).append((ts, src, sign))

    def finish(self) -> None:
        for lst in self.adj.values():
            lst.sort(key=lambda r: r[0])

    def live_neighbours(self, node: int, t: int, cap: int) -> list[int]:
        """Live neighbours as of `t`, newest first, at most `cap` (as resolver.rs)."""
        lst = self.adj.get(node)
        if not lst:
            return []
        hi = bisect_right(lst, t, key=lambda r: r[0])
        seen: set[int] = set()
        out: list[int] = []
        for j in range(hi - 1, -1, -1):
            _, nbr, sign = lst[j]
            if nbr in seen:
                continue
            seen.add(nbr)
            if sign > 0:
                out.append(nbr)
                if len(out) >= cap:
                    break
        return out


def read_trace(path: Path) -> Graph:
    g = Graph()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            op = r["op"]
            if op == "upsert_node":
                nid = r["node_id"]
                g.type[nid] = r["node_type"]
                if r["node_type"] == "Encounter":
                    g.encounter_props[nid] = r["properties"]
                    g.encounter_us[nid] = r["timestamp_us"]
            elif op == "add_edge":
                g.add_edge(r["src"], r["dst"], r["timestamp_us"], +1)
                if r["edge_type"] == 6:  # HAS_ENCOUNTER: patient -> encounter
                    g.encounter_patient[r["dst"]] = r["src"]
            elif op == "remove_edge":
                g.add_edge(r["src"], r["dst"], r["timestamp_us"], -1)
    g.finish()
    return g


# ------------------------------------------------------------------------ features


def composition(g: Graph, nbrs: list[int]) -> np.ndarray:
    """Fraction of a neighbourhood in each node type, plus log(1 + size)."""
    counts = np.zeros(len(NODE_TYPES))
    for n in nbrs:
        counts[TYPE_INDEX[g.type[n]]] += 1
    total = counts.sum()
    frac = counts / total if total else counts
    return np.concatenate([frac, [np.log1p(total)]])


def structural(g: Graph, patient: int, t: int) -> np.ndarray:
    nbrs = g.live_neighbours(patient, t, 10_000)
    c = {k: 0 for k in NODE_TYPES}
    for n in nbrs:
        c[g.type[n]] += 1
    prior = max(c["Encounter"] - 1, 0)  # this encounter is already in the graph
    return np.array([prior, c["Condition"], c["Medication"], c["Provider"], c["LabResult"]], float)


def clinical(props: dict, categories: dict[str, list[str]]) -> np.ndarray:
    row = [float(props.get(f, 0) or 0) for f in CLINICAL_NUMERIC]
    for f in CLINICAL_CATEGORICAL:
        v = str(props.get(f, ""))
        row.extend(1.0 if v == c else 0.0 for c in categories[f])
    return np.array(row, float)


def embed_batch(model, g: Graph, samples: list[tuple[int, int, int]], ring2: int):
    """Embeddings of (encounter, patient) for each sample, each over its own as-of graph."""
    xs, srcs, dsts, rows = [], [], [], []
    base = 0
    for enc, pat, t in samples:
        local: dict[int, int] = {}
        edges: set[tuple[int, int]] = set()

        def idx(n: int) -> int:
            if n not in local:
                local[n] = len(local)
            return local[n]

        idx(enc)
        idx(pat)
        edges.add((local[enc], local[pat]))
        p_nbrs = g.live_neighbours(pat, t, 512)
        for n in p_nbrs:
            edges.add((idx(pat), idx(n)))
        for n in p_nbrs:
            # `pat` is always among n's neighbours; keep it even if the cap would drop it.
            edges.add((idx(n), idx(pat)))
            for m in g.live_neighbours(n, t, ring2):
                edges.add((idx(n), idx(m)))
        order = sorted(local, key=local.get)
        xs.extend(TYPE_INDEX[g.type[n]] for n in order)
        for a, b in edges:
            if a != b:
                srcs.extend((a + base, b + base))
                dsts.extend((b + base, a + base))
        rows.append((base + local[enc], base + local[pat]))
        base += len(local)
    x = torch.zeros(base, len(NODE_TYPES))
    x[torch.arange(base), torch.tensor(xs)] = 1.0
    ei = torch.tensor([srcs, dsts], dtype=torch.long)
    with torch.no_grad():
        z = model(x, ei)
    r = torch.tensor(rows)
    return z[r[:, 0]].numpy(), z[r[:, 1]].numpy()


# ------------------------------------------------------------------------- metrics


def auc(y: np.ndarray, s: np.ndarray) -> float:
    _, inv, counts = np.unique(s, return_inverse=True, return_counts=True)
    avg_rank = np.cumsum(counts) - (counts - 1) / 2
    ranks = avg_rank[inv]
    n1 = float(y.sum())
    n0 = len(y) - n1
    return float((ranks[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def average_precision(y: np.ndarray, s: np.ndarray) -> float:
    order = np.argsort(-s, kind="mergesort")
    hits = y[order]
    precision = np.cumsum(hits) / np.arange(1, len(hits) + 1)
    return float(precision[hits == 1].sum() / max(hits.sum(), 1))


def fit_logistic(x_train: np.ndarray, y_train: np.ndarray, l2: float = 1e-3):
    mu, sd = x_train.mean(0), x_train.std(0) + 1e-6
    x = torch.tensor((x_train - mu) / sd, dtype=torch.float32)
    y = torch.tensor(y_train, dtype=torch.float32)
    w = torch.zeros(x.shape[1], requires_grad=True)
    b = torch.zeros(1, requires_grad=True)
    opt = torch.optim.LBFGS([w, b], lr=1.0, max_iter=300, line_search_fn="strong_wolfe")

    def closure():
        opt.zero_grad()
        loss = torch.nn.functional.binary_cross_entropy_with_logits(x @ w + b, y)
        loss = loss + l2 * (w**2).sum()
        loss.backward()
        return loss

    opt.step(closure)

    def score(x_new: np.ndarray) -> np.ndarray:
        xn = torch.tensor((x_new - mu) / sd, dtype=torch.float32)
        return (xn @ w + b).detach().numpy()

    return score


def r_squared(train_x, train_y, test_x, test_y) -> float:
    """Variance of `test_y` explained by a least-squares fit from `train_x`."""
    a = np.hstack([train_x, np.ones((len(train_x), 1))])
    coef, *_ = np.linalg.lstsq(a, train_y, rcond=None)
    pred = np.hstack([test_x, np.ones((len(test_x), 1))]) @ coef
    ss_res = ((test_y - pred) ** 2).sum()
    ss_tot = ((test_y - test_y.mean(0)) ** 2).sum()
    return float(1 - ss_res / ss_tot)


def quadratic(x: np.ndarray) -> np.ndarray:
    cols = [x]
    for i in range(x.shape[1]):
        for j in range(i, x.shape[1]):
            cols.append((x[:, i] * x[:, j])[:, None])
    return np.hstack(cols)


# ----------------------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--trace", type=Path, required=True)
    ap.add_argument("--model", default="diabetes130_graphsage")
    ap.add_argument("--samples", type=int, default=20_000)
    ap.add_argument("--ring2", type=int, default=16, help="neighbours kept per second-ring node")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--bootstrap", type=int, default=200)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    t0 = time.time()
    print(f"reading {args.trace} ...", flush=True)
    g = read_trace(args.trace)
    encounters = sorted(g.encounter_us, key=g.encounter_us.get)
    print(f"  {len(g.type):,} nodes, {len(encounters):,} encounters ({time.time() - t0:.0f}s)")

    # Sampling and resampling only: the stdlib RNG picks which encounters to
    # evaluate and bootstraps the test set. No vector in this script is random.
    rng = random.Random(args.seed)
    n = min(args.samples, len(encounters))
    picked = sorted(rng.sample(range(len(encounters)), n))
    sample_enc = [encounters[i] for i in picked]  # already in time order
    categories = {
        f: sorted({str(p.get(f, "")) for p in g.encounter_props.values()}) for f in CLINICAL_CATEGORICAL
    }

    model, _ = load_model(Path(__file__).resolve().parent / "deployed" / args.model)
    samples = [(e, g.encounter_patient[e], g.encounter_us[e] + BLOCK_US) for e in sample_enc]

    print(f"embedding {n:,} encounters with {args.model} (as-of, causal) ...", flush=True)
    enc_emb, pat_emb = [], []
    for i in range(0, n, 1_000):
        a, b = embed_batch(model, g, samples[i : i + 1_000], args.ring2)
        enc_emb.append(a)
        pat_emb.append(b)
        if (i // 1_000) % 5 == 0:
            print(f"  {i + len(a):,}/{n:,}", flush=True)
    enc_emb, pat_emb = np.vstack(enc_emb), np.vstack(pat_emb)

    y = np.array([1 if g.encounter_props[e].get("readmitted") == "<30" else 0 for e in sample_enc])
    clin = np.vstack([clinical(g.encounter_props[e], categories) for e in sample_enc])
    struct = np.vstack([structural(g, p, t) for _, p, t in samples])
    comp = np.vstack([composition(g, g.live_neighbours(p, t, 512)) for _, p, t in samples])

    cut = int(n * 0.7)
    tr, te = slice(0, cut), slice(cut, n)
    print(f"train {cut:,} / test {n - cut:,}; readmitted<30: train {y[tr].mean():.3%}, test {y[te].mean():.3%}")

    emb = np.hstack([enc_emb, pat_emb])
    sets = {
        "structural": struct,
        "clinical": clin,
        "embedding (encounter)": enc_emb,
        "embedding (patient)": pat_emb,
        "embedding (both)": emb,
        "clinical + structural": np.hstack([clin, struct]),
        "clinical + embedding": np.hstack([clin, emb]),
        "clinical + structural + embedding": np.hstack([clin, struct, emb]),
    }

    m = n - cut
    boot = np.array([rng.choices(range(m), k=m) for _ in range(args.bootstrap)])
    y_te = y[te]
    scores, results = {}, {}
    for name, x in sets.items():
        s = fit_logistic(x[tr], y[tr])(x[te])
        scores[name] = s
        point = auc(y_te, s)
        lo, hi = np.percentile([auc(y_te[b], s[b]) for b in boot], [2.5, 97.5])
        results[name] = {
            "auc": point,
            "auc_ci95": [float(lo), float(hi)],
            "average_precision": average_precision(y_te, s),
            "dims": int(x.shape[1]),
        }

    def delta(a: str, b: str) -> dict:
        d = [auc(y_te[i], scores[a][i]) - auc(y_te[i], scores[b][i]) for i in boot]
        lo, hi = np.percentile(d, [2.5, 97.5])
        return {"delta_auc": results[a]["auc"] - results[b]["auc"], "ci95": [float(lo), float(hi)]}

    paired = {
        "clinical + embedding  vs  clinical": delta("clinical + embedding", "clinical"),
        "clinical + structural + embedding  vs  clinical + structural": delta(
            "clinical + structural + embedding", "clinical + structural"
        ),
        "embedding (both)  vs  structural": delta("embedding (both)", "structural"),
    }

    # What do the embeddings encode? How much is nothing but neighbourhood type mix?
    explain = {
        "patient_embedding_from_type_mix_linear": r_squared(comp[tr], pat_emb[tr], comp[te], pat_emb[te]),
        "patient_embedding_from_type_mix_quadratic": r_squared(
            quadratic(comp[tr]), pat_emb[tr], quadratic(comp[te]), pat_emb[te]
        ),
        "encounter_embedding_from_type_mix_quadratic": r_squared(
            quadratic(comp[tr]), enc_emb[tr], quadratic(comp[te]), enc_emb[te]
        ),
    }

    print("\n30-day readmission, test set (AUC, 95% bootstrap interval; AP):")
    width = max(len(k) for k in results)
    print(f"  {'majority class':<{width}}  AUC 0.500  (AP {y_te.mean():.3f} = the positive rate)")
    for name, r in results.items():
        lo, hi = r["auc_ci95"]
        print(f"  {name:<{width}}  AUC {r['auc']:.3f} [{lo:.3f}, {hi:.3f}]  AP {r['average_precision']:.3f}")
    print("\npaired differences in AUC:")
    for name, d in paired.items():
        print(f"  {name}: {d['delta_auc']:+.4f}  [{d['ci95'][0]:+.4f}, {d['ci95'][1]:+.4f}]")
    print("\nvariance of the embeddings explained by neighbourhood type mix alone (test R^2):")
    for name, v in explain.items():
        print(f"  {name}: {v:.3f}")

    out = {
        "benchmark": "readmission_30d_causal",
        "model": args.model,
        "trace": str(args.trace),
        "samples": n,
        "seed": args.seed,
        "ring2_cap": args.ring2,
        "train": cut,
        "test": n - cut,
        "positive_rate_test": float(y_te.mean()),
        "results": results,
        "paired_differences": paired,
        "embedding_variance_explained_by_type_mix": explain,
        "elapsed_seconds": time.time() - t0,
        "torch": torch.__version__,
    }
    path = args.out or Path("benchmarks/results") / f"readmission_eval_{int(time.time())}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2))
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
