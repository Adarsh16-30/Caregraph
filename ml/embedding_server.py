#!/usr/bin/env python3
"""Persistent GraphSAGE forward-pass worker, called from Rust over stdio.

Rule 3 requires embeddings to come from a real forward pass through a model
implemented in DGL or PyTorch Geometric. PyO3 was tried first as the calling
mechanism the PRD names — verified against this machine's Python 3.14 install
and rejected for a concrete, reproducible reason, not a guess: PyO3 0.24 has no
support for CPython 3.14 (its own build script refuses to compile), and its
documented forward-compatibility escape hatch (PYO3_USE_ABI3_FORWARD_COMPATIBILITY,
stable-ABI mode) builds but fails at runtime — `_ctypes` and, by extension,
torch's own C extensions are not abi3-limited, so CPython's own ABI-mismatch
guard refuses to load them under the compatibility shim
("Module use of python314.dll conflicts with this version of Python").
That is a version-support gap in PyO3 for this Python release, not a
configuration problem to work around.

This process is spawned once by CareGraph and kept alive: one line of JSON in
on stdin is one forward-pass request, one line of JSON out on stdout is the
response. A fresh process per mutation would pay PyTorch's import cost (whole
seconds) on every request, which alone would blow the Phase 4 p95 target.

Protocol (newline-delimited JSON, no batching across lines):
    request  {"node_features": [[...]], "edge_index": [[src...], [dst...]],
              "target_indices": [...],
              "attribution": {                       optional, Phase 9 Feature 1
                "targets": [...], "edge_index_before": [[...],[...]],
                "steps": 16, "top_k": 20
              }}
    response {"embeddings": [[...]],
              "attribution": [ { ... } ]}             on success
              {"error": "..."}                         on failure

`target_indices` selects which rows of the forward pass to return — the caller
asks for embeddings of specific nodes without the worker needing to know
CareGraph's node-id scheme, which lives entirely on the Rust side.

# Attribution (Phase 9, Feature 1) — integrated gradients over an edge mask

`"attribution"`, when present, asks for a per-edge explanation of *why* each
of `attribution.targets`' embedding changed between `edge_index_before` (the
pre-mutation graph) and `edge_index` (the post-mutation graph this request
already computes embeddings for) — see `src/embedding/meta.rs::Attribution`'s
own doc for the record this feeds and `docs/patent_hooks.md` Claim 6 for the
completeness identity it is verified against.

Mechanism: attribute the scalar `f(m) = <z_v(m), u>`, where `m` is a
continuous `[0,1]` edge mask (`torch_geometric.explain.algorithm.utils.set_masks`,
`apply_sigmoid=False` — a raw mask, not a logit) and `u` is the unit direction
of the target's *real* embedding change (computed from ordinary, unmasked
forward passes on both graphs, never from a masked one). Integrated gradients
along the straight-line path from the all-zero mask to the real graph
(`m=1` everywhere) is run separately on each graph state, and the two results
are combined per undirected edge pair. This is deliberately **not** the
textbook "attributions sum exactly to the output" identity: PyTorch Geometric
pins a `GATConv`'s self-loop mask entries to `1` regardless of `m`
(`MessagePassing.explain_message`), so the all-zero-mask baseline is not
truly an empty graph for an attention model, and `baseline_delta` below
carries that nonzero term explicitly rather than absorbing it into rounding
error. For an associative model (mean aggregation, no self-loop pinning)
`baseline_delta` is exactly `0.0` — measured, not assumed; see
`tests/embedding/attribution_completeness_test.rs`.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import torch
from torch_geometric.explain.algorithm.utils import clear_masks, set_masks
from torch_geometric.nn import GATConv, SAGEConv


class GraphSAGEEncoder(torch.nn.Module):
    """Must match ml/train_graphsage.py exactly — same layers, same aggr."""

    def __init__(self, in_dim: int, hidden_dim: int, out_dim: int) -> None:
        super().__init__()
        self.conv1 = SAGEConv(in_dim, hidden_dim, aggr="mean")
        self.conv2 = SAGEConv(hidden_dim, out_dim, aggr="mean")

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        h = torch.relu(self.conv1(x, edge_index))
        return self.conv2(h, edge_index)


class GATEncoder(torch.nn.Module):
    """Must match ml/train_gat.py's GATEncoder exactly — same layers, same
    heads. Dropout is inert here regardless of the trained value: `.eval()`
    below disables it, which is what makes the forward pass deterministic —
    load-bearing for `tests/embedding`'s incremental-vs-full-recompute
    exactness comparison, the same reason GraphSAGE's server-side copy is
    checked into eval mode too.
    """

    def __init__(self, in_dim: int, hidden_dim: int, out_dim: int, heads: int) -> None:
        super().__init__()
        self.conv1 = GATConv(in_dim, hidden_dim, heads=heads, dropout=0.6)
        self.conv2 = GATConv(hidden_dim * heads, out_dim, heads=1, concat=False, dropout=0.6)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        import torch.nn.functional as F

        h = F.elu(self.conv1(x, edge_index))
        return self.conv2(h, edge_index)


# EmbeddingModel::spawn (model_bridge.rs) passes only a model_id; the
# checkpoint itself says which architecture to build, so this worker serves
# whichever model — GraphSAGE (Phase 4) or GAT (Phase 5) — was deployed at
# that path, without the Rust side needing to know or care.
def load_model(model_dir: Path) -> tuple[torch.nn.Module, dict]:
    checkpoint = torch.load(model_dir / "model.pt", map_location="cpu", weights_only=True)
    architecture = checkpoint.get("architecture", "GraphSAGE")

    if architecture == "GAT":
        model: torch.nn.Module = GATEncoder(
            checkpoint["in_dim"], checkpoint["hidden_dim"], checkpoint["out_dim"], checkpoint["heads"]
        )
        aggregation = "attention (softmax over each node's own neighbours)"
        is_associative = False
    elif architecture == "GraphSAGE":
        model = GraphSAGEEncoder(checkpoint["in_dim"], checkpoint["hidden_dim"], checkpoint["out_dim"])
        aggregation = "mean"
        is_associative = True
    else:
        raise ValueError(f"unknown architecture {architecture!r} in {model_dir / 'model.pt'}")

    model.load_state_dict(checkpoint["state_dict"])
    model.eval()

    # Self-report, read from the checkpoint itself rather than trusted from
    # the caller — this is the independent half of the Phase 9 manifest
    # cross-check (model_bridge.rs::EmbeddingModel::spawn hard-errors if this
    # disagrees with ml/deployed/<id>/dataset_manifest.json). Before this,
    # Rust never learned which architecture it actually got; a model_id/
    # model_kind mismatch was a documented, unchecked footgun
    # (tests/fault_injection's own doc comment names it).
    self_report = {
        "architecture": architecture,
        "aggregation": aggregation,
        "is_associative": is_associative,
        "layers": 2,
        "embedding_dim": checkpoint["out_dim"],
    }
    return model, self_report


def _integrated_gradients_for_target(
    model: torch.nn.Module,
    x: torch.Tensor,
    edge_index: torch.Tensor,
    target_idx: int,
    direction: torch.Tensor,
    steps: int,
) -> tuple[float, torch.Tensor]:
    """IG of `f(m) = <model(x, m*edge_index)[target_idx], direction>` over a
    `[0,1]` edge mask, from the all-zero baseline to the real graph (`m=1`),
    via the midpoint rule. Since the path's "input minus baseline" is exactly
    `1` per edge, the per-edge integrated gradient is just the average of
    `df/dm_e` sampled at each midpoint — no separate multiply-by-delta step.

    Returns `(f(0), attribution)`: `f(0)` is the scalar projection at the
    all-zero mask (assumed baseline for the outer completeness identity, but
    only *actually* zero for an associative model — see this module's own
    doc), and `attribution` is one value per column of `edge_index`, in that
    same column order.

    `model` is mutated in place for the duration of this call
    (`set_masks`/`clear_masks`) and is restored before returning, including on
    an exception — this worker serves one request at a time from a single
    `for line in sys.stdin` loop, so there is no concurrent forward pass this
    could race against.
    """
    num_edges = edge_index.size(1)
    mask = torch.zeros(num_edges, requires_grad=True)
    set_masks(model, mask, edge_index, apply_sigmoid=False)
    try:
        with torch.no_grad():
            z0 = model(x, edge_index)
        f0 = float((z0[target_idx] * direction).sum().item())

        grad_sum = torch.zeros(num_edges)
        for step in range(1, steps + 1):
            alpha = (step - 0.5) / steps
            with torch.no_grad():
                mask.fill_(alpha)
            if mask.grad is not None:
                mask.grad.zero_()
            z = model(x, edge_index)
            f = (z[target_idx] * direction).sum()
            f.backward()
            assert mask.grad is not None, "mask did not receive a gradient — masking did not reach model()"
            grad_sum += mask.grad.detach()

        attribution = grad_sum / steps
        return f0, attribution
    finally:
        clear_masks(model)


def _attribute_one_target(
    model: torch.nn.Module,
    x: torch.Tensor,
    edge_index: torch.Tensor,
    edge_index_before: torch.Tensor,
    z_after: torch.Tensor,
    z_before: torch.Tensor,
    target_idx: int,
    steps: int,
    top_k: int,
) -> dict:
    delta = z_after[target_idx] - z_before[target_idx]
    delta_norm = float(delta.norm().item())

    # A target whose embedding did not meaningfully change has no direction to
    # attribute along — reported honestly as "nothing to explain", not run
    # through IG against an arbitrary or degenerate direction vector.
    if delta_norm < 1e-9:
        return {
            "target_index": target_idx,
            "delta_norm": 0.0,
            "baseline_delta": 0.0,
            "completeness_residual": 0.0,
            "edges_total": 0,
            "edges": [],
            "rest_sum": 0.0,
            "rest_count": 0,
        }

    direction = (delta / delta_norm).detach()
    f0_after, attr_after = _integrated_gradients_for_target(
        model, x, edge_index, target_idx, direction, steps
    )
    f0_before, attr_before = _integrated_gradients_for_target(
        model, x, edge_index_before, target_idx, direction, steps
    )
    baseline_delta = f0_after - f0_before

    # Fold each direction's column back onto its undirected pair. The two
    # directed columns `build_model_input` (resolver.rs) emits for one
    # undirected edge feed independent, disjoint messages, so their IG
    # contributions are additive — summing preserves completeness exactly.
    # An edge present in only one graph state contributes only from that
    # state, which is correct: it was never a candidate in the other.
    combined: dict[tuple[int, int], float] = {}
    for col in range(edge_index.size(1)):
        u, v = int(edge_index[0, col]), int(edge_index[1, col])
        key = (min(u, v), max(u, v))
        combined[key] = combined.get(key, 0.0) + float(attr_after[col])
    for col in range(edge_index_before.size(1)):
        u, v = int(edge_index_before[0, col]), int(edge_index_before[1, col])
        key = (min(u, v), max(u, v))
        combined[key] = combined.get(key, 0.0) - float(attr_before[col])

    ranked = sorted(combined.items(), key=lambda kv: -abs(kv[1]))
    top = ranked[:top_k]
    rest = ranked[top_k:]
    sum_attr = sum(v for _, v in combined.items())
    rest_sum = sum(v for _, v in rest)
    completeness_residual = delta_norm - (sum_attr + baseline_delta)

    return {
        "target_index": target_idx,
        "delta_norm": delta_norm,
        "baseline_delta": baseline_delta,
        "completeness_residual": completeness_residual,
        "edges_total": len(combined),
        "edges": [[u, v, value] for (u, v), value in top],
        "rest_sum": rest_sum,
        "rest_count": len(rest),
    }


def serve(model_dir: Path) -> None:
    model, self_report = load_model(model_dir)
    print(json.dumps({"ready": True, **self_report}), flush=True)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            x = torch.tensor(req["node_features"], dtype=torch.float32)
            edge_index = torch.tensor(req["edge_index"], dtype=torch.long)
            targets = req["target_indices"]

            with torch.no_grad():
                z = model(x, edge_index)
            out = z[targets].tolist()
            response: dict = {"embeddings": out}

            attribution_req = req.get("attribution")
            if attribution_req:
                edge_index_before = torch.tensor(
                    attribution_req["edge_index_before"], dtype=torch.long
                )
                steps = int(attribution_req.get("steps", 16))
                top_k = int(attribution_req.get("top_k", 20))

                with torch.no_grad():
                    z_before = model(x, edge_index_before)

                response["attribution"] = [
                    _attribute_one_target(
                        model, x, edge_index, edge_index_before, z, z_before, t, steps, top_k
                    )
                    for t in attribution_req["targets"]
                ]

            print(json.dumps(response), flush=True)
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not swallowed
            print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(json.dumps({"error": "usage: embedding_server.py <model_dir>"}))
        raise SystemExit(1)
    serve(Path(sys.argv[1]))
