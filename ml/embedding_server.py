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
              "target_indices": [...]}
    response {"embeddings": [[...]]}                    on success
              {"error": "..."}                            on failure

`target_indices` selects which rows of the forward pass to return — the caller
asks for embeddings of specific nodes without the worker needing to know
CareGraph's node-id scheme, which lives entirely on the Rust side.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import torch
from torch_geometric.nn import SAGEConv


class GraphSAGEEncoder(torch.nn.Module):
    """Must match ml/train_graphsage.py exactly — same layers, same aggr."""

    def __init__(self, in_dim: int, hidden_dim: int, out_dim: int) -> None:
        super().__init__()
        self.conv1 = SAGEConv(in_dim, hidden_dim, aggr="mean")
        self.conv2 = SAGEConv(hidden_dim, out_dim, aggr="mean")

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        h = torch.relu(self.conv1(x, edge_index))
        return self.conv2(h, edge_index)


def load_model(model_dir: Path) -> GraphSAGEEncoder:
    checkpoint = torch.load(model_dir / "model.pt", map_location="cpu", weights_only=True)
    model = GraphSAGEEncoder(
        checkpoint["in_dim"], checkpoint["hidden_dim"], checkpoint["out_dim"]
    )
    model.load_state_dict(checkpoint["state_dict"])
    model.eval()
    return model


def serve(model_dir: Path) -> None:
    model = load_model(model_dir)
    print(json.dumps({"ready": True}), flush=True)

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
            print(json.dumps({"embeddings": out}), flush=True)
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not swallowed
            print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(json.dumps({"error": "usage: embedding_server.py <model_dir>"}))
        raise SystemExit(1)
    serve(Path(sys.argv[1]))
