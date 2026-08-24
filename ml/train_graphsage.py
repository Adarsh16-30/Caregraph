#!/usr/bin/env python3
"""Train a GraphSAGE encoder on the real clinical graph and deploy it.

Reads the same JSONL mutation trace `caregraph-load` applies to RocksDB, builds
the graph as it stands at a given instant, and trains an unsupervised GraphSAGE
encoder over it. The trained weights, a dataset manifest, and a SHA-256 of the
model are written to `ml/deployed/<name>/`, which is what Rule 3's gate in
`scripts/check_rules.sh` looks for.

Rule 3 (PRD Section 0): every node embedding must come from a real forward or
incremental pass through a GraphSAGE, GCN, or GAT model implemented in DGL or
PyTorch Geometric. There is no random-vector path here and no `--synthetic`
flag; if the trace cannot be read the script exits non-zero.

**Framework note.** The PRD's Phase 4 task text names DGL. DGL 2.x has no
distribution for the Python version available on this machine (the index offers
only the 2018-era 0.1.x stubs), so this uses PyTorch Geometric, which Rule 3
names as an equally acceptable implementation. The deviation is from the task
wording, not from the rule that gates the phase.

Unsupervised objective: link prediction over the graph's own edges. A pair of
nodes joined by a real clinical relationship should score higher than a random
pair. That needs no labels, which matters because the derived timeline in this
dataset makes any temporal label leaky (see docs/benchmark_report.md).

Usage:
    python ml/train_graphsage.py \\
        --trace benchmarks/traces/diabetes130_full.jsonl \\
        --name diabetes130_graphsage
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import SAGEConv
from torch_geometric.utils import negative_sampling

# Node-type one-hot dimensions. Structural features only: the model must learn
# from the graph, and feeding raw clinical attributes in as features would leak
# exactly the properties a care-pathway embedding is supposed to infer.
NODE_TYPES = ("Patient", "Condition", "Medication", "Provider", "LabResult", "Encounter")


class SourceUnavailable(RuntimeError):
    """The real trace could not be read. There is no fallback."""


class GraphSAGEEncoder(torch.nn.Module):
    """Two-layer GraphSAGE. Two layers because the PRD benchmarks 2-hop
    traversal, so the embedding's receptive field matches the query shape."""

    def __init__(self, in_dim: int, hidden_dim: int, out_dim: int) -> None:
        super().__init__()
        # aggr="mean" is the associative aggregation Phase 4 depends on: the
        # incremental path can update a mean from a delta without revisiting
        # every neighbour, which is what makes GraphSAGE/GCN the "associative"
        # ModelKind in src/types.rs and GAT the constrained one.
        self.conv1 = SAGEConv(in_dim, hidden_dim, aggr="mean")
        self.conv2 = SAGEConv(hidden_dim, out_dim, aggr="mean")

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        h = F.relu(self.conv1(x, edge_index))
        return self.conv2(h, edge_index)


def read_graph(trace_path: Path, as_of: int | None) -> tuple[Data, dict]:
    """Build the graph as it stood at `as_of` from the mutation trace.

    Applies retractions, so a medication stopped before `as_of` is absent from
    the training graph rather than silently present.
    """
    if not trace_path.exists():
        raise SourceUnavailable(
            f"{trace_path} not found.\n"
            "Generate it first:\n"
            "    python data/diabetes130_loader.py --out benchmarks/traces/diabetes130_full.jsonl"
        )

    node_type: dict[int, str] = {}
    live: dict[tuple[int, int, int], int] = {}  # (src, dst, edge_type) -> timestamp

    with trace_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            ts = r.get("timestamp_us", 0)
            if as_of is not None and ts > as_of:
                continue
            op = r.get("op")
            if op == "upsert_node":
                node_type[r["node_id"]] = r.get("node_type", "Unknown")
            elif op == "add_edge":
                live[(r["src"], r["dst"], r["edge_type"])] = ts
            elif op == "remove_edge":
                live.pop((r["src"], r["dst"], r["edge_type"]), None)

    if not live:
        raise SourceUnavailable(
            f"{trace_path} yielded no live edges at as_of={as_of}; refusing to "
            "train on an empty graph."
        )

    # Dense contiguous indices — PyG needs 0..N-1, the graph uses namespaced ids.
    ids = sorted({n for e in live for n in (e[0], e[1])} | set(node_type))
    index = {nid: i for i, nid in enumerate(ids)}

    x = torch.zeros(len(ids), len(NODE_TYPES))
    for nid, i in index.items():
        t = node_type.get(nid)
        if t in NODE_TYPES:
            x[i, NODE_TYPES.index(t)] = 1.0

    src = [index[s] for (s, _, _) in live]
    dst = [index[d] for (_, d, _) in live]
    # Undirected for message passing: a patient must see its condition and the
    # condition must see its patients, or 2-hop co-patient structure is invisible.
    edge_index = torch.tensor([src + dst, dst + src], dtype=torch.long)

    stats = {
        "nodes": len(ids),
        "live_edges": len(live),
        "node_type_counts": {
            t: int(x[:, i].sum().item()) for i, t in enumerate(NODE_TYPES)
        },
        "as_of_us": as_of,
    }
    return Data(x=x, edge_index=edge_index), stats


def train(data: Data, dim: int, epochs: int, lr: float, seed: int) -> tuple[GraphSAGEEncoder, list[float]]:
    torch.manual_seed(seed)
    model = GraphSAGEEncoder(data.num_node_features, dim * 2, dim)
    opt = torch.optim.Adam(model.parameters(), lr=lr)

    # Score only the forward half: edge_index was doubled for undirected message
    # passing, so training on all of it would score every edge twice.
    pos = data.edge_index[:, : data.edge_index.size(1) // 2]
    losses = []

    model.train()
    for epoch in range(epochs):
        opt.zero_grad()
        z = model(data.x, data.edge_index)

        neg = negative_sampling(
            edge_index=data.edge_index,
            num_nodes=data.num_nodes,
            num_neg_samples=pos.size(1),
        )

        pos_score = (z[pos[0]] * z[pos[1]]).sum(dim=-1)
        neg_score = (z[neg[0]] * z[neg[1]]).sum(dim=-1)

        loss = F.binary_cross_entropy_with_logits(
            torch.cat([pos_score, neg_score]),
            torch.cat([torch.ones_like(pos_score), torch.zeros_like(neg_score)]),
        )
        loss.backward()
        opt.step()
        losses.append(float(loss))
        if epoch % 10 == 0 or epoch == epochs - 1:
            print(f"  epoch {epoch:>4}  loss {float(loss):.4f}", file=sys.stderr)

    return model, losses


def deploy(model, stats, losses, args, trace_path: Path) -> dict:
    """Write weights, manifest, and checksum — the Rule 3 gate."""
    out_dir = Path("ml/deployed") / args.name
    out_dir.mkdir(parents=True, exist_ok=True)

    weights = out_dir / "model.pt"
    torch.save(
        {
            "state_dict": model.state_dict(),
            "architecture": "GraphSAGE",
            "in_dim": len(NODE_TYPES),
            "hidden_dim": args.dim * 2,
            "out_dim": args.dim,
            "aggr": "mean",
            "node_types": list(NODE_TYPES),
        },
        weights,
    )

    digest = hashlib.sha256(weights.read_bytes()).hexdigest()
    (out_dir / "model.sha256").write_text(f"{digest}  model.pt\n", encoding="utf-8")

    manifest = {
        "model_id": args.name,
        "architecture": "GraphSAGE",
        "framework": f"pytorch-geometric {__import__('torch_geometric').__version__}",
        "framework_note": (
            "PRD Phase 4 names DGL; DGL 2.x has no distribution for this Python "
            "version, and Rule 3 names PyTorch Geometric as equally acceptable."
        ),
        "torch": torch.__version__,
        "trained_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "objective": "unsupervised link prediction (positive edges vs negative sampling)",
        "aggregation": "mean",
        "is_associative": True,
        "layers": 2,
        "embedding_dim": args.dim,
        "epochs": args.epochs,
        "learning_rate": args.lr,
        "seed": args.seed,
        "final_loss": losses[-1],
        "initial_loss": losses[0],
        "dataset": {
            "trace_path": str(trace_path),
            "trace_sha256": hashlib.sha256(trace_path.read_bytes()).hexdigest(),
            **stats,
        },
        "model_sha256": digest,
        "node_feature_encoding": "one-hot node type; no clinical attributes as features",
    }
    (out_dir / "dataset_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--trace", type=Path, default=Path("benchmarks/traces/diabetes130_full.jsonl"))
    p.add_argument("--name", default="diabetes130_graphsage")
    p.add_argument("--dim", type=int, default=32)
    p.add_argument("--epochs", type=int, default=60)
    p.add_argument("--lr", type=float, default=0.01)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--as-of", type=int, default=None,
                   help="build the graph as it stood at this timestamp (default: latest)")
    args = p.parse_args()

    try:
        data, stats = read_graph(args.trace, args.as_of)
    except SourceUnavailable as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    print(f"graph: {stats['nodes']} nodes, {stats['live_edges']} live edges", file=sys.stderr)
    model, losses = train(data, args.dim, args.epochs, args.lr, args.seed)

    if losses[-1] >= losses[0]:
        print(
            f"error: loss did not improve ({losses[0]:.4f} -> {losses[-1]:.4f}); "
            "refusing to deploy a model that did not learn.",
            file=sys.stderr,
        )
        return 1

    manifest = deploy(model, stats, losses, args, args.trace)
    print(json.dumps(manifest, indent=2))
    print(f"\ndeployed: ml/deployed/{args.name}/", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
