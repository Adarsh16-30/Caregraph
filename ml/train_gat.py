#!/usr/bin/env python3
"""Train a GAT encoder on the real clinical graph and deploy it.

Mirrors `ml/train_graphsage.py` in every way that isn't the model itself:
same trace format, same unsupervised link-prediction objective, same
deploy-to-`ml/deployed/<name>/` contract that Rule 3's gate in
`scripts/check_rules.sh` looks for. The only thing that changes is the
encoder architecture — see `GATEncoder`'s docstring for why GAT gets its own
Rust-side incremental path (`src/embedding/gat_incremental.rs`) even though
the forward-pass mechanics here are otherwise identical to GraphSAGE's.

Usage:
    python ml/train_gat.py \\
        --trace benchmarks/traces/diabetes130_full.jsonl \\
        --name diabetes130_gat
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
from torch_geometric.nn import GATConv
from torch_geometric.utils import negative_sampling

# Must match train_graphsage.py's NODE_TYPES exactly — this is the interface
# contract with src/embedding/resolver.rs::NODE_TYPES on the Rust side.
NODE_TYPES = ("Patient", "Condition", "Medication", "Provider", "LabResult", "Encounter")


class SourceUnavailable(RuntimeError):
    """The real trace could not be read. There is no fallback."""


class GATEncoder(torch.nn.Module):
    """Two-layer GAT (Velickovic et al.). Two layers for the same reason as
    GraphSAGE: the PRD benchmarks 2-hop traversal, and each `GATConv` layer's
    attention only ever aggregates a node's own immediate neighbours, so the
    receptive field is exactly 2 hops here too — resolving a bounded 2-hop
    subgraph (`resolver.rs`) and recomputing the forward pass over it is
    exact for this model, the same argument `associative.rs`'s module doc
    makes for GraphSAGE/GCN.

    What's genuinely different is the aggregation itself. GraphSAGE's mean is
    associative in the literal sense: mean = sum / count, and both terms can
    be updated by a single neighbour's delta without touching the others.
    GAT's attention weight for every surviving edge is a softmax over *all*
    of a node's neighbours — adding or removing one neighbour renormalizes
    every other edge's weight, not just contributes an independent term.
    That's what `ModelKind::is_associative()` (src/types.rs) is actually
    naming, and why GAT is routed through its own
    `gat_incremental.rs::GATUpdatePath` rather than
    `associative.rs::IncrementalAggregator` on the Rust side, with embeddings
    tagged `ComputationPath::GatConstrained` instead of `Associative` so
    Rule 7's audit trail can tell the two apart. Both paths still do the same
    *kind* of computation underneath (forward pass over a resolved subgraph,
    not a literal streaming delta-update) — see gat_incremental.rs's module
    doc for exactly what "constrained" means for the Rust-side fallback
    policy.
    """

    def __init__(self, in_dim: int, hidden_dim: int, out_dim: int, heads: int = 4) -> None:
        super().__init__()
        self.conv1 = GATConv(in_dim, hidden_dim, heads=heads, dropout=0.6)
        self.conv2 = GATConv(hidden_dim * heads, out_dim, heads=1, concat=False, dropout=0.6)
        self.heads = heads

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        h = F.elu(self.conv1(x, edge_index))
        return self.conv2(h, edge_index)


def read_graph(trace_path: Path, as_of: int | None) -> tuple[Data, dict]:
    """Identical to train_graphsage.py's read_graph — same trace, same
    retraction-aware reconstruction. Duplicated rather than imported so this
    script has no import-time dependency on the sibling trainer; the two are
    independent PRD deliverables (Phase 4 vs Phase 5) that happen to share a
    data format, not one script's private helper.
    """
    if not trace_path.exists():
        raise SourceUnavailable(
            f"{trace_path} not found.\n"
            "Generate it first:\n"
            "    python data/diabetes130_loader.py --out benchmarks/traces/diabetes130_full.jsonl"
        )

    node_type: dict[int, str] = {}
    live: dict[tuple[int, int, int], int] = {}

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

    ids = sorted({n for e in live for n in (e[0], e[1])} | set(node_type))
    index = {nid: i for i, nid in enumerate(ids)}

    x = torch.zeros(len(ids), len(NODE_TYPES))
    for nid, i in index.items():
        t = node_type.get(nid)
        if t in NODE_TYPES:
            x[i, NODE_TYPES.index(t)] = 1.0

    src = [index[s] for (s, _, _) in live]
    dst = [index[d] for (_, d, _) in live]
    edge_index = torch.tensor([src + dst, dst + src], dtype=torch.long)

    stats = {
        "nodes": len(ids),
        "live_edges": len(live),
        "node_type_counts": {t: int(x[:, i].sum().item()) for i, t in enumerate(NODE_TYPES)},
        "as_of_us": as_of,
    }
    return Data(x=x, edge_index=edge_index), stats


def train(data: Data, dim: int, heads: int, epochs: int, lr: float, seed: int) -> tuple[GATEncoder, list[float]]:
    torch.manual_seed(seed)
    model = GATEncoder(data.num_node_features, dim * 2, dim, heads=heads)
    opt = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=5e-4)

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


def deploy(model: GATEncoder, stats: dict, losses: list[float], args, trace_path: Path) -> dict:
    out_dir = Path("ml/deployed") / args.name
    out_dir.mkdir(parents=True, exist_ok=True)

    weights = out_dir / "model.pt"
    torch.save(
        {
            "state_dict": model.state_dict(),
            "architecture": "GAT",
            "in_dim": len(NODE_TYPES),
            "hidden_dim": args.dim * 2,
            "out_dim": args.dim,
            "heads": args.heads,
            "node_types": list(NODE_TYPES),
        },
        weights,
    )

    digest = hashlib.sha256(weights.read_bytes()).hexdigest()
    (out_dir / "model.sha256").write_text(f"{digest}  model.pt\n", encoding="utf-8")

    manifest = {
        "model_id": args.name,
        "architecture": "GAT",
        "framework": f"pytorch-geometric {__import__('torch_geometric').__version__}",
        "torch": torch.__version__,
        "trained_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "objective": "unsupervised link prediction (positive edges vs negative sampling)",
        "aggregation": "attention (softmax over each node's own neighbours)",
        "is_associative": False,
        "attention_heads": args.heads,
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
    p.add_argument("--name", default="diabetes130_gat")
    p.add_argument("--dim", type=int, default=32)
    p.add_argument("--heads", type=int, default=4)
    p.add_argument("--epochs", type=int, default=60)
    p.add_argument("--lr", type=float, default=0.005)
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
    model, losses = train(data, args.dim, args.heads, args.epochs, args.lr, args.seed)

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
