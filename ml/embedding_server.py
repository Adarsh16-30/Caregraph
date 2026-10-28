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

This process is spawned (and supervised, and respawned if it dies) by CareGraph
and kept alive: a fresh process per mutation would pay PyTorch's import cost
(whole seconds) on every request.

Protocol v2 — length-prefixed binary frames over stdin/stdout:

    frame := u32 LE len | u8 type | payload                  (len = 1 + len(payload))
    types: 0 HANDSHAKE (worker->rust, JSON: ready, protocol, architecture, ...)
           1 FORWARD   (rust->worker)      2 PING  (rust->worker)
           0x81 FORWARD_OK  0x82 PONG  0x8F ERROR (utf-8 message)   (worker->rust)
    FORWARD   := u32 LE header_len | header JSON {n,d,e,t,attr|null}
                 | features f32[n*d] | src u32[e] | dst u32[e] | targets u32[t]
                 | (attr) attr_targets u32[at] | before_src u32[eb] | before_dst u32[eb]
    FORWARD_OK:= u32 LE header_len | header JSON {t,dim,attribution?}
                 | embeddings f32[t*dim]

f32 -> bytes -> f32 is lossless, so this is bit-identical to the JSON protocol
it replaced (v1 serialized every float as decimal text). This process moves its
framing onto a private dup of fd 1 and points fd 1 / sys.stdout at stderr, so a
stray print from any library cannot corrupt the stream; stderr is captured and
re-logged by the Rust side.

`target_indices` selects which rows of the forward pass to return — the caller
asks for embeddings of specific nodes without the worker needing to know
CareGraph's node-id scheme, which lives entirely on the Rust side.

# Attribution (Phase 9, Feature 1) — integrated gradients over an edge mask

`"attribution"`, when present, asks for a per-edge explanation of *why* each
of `attribution.targets`' embedding changed between `edge_index_before` (the
pre-mutation graph) and `edge_index` (the post-mutation graph this request
already computes embeddings for) — see `src/embedding/meta.rs::Attribution`'s
own doc for the record this feeds; see
`tests/embedding/attribution_completeness_test.rs` for the completeness
identity it is verified against.

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

import array
import json
import os
import struct
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


PROTOCOL_VERSION = 2
T_HANDSHAKE, T_FORWARD, T_PING = 0, 1, 2
T_FORWARD_OK, T_PONG, T_ERROR = 0x81, 0x82, 0x8F
MAX_FRAME = 512 * 1024 * 1024


def _isolate_stdout():
    """Return a binary stream on a private dup of fd 1, then point fd 1 (and
    sys.stdout) at stderr so a stray print from any library cannot corrupt
    the framed protocol."""
    out_fd = os.dup(1)
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    if sys.platform == "win32":
        import msvcrt

        msvcrt.setmode(out_fd, os.O_BINARY)
    return os.fdopen(out_fd, "wb", buffering=0)


def _read_exact(stream, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError("parent closed the pipe")
        buf += chunk
    return bytes(buf)


def _read_frame(stream):
    (length,) = struct.unpack("<I", _read_exact(stream, 4))
    if length == 0 or length > MAX_FRAME:
        raise ValueError(f"bad frame length {length}")
    body = _read_exact(stream, length)
    return body[0], body[1:]


def _write_frame(out, frame_type: int, payload: bytes) -> None:
    out.write(struct.pack("<I", len(payload) + 1) + bytes([frame_type]) + payload)


def _u32_tensor(buf: bytes, offset: int, count: int) -> torch.Tensor:
    """u32 little-endian -> int64 tensor (values are node row indices < 2^31)."""
    if count == 0:
        return torch.zeros(0, dtype=torch.long)
    raw = bytearray(buf[offset : offset + count * 4])
    return torch.frombuffer(raw, dtype=torch.int32).to(torch.long)


def _handle_forward(model, payload: bytes) -> bytes:
    (hl,) = struct.unpack("<I", payload[:4])
    header = json.loads(payload[4 : 4 + hl])
    n, d, e, t = header["n"], header["d"], header["e"], header["t"]
    off = 4 + hl

    if n * d:
        x = torch.frombuffer(bytearray(payload[off : off + n * d * 4]), dtype=torch.float32).reshape(n, d)
    else:
        x = torch.zeros((n, d), dtype=torch.float32)
    off += n * d * 4
    src = _u32_tensor(payload, off, e)
    off += e * 4
    dst = _u32_tensor(payload, off, e)
    off += e * 4
    edge_index = torch.stack([src, dst]) if e else torch.zeros((2, 0), dtype=torch.long)
    targets = _u32_tensor(payload, off, t)
    off += t * 4

    with torch.no_grad():
        z = model(x, edge_index)
    out = z[targets].to(torch.float32).contiguous()
    dim = int(out.shape[1]) if out.dim() == 2 else 0
    resp_header: dict = {"t": t, "dim": dim}

    attr = header.get("attr")
    if attr:
        at, eb = attr["targets"], attr["edges_before"]
        attr_targets = _u32_tensor(payload, off, at).tolist()
        off += at * 4
        bsrc = _u32_tensor(payload, off, eb)
        off += eb * 4
        bdst = _u32_tensor(payload, off, eb)
        edge_index_before = (
            torch.stack([bsrc, bdst]) if eb else torch.zeros((2, 0), dtype=torch.long)
        )
        steps, top_k = int(attr["steps"]), int(attr["top_k"])
        with torch.no_grad():
            z_before = model(x, edge_index_before)
        resp_header["attribution"] = [
            _attribute_one_target(
                model, x, edge_index, edge_index_before, z, z_before, tt, steps, top_k
            )
            for tt in attr_targets
        ]

    flat = array.array("f", out.flatten().tolist())
    if sys.byteorder != "little":
        flat.byteswap()
    hb = json.dumps(resp_header).encode()
    return struct.pack("<I", len(hb)) + hb + flat.tobytes()


def serve(model_dir: Path) -> None:
    out = _isolate_stdout()
    threads = int(os.environ.get("CAREGRAPH_TORCH_THREADS", "0"))
    if threads > 0:
        torch.set_num_threads(threads)

    model, self_report = load_model(model_dir)
    hello = json.dumps({"ready": True, "protocol": PROTOCOL_VERSION, **self_report}).encode()
    _write_frame(out, T_HANDSHAKE, hello)

    stdin = sys.stdin.buffer
    while True:
        try:
            frame_type, payload = _read_frame(stdin)
        except EOFError:
            return  # parent closed our stdin: clean shutdown
        try:
            if frame_type == T_PING:
                _write_frame(out, T_PONG, b"")
            elif frame_type == T_FORWARD:
                _write_frame(out, T_FORWARD_OK, _handle_forward(model, payload))
            else:
                raise ValueError(f"unknown frame type {frame_type}")
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not swallowed
            _write_frame(out, T_ERROR, f"{type(exc).__name__}: {exc}".encode())


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: embedding_server.py <model_dir>", file=sys.stderr)
        raise SystemExit(1)
    serve(Path(sys.argv[1]))
