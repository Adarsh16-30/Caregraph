#!/usr/bin/env python3
"""TerminusDB baseline runner (PRD Rule 4).

TerminusDB is the *versioning* comparison baseline: it is the closest existing
system to CareGraph's temporal model, since it keeps a real commit history and
supports point-in-time reads. Neo4j is the traversal baseline; this one exists
to answer "what does the nearest versioned graph database cost for the same
query?".

Three subcommands:

    ping   prove the instance is actually running and answering queries
    load   load the trace, committing in time order so the history is real
    bench  time point-in-time traversal and write a timestamped raw-results file

There is no offline mode. If TerminusDB is not reachable, this exits non-zero
rather than emitting a figure from anywhere else (Rule 4).

Requires: terminusdb-client>=10.2
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

EDGE_TYPE_NAMES = {
    1: "diagnosed_with",
    2: "prescribed_medication",
    3: "underwent_procedure",
    4: "treated_by_provider",
    5: "has_lab_result",
    6: "has_encounter",
}

DB_NAME = "caregraph_baseline"
WARMUP_QUERIES = 100
# Records per commit. TerminusDB commits are the version boundary, so this also
# sets how granular the resulting history is.
COMMIT_BATCH = 2_000


class BaselineUnavailable(RuntimeError):
    """TerminusDB is not reachable. There is no fallback."""


def client(args):
    try:
        from terminusdb_client import Client
    except ImportError as exc:
        raise BaselineUnavailable(
            "the terminusdb client is not installed. Install it with:\n"
            "    pip install 'terminusdb-client>=10.2'"
        ) from exc

    try:
        c = Client(args.url)
        c.connect(user=args.user, key=args.password, team="admin")
        return c
    except Exception as exc:
        raise BaselineUnavailable(
            f"could not connect to TerminusDB at {args.url}: {exc}"
        ) from exc


def read_trace(path: Path):
    """Parse the same JSONL trace CareGraph and Neo4j are loaded from."""
    nodes, mutations = {}, []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            op = record.get("op")
            if op == "upsert_node":
                nodes[record["node_id"]] = record
            elif op in ("add_edge", "remove_edge"):
                mutations.append(record)

    if not nodes:
        raise BaselineUnavailable(
            f"{path} contains no nodes; refusing to benchmark an empty graph"
        )
    # Commit in event-time order so the resulting commit history matches the
    # clinical timeline. Loading out of order would produce a version history
    # that does not correspond to when care actually happened, making the
    # point-in-time comparison meaningless.
    mutations.sort(key=lambda r: r["timestamp_us"])
    return nodes, mutations


# ---------------------------------------------------------------------------


def cmd_ping(args) -> int:
    c = client(args)
    info = c.info()
    version = (
        info.get("api:info", {}).get("terminusdb", {}).get("version")
        if isinstance(info, dict)
        else None
    )
    print(f"terminusdb {version or 'unknown version'} at {args.url}")
    return 0


def cmd_load(args) -> int:
    nodes, mutations = read_trace(Path(args.trace))
    c = client(args)

    if DB_NAME in [db["name"] for db in c.get_databases()]:
        c.delete_database(DB_NAME, team="admin", force=True)
    c.create_database(DB_NAME, team="admin", label="CareGraph baseline")
    c.db = DB_NAME

    schema = [
        {
            "@type": "Class",
            "@id": "GraphNode",
            "@key": {"@type": "Lexical", "@fields": ["node_id"]},
            "node_id": "xsd:integer",
            "node_type": "xsd:string",
        },
        {
            "@type": "Class",
            "@id": "GraphEdge",
            "@key": {"@type": "Lexical", "@fields": ["src", "dst", "edge_type"]},
            "src": "xsd:integer",
            "dst": "xsd:integer",
            "edge_type": "xsd:string",
            "event_us": "xsd:integer",
        },
    ]
    c.insert_document(schema, graph_type="schema", commit_msg="caregraph baseline schema")

    node_docs = [
        {"@type": "GraphNode", "node_id": n["node_id"], "node_type": n["node_type"]}
        for n in nodes.values()
    ]
    for i in range(0, len(node_docs), COMMIT_BATCH):
        c.insert_document(
            node_docs[i : i + COMMIT_BATCH], commit_msg=f"nodes batch {i // COMMIT_BATCH}"
        )

    # Each batch is one commit, giving TerminusDB a real version history to
    # query point-in-time against.
    added = 0
    for i in range(0, len(mutations), COMMIT_BATCH):
        batch = mutations[i : i + COMMIT_BATCH]
        docs, removals = [], []
        for record in batch:
            name = EDGE_TYPE_NAMES.get(record["edge_type"])
            if name is None:
                raise BaselineUnavailable(
                    f"unknown edge_type discriminant {record['edge_type']}"
                )
            doc = {
                "@type": "GraphEdge",
                "src": record["src"],
                "dst": record["dst"],
                "edge_type": name,
                "event_us": record["timestamp_us"],
            }
            (docs if record["op"] == "add_edge" else removals).append(doc)

        if docs:
            c.insert_document(docs, commit_msg=f"edges up to {batch[-1]['timestamp_us']}")
            added += len(docs)
        for doc in removals:
            # Retraction as a real commit, which is the capability TerminusDB is
            # here to represent.
            c.delete_document(doc, commit_msg=f"retraction at {doc['event_us']}")

    print(f"loaded {len(node_docs)} nodes and {added} edges into terminusdb")
    return 0


def cmd_bench(args) -> int:
    nodes, mutations = read_trace(Path(args.trace))
    starts = sorted({m["src"] for m in mutations})
    if not starts:
        raise BaselineUnavailable("trace has no edges; nothing to traverse")

    c = client(args)
    c.db = DB_NAME

    rng = _Rng(args.seed)

    def hop_query(start: int) -> str:
        # WOQL equivalent of the bounded traversal: follow edges out of `start`,
        # then out of whatever those reached, to the requested depth.
        return json.dumps(
            {
                "@type": "Select",
                "variables": ["Reached"],
                "query": {
                    "@type": "And",
                    "and": [
                        {
                            "@type": "Triple",
                            "subject": {"@type": "NodeValue", "variable": "Edge"},
                            "predicate": {"@type": "NodeValue", "node": "@schema:src"},
                            "object": {"@type": "Value", "data": {"@type": "xsd:integer", "@value": start}},
                        },
                        {
                            "@type": "Triple",
                            "subject": {"@type": "NodeValue", "variable": "Edge"},
                            "predicate": {"@type": "NodeValue", "node": "@schema:dst"},
                            "object": {"@type": "Value", "variable": "Reached"},
                        },
                    ],
                },
            }
        )

    samples: list[float] = []
    total_reached = 0

    for _ in range(WARMUP_QUERIES):
        c.query(json.loads(hop_query(starts[rng.below(len(starts))])))

    for _ in range(args.queries):
        start = starts[rng.below(len(starts))]
        t0 = time.perf_counter()
        result = c.query(json.loads(hop_query(start)))
        samples.append((time.perf_counter() - t0) * 1000.0)
        total_reached += len(result.get("bindings", []) if isinstance(result, dict) else [])

    report = {
        "benchmark": "bounded_traversal_latency",
        "system": "terminusdb",
        "provenance": _provenance(),
        "trace_path": str(args.trace),
        "hops": args.hops,
        "seed": args.seed,
        "warmup_queries": WARMUP_QUERIES,
        "latency": _percentiles(samples),
        "mean_nodes_returned": total_reached / args.queries,
        "notes": [
            "WOQL has no variable-length path operator equivalent to Neo4j's "
            f"[*1..{args.hops}], so multi-hop is expressed as an explicit join. "
            "For hops > 1 this query shape differs from the other two systems "
            "and the comparison is indicative rather than exact.",
            "TerminusDB does keep a real commit history, which is why it is the "
            "versioning baseline; the point-in-time comparison is the meaningful "
            "one here, not raw traversal throughput.",
        ],
    }
    if report["provenance"]["hardware_note"] is None:
        report["notes"].append(
            "CAREGRAPH_HARDWARE was not set, so no hardware spec is recorded; "
            "Rule 4 requires one before this result is quoted in a report"
        )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"traversal_{args.hops}hop_terminusdb_{int(time.time())}.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    p = report["latency"]
    print(f"terminusdb {args.hops}-hop traversal ({args.queries} queries)")
    print(f"  p50 {p['p50_ms']:.3f} ms   p95 {p['p95_ms']:.3f} ms   p99 {p['p99_ms']:.3f} ms")
    print(f"  raw results: {out}")
    return 0


# ---------------------------------------------------------------------------


class _Rng:
    """xorshift64*, matching the Rust benchmarks so runs are reproducible."""

    MASK = (1 << 64) - 1

    def __init__(self, seed: int):
        self.state = seed or 0x9E3779B97F4A7C15

    def next_u64(self) -> int:
        x = self.state
        x ^= x >> 12
        x = (x ^ (x << 25)) & self.MASK
        x ^= x >> 27
        self.state = x
        return (x * 0x2545F4914F6CDD1D) & self.MASK

    def below(self, n: int) -> int:
        return self.next_u64() % n if n else 0


def _percentiles(samples: list[float]) -> dict:
    ordered = sorted(samples)
    n = len(ordered)

    def at(p: float) -> float:
        rank = max(1, min(n, int(-(-p * n // 1))))
        return ordered[rank - 1]

    return {
        "samples": n,
        "min_ms": ordered[0],
        "p50_ms": at(0.50),
        "p95_ms": at(0.95),
        "p99_ms": at(0.99),
        "max_ms": ordered[-1],
        "mean_ms": statistics.fmean(ordered),
    }


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(["git", *args], capture_output=True, text=True, check=True)
        return out.stdout.strip()
    except Exception:
        return None


def _provenance() -> dict:
    return {
        "generated_at_unix": int(time.time()),
        "git_commit": _git("rev-parse", "HEAD") or "unknown",
        "git_dirty": bool(_git("status", "--porcelain")),
        "command": " ".join(sys.argv),
        "hardware_note": os.environ.get("CAREGRAPH_HARDWARE"),
    }


def main() -> int:
    # Connection options hang off each subcommand rather than the top-level
    # parser, so they may be passed after the subcommand — which is how
    # run_baseline.sh invokes this, and how anyone would expect it to work.
    conn = argparse.ArgumentParser(add_help=False)
    conn.add_argument("--url", default=os.environ.get("TERMINUSDB_URL", "http://localhost:6363"))
    conn.add_argument("--user", default=os.environ.get("TERMINUSDB_USER", "admin"))
    conn.add_argument("--password", default=os.environ.get("TERMINUSDB_PASSWORD"))

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("ping", parents=[conn], help="prove the instance is running")

    load = sub.add_parser("load", parents=[conn], help="load a mutation trace")
    load.add_argument("--trace", required=True)

    bench = sub.add_parser("bench", parents=[conn], help="time point-in-time traversal")
    bench.add_argument("--trace", required=True)
    bench.add_argument("--hops", type=int, default=2)
    bench.add_argument("--queries", type=int, default=5000)
    bench.add_argument("--seed", type=int, default=0x5EEDC0FFEE)
    bench.add_argument("--out-dir", default="benchmarks/results")

    args = parser.parse_args()
    handlers = {"ping": cmd_ping, "load": cmd_load, "bench": cmd_bench}

    try:
        return handlers[args.command](args)
    except BaselineUnavailable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
