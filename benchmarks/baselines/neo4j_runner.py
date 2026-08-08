#!/usr/bin/env python3
"""Neo4j Community + GDS baseline runner (PRD Rule 4).

Loads the *identical* mutation trace that CareGraph is loaded from, then times
the same bounded traversal workload. Same graph, same query shape, same machine
— otherwise the comparison is not apples-to-apples and the number is worthless.

Three subcommands:

    ping   prove the instance is actually running and answering queries
    load   load the trace into Neo4j
    bench  time bounded traversal and write a timestamped raw-results file

There is no offline mode. If Neo4j is not reachable, this exits non-zero rather
than emitting a figure from anywhere else (Rule 4).

Requires: neo4j>=5.0
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

# Edge discriminants must match src/types.rs::EdgeType.
EDGE_TYPE_NAMES = {
    1: "DIAGNOSED_WITH",
    2: "PRESCRIBED_MEDICATION",
    3: "UNDERWENT_PROCEDURE",
    4: "TREATED_BY_PROVIDER",
    5: "HAS_LAB_RESULT",
    6: "HAS_ENCOUNTER",
}

BATCH_SIZE = 5_000
WARMUP_QUERIES = 200


class BaselineUnavailable(RuntimeError):
    """Neo4j is not reachable. There is no fallback."""


def driver(args):
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise BaselineUnavailable(
            "the neo4j driver is not installed. Install it with:\n"
            "    pip install 'neo4j>=5.0'"
        ) from exc

    try:
        drv = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
        drv.verify_connectivity()
        return drv
    except Exception as exc:
        raise BaselineUnavailable(f"could not connect to Neo4j at {args.uri}: {exc}") from exc


def read_trace(path: Path):
    """Parse the same JSONL trace CareGraph is loaded from."""
    nodes, edges = {}, []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            op = record.get("op")
            if op == "upsert_node":
                nodes[record["node_id"]] = {
                    "id": record["node_id"],
                    "type": record["node_type"],
                    "ts": record["timestamp_us"],
                }
            elif op == "add_edge":
                edges.append(record)
            # remove_edge is skipped: Neo4j has no native version timeline, so
            # the baseline graph is the *current* state. This asymmetry is
            # recorded in the results file rather than papered over — it is
            # precisely the capability gap the PRD is about.
    if not nodes:
        raise BaselineUnavailable(
            f"{path} contains no nodes; refusing to benchmark an empty graph"
        )
    return nodes, edges


# ---------------------------------------------------------------------------


def cmd_ping(args) -> int:
    drv = driver(args)
    with drv.session() as session:
        version = session.run(
            "CALL dbms.components() YIELD name, versions "
            "RETURN name AS name, versions[0] AS version"
        ).single()

        # GDS must be present: Rule 4 names "Neo4j Community + Neo4j GDS" as the
        # baseline, and a plain Neo4j without GDS cannot produce the embedding
        # comparison the later phases need.
        try:
            gds = session.run("RETURN gds.version() AS v").single()["v"]
        except Exception as exc:
            raise BaselineUnavailable(
                f"Neo4j is up but the GDS plugin is not available ({exc}).\n"
                "The dev stack installs it via NEO4J_PLUGINS; check the container logs."
            ) from exc

    drv.close()
    print(f"neo4j {version['version']} with GDS {gds} at {args.uri}")
    return 0


def cmd_load(args) -> int:
    nodes, edges = read_trace(Path(args.trace))
    drv = driver(args)

    with drv.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(
            "CREATE CONSTRAINT caregraph_node_id IF NOT EXISTS "
            "FOR (n:GraphNode) REQUIRE n.id IS UNIQUE"
        )

        node_rows = list(nodes.values())
        for i in range(0, len(node_rows), BATCH_SIZE):
            session.run(
                "UNWIND $rows AS row "
                "MERGE (n:GraphNode {id: row.id}) "
                "SET n.type = row.type, n.created_us = row.ts",
                rows=node_rows[i : i + BATCH_SIZE],
            )

        # One relationship type per discriminant, so traversal can filter the
        # same way CareGraph does.
        by_type: dict[int, list] = {}
        for edge in edges:
            by_type.setdefault(edge["edge_type"], []).append(
                {"src": edge["src"], "dst": edge["dst"], "ts": edge["timestamp_us"]}
            )

        for discriminant, rows in by_type.items():
            name = EDGE_TYPE_NAMES.get(discriminant)
            if name is None:
                raise BaselineUnavailable(
                    f"unknown edge_type discriminant {discriminant} in {args.trace}"
                )
            for i in range(0, len(rows), BATCH_SIZE):
                session.run(
                    f"UNWIND $rows AS row "
                    f"MATCH (a:GraphNode {{id: row.src}}), (b:GraphNode {{id: row.dst}}) "
                    f"MERGE (a)-[r:{name}]->(b) "
                    f"SET r.event_us = row.ts",
                    rows=rows[i : i + BATCH_SIZE],
                )

        counted = session.run("MATCH (n:GraphNode) RETURN count(n) AS c").single()["c"]

    drv.close()
    print(f"loaded {counted} nodes and {len(edges)} edges into neo4j")
    return 0


def cmd_bench(args) -> int:
    nodes, edges = read_trace(Path(args.trace))
    starts = sorted({e["src"] for e in edges})
    if not starts:
        raise BaselineUnavailable("trace has no edges; nothing to traverse")

    drv = driver(args)

    # Same shape as CareGraph's traversal: undirected, hop-bounded, from a
    # patient. Neo4j's variable-length match is the idiomatic equivalent.
    query = (
        f"MATCH path = (a:GraphNode {{id: $start}})-[*1..{args.hops}]-(b:GraphNode) "
        f"RETURN count(DISTINCT b) AS nodes, count(path) AS paths"
    )

    # Deterministic query selection, matching the Rust benchmark's approach so
    # both systems see a comparable workload.
    rng = _Rng(args.seed)
    pick = lambda: starts[rng.below(len(starts))]  # noqa: E731

    samples: list[float] = []
    total_nodes = 0

    with drv.session() as session:
        for _ in range(WARMUP_QUERIES):
            session.run(query, start=pick()).consume()

        for _ in range(args.queries):
            start = pick()
            t0 = time.perf_counter()
            record = session.run(query, start=start).single()
            samples.append((time.perf_counter() - t0) * 1000.0)
            if record:
                total_nodes += record["nodes"]

    drv.close()

    report = {
        "benchmark": "bounded_traversal_latency",
        "system": "neo4j-gds",
        "provenance": _provenance(),
        "trace_path": str(args.trace),
        "hops": args.hops,
        "seed": args.seed,
        "warmup_queries": WARMUP_QUERIES,
        "latency": _percentiles(samples),
        "mean_nodes_returned": total_nodes / args.queries,
        "notes": [
            "Neo4j holds the current graph state only: it has no version "
            "timeline, so remove_edge records in the trace were not applied. "
            "The comparison is therefore CareGraph's point-in-time traversal "
            "against Neo4j's current-state traversal, which favours Neo4j.",
            "Traversal is unbounded in fan-out, unlike CareGraph's capped "
            "traversal; read the two latencies with that in mind.",
        ],
    }
    if report["provenance"]["hardware_note"] is None:
        report["notes"].append(
            "CAREGRAPH_HARDWARE was not set, so no hardware spec is recorded; "
            "Rule 4 requires one before this result is quoted in a report"
        )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"traversal_{args.hops}hop_neo4j_{int(time.time())}.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    p = report["latency"]
    print(f"neo4j {args.hops}-hop traversal ({args.queries} queries)")
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
        rank = max(1, min(n, int(-(-p * n // 1))))  # ceil
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
        out = subprocess.run(
            ["git", *args], capture_output=True, text=True, check=True
        )
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
    conn.add_argument("--uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    conn.add_argument("--user", default=os.environ.get("NEO4J_USER", "neo4j"))
    conn.add_argument("--password", default=os.environ.get("NEO4J_PASSWORD"))

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("ping", parents=[conn], help="prove the instance is running")

    load = sub.add_parser("load", parents=[conn], help="load a mutation trace")
    load.add_argument("--trace", required=True)

    bench = sub.add_parser("bench", parents=[conn], help="time bounded traversal")
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
