#!/usr/bin/env python3
"""Summarises the output of scripts/p10_compare.sh.

Reads <out>/<system>/<workload>/rep<k>/load_*.json (one sweep per file) and prints, per
workload, for each system:

* the highest offered rate it sustained within a p99 limit (completed at least 95% of the
  offered rate, under 1% errors, p99 under the limit), for each repeat, so the spread is
  visible, with the median across repeats;
* its latency (p50 / p99) at the rates every system was driven at, as median [min-max]
  across repeats;
* how much each answer carried (mean items) and, for CareGraph, how often a cap cut it.

Writes <out>/summary.json and <out>/summary.md.
"""
import json
import statistics
import sys
from pathlib import Path

LIMITS_MS = (50.0, 250.0, 1000.0)


def load(out: Path):
    data = {}
    for f in sorted(out.glob("*/*/rep*/load_*.json")):
        system, workload, rep = f.parts[-4], f.parts[-3], f.parts[-2]
        doc = json.loads(f.read_text(encoding="utf-8"))
        data.setdefault(workload, {}).setdefault(system, {})[rep] = doc["results"]
    return data


def healthy(r):
    allop = r["per_op"].get("all")
    if not allop:
        return False
    failed = sum(r["errors_by_code"].values())
    return r["achieved_ok_rps"] >= 0.95 * r["offered_rps"] and failed <= 0.01 * max(r["sent"], 1)


def sustained(results, limit):
    best = 0.0
    for r in results:
        if healthy(r) and r["per_op"]["all"]["p99_ms"] < limit:
            best = max(best, r["achieved_ok_rps"])
    return best


def spread(values, fmt="{:.2f}"):
    if not values:
        return "-"
    return f"{fmt.format(statistics.median(values))} [{fmt.format(min(values))}-{fmt.format(max(values))}]"


def main():
    out = Path(sys.argv[1])
    data = load(out)
    summary, lines = {}, []
    for workload in ("ping", "snapshot", "traverse", "traverse_bounded", "mixed"):
        systems = data.get(workload)
        if not systems:
            continue
        lines.append(f"\n### {workload}\n")
        summary[workload] = {}
        lines.append("| system | " + " | ".join(f"max rate, p99 < {int(l)} ms (per repeat; median)" for l in LIMITS_MS) + " | answer size (mean items) | share cut by a cap |")
        lines.append("|---|" + "---|" * (len(LIMITS_MS) + 2))
        for system in ("caregraph", "neo4j", "terminusdb"):
            reps = systems.get(system)
            if not reps:
                continue
            cells, rec = [], {}
            for limit in LIMITS_MS:
                per = [sustained(res, limit) for res in reps.values()]
                cells.append(f"{', '.join(f'{v:.0f}' for v in per)}; **{statistics.median(per):.0f}**/s")
                rec[f"sustained_p99_lt_{int(limit)}ms"] = per
            ops = [
                r["per_op"].get({"mixed": "traverse", "traverse_bounded": "traverse"}.get(workload, workload))
                for res in reps.values()
                for r in res
                if healthy(r)
            ]
            ops = [o for o in ops if o]
            items = statistics.fmean(o["mean_items"] for o in ops) if ops else 0.0
            cut = statistics.fmean(o["truncated_share"] for o in ops) if ops else 0.0
            rec["mean_items"], rec["truncated_share"] = items, cut
            lines.append(f"| {system} | " + " | ".join(cells) + f" | {items:.1f} | {cut * 100:.0f}% |")
            summary[workload][system] = rec

        # Latency at the rates all the systems in this workload were driven at.
        common = None
        for system, reps in systems.items():
            rates = {r["offered_rps"] for res in reps.values() for r in res}
            common = rates if common is None else common & rates
        if common:
            lines.append("\nLatency at offered rates every system was driven at (median [min-max] over repeats):\n")
            lines.append("| offered /s | system | p50 ms | p99 ms | completed /s | errors |")
            lines.append("|---|---|---|---|---|---|")
            for rate in sorted(common):
                for system in ("caregraph", "neo4j", "terminusdb"):
                    reps = systems.get(system)
                    if not reps:
                        continue
                    rs = [r for res in reps.values() for r in res if r["offered_rps"] == rate]
                    p50 = [r["per_op"]["all"]["p50_ms"] for r in rs if "all" in r["per_op"]]
                    p99 = [r["per_op"]["all"]["p99_ms"] for r in rs if "all" in r["per_op"]]
                    done = [r["achieved_ok_rps"] for r in rs]
                    errs = sum(sum(r["errors_by_code"].values()) for r in rs)
                    lines.append(f"| {rate:g} | {system} | {spread(p50)} | {spread(p99)} | {spread(done, '{:.0f}')} | {errs} |")
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    text = "\n".join(lines) + "\n"
    (out / "summary.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
