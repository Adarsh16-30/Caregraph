#!/usr/bin/env python3
"""Checks that every metric an alert, recording rule or dashboard panel uses exists.

A metric is "known" if the server registers it (found by scanning src/ for the
constructor calls that create Prometheus series) or it is a standard kubelet/Prometheus
series on the allowlist. A renamed or misspelled metric then fails here instead of
leaving an alert that can never fire.

Also checks the rules parse, each alert has a severity and a runbook that exists, and
that the chart's copy of the rules is identical to observability/prometheus/alert-rules.yaml.

Usage: python scripts/check_alert_rules.py
"""
import json
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
RULES = ROOT / "observability/prometheus/alert-rules.yaml"
CHART_RULES = ROOT / "infrastructure/helm/caregraph/files/alert-rules.yaml"
RECORDING = ROOT / "observability/prometheus/recording-rules.yaml"
DASHBOARDS = ROOT / "observability/grafana/dashboards"

# Series that do not come from this codebase.
EXTERNAL = {
    "kubelet_volume_stats_used_bytes",
    "kubelet_volume_stats_capacity_bytes",
    "kubelet_volume_stats_available_bytes",
}
# Words with an underscore that are PromQL functions or label names, not metrics.
PROMQL_WORDS = {
    "histogram_quantile", "clamp_min", "clamp_max", "predict_linear", "label_replace",
    "count_values", "group_left", "group_right", "label_join",
}
LABEL_WORDS = {"persistentvolumeclaim", "epoch_value", "caregraph_io_component", "computation_path", "max_hops"}
HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")

CONSTRUCTOR = re.compile(
    r"(?:Opts::new|IntGauge::new|IntCounter::new|IntGaugeVec::new|IntCounterVec::new"
    r"|HistogramVec::new|\bg)\(\s*\"([a-z][a-z0-9_]*)\"",
    re.S,
)


def known_metrics():
    names = set()
    for path in (ROOT / "src").rglob("*.rs"):
        for m in CONSTRUCTOR.finditer(path.read_text(encoding="utf-8")):
            names.add(m.group(1))
    return names


def words(expr):
    return set(re.findall(r"[a-zA-Z_:][a-zA-Z0-9_:]*", expr))


def metrics_in(expr, known):
    """Returns (metrics used, unknown names with an underscore)."""
    used, unknown = set(), set()
    # Drop label matchers' values and recording-rule names so only metric words remain.
    stripped = re.sub(r'"[^"]*"', '""', expr)
    for w in words(stripped):
        if "_" not in w and ":" not in w:
            continue
        if w in PROMQL_WORDS or w in LABEL_WORDS:
            continue
        base = w
        for suf in HISTOGRAM_SUFFIXES:
            if w.endswith(suf) and w[: -len(suf)] in known:
                base = w[: -len(suf)]
        if base in known or base in EXTERNAL:
            used.add(base)
        elif w.startswith("caregraph:"):
            used.add(w)  # a recording rule, checked separately
        else:
            unknown.add(w)
    return used, unknown


def main():
    known = known_metrics()
    problems = []
    if len(known) < 20:
        problems.append(f"only {len(known)} server metrics found; the scan pattern is stale")

    doc = yaml.safe_load(RULES.read_text(encoding="utf-8"))
    alerts = 0
    for group in doc["groups"]:
        for rule in group["rules"]:
            alerts += 1
            name = rule["alert"]
            sev = rule.get("labels", {}).get("severity")
            if sev not in ("page", "ticket"):
                problems.append(f"{name}: severity must be page or ticket, got {sev!r}")
            runbook = rule.get("annotations", {}).get("runbook")
            if not runbook or not (ROOT / runbook).exists():
                problems.append(f"{name}: runbook {runbook!r} does not exist")
            if not rule.get("annotations", {}).get("summary"):
                problems.append(f"{name}: no summary")
            _, unknown = metrics_in(rule["expr"], known)
            for u in sorted(unknown):
                problems.append(f"{name}: unknown metric `{u}`")

    recorded = set()
    rec = yaml.safe_load(RECORDING.read_text(encoding="utf-8"))
    for group in rec["groups"]:
        for rule in group["rules"]:
            recorded.add(rule["record"])
            _, unknown = metrics_in(rule["expr"], known)
            for u in sorted(unknown):
                problems.append(f"recording rule {rule['record']}: unknown metric `{u}`")

    panels = 0
    for path in sorted(DASHBOARDS.glob("*.json")):
        dash = json.loads(path.read_text(encoding="utf-8"))
        for panel in dash.get("panels", []):
            for target in panel.get("targets", []):
                panels += 1
                _, unknown = metrics_in(target["expr"], known)
                for u in sorted(unknown):
                    if u not in recorded:
                        problems.append(f"{path.name} / {panel.get('title')}: unknown series `{u}`")

    if CHART_RULES.exists():
        if CHART_RULES.read_bytes() != RULES.read_bytes():
            problems.append(
                "the chart's alert-rules.yaml differs from observability/prometheus/alert-rules.yaml; "
                "run scripts/sync_chart_files.sh"
            )
    else:
        problems.append(f"{CHART_RULES.relative_to(ROOT)} is missing; run scripts/sync_chart_files.sh")

    if problems:
        print("FAIL")
        for p in problems:
            print("  -", p)
        sys.exit(1)
    print(f"OK: {alerts} alerts, {panels} dashboard queries, {len(known)} server metrics known")


if __name__ == "__main__":
    main()
