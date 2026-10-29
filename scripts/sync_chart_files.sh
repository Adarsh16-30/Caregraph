#!/usr/bin/env bash
# Copies the single sources of the alert rules and the dashboards into the Helm chart,
# which cannot read files outside its own directory. CI runs
# scripts/check_alert_rules.py, which fails if the copies differ from the sources.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
chart=infrastructure/helm/caregraph/files
mkdir -p "$chart/dashboards"
cp observability/prometheus/alert-rules.yaml "$chart/alert-rules.yaml"
for f in observability/grafana/dashboards/*.json; do
    cp "$f" "$chart/dashboards/$(basename "$f")"
done
echo "chart files synced"
