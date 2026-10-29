#!/usr/bin/env bash
# Memory of the load generator itself (caregraph-bench-load) while it drives a
# server, so a benchmark's own footprint is known. Windows (Git Bash) only.
#
# Usage: bash scripts/measure_loadgen_memory.sh <scratch-dir>
set -uo pipefail
W="${1:?usage: measure_loadgen_memory.sh <scratch-dir>}"
rm -rf "$W"; mkdir -p "$W"; cp -r data/db/diabetes130 "$W/db"

(
    export CAREGRAPH_API_KEY=k CAREGRAPH_DB_PATH="$W/db" CAREGRAPH_GRPC_ADDR=127.0.0.1:50096
    export CAREGRAPH_METRICS_ADDR=127.0.0.1:9196 CAREGRAPH_GRAPHSAGE_MODEL=none CAREGRAPH_GAT_MODEL=none
    exec target/release/caregraph
) > "$W/server.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null; taskkill //F //PID $SERVER >/dev/null 2>&1' EXIT
for _ in $(seq 1 60); do curl -fs http://127.0.0.1:9196/readyz >/dev/null 2>&1 && break; sleep 0.5; done

target/release/caregraph-bench-load --addr http://127.0.0.1:50096 --api-key k \
    --trace benchmarks/traces/diabetes130_full.jsonl --rates 500 --duration-secs 12 --warmup-secs 2 \
    --mix snapshot=100,traverse=0,similar=0,add_edge=0 --out-dir "$W/out" > "$W/load.txt" 2>&1 &
LOAD=$!
sleep 8
powershell -NoProfile -Command 'Get-Process caregraph-bench-load -ErrorAction SilentlyContinue | ForEach-Object { "bench-load: working set {0} MB, peak {1} MB, private {2} MB" -f [math]::Round($_.WorkingSet64/1MB), [math]::Round($_.PeakWorkingSet64/1MB), [math]::Round($_.PrivateMemorySize64/1MB) }'
wait $LOAD
grep achieved "$W/load.txt"
echo "trace file: $(du -m benchmarks/traces/diabetes130_full.jsonl | cut -f1) MB on disk"
