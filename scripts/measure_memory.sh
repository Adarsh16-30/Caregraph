#!/usr/bin/env bash
# Memory of a running CareGraph server's whole process tree (the Rust server and
# its Python embedding workers) at four points: idle after start, after a read
# load, after a write load, and after settling. Windows (Git Bash) only: it asks
# PowerShell for the working set and private bytes of every process under the
# server.
#
# Usage: bash scripts/measure_memory.sh <scratch-dir> [env assignments...]
#   e.g. bash scripts/measure_memory.sh /c/tmp/mem CAREGRAPH_MEMORY_MB=2048
set -uo pipefail

W="${1:?usage: measure_memory.sh <scratch-dir> [VAR=value ...]}"; shift || true
SERVER=target/release/caregraph
LOAD=target/release/caregraph-bench-load
TRACE=benchmarks/traces/diabetes130_full.jsonl
GRPC=127.0.0.1:50095; METRICS=127.0.0.1:9195; KEY=mem-key

rm -rf "$W"; mkdir -p "$W"
cp -r data/db/diabetes130 "$W/db"

tree_memory() { # prints: <name> <pid> <working-set MB> <private MB> for the server and each descendant
    powershell -NoProfile -Command '
        $root = [int]'"$1"'
        $all = Get-CimInstance Win32_Process
        $ids = @($root); $queue = @($root)
        while ($queue.Count) {
            $p = $queue[0]; $queue = @($queue | Select-Object -Skip 1)
            foreach ($c in ($all | Where-Object { $_.ParentProcessId -eq $p })) { $ids += $c.ProcessId; $queue += $c.ProcessId }
        }
        $total_ws = 0; $total_pv = 0
        foreach ($i in $ids) {
            $pr = Get-Process -Id $i -ErrorAction SilentlyContinue
            if ($pr) {
                $ws = [math]::Round($pr.WorkingSet64 / 1MB); $pv = [math]::Round($pr.PrivateMemorySize64 / 1MB)
                $total_ws += $ws; $total_pv += $pv
                "{0,-14} pid {1,-7} working set {2,6} MB   private {3,6} MB" -f $pr.ProcessName, $i, $ws, $pv
            }
        }
        "{0,-14} {1,-11} working set {2,6} MB   private {3,6} MB" -f "TOTAL", "", $total_ws, $total_pv
    '
}

(
    export CAREGRAPH_API_KEY=$KEY CAREGRAPH_DB_PATH="$W/db" CAREGRAPH_GRPC_ADDR=$GRPC
    export CAREGRAPH_METRICS_ADDR=$METRICS CAREGRAPH_AUDIT_DIR="$W/audit" CAREGRAPH_POD_NAME=mem
    export CAREGRAPH_TS_POLICY=strict CAREGRAPH_TORCH_THREADS=1
    for kv in "$@"; do export "$kv"; done
    exec $SERVER
) > "$W/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null; taskkill //F //PID $SERVER_PID >/dev/null 2>&1' EXIT
for _ in $(seq 1 120); do curl -fs "http://$METRICS/readyz" >/dev/null 2>&1 && break; sleep 0.5; done
sleep 3

# Windows reports the MSYS pid for the shell child; find the real server pid.
REAL_PID=$(powershell -NoProfile -Command "(Get-Process caregraph -ErrorAction SilentlyContinue | Sort-Object StartTime -Descending | Select-Object -First 1).Id")

echo "== settings: $*"
echo "== 1. idle, after start"; tree_memory "$REAL_PID"
$LOAD --addr http://$GRPC --api-key $KEY --trace $TRACE --rates 1000 --duration-secs 15 --warmup-secs 3 \
    --mix snapshot=85,traverse=10,similar=5,add_edge=0 --out-dir "$W/out_r" > "$W/load_r.txt" 2>&1
echo "== 2. after 1,000 req/s of reads for 15 s"; tree_memory "$REAL_PID"
$LOAD --addr http://$GRPC --api-key $KEY --trace $TRACE --rates 8 --duration-secs 20 --warmup-secs 3 \
    --mix add_edge=100 --server-timestamps --out-dir "$W/out_w" > "$W/load_w.txt" 2>&1
echo "== 3. after 8 req/s of writes for 20 s"; tree_memory "$REAL_PID"
sleep 10
echo "== 4. 10 s after the load stopped"; tree_memory "$REAL_PID"
grep -E "achieved" "$W/load_r.txt" "$W/load_w.txt" | sed 's/^/   /'
