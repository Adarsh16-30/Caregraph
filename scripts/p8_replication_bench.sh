#!/usr/bin/env bash
# Replication cost on a copy of the real diabetes130 database.
#
#   1. write latency with no replica, an asynchronous replica and a synchronous
#      replica (interleaved, same machine, fresh database copy per run)
#   2. how fast a follower that was down catches up
#   3. read latency served by the follower against the leader
#
# Usage: bash scripts/p8_replication_bench.sh <scratch-dir> <out-dir>
set -uo pipefail

W="${1:?usage: p8_replication_bench.sh <scratch-dir> <out-dir>}"
OUT="${2:?usage: p8_replication_bench.sh <scratch-dir> <out-dir>}"
SERVER=target/release/caregraph
LOAD=target/release/caregraph-bench-load
SRC=data/db/diabetes130
TRACE=benchmarks/traces/diabetes130_full.jsonl
KEY=bench-key
L_GRPC=127.0.0.1:50091; L_METRICS=127.0.0.1:9191
F_GRPC=127.0.0.1:50092; F_METRICS=127.0.0.1:9192

mkdir -p "$W" "$OUT"
LP=""; FP=""

now_ms() { echo $(( $(date +%s%N) / 1000000 )); }

stop_servers() {
    for p in $LP $FP; do kill "$p" 2>/dev/null || true; taskkill //F //PID "$p" >/dev/null 2>&1 || true; done
    LP=""; FP=""
    sleep 2
}
trap stop_servers EXIT

common_env() {
    export CAREGRAPH_API_KEY=$KEY CAREGRAPH_TS_POLICY=strict CAREGRAPH_SYNC_WRITES=true
    export CAREGRAPH_TORCH_THREADS=1 CAREGRAPH_EXPLAIN_ENABLED=false
}

wait_ready() { # wait_ready <metrics-addr>
    for _ in $(seq 1 120); do
        curl -fs "http://$1/readyz" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    echo "server at $1 never became ready" >&2; return 1
}

metric() { # metric <metrics-addr> <name>
    curl -s "http://$1/metrics" | grep -E "^$2 " | awk '{print $2}' | head -1
}

start_leader() { # start_leader <db> <role> <min_acks>
    ( common_env
      export CAREGRAPH_DB_PATH="$1" CAREGRAPH_GRPC_ADDR=$L_GRPC CAREGRAPH_METRICS_ADDR=$L_METRICS
      export CAREGRAPH_POD_NAME=leader-0 CAREGRAPH_GAT_MODEL=none
      if [ "$2" != "standalone" ]; then
          export CAREGRAPH_ROLE=$2 CAREGRAPH_REPLICATION_MIN_ACKS=$3
      fi
      exec $SERVER ) > "$W/leader.log" 2>&1 &
    LP=$!
    wait_ready $L_METRICS
}

start_follower() { # start_follower <db>
    ( common_env
      export CAREGRAPH_DB_PATH="$1" CAREGRAPH_GRPC_ADDR=$F_GRPC CAREGRAPH_METRICS_ADDR=$F_METRICS
      export CAREGRAPH_POD_NAME=follower-1 CAREGRAPH_ROLE=follower
      export CAREGRAPH_LEADER_ADDR=http://$L_GRPC CAREGRAPH_REPLICATION_TOKEN=$KEY
      export CAREGRAPH_GRAPHSAGE_MODEL=none CAREGRAPH_GAT_MODEL=none
      exec $SERVER ) > "$W/follower.log" 2>&1 &
    FP=$!
    wait_ready $F_METRICS
}

fresh_copy() { rm -rf "$1"; cp -r "$2" "$1"; rm -f "$1.role.json"; }

echo "== commit $(git rev-parse --short HEAD), $(date -u +%FT%TZ)"
echo "== host: $(nproc) logical CPUs, load generator and both servers share them"

# A seed database that already carries an epoch record, so a copy of it is a
# valid follower of a leader started from another copy.
fresh_copy "$W/seed" "$SRC"
start_leader "$W/seed" leader 0
stop_servers
echo "== seed prepared (epoch recorded)"

write_run() { # write_run <label> <leader-role> <min_acks> <with-follower>
    local label="$1" role="$2" acks="$3" withf="$4"
    fresh_copy "$W/l" "$W/seed"
    start_leader "$W/l" "$role" "$acks"
    if [ "$withf" = yes ]; then
        fresh_copy "$W/f" "$W/seed"
        start_follower "$W/f"
        for _ in $(seq 1 60); do
            [ "$(metric $L_METRICS replication_connected_followers)" = "1" ] && break
            sleep 0.5
        done
    fi
    # Sample the follower's lag in sequence numbers while the load runs.
    : > "$W/lag_$label.txt"
    ( while true; do
          if [ "$withf" = yes ]; then metric $F_METRICS replication_lag_sequences >> "$W/lag_$label.txt"; fi
          sleep 0.25
      done ) &
    local sampler=$!
    $LOAD --addr http://$L_GRPC --api-key $KEY --trace $TRACE --rates 8,12 \
        --duration-secs 20 --warmup-secs 5 --mix add_edge=100 --server-timestamps \
        --out-dir "$W/out_$label" > "$W/load_$label.txt" 2>&1
    kill $sampler 2>/dev/null || true
    mkdir -p "$OUT"
    cp "$W"/out_$label/*.json "$OUT/prod8_write_$label.json" 2>/dev/null || cp "$W"/out_$label/* "$OUT/" 2>/dev/null
    {
        echo "# $label"
        for m in replication_ack_wait_seconds_count replication_ack_wait_seconds_sum \
                 replication_degraded_writes_total replication_ack_timeouts_total \
                 replication_connected_followers; do
            echo "leader $m $(metric $L_METRICS $m)"
        done
        if [ "$acks" != 0 ] && [ "$withf" = yes ]; then
            echo "leader ack wait histogram (seconds, cumulative):"
            curl -s "http://$L_METRICS/metrics" | grep -E '^replication_ack_wait_seconds_bucket' | sed 's/^replication_ack_wait_seconds_bucket//' | sed 's/^/  /'
        fi
        if [ "$withf" = yes ]; then
            echo "follower replication_batches_applied_total $(metric $F_METRICS replication_batches_applied_total)"
            echo "follower final lag $(metric $F_METRICS replication_lag_sequences)"
            echo "lag samples (sequence numbers): n=$(wc -l < "$W/lag_$label.txt") max=$(sort -n "$W/lag_$label.txt" | tail -1) p50=$(sort -n "$W/lag_$label.txt" | awk '{a[NR]=$1} END{print a[int(NR*0.5)+1]}') p99=$(sort -n "$W/lag_$label.txt" | awk '{a[NR]=$1} END{print a[int(NR*0.99)+1]}')"
        fi
        grep -E "offered|p50|achieved" "$W/load_$label.txt" | head -12
    } >> "$OUT/prod8_write_summary.txt"
    stop_servers
}

: > "$OUT/prod8_write_summary.txt"
for round in 1 2; do
    write_run "standalone_$round" standalone 0 no
    write_run "sync_$round" leader 1 yes
    write_run "async_$round" leader 0 yes
done

echo "== catch-up: follower down while the leader takes writes, then started"
fresh_copy "$W/l" "$W/seed"; fresh_copy "$W/f" "$W/seed"
start_leader "$W/l" leader 0
$LOAD --addr http://$L_GRPC --api-key $KEY --trace $TRACE --rates 8 --duration-secs 30 \
    --warmup-secs 2 --mix add_edge=100 --server-timestamps --out-dir "$W/out_catchup" > "$W/load_catchup.txt" 2>&1
LEADER_SEQ=$(curl -s "http://$L_METRICS/metrics" | grep -E '^replication_epoch ' >/dev/null && echo ok)
T0=$(now_ms)
start_follower "$W/f"
for _ in $(seq 1 600); do
    LAG=$(metric $F_METRICS replication_lag_sequences)
    APPLIED=$(metric $F_METRICS replication_applied_sequence)
    BATCHES=$(metric $F_METRICS replication_batches_applied_total)
    [ -n "$LAG" ] && [ "$LAG" = "0" ] && [ "${BATCHES:-0}" != "0" ] && break
    sleep 0.1
done
T1=$(now_ms)
{
    echo "# catch-up"
    echo "follower applied up to sequence $APPLIED in $((T1 - T0)) ms (includes process start and readiness), $BATCHES batches"
    echo "approx batches/s including startup: $(( BATCHES * 1000 / (T1 - T0 + 1) ))"
} >> "$OUT/prod8_write_summary.txt"
stop_servers

echo "== reads: follower against leader"
fresh_copy "$W/l" "$W/seed"; fresh_copy "$W/f" "$W/seed"
start_leader "$W/l" leader 0
start_follower "$W/f"
sleep 3
for target in leader follower leader follower; do
    if [ $target = leader ]; then ADDR=$L_GRPC; else ADDR=$F_GRPC; fi
    $LOAD --addr http://$ADDR --api-key $KEY --trace $TRACE --rates 1000,3000 --duration-secs 12 \
        --warmup-secs 3 --mix snapshot=85,traverse=10,similar=5,add_edge=0 \
        --out-dir "$W/out_read_$target" > "$W/load_read_$target.txt" 2>&1
    {
        echo "# reads served by the $target"
        grep -E "offered|p50|achieved" "$W/load_read_$target.txt" | head -8
    } >> "$OUT/prod8_write_summary.txt"
done
stop_servers
echo "done; see $OUT/prod8_write_summary.txt"
