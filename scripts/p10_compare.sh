#!/usr/bin/env bash
#
# Concurrent-load comparison of CareGraph, Neo4j and TerminusDB on the same graph, with
# the same load generator (caregraph-bench-load), one system at a time.
#
# For each system and each workload it runs an open-loop rate sweep (the offered rate
# doubles until the system cannot keep up), and repeats the whole sweep REPEATS times so
# the spread between runs is part of the result.
#
# Layout of a run (Windows laptop, Git Bash):
#   * CareGraph runs natively on a scratch copy of data/db/diabetes130, pinned to four
#     logical CPUs, memory budget CAREGRAPH_MEMORY_MB.
#   * Neo4j and TerminusDB run in Docker with the same CPU count and a memory ceiling
#     (dev-stack.yml: BASELINE_CPUS, NEO4J_MEM_LIMIT, ...), loaded beforehand with
#     benchmarks/baselines/*_runner.py load from the same trace.
#   * The load generator runs on the host. Traffic to the containers crosses Docker
#     Desktop's port forwarding and CareGraph's does not; the `ping` workload measures
#     exactly that overhead for every system so it can be read next to the results.
#
# Usage: bash scripts/p10_compare.sh <out-dir> [systems=caregraph,neo4j,terminusdb] [repeats=3]
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

OUT="${1:?usage: p10_compare.sh <out-dir> [systems] [repeats]}"
SYSTEMS="${2:-caregraph,neo4j,terminusdb}"
REPEATS="${3:-3}"
TRACE=benchmarks/traces/diabetes130_full.jsonl
LOAD=target/release/caregraph-bench-load
SERVER=target/release/caregraph
COMPOSE="docker compose -f infrastructure/docker-compose/dev-stack.yml"
KEY=bench-key
CPUS=4
DURATION="${DURATION:-12}"
WARMUP="${WARMUP:-3}"
export NEO4J_MEM_LIMIT="${NEO4J_MEM_LIMIT:-3g}" NEO4J_HEAP="${NEO4J_HEAP:-1G}" NEO4J_PAGECACHE="${NEO4J_PAGECACHE:-1G}"
export TERMINUSDB_MEM_LIMIT="${TERMINUSDB_MEM_LIMIT:-3g}" BASELINE_CPUS=$CPUS

mkdir -p "$OUT"
SERVER_PID=""

log() { printf '\n== %s %s\n' "$(date +%T)" "$*"; }

stop_all() {
    if [[ -n "$SERVER_PID" ]]; then
        taskkill //F //T //PID "$SERVER_PID" >/dev/null 2>&1 || true
        taskkill //F //IM caregraph.exe >/dev/null 2>&1 || true
        SERVER_PID=""
    fi
    $COMPOSE stop neo4j terminusdb >/dev/null 2>&1 || true
}
trap stop_all EXIT

# Workloads and the rate each sweep starts from, per system. A sweep doubles the rate
# until the system saturates (the load generator's --stop-when-saturated).
# Every system is driven at the same offered rates (a lattice that doubles from a common
# start), so latencies can be compared at the same load. The sweep ends at the first
# rate a system cannot sustain (the load generator's --stop-when-saturated).
workloads() { # system -> "name:mix:start-rate[:extra args, + for spaces] ..."
    local common="ping:ping=100:100 snapshot:snapshot=100:10 traverse:traverse=100:5 mixed:snapshot=90,traverse=10:10"
    case "$1" in
        caregraph)  echo "$common" ;;
        # The same traversal, with the answer bounded at the 10,000 nodes CareGraph's own
        # limit allows, so Neo4j is not asked for more than CareGraph can ever return.
        neo4j)      echo "$common traverse_bounded:traverse=100:5:--neo4j-limit+10000" ;;
        # TerminusDB sustains far lower rates, so it is also driven at a literal list of
        # low rates (L, rates joined by /); 10, 20, 40, ... coincide with the lattice above.
        terminusdb) echo "ping:ping=100:100 snapshot:snapshot=100:L2/5/10/20/40/80/160/320 traverse:traverse=100:L1/2/5 mixed:snapshot=90,traverse=10:L2/5/10/20/40/80" ;;
    esac
}

rates_from() { # start -> doubling list, 12 steps
    local r=$1 out=""
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12; do out+="${out:+,}$r"; r=$((r * 2)); done
    echo "$out"
}

start_caregraph() {
    log "starting CareGraph on a scratch copy of the database"
    rm -rf "$OUT/cg-db"; cp -r data/db/diabetes130 "$OUT/cg-db"
    (
        export CAREGRAPH_API_KEY=$KEY CAREGRAPH_DB_PATH="$OUT/cg-db" CAREGRAPH_GRPC_ADDR=127.0.0.1:50091
        export CAREGRAPH_METRICS_ADDR=127.0.0.1:9191 CAREGRAPH_GRAPHSAGE_MODEL=none CAREGRAPH_GAT_MODEL=none
        export CAREGRAPH_MEMORY_MB=2048
        exec "$SERVER"
    ) >"$OUT/caregraph-server.log" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 120); do curl -fs http://127.0.0.1:9191/readyz >/dev/null 2>&1 && break; sleep 1; done
    # Four logical CPUs, the same count the containers get; the load generator and the
    # rest of the system use the others.
    powershell -NoProfile -Command "(Get-Process caregraph).ProcessorAffinity = 0xF" >/dev/null 2>&1 || true
    ADDR_ARGS=(--target caregraph --addr http://127.0.0.1:50091 --api-key "$KEY")
}

start_neo4j() {
    log "starting Neo4j (${CPUS} CPUs, $NEO4J_MEM_LIMIT, heap $NEO4J_HEAP, page cache $NEO4J_PAGECACHE)"
    $COMPOSE up -d neo4j >/dev/null
    for _ in $(seq 1 90); do
        docker exec caregraph-neo4j cypher-shell -u neo4j -p caregraph-dev-only "RETURN 1" >/dev/null 2>&1 && break; sleep 3
    done
    local counts
    counts=$(docker exec caregraph-neo4j cypher-shell -u neo4j -p caregraph-dev-only --format plain \
        "MATCH (n:GraphNode) WITH count(n) AS n MATCH ()-[r]->() RETURN n, count(r)" | tail -1)
    echo "neo4j holds: nodes, relationships = $counts" | tee "$OUT/neo4j-counts.txt"
    ADDR_ARGS=(--target neo4j --neo4j-uri 127.0.0.1:7687)
}

start_terminus() {
    log "starting TerminusDB (${CPUS} CPUs, $TERMINUSDB_MEM_LIMIT)"
    $COMPOSE up -d terminusdb >/dev/null
    for _ in $(seq 1 60); do curl -fs -u admin:caregraph-dev-only http://127.0.0.1:6363/api/info >/dev/null 2>&1 && break; sleep 2; done
    ADDR_ARGS=(--target terminusdb --terminus-url http://127.0.0.1:6363)
}

for system in ${SYSTEMS//,/ }; do
    stop_all
    case "$system" in
        caregraph) start_caregraph ;;
        neo4j) start_neo4j ;;
        terminusdb) start_terminus ;;
        *) echo "unknown system $system" >&2; exit 1 ;;
    esac
    commit="$(git rev-parse --short HEAD)"
    echo "system=$system commit=$commit tree_dirty=$(git status --porcelain | wc -l)" > "$OUT/$system.provenance.txt"

    log "$system: warm-up (not recorded)"
    "$LOAD" "${ADDR_ARGS[@]}" --trace "$TRACE" --rates "$(rates_from 20 | cut -d, -f1-3)" \
        --duration-secs 8 --warmup-secs 2 --mix snapshot=80,traverse=20 \
        --out-dir "$OUT/$system/warmup" --stop-when-saturated >/dev/null 2>&1 || true

    for rep in $(seq 1 "$REPEATS"); do
        for spec in $(workloads "$system"); do
            IFS=: read -r name mix start extra <<<"$spec"
            extra="${extra//+/ }"
            dir="$OUT/$system/$name/rep$rep"
            # A TerminusDB query abandoned by the client keeps running on the server
            # (it used 4 CPUs for minutes after the client gave up), so start every
            # TerminusDB workload from a freshly restarted server.
            if [[ "$system" == terminusdb ]]; then
                $COMPOSE restart terminusdb >/dev/null 2>&1
                for _ in $(seq 1 60); do curl -fs -u admin:caregraph-dev-only http://127.0.0.1:6363/api/info >/dev/null 2>&1 && break; sleep 2; done
                # A freshly started TerminusDB answers its first queries very slowly
                # while it pages the graph in (every request of a 12 s step timed out
                # without this). Warm it with a trickle of snapshots, not recorded.
                "$LOAD" "${ADDR_ARGS[@]}" --trace "$TRACE" --rates 2 --duration-secs 60 --warmup-secs 0                     --mix snapshot=100 --request-timeout-secs 120 --out-dir "$OUT/$system/restart-warmup" >/dev/null 2>&1 || true
            fi
            log "$system $name repeat $rep (start $start/s)"
            if [[ "$start" == L* ]]; then
                rates="${start#L}"; rates="${rates//\//,}"
            else
                rates="$(rates_from "$start")"
            fi
            "$LOAD" "${ADDR_ARGS[@]}" --trace "$TRACE" --rates "$rates" \
                --duration-secs "$DURATION" --warmup-secs "$WARMUP" --mix "$mix" \
                --out-dir "$dir" --stop-when-saturated --seed "$((rep * 7919))"                 --request-timeout-secs 10 $extra 2>&1 | grep -E "achieved|saturated" || true
        done
    done
    stop_all
done
log "done: $OUT"
