#!/usr/bin/env bash
#
# run_baseline.sh — comparative benchmark against real, running baselines.
#
# Rule 4: comparative numbers MUST come from a real, running Neo4j Community +
# GDS instance and a real, running TerminusDB instance. Never published numbers
# from other papers, never estimates. No result may be reported without the
# exact command and hardware spec used to produce it.
#
# This script therefore refuses to produce output unless it has first proven,
# by connecting to them, that both baselines are up. "The container is running"
# is not proof; it queries each one.
#
# Every run writes a timestamped raw-results file. The exit code is the gate:
# check_rules.sh will not accept a benchmark number without one.
#
# Usage:
#   export CAREGRAPH_HARDWARE="Ryzen 7 5800X, 32GB DDR4-3600, Samsung 980 Pro NVMe"
#   benchmarks/run_baseline.sh --trace benchmarks/traces/ukpds_full.jsonl
#
# Options:
#   --trace FILE     mutation trace defining the graph (required)
#   --hops N         traversal depth (default 2)
#   --queries N      queries per system (default 5000)
#   --skip-load      graphs are already loaded; measure only
#   --only SYSTEM    caregraph | neo4j | terminusdb

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TRACE=""
HOPS=2
QUERIES=5000
SKIP_LOAD=0
ONLY=""
RESULTS_DIR="benchmarks/results"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-caregraph-dev-only}"
TERMINUSDB_URL="${TERMINUSDB_URL:-http://localhost:6363}"
TERMINUSDB_USER="${TERMINUSDB_USER:-admin}"
TERMINUSDB_PASSWORD="${TERMINUSDB_PASSWORD:-caregraph-dev-only}"
CAREGRAPH_DB="${CAREGRAPH_DB_PATH:-data/db/caregraph}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --trace)     TRACE="$2";     shift 2 ;;
        --hops)      HOPS="$2";      shift 2 ;;
        --queries)   QUERIES="$2";   shift 2 ;;
        --skip-load) SKIP_LOAD=1;    shift ;;
        --only)      ONLY="$2";      shift 2 ;;
        -h|--help)   sed -n '2,28p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
[[ -t 1 ]] || { RED=""; GREEN=""; YELLOW=""; BOLD=""; RESET=""; }

die()  { printf '%serror:%s %s\n' "$RED" "$RESET" "$*" >&2; exit 1; }
note() { printf '  %s\n' "$*"; }
step() { printf '\n%s==> %s%s\n' "$BOLD" "$*" "$RESET"; }

wants() { [[ -z "$ONLY" || "$ONLY" == "$1" ]]; }

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------

step "Preconditions"

[[ -n "$TRACE" ]] || die "--trace is required"
[[ -f "$TRACE" ]] || die "trace not found: $TRACE

The trace is produced from the real IDPIP UKPDS tables:
    export IDPIP_DATABASE_URL=...
    python data/idpip_ukpds_loader.py --out $TRACE

Rule 6 permits no synthetic substitute in an evaluation path."

if [[ -z "${CAREGRAPH_HARDWARE:-}" ]]; then
    die "CAREGRAPH_HARDWARE is not set.

Rule 4 requires the hardware spec used to produce a benchmark number. Set it to
a description of this machine, for example:

    export CAREGRAPH_HARDWARE=\"Ryzen 7 5800X, 32GB DDR4-3600, 980 Pro NVMe, Ubuntu 24.04\"

An absent spec is preferable to an invented one, so this is a hard stop."
fi

# Resolve a Python that actually runs. On Windows `python3` may resolve to the
# Microsoft Store alias, which exists on PATH but is not an interpreter, so
# presence on PATH is not enough — it has to answer.
PYTHON=""
for candidate in python3 python; do
    if "$candidate" -c 'import sys; sys.exit(0)' >/dev/null 2>&1; then
        PYTHON="$candidate"
        break
    fi
done
[[ -n "$PYTHON" ]] || die "no working Python interpreter found (tried python3, python).
The baseline runners need Python 3.10+ with the neo4j and terminusdb-client packages."

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_DIRTY="clean"
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
    GIT_DIRTY="dirty"
    printf '  %sWARNING%s working tree is dirty; results will not be reproducible\n' \
        "$YELLOW" "$RESET"
    printf '          from commit %s alone.\n' "$GIT_COMMIT"
fi

note "run id:    $RUN_ID"
note "commit:    $GIT_COMMIT ($GIT_DIRTY)"
note "hardware:  $CAREGRAPH_HARDWARE"
note "trace:     $TRACE"
note "workload:  ${HOPS}-hop traversal, $QUERIES queries per system"

mkdir -p "$RESULTS_DIR"

# ---------------------------------------------------------------------------
# Prove the baselines are actually running (Rule 4)
# ---------------------------------------------------------------------------

step "Verifying baselines are live"

if wants neo4j; then
    "$PYTHON" benchmarks/baselines/neo4j_runner.py ping \
        --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASSWORD" \
        || die "Neo4j is not reachable at $NEO4J_URI.

Rule 4 forbids substituting published or estimated figures for a baseline that
is not running. Start the stack first:

    docker compose -f infrastructure/docker-compose/dev-stack.yml up -d neo4j"
    note "${GREEN}Neo4j + GDS reachable${RESET} at $NEO4J_URI"
fi

if wants terminusdb; then
    "$PYTHON" benchmarks/baselines/terminusdb_runner.py ping \
        --url "$TERMINUSDB_URL" --user "$TERMINUSDB_USER" --password "$TERMINUSDB_PASSWORD" \
        || die "TerminusDB is not reachable at $TERMINUSDB_URL.

    docker compose -f infrastructure/docker-compose/dev-stack.yml up -d terminusdb"
    note "${GREEN}TerminusDB reachable${RESET} at $TERMINUSDB_URL"
fi

# ---------------------------------------------------------------------------
# Load the identical graph into every system
# ---------------------------------------------------------------------------

if (( SKIP_LOAD == 0 )); then
    step "Loading the identical graph into every system"

    if wants caregraph; then
        note "caregraph ..."
        cargo run --release --quiet --bin caregraph-load -- \
            --trace "$TRACE" --db "$CAREGRAPH_DB"
    fi
    if wants neo4j; then
        note "neo4j ..."
        "$PYTHON" benchmarks/baselines/neo4j_runner.py load \
            --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASSWORD" \
            --trace "$TRACE"
    fi
    if wants terminusdb; then
        note "terminusdb ..."
        "$PYTHON" benchmarks/baselines/terminusdb_runner.py load \
            --url "$TERMINUSDB_URL" --user "$TERMINUSDB_USER" \
            --password "$TERMINUSDB_PASSWORD" --trace "$TRACE"
    fi
else
    step "Skipping load (--skip-load)"
    printf '  %sWARNING%s the three systems are assumed to hold identical graphs.\n' \
        "$YELLOW" "$RESET"
    printf '          An apples-to-apples claim is only valid if that is true.\n'
fi

# ---------------------------------------------------------------------------
# Measure
# ---------------------------------------------------------------------------

step "Measuring ${HOPS}-hop bounded traversal"

WROTE_ANY=0

if wants caregraph; then
    note "caregraph ..."
    cargo run --release --quiet --bin caregraph-bench-traversal -- \
        --db "$CAREGRAPH_DB" --trace "$TRACE" \
        --hops "$HOPS" --queries "$QUERIES" --out-dir "$RESULTS_DIR"
    WROTE_ANY=1
fi

if wants neo4j; then
    note "neo4j + gds ..."
    "$PYTHON" benchmarks/baselines/neo4j_runner.py bench \
        --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASSWORD" \
        --trace "$TRACE" --hops "$HOPS" --queries "$QUERIES" \
        --out-dir "$RESULTS_DIR"
    WROTE_ANY=1
fi

if wants terminusdb; then
    note "terminusdb ..."
    "$PYTHON" benchmarks/baselines/terminusdb_runner.py bench \
        --url "$TERMINUSDB_URL" --user "$TERMINUSDB_USER" \
        --password "$TERMINUSDB_PASSWORD" \
        --trace "$TRACE" --hops "$HOPS" --queries "$QUERIES" \
        --out-dir "$RESULTS_DIR"
    WROTE_ANY=1
fi

(( WROTE_ANY == 1 )) || die "no system was benchmarked (--only $ONLY matched nothing)"

# ---------------------------------------------------------------------------
# Run manifest — what Rule 10 citations point at
# ---------------------------------------------------------------------------

step "Writing run manifest"

MANIFEST="$RESULTS_DIR/run_${RUN_ID}.manifest.json"
"$PYTHON" - "$MANIFEST" <<PY
import json, os, subprocess, sys, glob, time

manifest_path = sys.argv[1]
results = sorted(
    p for p in glob.glob("$RESULTS_DIR/*.json")
    if not p.endswith(".manifest.json")
    and os.path.getmtime(p) >= time.time() - 86400
)

json.dump({
    "run_id": "$RUN_ID",
    "git_commit": "$GIT_COMMIT",
    "git_state": "$GIT_DIRTY",
    "hardware": os.environ["CAREGRAPH_HARDWARE"],
    "command": "benchmarks/run_baseline.sh --trace $TRACE --hops $HOPS --queries $QUERIES",
    "trace": "$TRACE",
    "hops": $HOPS,
    "queries_per_system": $QUERIES,
    "systems": ["caregraph", "neo4j-gds", "terminusdb"] if not "$ONLY" else ["$ONLY"],
    "result_files": results,
}, open(manifest_path, "w"), indent=2)
print(f"  manifest: {manifest_path}")
PY

printf '\n%sBaseline run complete.%s\n' "$GREEN" "$RESET"
printf 'Cite results as [benchmark: %s] (Rule 10).\n' "$MANIFEST"
