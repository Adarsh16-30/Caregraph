#!/usr/bin/env bash
#
# run_demo.sh — PRD Phase 8's end-to-end demo: seed patients, replay
# mutations, show traversal/snapshot/similarity queries live.
#
# Runs against a real caregraph server backed by a real RocksDB instance
# and a real trained GraphSAGE model, on a real slice of the Diabetes 130
# clinical dataset (Rule 6 — no synthetic patient data anywhere in this
# path). One command, no manual steps, and safe to re-run: it seeds its own
# fresh database under data/db/demo/ every time rather than depending on
# state a previous run left behind.
#
# What "seed patients, replay mutations" means concretely: most of the
# trace is loaded structurally with `caregraph-load` (fast, no embeddings —
# see that binary's own module doc for why embeddings are deliberately
# absent from that path, Rule 3). The final real encounter for the last
# three patients in the trace is deliberately held back from that bulk
# load and replayed instead through live gRPC AddEdge calls
# (src/bin/demo_client.rs) — the same distinction between bulk historical
# load and live Phase-6 arrival that load_trace.rs's own module doc draws.
# Every mutation you see land here computes a real embedding atomically,
# and every traversal/snapshot/similarity answer after it is read straight
# back out of the database those live calls just wrote to.
#
# Usage: bash scripts/run_demo.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TRACE="benchmarks/traces/diabetes130_smoke_100.jsonl"
# The timestamp of the first held-out edge (patient 7's second encounter,
# see the module doc above) — every add_edge at or after this instant is
# replayed live instead of bulk-loaded. Every upsert_node and remove_edge
# record is always bulk-loaded regardless of its own timestamp, because
# there is no gRPC RPC for node upserts, and holding out a removal live
# would add nothing this demo's four RPCs already need to demonstrate.
LIVE_CUTOFF_US=1223168914285645

DEMO_TMP=".demo_tmp"
DEMO_DB="data/db/demo"
GRPC_ADDR="127.0.0.1:50061"
METRICS_ADDR="127.0.0.1:9101"

# ---------------------------------------------------------------------------
# Preflight — fail fast with a clear message rather than partway through a
# multi-minute release build.
# ---------------------------------------------------------------------------
command -v cargo >/dev/null 2>&1 || {
    echo "cargo not found — see docs/TOOLCHAIN.md" >&2
    exit 1
}
command -v protoc >/dev/null 2>&1 || {
    echo "protoc not found on PATH — see docs/TOOLCHAIN.md's new protoc section" >&2
    exit 1
}
if [[ ! -f "$TRACE" ]]; then
    echo "$TRACE is missing — this repository ships it committed; check your checkout" >&2
    exit 1
fi
if [[ ! -f "ml/deployed/diabetes130_graphsage/model.pt" ]]; then
    echo "ml/deployed/diabetes130_graphsage/model.pt is missing — this demo needs a real" >&2
    echo "trained model (Rule 3). Run ml/train_graphsage.py, or check your checkout." >&2
    exit 1
fi
python3 -c "import torch, torch_geometric" >/dev/null 2>&1 || python -c "import torch, torch_geometric" >/dev/null 2>&1 || {
    echo "python with torch + torch_geometric not found — ml/embedding_server.py needs" >&2
    echo "both. See the CI workflow's own 'Install torch + torch_geometric' step for" >&2
    echo "the exact pip invocation this project uses." >&2
    exit 1
}

# ---------------------------------------------------------------------------
# Build.
# ---------------------------------------------------------------------------
echo "== Building caregraph, caregraph-load, caregraph-demo-client (release) =="
cargo build --release --bin caregraph --bin caregraph-load --bin caregraph-demo-client

# ---------------------------------------------------------------------------
# Split the trace: everything except the held-out live edges goes through
# the bulk loader; the held-out edges get replayed live by demo_client.
# A small inline script rather than grep/awk because the split has to
# parse JSON to tell "op" and "timestamp_us" apart from properties that
# might themselves contain those substrings — same reasoning the project's
# other data-handling code (data/*_loader.py) already follows.
# ---------------------------------------------------------------------------
echo "== Splitting $TRACE into a bulk-load pass and a live-replay pass =="
rm -rf "$DEMO_TMP"
mkdir -p "$DEMO_TMP"

PYTHON_BIN="python3"
command -v python3 >/dev/null 2>&1 || PYTHON_BIN="python"

"$PYTHON_BIN" - "$TRACE" "$LIVE_CUTOFF_US" "$DEMO_TMP/bulk.jsonl" "$DEMO_TMP/live.jsonl" <<'PYEOF'
import json
import sys

trace_path, cutoff, bulk_path, live_path = sys.argv[1:5]
cutoff = int(cutoff)

bulk_count = 0
live_count = 0

with open(trace_path, encoding="utf-8") as src, \
     open(bulk_path, "w", encoding="utf-8") as bulk, \
     open(live_path, "w", encoding="utf-8") as live:
    for line in src:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if rec.get("op") == "add_edge" and rec.get("timestamp_us", 0) >= cutoff:
            live.write(line + "\n")
            live_count += 1
        else:
            bulk.write(line + "\n")
            bulk_count += 1

print(f"  bulk: {bulk_count} records -> {bulk_path}")
print(f"  live: {live_count} records -> {live_path}")
if live_count == 0:
    raise SystemExit("no records held out for live replay — LIVE_CUTOFF_US is wrong")
PYEOF

# ---------------------------------------------------------------------------
# Fresh database, bulk load.
# ---------------------------------------------------------------------------
echo "== Seeding a fresh database at $DEMO_DB =="
rm -rf "$DEMO_DB"
mkdir -p "$(dirname "$DEMO_DB")"
./target/release/caregraph-load --trace "$DEMO_TMP/bulk.jsonl" --db "$DEMO_DB"

# ---------------------------------------------------------------------------
# Start the server. Encryption/mTLS are deliberately left off for this demo
# — both are real, opt-in Phase 7 features exercised by their own
# integration tests (tests/integration/encryption_at_rest.rs,
# tests/integration/mtls_test.rs), not by this walkthrough, whose job is
# the four gRPC capability groups, not the transport/at-rest security
# layered underneath them.
# ---------------------------------------------------------------------------
CAREGRAPH_API_KEY="$(openssl rand -hex 32 2>/dev/null || head -c32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
export CAREGRAPH_API_KEY
export CAREGRAPH_DB_PATH="$DEMO_DB"
export CAREGRAPH_GRPC_ADDR="$GRPC_ADDR"
export CAREGRAPH_METRICS_ADDR="$METRICS_ADDR"

echo "== Starting caregraph server on $GRPC_ADDR (metrics on $METRICS_ADDR) =="
./target/release/caregraph > "$DEMO_TMP/server.log" 2>&1 &
SERVER_PID=$!

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        # A plain `kill` does not reach the server's own spawned Python
        # embedding-server child on Windows (no job-object grouping — see
        # tests/fault_injection's module doc for the same limitation hit
        # there). `taskkill /T` kills the whole process tree; the escaped
        # `//` form is needed under Git Bash / MSYS so its path-conversion
        # layer doesn't mangle the single-slash flags into paths.
        #
        # Verified the hard way: under Git Bash / MSYS, `$!` is an
        # MSYS-internal pid, not the real Windows pid `taskkill` expects —
        # `taskkill //PID "$!"` reports "process not found" while the real
        # server keeps right on running. MSYS `ps`'s own output carries the
        # real Windows pid as its 4th column (WINPID); resolve that first
        # and fall back to $SERVER_PID itself if the lookup comes up empty
        # for any reason (e.g. a non-MSYS `ps`, or the process already gone).
        if command -v taskkill >/dev/null 2>&1; then
            local winpid
            winpid="$(ps 2>/dev/null | awk -v p="$SERVER_PID" '$1 == p {print $4}')"
            taskkill //F //T //PID "${winpid:-$SERVER_PID}" >/dev/null 2>&1 || true
        else
            kill "$SERVER_PID" >/dev/null 2>&1 || true
        fi
    fi
}
trap cleanup EXIT

echo "== Waiting for the server to accept connections =="
for _ in $(seq 1 50); do
    if "$PYTHON_BIN" -c "
import socket, sys
host, port = '$GRPC_ADDR'.split(':')
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(0.2)
try:
    s.connect((host, int(port)))
    sys.exit(0)
except OSError:
    sys.exit(1)
"; then
        break
    fi
    sleep 0.5
done

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "server exited before becoming ready — see $DEMO_TMP/server.log" >&2
    cat "$DEMO_TMP/server.log" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Run the live demo.
# ---------------------------------------------------------------------------
echo "== Running the live demo client =="
./target/release/caregraph-demo-client \
    --addr "http://$GRPC_ADDR" \
    --api-key "$CAREGRAPH_API_KEY" \
    --live-edges "$DEMO_TMP/live.jsonl"

echo
echo "== Server log (for reference) =="
tail -n 20 "$DEMO_TMP/server.log"
