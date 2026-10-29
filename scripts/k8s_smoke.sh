#!/usr/bin/env bash
#
# Kubernetes smoke test on a throwaway kind cluster: install the chart, write through the
# write Service, read from the read replica, fail over to the standby, and confirm the
# Services followed and the data survived.
#
# What it exercises is the Kubernetes mechanics: the init container's start decision,
# the checkpoint bootstrap of the standby and the reader, the role-label sidecar, the
# Services, replication, and an operator-run failover. It runs the chart in development
# mode (plaintext, one API key, a database seeded from the node's disk); the production
# security posture is covered by the security test suite and `helm lint` with
# values-production.yaml.
#
# Needs: docker, kind, kubectl, helm, cargo, python3. Runs in CI (.github/workflows/k8s.yml).
# Usage: bash scripts/k8s_smoke.sh      (KEEP=1 leaves the cluster running)

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

for t in docker kind kubectl helm cargo; do
    command -v "$t" >/dev/null 2>&1 || { echo "$t not found" >&2; exit 1; }
done
PY="$(command -v python3 || command -v python)" || { echo "python not found" >&2; exit 1; }

CLUSTER="${CLUSTER:-cg-smoke}"
NS=cg
REL=cg
FULL="$REL-caregraph"
KEY=kind-smoke-key
TRACE=benchmarks/traces/diabetes130_smoke_100.jsonl
LIVE_CUTOFF_US=1223168914285645
W="$(mktemp -d)"
PIDS=()

say() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; dump; exit 1; }

dump() {
    kubectl -n "$NS" get pods -o wide -L caregraph.io/role 2>&1 || true
    for p in "$FULL-writer-0" "$FULL-writer-1" "$FULL-reader-0"; do
        for c in init-role bootstrap caregraph role-label; do
            echo "--- $p / $c"
            kubectl -n "$NS" logs "$p" -c "$c" --tail=40 2>&1 | tail -40 || true
        done
    done
}

cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" >/dev/null 2>&1 || true; done
    if [[ -z "${KEEP:-}" ]]; then
        kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
        rm -rf "$W"
    fi
}
trap cleanup EXIT

forward() { # forward <pod|svc/name> <local-port>
    kubectl -n "$NS" port-forward "$1" "$2:50051" >"$W/pf-$2.log" 2>&1 &
    PIDS+=("$!")
    for _ in $(seq 1 40); do
        "$PY" - "$2" <<'PY' && return 0 || sleep 0.25
import socket, sys
s = socket.socket(); s.settimeout(0.2)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); sys.exit(0)
except OSError:
    sys.exit(1)
PY
    done
    fail "port-forward to $1 never came up"
}

ADMIN=target/release/caregraph-admin
status() { # status <port> <field>
    "$ADMIN" --json remote status --addr "http://127.0.0.1:$1" --token "$KEY" |
        "$PY" -c "import json,sys; print(json.load(sys.stdin)['$2'])"
}

# wait_for <what> <timeout-secs> '<shell expression>': the expression is evaluated again on
# every attempt (so command substitutions inside it are re-run), until it succeeds.
wait_for() {
    local what=$1 deadline=$((SECONDS + $2)) expr=$3
    until eval "$expr" >/dev/null 2>&1; do
        (( SECONDS < deadline )) || fail "timed out waiting for $what"
        sleep 3
    done
}

role_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.metadata.labels.caregraph\.io/role}' 2>/dev/null; }

# ---------------------------------------------------------------------------
say "Building the CLI tools and seeding a database from the real smoke trace"
cargo build --release --bin caregraph-load --bin caregraph-admin --bin caregraph-demo-client

"$PY" - "$TRACE" "$LIVE_CUTOFF_US" "$W/bulk.jsonl" "$W/live.jsonl" <<'PY'
import json, sys
trace, cutoff, bulk_path, live_path = sys.argv[1:5]
cutoff = int(cutoff)
live = []
with open(trace, encoding="utf-8") as src, open(bulk_path, "w", encoding="utf-8") as bulk:
    for line in src:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if rec.get("op") == "add_edge" and rec.get("timestamp_us", 0) >= cutoff:
            live.append(line)
        else:
            bulk.write(line + "\n")
half = len(live) // 2
open(live_path, "w", encoding="utf-8").write("\n".join(live[:half]) + "\n")
open(live_path + ".after", "w", encoding="utf-8").write("\n".join(live[half:]) + "\n")
print(f"live edges: {half} before the failover, {len(live) - half} after")
if half == 0 or len(live) == half:
    raise SystemExit("not enough held-out edges to split")
PY
mkdir -p "$W/seed"
# Docker needs the host path in its own notation (a Windows path under Git Bash).
SEED_HOST="$W/seed"
command -v cygpath >/dev/null 2>&1 && SEED_HOST="$(cygpath -m "$W/seed")"
target/release/caregraph-load --trace "$W/bulk.jsonl" --db "$W/seed/db"
chmod -R a+rX "$W/seed"

# ---------------------------------------------------------------------------
say "Creating the kind cluster"
cat >"$W/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: $SEED_HOST
        containerPath: /seed
YAML
kind create cluster --name "$CLUSTER" --config "$W/kind.yaml" --wait 180s

say "Building and loading the images"
docker build -f infrastructure/docker-compose/Dockerfile --target runtime-writer -t local/caregraph-writer:smoke .
docker build -f infrastructure/docker-compose/Dockerfile --target runtime-reader -t local/caregraph-reader:smoke .
kind load docker-image local/caregraph-writer:smoke local/caregraph-reader:smoke --name "$CLUSTER"

# ---------------------------------------------------------------------------
say "Installing the chart"
helm lint infrastructure/helm/caregraph -f infrastructure/helm/caregraph/values-kind.yaml
helm upgrade --install "$REL" infrastructure/helm/caregraph -n "$NS" --create-namespace \
    -f infrastructure/helm/caregraph/values-kind.yaml

wait_for "the first leader to be ready" 900 \
    'kubectl -n "$NS" wait --for=condition=ready "pod/$FULL-writer-0" --timeout=10s'
wait_for "the standby to be ready" 900 \
    'kubectl -n "$NS" wait --for=condition=ready "pod/$FULL-writer-1" --timeout=10s'
wait_for "the read replica to be ready" 900 \
    'kubectl -n "$NS" wait --for=condition=ready "pod/$FULL-reader-0" --timeout=10s'

say "Roles: exactly one leader, and the Services follow the labels"
wait_for "the role labels" 60 \
    '[ "$(role_of "$FULL-writer-0")" = leader ] && [ "$(role_of "$FULL-writer-1")" = follower ]'
[[ -z "$(role_of "$FULL-reader-0")" ]] || fail "the reader must not carry a writer role label"
leader_ep="$(kubectl -n "$NS" get endpoints "$FULL-write" -o jsonpath='{.subsets[*].addresses[*].targetRef.name}')"
[[ "$leader_ep" == "$FULL-writer-0" ]] || fail "the write Service points at '$leader_ep', expected $FULL-writer-0"
standby_ep="$(kubectl -n "$NS" get endpoints "$FULL-standby" -o jsonpath='{.subsets[*].addresses[*].targetRef.name}')"
[[ "$standby_ep" == "$FULL-writer-1" ]] || fail "the standby Service points at '$standby_ep'"

# ---------------------------------------------------------------------------
say "Writing through the write Service"
forward "svc/$FULL-write" 50061
target/release/caregraph-demo-client --addr http://127.0.0.1:50061 --api-key "$KEY" --live-edges "$W/live.jsonl"
seq_after_writes="$(status 50061 latest_sequence)"
echo "leader sequence after the writes: $seq_after_writes"

say "The standby and the read replica converge to the leader's sequence"
forward "pod/$FULL-writer-1" 50062
forward "svc/$FULL-read" 50063
wait_for "the standby to catch up" 120 '[ "$(status 50062 latest_sequence)" -ge "$seq_after_writes" ]'
wait_for "the read replica to catch up" 120 '[ "$(status 50063 latest_sequence)" -ge "$seq_after_writes" ]'
[[ "$(status 50062 role)" == follower ]] || fail "writer-1 is not a follower"
[[ "$(status 50063 ready_for_reads)" == True ]] || fail "the reader is not ready for reads"

# ---------------------------------------------------------------------------
say "Operator failover: writer-0 -> writer-1"
forward "pod/$FULL-writer-0" 50064
"$ADMIN" remote failover --addr http://127.0.0.1:50064 --new-addr http://127.0.0.1:50062 --token "$KEY"
wait_for "the labels to move" 90 '[ "$(role_of "$FULL-writer-1")" = leader ]'
wait_for "the write Service to point at the new leader" 90 \
    '[ "$(kubectl -n "$NS" get endpoints "$FULL-write" -o jsonpath="{.subsets[*].addresses[*].targetRef.name}")" = "$FULL-writer-1" ]'
[[ "$(status 50062 role)" == leader ]] || fail "writer-1 did not become the leader"
old_role="$(status 50064 role)"
[[ "$old_role" == demoted || "$old_role" == fenced ]] || fail "the old leader is '$old_role', expected demoted or fenced"
[[ "$(role_of "$FULL-writer-0")" != leader ]] || fail "the old leader still carries the leader label"

say "Writing through the same Service after the failover, and reading it on the replica"
kill "${PIDS[0]}" >/dev/null 2>&1 || true
forward "svc/$FULL-write" 50065
target/release/caregraph-demo-client --addr http://127.0.0.1:50065 --api-key "$KEY" --live-edges "$W/live.jsonl.after"
new_seq="$(status 50065 latest_sequence)"
[[ "$new_seq" -gt "$seq_after_writes" ]] || fail "no write landed on the new leader ($new_seq vs $seq_after_writes)"
wait_for "the read replica to follow the new leader" 120 '[ "$(status 50063 latest_sequence)" -ge "$new_seq" ]'

say "The old leader restarts and does NOT become a second leader"
kubectl -n "$NS" delete pod "$FULL-writer-0" --wait=true
wait_for "the old leader to come back up (alive, not ready)" 300 \
    'kubectl -n "$NS" wait --for=condition=initialized "pod/$FULL-writer-0" --timeout=10s'
sleep 20
[[ "$(role_of "$FULL-writer-0")" != leader ]] || fail "the restarted old leader labelled itself leader"
[[ "$(role_of "$FULL-writer-1")" == leader ]] || fail "the new leader lost its label"

say "SMOKE TEST PASSED"
