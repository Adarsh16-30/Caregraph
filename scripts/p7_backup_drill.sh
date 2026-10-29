#!/usr/bin/env bash
# Backup / restore drill on a copy of the real diabetes130 database, with
# measured wall times. Produces the output committed as
# benchmarks/results/gate/prod7_backup_restore.txt.
#
# Usage: bash scripts/p7_backup_drill.sh <scratch-dir>
set -euo pipefail

W="${1:?usage: p7_backup_drill.sh <scratch-dir>}"
ADMIN=target/release/caregraph-admin
SERVER=target/release/caregraph
SRC=data/db/diabetes130
ADDR=127.0.0.1:50081
METRICS=127.0.0.1:9181
KEY=drill-key

now_ms() { echo $(( $(date +%s%N) / 1000000 )); }
timed() { # timed <label> <command...>
    local label="$1"; shift
    local t0 t1
    t0=$(now_ms); "$@"; t1=$(now_ms)
    echo "  [$label: $((t1 - t0)) ms]"
}
dir_bytes() { du -sb "$1" 2>/dev/null | cut -f1; }

rm -rf "$W"; mkdir -p "$W"
echo "== commit $(git rev-parse --short HEAD), $(date -u +%FT%TZ)"
echo "== source database: $SRC ($(dir_bytes "$SRC") bytes)"

echo; echo "== 1. encrypt a copy of the real database under a fresh data key"
$ADMIN kek new --out "$W/kek"
timed reencrypt $ADMIN reencrypt --src "$SRC" --plaintext --dst "$W/db" \
    --new-kek-file "$W/kek" --new-wrapped-dek "$W/dek.json"
echo "  encrypted database: $(dir_bytes "$W/db") bytes"

echo; echo "== 2. start the server with backups enabled"
export CAREGRAPH_DB_PATH="$W/db" CAREGRAPH_KEK_FILE="$W/kek" CAREGRAPH_WRAPPED_DEK="$W/dek.json"
export CAREGRAPH_BACKUP_DIR="$W/backups" CAREGRAPH_API_KEY="$KEY"
export CAREGRAPH_GRPC_ADDR="$ADDR" CAREGRAPH_METRICS_ADDR="$METRICS"
export CAREGRAPH_GRAPHSAGE_MODEL=none CAREGRAPH_GAT_MODEL=none
$SERVER > "$W/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true; taskkill //F //PID $SERVER_PID >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 60); do
    curl -fs "http://$METRICS/readyz" >/dev/null 2>&1 && break
    sleep 0.5
done
curl -fs "http://$METRICS/healthz" >/dev/null && echo "  server healthy"

R="--addr http://$ADDR --token $KEY"
echo; echo "== 3. admin RPCs against the running server"
$ADMIN remote status $R
timed backup-1 $ADMIN remote create-backup $R
timed backup-2-nothing-changed $ADMIN remote create-backup $R
echo "  backup directory after two backups: $(dir_bytes "$W/backups") bytes"
$ADMIN remote list-backups $R
timed checkpoint $ADMIN remote checkpoint $R c1
echo "  checkpoint directory: $(dir_bytes "$W/backups/checkpoints/c1") bytes (hard links, shares the database's files)"
timed verify-audit $ADMIN remote verify-audit $R
echo "  metrics:"; curl -s "http://$METRICS/metrics" | grep -E '^backup_' | sed 's/^/    /'

kill $SERVER_PID 2>/dev/null || true
taskkill //F //PID $SERVER_PID >/dev/null 2>&1 || true
sleep 2

echo; echo "== 4. offline: verify, restore, compare"
unset CAREGRAPH_API_KEY
timed verify-backup-2 $ADMIN backup verify --dir "$W/backups" --id 2 \
    --scratch "$W/verify-scratch"
timed restore $ADMIN backup restore --dir "$W/backups" --id 2 --to "$W/restored"
echo "  restored database: $(dir_bytes "$W/restored") bytes"
echo "  original:"; $ADMIN check --db "$W/db" | sed 's/^/    /'
echo "  restored:"; $ADMIN check --db "$W/restored" | sed 's/^/    /'
diff <($ADMIN check --db "$W/db") <($ADMIN check --db "$W/restored") \
    && echo "  per-column-family key counts and audit chain are identical"

echo; echo "== 5. the wrong key, and no key, are refused"
# The CLI also reads CAREGRAPH_KEK_FILE / CAREGRAPH_WRAPPED_DEK from the
# environment, so clear them to make "no key" really mean none.
unset CAREGRAPH_KEK_FILE CAREGRAPH_WRAPPED_DEK CAREGRAPH_ENCRYPTION_KEY
$ADMIN kek new --out "$W/other-kek" >/dev/null
echo "  with another key-encryption key:"
$ADMIN backup list --dir "$W/backups" --kek-file "$W/other-kek" --wrapped-dek "$W/dek.json" 2>&1 | sed 's/^/    /' || true
echo "  with a data key that is not the one the backups were made under:"
$ADMIN kek new-dek --kek-file "$W/other-kek" --out "$W/other-dek.json" >/dev/null
$ADMIN backup list --dir "$W/backups" --kek-file "$W/other-kek" --wrapped-dek "$W/other-dek.json" 2>&1 | sed 's/^/    /' || true
echo "  with no key at all:"
$ADMIN backup list --dir "$W/backups" 2>&1 | sed 's/^/    /' || true
echo "  declaring the directory plaintext:"
$ADMIN backup list --dir "$W/backups" --plaintext 2>&1 | sed 's/^/    /' || true
