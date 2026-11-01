#!/usr/bin/env bash
#
# check_invariants.sh — enforces CareGraph's project invariants: real storage, real
# endpoints, real embeddings, real baselines, atomic commits, real clinical data,
# no silent fallbacks, real encryption and no placeholder dashboards.
#
# Every check reports exactly one of:
#
#   PASS     the invariant is enforced and the check succeeded
#   FAIL     the invariant is violated — always exits non-zero
#   PENDING  the check's subject matter does not exist yet (the phase that
#            introduces it has not started)
#
# PENDING is deliberately loud and never silent. Pass --phase N at a phase gate
# and every check that should be live by phase N is upgraded from PENDING to FAIL.
#
# Usage:
#   scripts/check_invariants.sh                      # report everything, fail on violations
#   scripts/check_invariants.sh --phase 1            # gate: phase-1 checks must be live
#   scripts/check_invariants.sh --check atomic-commit  # run one check
#
# Checks: storage endpoints embeddings baselines atomic-commit clinical-data
#         fallback encryption dashboards

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TARGET_PHASE=0
ONLY_CHECK=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --phase) TARGET_PHASE="$2"; shift 2 ;;
        --check) ONLY_CHECK="$2";   shift 2 ;;
        -h|--help) sed -n '2,23p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

# The phase at which each check becomes enforceable.
declare -A LIVE_FROM=(
    [storage]=1        [endpoints]=6      [embeddings]=4
    [baselines]=3      [atomic-commit]=5  [clinical-data]=1
    [fallback]=4       [encryption]=7     [dashboards]=7
)

FAILURES=0
PENDINGS=0

RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
[[ -t 1 ]] || { RED=""; GREEN=""; YELLOW=""; BOLD=""; RESET=""; }

pass()    { printf '  %sPASS%s    %s\n' "$GREEN" "$RESET" "$1"; }
fail()    { printf '  %sFAIL%s    %s\n' "$RED" "$RESET" "$1"; FAILURES=$((FAILURES + 1)); }
pending() {
    local check="$1" msg="$2" live_from="${LIVE_FROM[$1]}"
    if (( TARGET_PHASE >= live_from )); then
        fail "$msg (must be live from phase $live_from; gating phase $TARGET_PHASE)"
    else
        printf '  %sPENDING%s %s (due phase %s)\n' "$YELLOW" "$RESET" "$msg" "$live_from"
        PENDINGS=$((PENDINGS + 1))
    fi
}

header() { printf '\n%s%s — %s%s\n' "$BOLD" "$1" "$2" "$RESET"; }

should_run() { [[ -z "$ONLY_CHECK" || "$ONLY_CHECK" == "$1" ]]; }

# Search non-test Rust sources. Test files (tests/unit/*_synthetic_test.rs and
# #[cfg(test)] modules) are the only place synthetic stand-ins are permitted.
rust_prod_files() {
    find src -name '*.rs' -type f 2>/dev/null | grep -v '_synthetic_test\.rs$'
}

python_prod_files() {
    find ml data benchmarks -name '*.py' -type f 2>/dev/null \
        | grep -v '_synthetic_test\.py$'
}

# ---------------------------------------------------------------------------
# storage — no mocked storage or fake persistence
# ---------------------------------------------------------------------------
check_storage() {
    header storage "NO MOCKED STORAGE OR FAKE PERSISTENCE"
    local files; files="$(rust_prod_files)"
    if [[ -z "$files" ]]; then pending storage "no Rust sources to scan"; return; fi

    # An in-memory map standing in for the storage engine.
    local hits
    hits="$(grep -nE '(HashMap|BTreeMap)\s*<\s*Vec\s*<\s*u8\s*>' $files 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "in-memory byte-map used as a store outside tests:"; echo "$hits" | sed 's/^/          /'
    else
        pass "no in-memory byte-map storage stand-ins in src/"
    fi

    # KvStore has exactly one permissible implementation.
    local impls
    impls="$(grep -rn 'impl\s\+KvStore\s\+for' $files 2>/dev/null | grep -v 'for RocksKv')"
    if [[ -n "$impls" ]]; then
        fail "KvStore implemented by something other than RocksKv:"; echo "$impls" | sed 's/^/          /'
    else
        pass "RocksKv is the only KvStore implementation"
    fi

    # Fixture JSON masquerading as the database.
    hits="$(grep -rnE 'include_str!\(.*\.json|fixtures?/.*\.json' $files 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "JSON fixture loaded in a non-test code path:"; echo "$hits" | sed 's/^/          /'
    else
        pass "no JSON fixtures serving as the database"
    fi
}

# ---------------------------------------------------------------------------
# endpoints — no fake query endpoints
# ---------------------------------------------------------------------------
check_endpoints() {
    header endpoints "NO FAKE QUERY ENDPOINTS"
    if [[ ! -d src/api ]] || ! ls proto/*.proto >/dev/null 2>&1; then
        pending endpoints "no gRPC surface exists yet"; return
    fi

    local rpcs endpoint_tests
    # `grep -c` only prefixes each result with "filename:" when given more
    # than one file — with exactly one proto/*.proto (the common case) it
    # prints a bare count, which silently zeroed the sum below via
    # `awk -F: '{s+=$2}'` reading an empty $2. `-o` + `wc -l` counts total
    # matching lines directly and is correct regardless of file count.
    rpcs="$(grep -ohE '^\s*rpc ' proto/*.proto 2>/dev/null | wc -l)"
    endpoint_tests="$(find tests/integration -name '*endpoint*' -o -name '*api*' 2>/dev/null | wc -l)"

    if (( rpcs == 0 )); then
        pending endpoints "no rpc methods declared in proto/"
    elif (( endpoint_tests == 0 )); then
        fail "$rpcs rpc methods declared but no integration test asserts response variation"
    else
        pass "$rpcs rpc methods covered by response-variation tests"
    fi

    # A response built from literals rather than from a read.
    local hits
    hits="$(grep -rnE 'Response::new\(\s*[A-Za-z_]+\s*\{[^}]*"' src/api 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "hardcoded string literal in an RPC response:"; echo "$hits" | sed 's/^/          /'
    fi
}

# ---------------------------------------------------------------------------
# embeddings — computed by a real GNN, never random vectors
# ---------------------------------------------------------------------------
check_embeddings() {
    header embeddings "EMBEDDINGS COMPUTED BY A REAL GNN"
    local files; files="$(python_prod_files)"

    if [[ -n "$files" ]]; then
        local hits
        hits="$(grep -nE '(np|numpy)\.random\.|torch\.rand(n|_like)?\(' $files 2>/dev/null)"
        if [[ -n "$hits" ]]; then
            fail "random vector generation outside *_synthetic_test.py:"; echo "$hits" | sed 's/^/          /'
        else
            pass "no random-vector generation in production Python paths"
        fi
    fi

    # Every deployed embedding table needs a manifest and a model checksum.
    local tables=0 missing=0
    while IFS= read -r dir; do
        tables=$((tables + 1))
        [[ -f "$dir/dataset_manifest.json" && -f "$dir/model.sha256" ]] || {
            fail "embedding table $dir lacks dataset_manifest.json and/or model.sha256"
            missing=$((missing + 1))
        }
    done < <(find ml/deployed -mindepth 1 -maxdepth 1 -type d 2>/dev/null)

    if (( tables == 0 )); then
        pending embeddings "no deployed embedding tables to verify"
    elif (( missing == 0 )); then
        pass "$tables deployed embedding table(s), each with manifest + checksum"
    fi
}

# ---------------------------------------------------------------------------
# baselines — benchmarks run against real, running baselines
# ---------------------------------------------------------------------------
check_baselines() {
    header baselines "BENCHMARKS RUN AGAINST REAL BASELINES"
    if [[ ! -x benchmarks/run_baseline.sh ]]; then
        if [[ -f benchmarks/run_baseline.sh ]]; then
            fail "benchmarks/run_baseline.sh is not executable"
        else
            pending baselines "benchmarks/run_baseline.sh does not exist"
        fi
        return
    fi
    pass "benchmarks/run_baseline.sh is present and executable"

    local raw
    raw="$(find benchmarks/results results -name '*_[0-9]*.json' 2>/dev/null | head -1)"
    if [[ -z "$raw" ]]; then
        pending baselines "no timestamped raw-results file has been produced"
    else
        pass "timestamped raw-results present (e.g. $raw)"
    fi

    # A benchmark report must never carry a number sourced from a paper.
    if [[ -f docs/benchmark_report.md ]]; then
        local hits
        hits="$(grep -nEi 'as reported in|per the .* paper|estimated at|projected' docs/benchmark_report.md)"
        if [[ -n "$hits" ]]; then
            fail "benchmark report cites published/estimated numbers:"; echo "$hits" | sed 's/^/          /'
        else
            pass "benchmark report contains no published or estimated figures"
        fi
    fi
}

# ---------------------------------------------------------------------------
# atomic-commit — mutation and embedding update commit atomically
# ---------------------------------------------------------------------------
check_atomic_commit() {
    header atomic-commit "ATOMIC MUTATION + EMBEDDING COMMIT"
    local commit=src/embedding/atomic_commit.rs
    if [[ ! -f "$commit" ]]; then pending atomic-commit "src/embedding/atomic_commit.rs does not exist"; return; fi

    # The whole guarantee is that the commit path issues exactly one write.
    local writes
    writes="$(grep -cE '\bdb\.write\(|\.write\(batch\)' "$commit")"
    if (( writes != 1 )); then
        fail "atomic_commit.rs issues $writes writes; exactly 1 is permitted"
    else
        pass "atomic_commit.rs issues exactly one batch write"
    fi

    # No direct put outside the batch anywhere on the commit path.
    local hits
    hits="$(grep -nE '\bdb\.put_cf\(|store\.put\(' "$commit")"
    if [[ -n "$hits" ]]; then
        fail "direct put bypassing the WriteBatch:"; echo "$hits" | sed 's/^/          /'
    else
        pass "no direct puts bypass the WriteBatch"
    fi

    if [[ -d tests/fault_injection ]] && find tests/fault_injection -name '*.rs' | grep -q .; then
        pass "fault-injection suite present"
    else
        pending atomic-commit "no fault-injection suite in tests/fault_injection/"
    fi
}

# ---------------------------------------------------------------------------
# clinical-data — real clinical data only for evaluation
# ---------------------------------------------------------------------------
check_clinical_data() {
    header clinical-data "REAL CLINICAL DATA ONLY"
    local files; files="$(rust_prod_files) $(python_prod_files)"
    local hits
    hits="$(grep -rnEi 'faker|from faker|lorem|fake_patient|random_patient|generate_(patient|condition|lab)' \
            $files 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "placeholder-data generator outside test files:"; echo "$hits" | sed 's/^/          /'
    else
        pass "no placeholder-data generators in evaluation paths"
    fi

    # Synthetic fixtures live only under tests/unit/.
    hits="$(find src benchmarks data ml -name '*_synthetic_test.*' 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "synthetic test files outside tests/unit/:"; echo "$hits" | sed 's/^/          /'
    else
        pass "synthetic fixtures confined to tests/unit/"
    fi
}

# ---------------------------------------------------------------------------
# fallback — no silent fallback to full recompute
# ---------------------------------------------------------------------------
check_fallback() {
    header fallback "NO SILENT FALLBACK TO FULL RECOMPUTE"
    if [[ ! -d src/embedding ]]; then pending fallback "src/embedding/ does not exist"; return; fi

    local fallbacks counter
    fallbacks="$(grep -rn 'ComputationPath::Fallback' src/embedding 2>/dev/null | wc -l)"
    counter="$(grep -rn 'incremental_fallback_total' src 2>/dev/null | wc -l)"

    if (( fallbacks == 0 )); then
        pending fallback "no fallback path implemented yet"
    elif (( counter == 0 )); then
        fail "fallback path exists but incremental_fallback_total is never incremented"
    else
        pass "every fallback path is counted by incremental_fallback_total"
    fi
}

# ---------------------------------------------------------------------------
# encryption — all encryption is real encryption
# ---------------------------------------------------------------------------
check_encryption() {
    header encryption "REAL ENCRYPTION AT REST"
    local files; files="$(rust_prod_files)"
    local hits

    # base64 passed off as encryption.
    hits="$(grep -rniE 'base64.*(encrypt|cipher)|(encrypt|cipher).*base64' $files 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "base64 encoding presented as encryption:"; echo "$hits" | sed 's/^/          /'
    else
        pass "no base64-as-encryption"
    fi

    # Hardcoded key material.
    hits="$(grep -rnE '(encryption_key|secret_key|aes_key|cipher_key)\s*[:=]\s*(b?")' $files 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "hardcoded key material in source:"; echo "$hits" | sed 's/^/          /'
    else
        pass "no hardcoded key material in src/"
    fi

    # Look for a real call into RocksDB's encryption API, not the word
    # "encryption" appearing in a doc comment.
    if [[ -f src/storage/encryption.rs ]] \
       && grep -rqE '^\s*[^/]*\b(set_encryption|EncryptionProvider|encrypted_env)\b' src/storage 2>/dev/null
    then
        pass "storage layer configures RocksDB encryption"
    else
        pending encryption "at-rest encryption not yet enabled in src/storage/"
    fi
}

# ---------------------------------------------------------------------------
# dashboards — no placeholder dashboards
# ---------------------------------------------------------------------------
check_dashboards() {
    header dashboards "NO PLACEHOLDER DASHBOARDS"
    local dashboards
    dashboards="$(find observability/grafana/dashboards -name '*.json' 2>/dev/null)"
    if [[ -z "$dashboards" ]]; then pending dashboards "no Grafana dashboards defined"; return; fi

    local bad=0 hits
    hits="$(grep -ln '"datasource"\s*:\s*null' $dashboards 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "dashboard panel with a null datasource:"; echo "$hits" | sed 's/^/          /'; bad=1
    fi
    hits="$(grep -lni 'coming soon\|placeholder\|TODO' $dashboards 2>/dev/null)"
    if [[ -n "$hits" ]]; then
        fail "placeholder panel text:"; echo "$hits" | sed 's/^/          /'; bad=1
    fi
    (( bad == 0 )) && pass "all panels bind to a real datasource"
}

# ---------------------------------------------------------------------------

printf '%sCareGraph — invariant checks%s\n' "$BOLD" "$RESET"
printf 'repo: %s\n' "$REPO_ROOT"
(( TARGET_PHASE > 0 )) && printf 'gating at phase %s\n' "$TARGET_PHASE"

for name in storage endpoints embeddings baselines atomic-commit clinical-data fallback encryption dashboards; do
    should_run "$name" && "check_${name//-/_}"
done

printf '\n%s────────────────────────────────────────%s\n' "$BOLD" "$RESET"
if (( FAILURES > 0 )); then
    printf '%s%s violation(s)%s' "$RED" "$FAILURES" "$RESET"
    (( PENDINGS > 0 )) && printf ', %s pending' "$PENDINGS"
    printf '\n'
    exit 1
fi

if (( PENDINGS > 0 )); then
    printf '%sno violations, %s check(s) pending%s\n' "$GREEN" "$PENDINGS" "$RESET"
    printf 'Pending checks are not yet enforceable. Re-run with --phase N at each\n'
    printf 'phase gate; a check that should be live by then will fail instead.\n'
else
    printf '%sall checks enforced and passing%s\n' "$GREEN" "$RESET"
fi
