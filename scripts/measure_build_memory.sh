#!/usr/bin/env bash
# Peak memory while rebuilding every test target after a source change, and
# which processes held it. Windows (Git Bash) only: samples through PowerShell
# once a second. Run it with nothing else going on, or the system figure includes
# the rest of the machine.
#
# Usage: bash scripts/measure_build_memory.sh [jobs]
#   jobs   cargo --jobs (default: cargo's own default, one per logical CPU)
set -uo pipefail

JOBS="${1:-}"
SAMPLES="$(mktemp)"
PROCS="$(mktemp)"

used_mb() {
    powershell -NoProfile -Command '$o = Get-CimInstance Win32_OperatingSystem; [math]::Round(($o.TotalVisibleMemorySize - $o.FreePhysicalMemory) / 1024)'
}

# One sample per second: system memory in use, then the five largest build
# processes (rustc, the linker, the C++ compiler) by working set.
sample() {
    powershell -NoProfile -Command '
        $o = Get-CimInstance Win32_OperatingSystem
        $used = [math]::Round(($o.TotalVisibleMemorySize - $o.FreePhysicalMemory) / 1024)
        $top = Get-Process rustc,link,lld-link,cl,cc1plus,clang,cargo -ErrorAction SilentlyContinue |
            Sort-Object WorkingSet64 -Descending | Select-Object -First 5 |
            ForEach-Object { "{0}:{1}" -f $_.ProcessName, [math]::Round($_.WorkingSet64 / 1MB) }
        "$used $($top -join " ")"
    '
}

BASE=$(used_mb)
( while true; do sample >> "$PROCS"; sleep 1; done ) &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null; rm -f "$SAMPLES" "$PROCS"' EXIT

touch src/lib.rs
START=$(date +%s)
if [ -n "$JOBS" ]; then
    cargo test --no-run --jobs "$JOBS" > /dev/null 2>&1
else
    cargo test --no-run > /dev/null 2>&1
fi
STATUS=$?
END=$(date +%s)
kill $SAMPLER 2>/dev/null

PEAK=$(awk '{print $1}' "$PROCS" | sort -n | tail -1)
echo "jobs=${JOBS:-default} exit=$STATUS wall=$((END - START))s baseline=${BASE}MB peak=${PEAK}MB growth=$((PEAK - BASE))MB samples=$(wc -l < "$PROCS")"
echo "largest single build processes seen (name:MB):"
tr ' ' '\n' < "$PROCS" | grep ':' | sort -t: -k2 -n -r | awk -F: '!seen[$1]++ {print "  " $1 ": " $2 " MB"}'
echo "the sample at the system peak:"
sort -n "$PROCS" | tail -1 | sed 's/^/  /'
