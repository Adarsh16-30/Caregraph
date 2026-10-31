#!/usr/bin/env bash
#
# Turns the interesting part of a CI step's log into one error annotation.
#
# A failed step's log needs a signed-in account with access to read, but its
# annotations are public and show on the run's summary page and through the API. Without
# this, a job that dies with "Process completed with exit code 101" says nothing about why
# to anyone who cannot open the log. The steps tee their output to a file, and a following
# `if: failure()` step calls this script on it.
#
# Usage: scripts/ci_annotate.sh LOGFILE TITLE
set -uo pipefail

log="${1:?usage: ci_annotate.sh LOGFILE TITLE}"
title="${2:?usage: ci_annotate.sh LOGFILE TITLE}"

if [[ ! -s "$log" ]]; then
    echo "::warning title=${title}::no output was captured in ${log}"
    exit 0
fi

# Compiler, linter, docker and shell diagnostics first, then the tail for context.
matches="$(grep -nE '^(error|warning)(\[[A-Za-z0-9_]+\])?:|^ +--> |ERROR|error:|panicked at|failed|Failed|cannot |unresolved|not found|No such file|denied|Killed|exit code' "$log" | head -n 40)"
tail_part="$(tail -n 40 "$log")"
body="${matches}"$'\n'"---- end of log ----"$'\n'"${tail_part}"

# Annotation messages encode these characters; the limit is 64 KB, keep well under it.
body="${body:0:30000}"
body="${body//'%'/'%25'}"
body="${body//$'\r'/'%0D'}"
body="${body//$'\n'/'%0A'}"
echo "::error title=${title}::${body}"
