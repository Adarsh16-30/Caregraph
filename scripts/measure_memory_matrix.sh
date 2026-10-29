#!/usr/bin/env bash
# The server's whole-process-tree memory in the configurations docs/memory.md
# compares, one after another, each on a fresh copy of the real database:
#
#   default      no budget (historical defaults), both models
#   budget       CAREGRAPH_MEMORY_MB=1300, both models
#   one model    CAREGRAPH_MEMORY_MB=700, GAT disabled
#
# Usage: bash scripts/measure_memory_matrix.sh <scratch-dir> > result.txt
set -uo pipefail
W="${1:?usage: measure_memory_matrix.sh <scratch-dir>}"

echo "== commit $(git rev-parse --short HEAD), $(date -u +%FT%TZ)"
bash scripts/measure_memory.sh "$W/default"
echo
bash scripts/measure_memory.sh "$W/budget" CAREGRAPH_MEMORY_MB=1300
echo
bash scripts/measure_memory.sh "$W/onemodel" CAREGRAPH_MEMORY_MB=700 CAREGRAPH_GAT_MODEL=none
