#!/usr/bin/env bash
# build-harness.sh — spec/HARNESS.md §6: compile and expose the five tools as
# harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}.
#
#   scripts/build-harness.sh            # tsc → dist/, wrappers run node
#   CHECKED=1 scripts/build-harness.sh  # wrappers add --trace-warnings and
#                                       # --unhandled-rejections=strict (JS has no
#                                       # sanitizer; strict TS + Atomics are the checks)
set -euo pipefail
cd "$(dirname "$0")/.."
[ -d node_modules/typescript ] || npm install --silent
npx tsc -p tsconfig.json
ROOT=$(pwd)
FLAGS=""
[ "${CHECKED:-0}" = 1 ] && FLAGS="--trace-warnings --unhandled-rejections=strict"
mkdir -p harness/bin
for t in orderrun ordererfuzz orderrecover ordersnap orderbench; do
  extra=""
  [ "$t" = orderbench ] && extra="--expose-gc --max-old-space-size=8192"
  printf '#!/bin/sh\nexec node %s %s "%s/dist/tools/%s.js" "$@"\n' "$FLAGS" "$extra" "$ROOT" "$t" > "harness/bin/$t"
  chmod +x "harness/bin/$t"
done
