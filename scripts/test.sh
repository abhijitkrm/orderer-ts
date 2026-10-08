#!/usr/bin/env bash
# test.sh — the full suite, as the spec repo's verify.sh runs it.
set -euo pipefail
cd "$(dirname "$0")/.."
CHECKED=1 scripts/build-harness.sh
for t in ring golden pipeline; do node dist/tests/$t.js; done
node dist/examples/quickstart.js > /dev/null
tests/vectors.sh harness/bin vectors
