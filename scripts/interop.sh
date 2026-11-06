#!/usr/bin/env bash
# Mixed-language cluster test: real replica processes, any mix of orderer-rust
# (orderer-node), orderer-cpp (orderer_node), orderer-go (orderernode) and
# orderer-ts (tools/orderernode.js), sharing one cluster, under load, with
# `kill -9` of the current leader several times. Passes only if every
# acknowledged batch survives on every replica, in the same order and exactly
# once, and all replicas (every language) end at the same state digest.
#
#   scripts/interop.sh                        # nodes: ts rust go
#   IMPLS="ts go cpp" SECS=30 KILLS=6 scripts/interop.sh
#   ORDERER_KEY=secret scripts/interop.sh     # authenticated transport
#   COMPACT=100 DOWN=5 scripts/interop.sh     # force snapshot transfer between languages
#   SM=recorder scripts/interop.sh            # language-neutral recorder instead of the matching pipeline
#   LOADER=rust|go|ts scripts/interop.sh      # which load tool drives the cluster
#
# The load tool is orderload (this repo) whenever a TypeScript replica is in the
# mix: matcher-ts holds order ids exactly only below 2^53 and the other tools'
# ids are wider. Otherwise it is orderer-go's orderload (LOADER=go) unless
# LOADER=rust. The other languages' repos are built here only when IMPLS or
# LOADER names them: ../orderer-rust (cargo), ../orderer-go, ../orderer-cpp.
set -euo pipefail

cd "$(dirname "$0")/.."
IMPLS=${IMPLS:-"ts rust go"}
SECS=${SECS:-25}
KILLS=${KILLS:-5}
SM=${SM:-pipeline}
COMPACT=${COMPACT:-0}   # >0: snapshot and compact the log every N entries
DOWN=${DOWN:-1}         # seconds a killed leader stays down
BASE=${BASE_PORT:-49500}
RUST=${RUST_DIR:-../orderer-rust}
GO=${GO_DIR:-../orderer-go}
CPP=${CPP_DIR:-../orderer-cpp}

read -r -a KINDS <<< "$IMPLS"
N=${#KINDS[@]}
has() { [[ " $IMPLS " == *" $1 "* ]]; }
if [[ -z "${LOADER:-}" ]]; then if has ts; then LOADER=ts; else LOADER=go; fi; fi
if has ts && [[ "$LOADER" != ts ]]; then echo "a TypeScript replica needs LOADER=ts (order ids must stay below 2^53)"; exit 1; fi

TMP=$(mktemp -d)
[ -d node_modules/typescript ] || npm install --silent
npx tsc -p tsconfig.json
TS_NODE="dist/tools/orderernode.js"
RUST_NODE="$RUST/target/release/orderer-node"
if has rust || [[ "$LOADER" == rust ]]; then
  (cd "$RUST" && cargo build --release -q -p orderer-cluster --bin orderer-node --bin orderer-load)
fi
GO_NODE="$TMP/orderernode"
if has go; then (cd "$GO" && go build -o "$GO_NODE" ./cmd/orderernode); fi
CPP_NODE="$CPP/build/orderer_node"
if has cpp; then
  cmake -S "$CPP" -B "$CPP/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build "$CPP/build" --target orderer_node -j >/dev/null
fi
case "$LOADER" in
  rust) LOAD=("$RUST/target/release/orderer-load") ;;
  go) (cd "$GO" && go build -o "$TMP/orderload" ./cmd/orderload); LOAD=("$TMP/orderload") ;;
  ts) LOAD=(node dist/tools/orderload.js) ;;
  *) echo "LOADER must be rust, go or ts"; exit 1 ;;
esac

MEMBERS=""
CLIENTS=""
for ((i = 0; i < N; i++)); do
  MEMBERS+="${MEMBERS:+,}$i=127.0.0.1:$((BASE + i))"
  CLIENTS+="${CLIENTS:+,}127.0.0.1:$((BASE + 100 + i))"
done
declare -a PIDS
for ((i = 0; i < N; i++)); do PIDS[i]=0; done

cleanup() {
  for ((i = 0; i < N; i++)); do kill -9 "${PIDS[$i]}" 2>/dev/null || true; done
  [[ -n "${LOAD_PID:-}" ]] && kill "$LOAD_PID" 2>/dev/null || true
  [[ -z "${KEEP:-}" ]] && rm -rf "$TMP"
}
trap cleanup EXIT

start_node() {
  local i=$1
  local args=(--id "$i" --members "$MEMBERS" --client "127.0.0.1:$((BASE + 100 + i))"
    --dir "$TMP/n$i" --audit --digest-every 64 --fsync on --sm "$SM" --compact-every "$COMPACT")
  case "${KINDS[$i]}" in
    cpp) "$CPP_NODE" "${args[@]}" >>"$TMP/n$i.log" 2>&1 & ;;
    go) "$GO_NODE" "${args[@]}" >>"$TMP/n$i.log" 2>&1 & ;;
    ts) node "$TS_NODE" "${args[@]}" >>"$TMP/n$i.log" 2>&1 & ;;
    *) "$RUST_NODE" "${args[@]}" >>"$TMP/n$i.log" 2>&1 & ;;
  esac
  PIDS[i]=$!
}

leader() {
  "${LOAD[@]}" status --clients "$CLIENTS" 2>/dev/null | grep "role=Leader" | head -1 \
    | sed -E 's/.*id=([0-9]+).*/\1/'
}

for ((i = 0; i < N; i++)); do start_node "$i"; done
echo "started $N nodes (${IMPLS// /, }); logs in $TMP"
for _ in $(seq 1 150); do [[ -n "$(leader)" ]] && break; sleep 0.2; done
[[ -n "$(leader)" ]] || { echo "FAIL: no leader elected"; exit 1; }

"${LOAD[@]}" run --clients "$CLIENTS" --secs "$SECS" --acked "$TMP/acked" --maybe "$TMP/maybe" \
  --batch 50 --threads 3 >"$TMP/load.out" 2>&1 &
LOAD_PID=$!

for k in $(seq 1 "$KILLS"); do
  sleep $(( (SECS - 4) / (KILLS + 1) + 1 ))
  L=$(leader || true)
  [[ -z "$L" ]] && { sleep 1; L=$(leader || true); }
  [[ -z "$L" ]] && { echo "FAIL: no leader before kill $k"; exit 1; }
  echo "kill $k/$KILLS: kill -9 leader node $L (${KINDS[$L]})"
  kill -9 "${PIDS[$L]}"
  wait "${PIDS[$L]}" 2>/dev/null || true
  sleep "$DOWN"
  start_node "$L"
done

wait "$LOAD_PID"
LOAD_PID=
cat "$TMP/load.out"

"${LOAD[@]}" settle --clients "$CLIENTS" --timeout 90
if [[ "$COMPACT" -gt 0 ]]; then
  # After a snapshot restore a node's audit log lacks the entries the snapshot
  # covers, so the per-batch comparison does not apply; the state digests
  # (settle, above) are the check. Show that logs were compacted.
  "${LOAD[@]}" status --clients "$CLIENTS"
  SAME=0; CROSS=0
  for ((i = 0; i < N; i++)); do
    while read -r from; do
      [[ -z "$from" ]] && continue
      if [[ "${KINDS[$i]}" == "${KINDS[$from]}" ]]; then SAME=$((SAME + 1)); else CROSS=$((CROSS + 1)); fi
    done < <(sed -n -E 's/.*installing snapshot [0-9]+ from node ([0-9]+).*/\1/p' "$TMP/n$i.log")
  done
  echo "snapshot installs: $SAME between same-language replicas, $CROSS across languages"
  BASES=$("${LOAD[@]}" status --clients "$CLIENTS" | sed -E 's/.* base=([0-9]+) .*/\1/' | sort -n | tail -1)
  [[ "$BASES" -gt 0 ]] || { echo "FAIL: no node compacted its log"; exit 1; }
  if [[ "${REQUIRE_CROSS:-0}" == 1 && "$CROSS" -eq 0 ]]; then echo "FAIL: no snapshot crossed a language boundary"; exit 1; fi
  echo "PASS: all replicas, across languages, hold the same state digest after snapshot transfer (highest log base $BASES)"
  exit 0
fi
APPLIED=""
for ((i = 0; i < N; i++)); do APPLIED+="${APPLIED:+,}$TMP/n$i/applied.log"; done
"${LOAD[@]}" verify --acked "$TMP/acked" --maybe "$TMP/maybe" --applied "$APPLIED"
