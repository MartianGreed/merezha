#!/usr/bin/env bash
# Runs the task board application scenario against the real Merezha host.
#
# Usage: scripts/task-board-demo.sh
#
# Builds the example components, the host and the scenario driver, starts
# the host on an ephemeral loopback port with a private scratch copy of
# examples/task-board and a fresh in-memory store, then drives it with
# commands, queries, projection catch-up and rebuild, and component reloads
# (services/scenario/src/main.rs lists every step and expectation).
#
# Environment:
#   MEREZHA_BIN    use this host binary instead of building merezha-cli
#   KEEP_SCRATCH=1 keep the scratch directory and host log after the run
# Requires bash and cargo.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
example="$root/examples/task-board"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/merezha-task-board.XXXXXX")
host_pid=""

cleanup() {
  if [[ -n $host_pid ]] && kill -0 "$host_pid" 2>/dev/null; then
    kill "$host_pid" 2>/dev/null || true
    wait "$host_pid" 2>/dev/null || true
  fi
  if [[ ${KEEP_SCRATCH:-} == 1 ]]; then
    echo "task-board-demo: kept $scratch"
  else
    rm -rf "$scratch"
  fi
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }
fail() {
  echo "task-board-demo: FAILED: $*" >&2
  [[ -f $scratch/host.log ]] && sed 's/^/host: /' "$scratch/host.log" >&2
  exit 1
}

step "Build the service components"
"$root/scripts/build-services.sh" "$scratch/components"

if [[ -n ${MEREZHA_BIN:-} ]]; then
  host=$MEREZHA_BIN
else
  step "Build the host"
  cargo build --manifest-path "$root/Cargo.toml" --locked --release -p merezha-cli
  host="${CARGO_TARGET_DIR:-$root/target}/release/merezha"
fi

step "Build the scenario driver"
cargo build --manifest-path "$root/services/Cargo.toml" --locked --release -p merezha-task-board-scenario
scenario="${CARGO_TARGET_DIR:-$root/services/target}/release/task-board-scenario"

step "Start the host with the task board application on an ephemeral loopback port"
cp "$example/runtime.toml" "$scratch/runtime.toml"
cp "$example/application.toml" "$scratch/application.toml"
"$host" serve --manifest "$scratch/runtime.toml" --application "$scratch/application.toml" \
  --listen 127.0.0.1:0 2>"$scratch/host.log" &
host_pid=$!
address=""
for _ in $(seq 100); do
  address=$(grep -o '"address":"[^"]*"' "$scratch/host.log" | head -n 1 | cut -d '"' -f 4 || true)
  [[ -n $address ]] && break
  kill -0 "$host_pid" 2>/dev/null || fail "host exited during startup"
  sleep 0.1
done
[[ -n $address ]] || fail "host did not report listener.ready"
echo "ok: listening on $address"

"$scenario" "$address" "$scratch" "$example" || fail "the scenario did not pass"
kill -0 "$host_pid" 2>/dev/null || fail "host exited during the scenario"
