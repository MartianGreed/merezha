#!/usr/bin/env bash
# Checks that the local task board keeps its tasks across a host crash.
#
# Usage: scripts/task-board-restart-demo.sh
#
# Starts scripts/task-board-local.sh on a private scratch DATA_DIR and an
# ephemeral loopback port, drives it with the task-board client, kills the
# host with SIGKILL, starts it again on the same database and checks that the
# tasks, their read model and the domain rules all survived. Finishes with a
# graceful stop.
#
# Environment:
#   MEREZHA_BIN    use this host binary instead of building merezha-cli
#   KEEP_SCRATCH=1 keep the scratch directory and host logs after the run
#   MEREZHA_STARTUP_SECONDS  how long to wait for the host to listen (default 120)
# Requires bash and cargo.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/merezha-task-board-restart.XXXXXX")
host_pid=""
starts=0

cleanup() {
  if [[ -n $host_pid ]] && kill -0 "$host_pid" 2>/dev/null; then
    kill "$host_pid" 2>/dev/null || true
    wait "$host_pid" 2>/dev/null || true
  fi
  if [[ ${KEEP_SCRATCH:-} == 1 ]]; then
    echo "task-board-restart-demo: kept $scratch"
  else
    rm -rf "$scratch"
  fi
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }
fail() {
  echo "task-board-restart-demo: FAILED: $*" >&2
  [[ -f $scratch/host-$starts.log ]] && sed 's/^/host: /' "$scratch/host-$starts.log" >&2
  exit 1
}

if [[ -z ${MEREZHA_BIN:-} ]]; then
  step "Build the host"
  cargo build --manifest-path "$root/Cargo.toml" --locked --release -p merezha-cli
  export MEREZHA_BIN="${CARGO_TARGET_DIR:-$root/target}/release/merezha"
fi

step "Build the components once, outside the startup deadline"
"$root/scripts/build-services.sh" "$scratch/data/components"

step "Build the task-board client"
cargo build --manifest-path "$root/services/Cargo.toml" --locked --release -p merezha-task-board-cli
client="${CARGO_TARGET_DIR:-$root/services/target}/release/task-board"

# The launcher execs the host, so its process is the host process.
start() {
  starts=$((starts + 1))
  local log="$scratch/host-$starts.log"
  MEREZHA_LISTEN=127.0.0.1:0 "$root/scripts/task-board-local.sh" "$scratch/data" >"$log" 2>&1 &
  host_pid=$!
  local deadline=$((SECONDS + ${MEREZHA_STARTUP_SECONDS:-120}))
  MEREZHA_ADDRESS=""
  while [[ -z $MEREZHA_ADDRESS ]]; do
    MEREZHA_ADDRESS=$(grep -o '"address":"[^"]*"' "$log" | head -n 1 | cut -d '"' -f 4 || true)
    kill -0 "$host_pid" 2>/dev/null || fail "host exited during startup"
    ((SECONDS < deadline)) || fail "host did not report listener.ready in time"
    [[ -n $MEREZHA_ADDRESS ]] || sleep 0.1
  done
  export MEREZHA_ADDRESS
  echo "ok: $(grep -o 'task-board-local: [a-z]* ' "$log" | head -n 1 | cut -d ' ' -f 2) the database, listening on $MEREZHA_ADDRESS"
}

# expect DESCRIPTION TEXT CLIENT_ARGS...: the client succeeds and prints TEXT.
expect() {
  local description=$1 text=$2 output
  shift 2
  output=$("$client" "$@") || fail "$description: task-board $* failed"
  [[ $output == *"$text"* ]] || fail "$description: expected $text in: $output"
  echo "ok: $description"
}

# expect_rejected DESCRIPTION CODE CLIENT_ARGS...: the host rejects with CODE.
expect_rejected() {
  local description=$1 code=$2 output
  shift 2
  if output=$("$client" "$@" 2>&1); then
    fail "$description: task-board $* was accepted"
  fi
  [[ $output == *"\"code\": \"$code\""* ]] || fail "$description: expected $code in: $output"
  echo "ok: $description ($code)"
}

step "Start on a fresh database and add tasks"
start
expect "create groceries" '"version": 1' create groceries "Buy milk"
expect "create report" '"version": 1' create report "Write the report"
expect "complete groceries" '"version": 2' complete groceries
expect "summary before the crash" '"completed": 1' summary
expect "both tasks listed" '"Write the report"' list

step "Crash the host and reopen the same database"
kill -KILL "$host_pid"
wait "$host_pid" 2>/dev/null || true
host_pid=""
start
expect "summary after the crash" '"total": 2' summary
expect "groceries is still completed" '"status": "completed"' get groceries
expect "report is still open" '"status": "open"' list open

step "The domain rules see the retained history"
expect_rejected "completing groceries again" task_already_completed complete groceries
expect_rejected "recreating report" task_exists create report "Again"
expect "rename report" '"version": 2' rename report "Write the final report"
expect "renamed title" '"Write the final report"' get report
expect "rebuild replays all retained events" '"checkpoint": 4' rebuild

step "Stop gracefully"
kill -TERM "$host_pid"
status=0
wait "$host_pid" || status=$?
host_pid=""
((status == 0)) || fail "host exited with status $status"
echo "ok: host stopped with status 0"

echo
echo "== Restart demo passed"
