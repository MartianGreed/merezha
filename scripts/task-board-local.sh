#!/usr/bin/env bash
# Serves the task board application locally with durable SQLite storage.
#
# Usage: scripts/task-board-local.sh [DATA_DIR]
#
# Builds the example components and the host, then serves the task board on
# MEREZHA_LISTEN until interrupted. DATA_DIR (default target/task-board-local)
# holds the generated manifests, the components and the database
# task-board.sqlite. The first run initializes the database; later runs reopen
# it, so tasks survive a restart or a crash. Talk to it from another terminal
# with the task-board client (examples/task-board/README.md). Delete DATA_DIR to
# start over; a changed examples/task-board/application.toml needs a fresh one.
#
# Environment:
#   MEREZHA_BIN     use this host binary instead of building merezha-cli
#   MEREZHA_LISTEN  loopback address (default 127.0.0.1:8787; port 0 picks one
#                   and the host reports it in its listener.ready event)
# Requires bash and cargo.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
example="$root/examples/task-board"
mkdir -p "${1:-$root/target/task-board-local}"
# SQLite storage needs an absolute path with symlinks resolved.
data=$(cd "${1:-$root/target/task-board-local}" && pwd -P)
database="$data/task-board.sqlite"
if [[ $database == *[\"\\]* ]]; then
  echo "task-board-local: DATA_DIR must not contain a quote or backslash: $data" >&2
  exit 2
fi
listen=${MEREZHA_LISTEN:-127.0.0.1:8787}

"$root/scripts/build-services.sh" "$data/components"

if [[ -n ${MEREZHA_BIN:-} ]]; then
  host=$MEREZHA_BIN
else
  cargo build --manifest-path "$root/Cargo.toml" --locked --release -p merezha-cli
  host="${CARGO_TARGET_DIR:-$root/target}/release/merezha"
fi

# Initialization only ever creates a missing database; reopen verifies the
# existing one and never resets it.
if [[ -e $database ]]; then
  initialize=false
  echo "task-board-local: reopening $database"
else
  initialize=true
  echo "task-board-local: initializing $database"
fi

cp "$example/runtime.toml" "$data/runtime.toml"
{
  cat "$example/application.toml"
  printf '\n[storage]\nbackend = "sqlite"\npath = "%s"\napplication = "task-board-local-v1"\ninitialize = %s\n' \
    "$database" "$initialize"
} >"$data/application.toml"

echo "task-board-local: starting the host on $listen; stop it with Ctrl-C"
exec "$host" serve --manifest "$data/runtime.toml" --application "$data/application.toml" --listen "$listen"
