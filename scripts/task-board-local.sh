#!/usr/bin/env bash
# Serves the task board application locally with durable SQLite storage.
#
# Usage: scripts/task-board-local.sh [DATA_DIR]
#
# Builds the example components and the host, then serves the task board on
# MEREZHA_LISTEN until interrupted. DATA_DIR (default .merezha/task-board,
# git-ignored and kept by cargo clean)
# holds the generated manifests, the components and the database
# task-board.sqlite. The first run initializes the database; later runs reopen
# it, so tasks survive a restart or a crash. Talk to it from another terminal
# with the task-board client (docs/services/README.md, "Local task board with
# durable storage"). Delete DATA_DIR to start over; a changed
# examples/task-board/application.toml needs a fresh one. Only one host can own
# DATA_DIR: a second launcher rewrites the generated files before the host
# refuses it, so do not reload a running host after starting another on it.
#
# Environment:
#   MEREZHA_BIN     use this host binary instead of building merezha-cli
#   MEREZHA_LISTEN  numeric loopback address (default 127.0.0.1:8787; port 0
#                   picks one and the host reports it in its listener.ready event)
# Requires bash and cargo.
set -euo pipefail
# The database and its sidecars stay private to their owner.
umask 077

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
example="$root/examples/task-board"
mkdir -p "${1:-$root/.merezha/task-board}"
# SQLite storage needs an absolute path with symlinks resolved.
data=$(cd "${1:-$root/.merezha/task-board}" && pwd -P)
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
