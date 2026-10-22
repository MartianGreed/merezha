#!/usr/bin/env bash
# Runs the required checks for the services workspace.
#
# Usage: scripts/check-services.sh
#
# The demo's response assertions, formatting, Clippy for native and
# component builds, native contract tests for both greeting and both
# task-view revisions (doctests included), then a full component build into a
# temporary directory. The application scenario against the real host is
# scripts/task-board-demo.sh.
# Honors CARGO_TARGET_DIR.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
manifest="$root/services/Cargo.toml"
components=(-p merezha-text-normalize -p merezha-greeting -p merezha-json-envelope
  -p merezha-task-domain -p merezha-task-view)

run() {
  printf '\n$ %s\n' "$*"
  "$@"
}

run "$root/scripts/test-demo-assertions.sh"
run cargo fmt --manifest-path "$manifest" --all --check
run cargo clippy --manifest-path "$manifest" --locked --workspace --all-targets -- -D warnings
run cargo clippy --manifest-path "$manifest" --locked -p merezha-greeting --features formal --all-targets -- -D warnings
run cargo clippy --manifest-path "$manifest" --locked -p merezha-task-view --features faulty --all-targets -- -D warnings
run cargo clippy --manifest-path "$manifest" --locked --target wasm32-unknown-unknown "${components[@]}" -- -D warnings
run cargo clippy --manifest-path "$manifest" --locked --target wasm32-unknown-unknown -p merezha-task-view --features faulty -- -D warnings
run cargo test --manifest-path "$manifest" --locked --workspace
run cargo test --manifest-path "$manifest" --locked -p merezha-greeting --features formal
run cargo test --manifest-path "$manifest" --locked -p merezha-task-view --features faulty

out=$(mktemp -d "${TMPDIR:-/tmp}/merezha-components.XXXXXX")
trap 'rm -rf "$out"' EXIT
run "$root/scripts/build-services.sh" "$out"
echo "check-services: all checks passed"
