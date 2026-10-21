#!/usr/bin/env bash
# Runs the required checks for the services workspace.
#
# Usage: scripts/check-services.sh
#
# Formatting, Clippy for native and component builds, native contract tests
# for both greeting revisions (doctests included), then a full component
# build into a temporary directory. Honors CARGO_TARGET_DIR.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
manifest="$root/services/Cargo.toml"
components=(-p merezha-text-normalize -p merezha-greeting -p merezha-json-envelope)

run() {
  printf '\n$ %s\n' "$*"
  "$@"
}

run cargo fmt --manifest-path "$manifest" --all --check
run cargo clippy --manifest-path "$manifest" --locked --workspace --all-targets -- -D warnings
run cargo clippy --manifest-path "$manifest" --locked -p merezha-greeting --features formal --all-targets -- -D warnings
run cargo clippy --manifest-path "$manifest" --locked --target wasm32-unknown-unknown "${components[@]}" -- -D warnings
run cargo test --manifest-path "$manifest" --locked --workspace
run cargo test --manifest-path "$manifest" --locked -p merezha-greeting --features formal

out=$(mktemp -d "${TMPDIR:-/tmp}/merezha-components.XXXXXX")
trap 'rm -rf "$out"' EXIT
run "$root/scripts/build-services.sh" "$out"
echo "check-services: all checks passed"
