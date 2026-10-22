#!/usr/bin/env bash
# Builds the example services, stateless and application, as import-free
# Merezha components.
#
# Usage: scripts/build-services.sh [OUTPUT_DIR]
#
# Each service compiles to a wasm32-unknown-unknown core module with the
# component type wit-bindgen embeds, then merezha-componentize wraps it and
# checks that it imports nothing and exports merezha:service/handler@0.1.0.
# Components land in OUTPUT_DIR, by default examples/welcome/components.
# Honors CARGO_TARGET_DIR. Requires `rustup target add wasm32-unknown-unknown`.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
manifest="$root/services/Cargo.toml"
target_dir=${CARGO_TARGET_DIR:-$root/services/target}
out=${1:-$root/examples/welcome/components}

if command -v rustup >/dev/null && ! rustup target list --installed | grep -qx wasm32-unknown-unknown; then
  echo "build-services: missing Rust target; run: rustup target add wasm32-unknown-unknown" >&2
  exit 1
fi

mkdir -p "$out"
cargo build --manifest-path "$manifest" --locked --release -p merezha-componentize
componentize="$target_dir/release/merezha-componentize"
core_dir="$target_dir/wasm32-unknown-unknown/release"

# build PACKAGE LIBRARY COMPONENT [CARGO_ARGS...]
build() {
  local package=$1 library=$2 component=$3
  shift 3
  cargo build --manifest-path "$manifest" --locked --release \
    --target wasm32-unknown-unknown -p "$package" "$@"
  "$componentize" "$core_dir/$library.wasm" "$out/$component.wasm"
  echo "build-services: wrote $out/$component.wasm"
}

build merezha-text-normalize text_normalize text-normalize
build merezha-greeting greeting greeting
build merezha-greeting greeting greeting-formal --features formal
build merezha-json-envelope json_envelope json-envelope
build merezha-task-domain task_domain task-domain
build merezha-task-view task_view task-view
build merezha-task-view task_view task-view-faulty --features faulty
