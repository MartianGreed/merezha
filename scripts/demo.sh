#!/usr/bin/env bash
# Runs the welcome demo against the real Merezha host and checks every step.
#
# Usage: scripts/demo.sh
#
# Builds the example components and the host, starts the host on an
# ephemeral loopback port with a private scratch copy of the example, then
# exercises a three-step pipeline, adding, replacing and removing services
# through reloads, and a rejected reload that keeps the previous registry.
# Each response is compared with the files in examples/welcome/expected.
#
# Environment:
#   MEREZHA_BIN    use this host binary instead of building merezha-cli
#   KEEP_SCRATCH=1 keep the scratch directory and host log after the run
# Requires bash, curl and cargo.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
example="$root/examples/welcome"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/merezha-demo.XXXXXX")
host_pid=""

cleanup() {
  if [[ -n $host_pid ]] && kill -0 "$host_pid" 2>/dev/null; then
    kill "$host_pid" 2>/dev/null || true
    wait "$host_pid" 2>/dev/null || true
  fi
  if [[ ${KEEP_SCRATCH:-} == 1 ]]; then
    echo "demo: kept $scratch"
  else
    rm -rf "$scratch"
  fi
}
trap cleanup EXIT

# shellcheck source=scripts/demo-assertions.sh
source "$root/scripts/demo-assertions.sh"

step() { printf '\n== %s\n' "$*"; }
fail() {
  echo "demo: FAILED: $*" >&2
  [[ -f $scratch/host.log ]] && sed 's/^/host: /' "$scratch/host.log" >&2
  exit 1
}

# install STAGE: atomically replaces the manifest the host was started with.
install() {
  cp "$example/$1" "$scratch/runtime.toml.next"
  mv "$scratch/runtime.toml.next" "$scratch/runtime.toml"
}

# request METHOD PATH [BODY_FILE]: sets $status and $body.
request() {
  local method=$1 path=$2 data=()
  [[ $# -ge 3 ]] && data=(-H 'Content-Type: application/json' --data-binary "@$3")
  status=$(curl -sS -o "$scratch/body" -w '%{http_code}' -X "$method" ${data[@]+"${data[@]}"} "http://$address$path") ||
    fail "$method $path: request failed"
  body=$(cat "$scratch/body")
}

# expect_file STATUS FILE: compares the last response with an expected file.
expect_file() {
  expect "$1" "$(cat "$example/expected/$2")"
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

step "Start the host with stage 1 on an ephemeral loopback port"
install stage-1.toml
"$host" serve --manifest "$scratch/runtime.toml" --listen 127.0.0.1:0 2>"$scratch/host.log" &
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

request GET /health
expect_json 200 '"status":"ok"' '"generation":1'
request GET /v1/services
expect_json 200 '"generation":1' '"services":["greet","normalize"]' '"pipelines":["welcome"]'

step "Pipeline welcome = normalize -> greet"
request POST /v1/pipelines/welcome/invoke "$example/fixtures/welcome.json"
expect_file 200 stage-1-welcome.json

step "A guest error is classified, and its text stays inside the guest"
printf '{"text":42}' > "$scratch/bad-text.json"
request POST /v1/services/normalize/invoke "$scratch/bad-text.json"
expect_error 422 guest_error

step "Add envelope: welcome = normalize -> greet -> envelope; other fields pass through exactly"
install stage-2.toml
request POST /v1/reload
expect_json 200 '"generation":2'
request POST /v1/pipelines/welcome/invoke "$example/fixtures/welcome.json"
expect_file 200 stage-2-welcome.json
request POST /v1/pipelines/welcome/invoke "$example/fixtures/precision.json"
expect_file 200 stage-2-precision.json

step "Replace greet with the formal revision"
install stage-3.toml
request POST /v1/reload
expect_json 200 '"generation":3'
request POST /v1/pipelines/welcome/invoke "$example/fixtures/welcome.json"
expect_file 200 stage-3-welcome.json

step "Reject an invalid manifest and keep serving generation 3"
install invalid-missing-service.toml
request POST /v1/reload
expect_error 400 invalid_manifest
request GET /health
expect_json 200 '"status":"ok"' '"generation":3'
request POST /v1/pipelines/welcome/invoke "$example/fixtures/welcome.json"
expect_file 200 stage-3-welcome.json

step "Remove normalize: welcome = greet -> envelope"
install stage-4.toml
request POST /v1/reload
expect_json 200 '"generation":4'
request GET /v1/services
expect_json 200 '"generation":4' '"services":["envelope","greet"]' '"pipelines":["welcome"]'
request POST /v1/services/normalize/invoke "$example/fixtures/welcome.json"
expect_error 404 not_found
request POST /v1/pipelines/welcome/invoke "$example/fixtures/grace.json"
expect_file 200 stage-4-grace.json

step "Demo passed"
