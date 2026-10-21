#!/usr/bin/env bash
# Tests the demo's response assertions against injected responses, including
# shapes the real host never returns, so a weak check cannot pass unnoticed.
#
# Usage: scripts/test-demo-assertions.sh
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# shellcheck source=scripts/demo-assertions.sh
source "$root/scripts/demo-assertions.sh"
fail() { exit 1; }

failures=0
# case NAME accept|reject STATUS BODY ASSERTION [ARGS...]
case_() {
  local name=$1 want=$2 got
  status=$3 body=$4
  shift 4
  if ("$@" >/dev/null 2>&1); then got=accept; else got=reject; fi
  if [[ $got == "$want" ]]; then
    echo "ok: $name"
  else
    echo "FAILED: $name: wanted $want, got $got" >&2
    failures=$((failures + 1))
  fi
}

case_ "flat member first" accept 200 '{"generation":2,"status":"ok"}' expect_json 200 '"generation":2'
case_ "flat member last" accept 200 '{"status":"ok","generation":2}' expect_json 200 '"generation":2' '"status":"ok"'
case_ "listing with arrays" accept 200 '{"generation":4,"services":["envelope","greet"],"pipelines":["welcome"]}' \
  expect_json 200 '"generation":4' '"services":["envelope","greet"]' '"pipelines":["welcome"]'
case_ "member only inside a nested object" reject 200 '{"generation":99,"shadow":{"generation":2}}' expect_json 200 '"generation":2'
case_ "number with a longer value" reject 200 '{"generation":12}' expect_json 200 '"generation":1'
case_ "key with a longer name" reject 200 '{"xgeneration":2}' expect_json 200 '"generation":2'
case_ "missing member" reject 200 '{"status":"ok"}' expect_json 200 '"generation":1'
case_ "wrong status" reject 500 '{"generation":1}' expect_json 200 '"generation":1'
case_ "not an object" reject 200 '"generation":1' expect_json 200 '"generation":1'
case_ "exact error" accept 404 '{"error":{"code":"not_found"}}' expect_error 404 not_found
case_ "error code nested deeper" reject 404 '{"error":{"detail":{"code":"not_found"},"code":"internal"}}' expect_error 404 not_found
case_ "error with another code" reject 404 '{"error":{"code":"not_found_extra"}}' expect_error 404 not_found
case_ "exact body" accept 200 'abc' expect 200 'abc'
case_ "different body" reject 200 'abcd' expect 200 'abc'

if [[ $failures -gt 0 ]]; then
  echo "test-demo-assertions: $failures failed" >&2
  exit 1
fi
echo "test-demo-assertions: all passed"
