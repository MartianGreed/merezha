# Response assertions shared by scripts/demo.sh and its test.
#
# Each function checks the last response, held in $status and $body, prints
# "ok: …" on success and calls the caller's `fail MESSAGE` otherwise.

# expect STATUS BODY: the response is exactly BODY.
expect() {
  [[ $status == "$1" && $body == "$2" ]] || fail "expected HTTP $1 $2, got HTTP $status $body"
  echo "ok: HTTP $status $body"
}

# expect_json STATUS MEMBER...: the response is a flat JSON object with every
# top-level MEMBER (`"key":value`), in any order, as host responses do not
# promise key order. A flat object has no nested objects, so a member cannot
# be matched inside one, and each MEMBER must be delimited on both sides.
expect_json() {
  local expected_status=$1 member
  shift
  [[ $status == "$expected_status" ]] || fail "expected HTTP $expected_status, got HTTP $status $body"
  [[ $body == "{"*"}" && ${body:1} != *"{"* ]] || fail "expected a flat JSON object, got HTTP $status $body"
  for member in "$@"; do
    [[ $body == *[{,]"$member"[,}]* ]] || fail "expected top-level $member in HTTP $status $body"
  done
  echo "ok: HTTP $status $body"
}

# expect_error STATUS CODE: the response is exactly the host error document
# {"error":{"code":"CODE"}}.
expect_error() {
  expect "$1" "{\"error\":{\"code\":\"$2\"}}"
}
