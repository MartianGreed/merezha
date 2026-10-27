#!/usr/bin/env bash
# Checks the telemetry artifacts in deploy/telemetry with promtool
# (docs/design/telemetry.md §7, TE-T7):
#   1. `promtool check rules` on the rule files;
#   2. `promtool test rules` on deploy/telemetry/tests (the alerts fire, and
#      stay quiet, as intended);
#   3. `promtool check rules` on every Prometheus query of the dashboards,
#      written as recording rules with Grafana's variables replaced, so a
#      dashboard query that does not parse fails here too.
# The dashboard structure and the metric and label names are checked by
# `cargo test -p merezha-telemetry --test artifacts`.
#
# Usage: scripts/check-telemetry-artifacts.sh
#
# Environment:
#   PROMTOOL  a promtool binary to use. Default: `promtool` on PATH, else the
#             pinned prom/prometheus image through docker.
# Requires bash, jq, and promtool or docker.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# Prometheus 3.15.0, the version grafana/otel-lgtm 0.38.0 ships; the digest
# pins the multi-architecture image.
image="prom/prometheus:v3.15.0@sha256:6b41f7a45cfbd1d259a78701ee5e14fc2ad9383c9aa5d0427345a18539bc3c91"

cd "$root"
if [[ -n ${PROMTOOL:-} ]]; then
  promtool=("$PROMTOOL")
elif command -v promtool >/dev/null; then
  promtool=(promtool)
else
  # Paths below are relative to the repository, mounted read-only at /w.
  promtool=(docker run --rm --network none -v "$root:/w:ro" -w /w --entrypoint promtool "$image")
fi

work=target/telemetry-check
mkdir -p "$work"
queries="$work/dashboard-queries.rules.json"
# A rule file is YAML, and JSON is YAML: one recording rule per query.
jq -s '{groups: [{name: "dashboard-queries", rules: (
    [ .[] | .panels[] | .targets[]? | select(.datasource.type == "prometheus") | .expr ]
    | to_entries
    | map({record: ("dashboard_query_" + (.key | tostring)),
           expr: (.value | gsub("\\$__rate_interval"; "5m") | gsub("\\$__range"; "1h")
                          | gsub("\\$__interval"; "1m") | gsub("\\$service"; ".+"))})
  )}]}' deploy/telemetry/dashboards/*.json >"$queries"
count=$(jq '.groups[0].rules | length' "$queries")
if ((count == 0)); then
  echo "check-telemetry-artifacts: no dashboard query found" >&2
  exit 1
fi

echo "== promtool check rules (rule files)"
"${promtool[@]}" check rules deploy/telemetry/rules/*.yaml
echo "== promtool test rules"
"${promtool[@]}" test rules deploy/telemetry/tests/*.yaml
echo "== promtool check rules ($count dashboard queries)"
"${promtool[@]}" check rules "$queries"
echo "check-telemetry-artifacts: ok"
