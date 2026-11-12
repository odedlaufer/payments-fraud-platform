#!/usr/bin/env bash
# Stops a running Flink job with a savepoint and prints the savepoint path.
# Exit codes: 0 stopped (path on stdout), 2 not running, 1 stop failed.
#   scripts/flink-savepoint-stop.sh bronze-cdc
set -euo pipefail
cd "$(dirname "$0")/.."

name="${1:?usage: flink-savepoint-stop.sh <job-name>}"
api="http://localhost:${FLINK_UI_PORT:-8081}"
overview=$(curl -fsS "$api/jobs/overview") || { echo "Flink REST API unreachable" >&2; exit 1; }
jid=$(grep -oE "\"jid\":\"[a-f0-9]+\",\"name\":\"${name}\"[^}]*\"state\":\"RUNNING\"" <<< "$overview" \
  | grep -oE '[a-f0-9]{32}' | head -1 || true)
if [[ -z "$jid" ]]; then
  echo "${name}: not running" >&2
  exit 2
fi
out=$(docker compose exec -T flink-jobmanager ./bin/flink stop "$jid" 2>&1) || true
path=$(grep -oE 's3://[^ ]+' <<< "$out" | tail -1 || true)
if [[ -z "$path" ]]; then
  echo "${name}: stop with savepoint failed" >&2
  echo "$out" | tail -20 >&2
  exit 1
fi
echo "$path"
