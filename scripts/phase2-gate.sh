#!/usr/bin/env bash
# Phase 2 gate (PRD section 9): silver row counts equal Postgres and SCD2 tests
# pass after at least 3 simulated days of profile changes.
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n== %s\n' "$1"; }
trino() { docker compose exec -T trino trino --output-format=TSV --execute "$1"; }

step "bronze: every CDC event exactly once"
uv run python scripts/bronze_reconcile.py | tail -1

step "dbt: silver, SCD2 and all tests"
(cd dbt && uv run dbt build --profiles-dir . 2>&1) | grep -E "ERROR|FAIL|Done\."

step "simulated history in silver (needs >= 3 days)"
span=$(trino "select date_diff('second', min(event_ts), max(event_ts)) from lake.silver.transactions_current")
days=$(awk -v s="$span" 'BEGIN { printf "%.2f", s / 86400 }')
echo "  transactions span ${days} simulated days"
if awk -v s="$span" 'BEGIN { exit !(s < 3 * 86400 - 3600) }'; then
  echo "  FAIL: run the simulator longer (uv run simulator run --fast --sim-days 3)"
  exit 1
fi

step "DQ-08: silver equals Postgres"
uv run python scripts/silver_reconcile.py | tail -10

echo
echo "Phase 2 gate passed."
