#!/usr/bin/env bash
# Phase 3 gate (PRD section 9): decision latency p95 < 5 s at 20 tx/s, and the
# card-testing pattern triggers its rule. Runs live traffic with boosted fraud.
#   scripts/phase3-gate.sh [minutes]   (default 3)
set -euo pipefail
cd "$(dirname "$0")/.."
minutes="${1:-3}"

step() { printf '\n== %s\n' "$1"; }

step "hot path healthy"
make --no-print-directory health 2>&1 | grep -E "job:|redis|sink|decision|healthy|Unhealthy|FAIL"

step "${minutes} min of live traffic at 20 tx/s, fraud x8"
uv run simulator run --duration "$(( minutes * 60 ))" --fraud-rate 8 2>&1 | grep sim_end
sleep 10
window=$(awk -v m="$minutes" 'BEGIN { print m + 0.5 }')

step "decisions: exactly once, exact snapshots, p95 < 5 s"
uv run python scripts/decisions_check.py --since-minutes "$window"

step "detection: card testing triggers its rule"
uv run python scripts/detection_report.py --since-minutes "$window" \
  --require card_testing=card_testing --require impossible_travel=impossible_travel

echo
echo "Phase 3 gate passed."
