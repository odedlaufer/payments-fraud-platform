#!/usr/bin/env bash
# CH-1 (bronze part): SIGKILL the Flink TaskManager while the simulator writes.
# Flink must restore the bronze job from its last checkpoint, and bronze must
# still hold every change event exactly once (FR-F4, FR-F5).
set -euo pipefail
cd "$(dirname "$0")/.."

API="http://localhost:${FLINK_UI_PORT:-8081}"
job_id() { curl -fsS "$API/jobs/overview" | grep -oE '"jid":"[a-f0-9]+","name":"bronze-cdc"' | cut -d'"' -f4 | head -1; }
job_state() { curl -fsS "$API/jobs/$1" | grep -oE '"state":"[A-Z]+"' | head -1 | cut -d'"' -f4; }
restores() { curl -fsS "$API/jobs/$1/checkpoints" | grep -oE '"restored":[0-9]+' | cut -d: -f2; }

echo "CH-1 bronze: kill the TaskManager mid-run"
JOB=$(job_id)
if [[ -z "$JOB" ]]; then
  echo "  bronze-cdc is not running (make flink-jobs)"
  exit 1
fi
before=$(restores "$JOB")
echo "  job ${JOB}, restores so far: ${before}"

echo "  simulator: 90 s at 50 tx/s"
uv run simulator run --duration 90 --tps 50 > /tmp/ch1-sim.log 2>&1 &
sim=$!
sleep 35

echo "  $(date +%T) SIGKILL flink-taskmanager"
docker compose kill -s SIGKILL flink-taskmanager > /dev/null
killed_at=$SECONDS
sleep 10
docker compose up -d --no-deps flink-taskmanager > /dev/null 2>&1
echo "  $(date +%T) taskmanager started again"

until [[ "$(job_state "$JOB")" == "RUNNING" && "$(restores "$JOB")" -gt "$before" ]]; do
  if (( SECONDS - killed_at > 180 )); then
    echo "  job did not recover within 180 s (state: $(job_state "$JOB"))"
    exit 1
  fi
  sleep 2
done
echo "  job RUNNING again $(( SECONDS - killed_at )) s after the kill, restored from a checkpoint"

wait "$sim"
echo "  simulator: $(tail -1 /tmp/ch1-sim.log)"
uv run python scripts/bronze_reconcile.py
