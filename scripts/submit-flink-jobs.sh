#!/usr/bin/env bash
# Submits the long-running Flink SQL jobs if they are not already running.
# Runs in the one-shot `flink-jobs` service (Flink image, cluster config applied).
set -euo pipefail
cd /opt/flink

JOBS=("bronze-cdc:/opt/flink/sql/bronze/bronze_cdc.sql")

running() { ./bin/flink list -r 2>/dev/null | grep -q ": $1 (RUNNING)"; }

for entry in "${JOBS[@]}"; do
  name="${entry%%:*}"
  file="${entry#*:}"
  if running "$name"; then
    echo "${name}: already running"
    continue
  fi
  echo "${name}: submitting ${file}"
  # The client JVM can stay alive after a failed statement, so bound it, and
  # treat any [ERROR] in its output as a failure even if it exits 0.
  if ! out=$(timeout 180 ./bin/sql-client.sh -f "$file" 2>&1) || grep -q "\[ERROR\]" <<< "$out"; then
    echo "${name}: submission failed"
    echo "$out" | grep -E "\[ERROR\]|Caused by|Exception" | tail -20
    exit 1
  fi
  for _ in $(seq 1 30); do
    running "$name" && break
    sleep 2
  done
  if ! running "$name"; then
    echo "${name}: not RUNNING after 60s"
    ./bin/flink list -a 2>&1 | tail -10
    exit 1
  fi
  echo "${name}: RUNNING"
done
