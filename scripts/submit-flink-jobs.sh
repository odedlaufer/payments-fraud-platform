#!/usr/bin/env bash
# Keeps the long-running Flink SQL jobs running. Runs in the `flink-jobs` service.
#
#   submit-flink-jobs.sh --supervise   every 15 s, (re)submit any job that is not active.
#                                      A JobManager restart loses its jobs (no Flink HA
#                                      store here); resubmitted jobs resume from their
#                                      committed Kafka offsets.
#   submit-flink-jobs.sh               one pass: submit jobs that are not active, then exit.
#
# RESTORE_PATH=<savepoint> ONLY_JOB=<name> (one pass): resume that job from a savepoint,
# used by scripts/schema-evolve.sh, which stops the supervisor around it.
set -euo pipefail
cd /opt/flink

JOBS=("bronze-cdc:/opt/flink/sql/bronze/bronze_cdc.sql")
RESTORE_PATH="${RESTORE_PATH:-}"
ONLY_JOB="${ONLY_JOB:-}"
HEARTBEAT=/tmp/supervisor.alive

# Active includes RESTARTING: Flink is already recovering it from a checkpoint, and
# submitting a second copy would write every event twice.
active() {
  ./bin/flink list -r 2>/dev/null | grep -qE ": $1 \((RUNNING|RESTARTING|CREATED|INITIALIZING)\)"
}

ensure() {
  local name="$1" file="$2"
  if active "$name"; then
    return 0
  fi
  if [[ -n "$RESTORE_PATH" ]]; then
    local restored
    restored=$(mktemp --suffix=.sql)
    { echo "SET 'execution.state-recovery.path' = '${RESTORE_PATH}';"; cat "$file"; } > "$restored"
    file="$restored"
    echo "${name}: restoring from ${RESTORE_PATH}"
  else
    echo "$(date +%T) ${name}: not active, submitting ${file}"
  fi
  # The client JVM can stay alive after a failed statement, so bound it, and
  # treat any [ERROR] in its output as a failure even if it exits 0.
  local out
  if ! out=$(timeout 180 ./bin/sql-client.sh -f "$file" 2>&1) || grep -q "\[ERROR\]" <<< "$out"; then
    echo "${name}: submission failed"
    echo "$out" | grep -E "\[ERROR\]|Caused by|Exception" | tail -20
    return 1
  fi
  for _ in $(seq 1 30); do
    active "$name" && { echo "${name}: RUNNING"; return 0; }
    sleep 2
  done
  echo "${name}: not RUNNING after 60s"
  ./bin/flink list -a 2>&1 | tail -10
  return 1
}

pass() {
  local failed=0
  for entry in "${JOBS[@]}"; do
    local name="${entry%%:*}" file="${entry#*:}"
    [[ -n "$ONLY_JOB" && "$name" != "$ONLY_JOB" ]] && continue
    ensure "$name" "$file" || failed=1
  done
  return "$failed"
}

if [[ "${1:-}" == "--supervise" ]]; then
  echo "supervising: ${JOBS[*]%%:*}"
  while true; do
    pass || echo "$(date +%T) will retry"
    touch "$HEARTBEAT"
    sleep 15
  done
else
  pass
fi
