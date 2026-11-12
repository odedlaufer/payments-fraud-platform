#!/usr/bin/env bash
# Applies a Postgres schema change through every layer without losing data (DC-4).
#
# Order matters: the bronze job reads Avro with the schema compiled into its SQL,
# so it is paused before the source changes and resumed after its definitions
# have caught up. Kafka buffers the change events in between.
#
#   0. stop the job supervisor, so it does not resubmit the paused job
#   1. pause the bronze job with a savepoint
#   2. migrate Postgres
#   3. regenerate bronze DDL, the Flink job and dbt staging from the new schema
#   4. add new fields to the bronze Iceberg tables (additive changes only)
#   5. resume the bronze job from the savepoint with the new schema
#   6. dbt: staging views and silver tables pick up the new columns
#   7. start the job supervisor again
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n[%s] %s\n' "$(date +%T)" "$1"; }

step "0/7 stop the job supervisor"
docker compose stop flink-jobs >/dev/null 2>&1
trap 'docker compose start flink-jobs >/dev/null 2>&1 || true' EXIT

step "1/7 pause bronze-cdc with a savepoint"
set +e
savepoint=$(scripts/flink-savepoint-stop.sh bronze-cdc)
status=$?
set -e
case "$status" in
  0) echo "  savepoint: ${savepoint}" ;;
  2) savepoint=""
     echo "  bronze-cdc was not running; it will start fresh from its committed offsets" ;;
  *) echo "  ABORT: could not pause bronze-cdc; nothing was changed" >&2
     exit 1 ;;
esac

step "2/7 migrate Postgres"
make --no-print-directory migrate 2>&1 | grep -E "Applying|Applied|Error|no pending" || echo "  no pending migrations"

step "3/7 regenerate definitions from the Postgres schema"
uv run python scripts/gen_bronze.py
git status --short trino/ddl flink/sql/bronze dbt/models/staging | sed 's/^/  changed: /' || true

step "4/7 evolve bronze Iceberg tables"
uv run python scripts/evolve_bronze.py

step "5/7 resume bronze-cdc"
docker compose run --rm --no-deps -e RESTORE_PATH="${savepoint}" -e ONLY_JOB=bronze-cdc \
  flink-jobs bash /submit-flink-jobs.sh 2>&1 | grep -E "bronze-cdc"

step "6/7 dbt run (staging views, silver new columns)"
(cd dbt && uv run dbt run --profiles-dir . 2>&1) | grep -E "ERROR|Done\."

step "7/7 start the job supervisor"
docker compose start flink-jobs >/dev/null 2>&1
trap - EXIT

echo
echo "Schema evolution applied. Verify with: make quick-check"
