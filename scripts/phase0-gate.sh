#!/usr/bin/env bash
# Phase 0 gate (PRD section 9, ADR-002 verification 1):
# Trino creates, writes and reads an Iceberg table through Lakekeeper on SeaweedFS.
set -euo pipefail

cd "$(dirname "$0")/.."

SCHEMA="gate_check"
NAME="smoke_$(date +%s)"
TABLE="lake.${SCHEMA}.${NAME}"
META="lake.${SCHEMA}.\"${NAME}"

sql() { docker compose exec -T trino trino --output-format=TSV --execute "$1"; }

# Runs a statement quietly; on failure prints Trino's error and stops.
run() {
  local out line
  if ! out=$(sql "$1" 2>&1); then
    echo "  FAILED: $1"
    while IFS= read -r line; do
      echo "    ${line}"
    done <<< "$out"
    exit 1
  fi
}

cleanup() {
  sql "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1 || true
  sql "DROP SCHEMA IF EXISTS lake.${SCHEMA}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

check() {
  local name="$1" expected="$2" actual="$3"
  if [[ "$actual" == "$expected" ]]; then
    printf "  %-34s OK\n" "$name"
  else
    printf "  %-34s FAIL (expected %s, got %s)\n" "$name" "$expected" "$actual"
    exit 1
  fi
}

table_count() {
  sql "SELECT count(*) FROM lake.information_schema.tables
       WHERE table_schema = '${SCHEMA}' AND table_name = '${NAME}'"
}

echo "Phase 0 gate: Trino -> Lakekeeper -> SeaweedFS"

run "CREATE SCHEMA IF NOT EXISTS lake.${SCHEMA}"
check "create schema" "1" \
  "$(sql "SELECT count(*) FROM lake.information_schema.schemata WHERE schema_name = '${SCHEMA}'")"

run "CREATE TABLE ${TABLE} (id bigint, note varchar) WITH (format = 'PARQUET')"
check "create iceberg table" "1" "$(table_count)"

run "INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c')"
check "read rows back" "3" "$(sql "SELECT count(*) FROM ${TABLE}")"
check "sum of ids" "6" "$(sql "SELECT sum(id) FROM ${TABLE}")"
check "latest snapshot added 3 records" "3" \
  "$(sql "SELECT summary['added-records'] FROM ${META}\$snapshots\" ORDER BY committed_at DESC LIMIT 1")"
check "data files on s3://lake/lakehouse" "true" \
  "$(sql "SELECT bool_and(file_path LIKE 's3://lake/lakehouse/%') FROM ${META}\$files\"")"

run "DROP TABLE ${TABLE}"
check "drop table" "0" "$(table_count)"

echo "Phase 0 gate passed."
