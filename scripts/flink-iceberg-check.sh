#!/usr/bin/env bash
# ADR-002 verification 2: Flink writes an Iceberg table through Lakekeeper on
# SeaweedFS, and Trino reads exactly what Flink wrote.
set -euo pipefail
cd "$(dirname "$0")/.."

trino() { docker compose exec -T trino trino --output-format=TSV --execute "$1"; }

check() {
  local name="$1" expected="$2" actual="$3"
  if [[ "$actual" == "$expected" ]]; then
    printf "  %-38s OK\n" "$name"
  else
    printf "  %-38s FAIL (expected %s, got %s)\n" "$name" "$expected" "$actual"
    exit 1
  fi
}

cleanup() {
  trino "DROP TABLE IF EXISTS lake.flink_check.smoke" >/dev/null 2>&1 || true
  trino "DROP SCHEMA IF EXISTS lake.flink_check" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

echo "ADR-002 verification 2: Flink -> Lakekeeper -> SeaweedFS -> Trino"
if ! out=$(docker compose exec -T flink-jobmanager ./bin/sql-client.sh \
      -f /opt/flink/sql/checks/iceberg_write_check.sql 2>&1); then
  echo "  Flink SQL failed:"
  tail -40 <<< "$out" | sed 's/^/    /'
  exit 1
fi
printf "  %-38s OK\n" "flink batch insert"

check "trino reads 3 rows" "3" "$(trino "SELECT count(*) FROM lake.flink_check.smoke")"
check "trino reads sum(id) = 6" "6" "$(trino "SELECT sum(id) FROM lake.flink_check.smoke")"
check "snapshot committed by a flink job" "true" \
  "$(trino "SELECT summary['flink.job-id'] IS NOT NULL FROM lake.flink_check.\"smoke\$snapshots\" ORDER BY committed_at DESC LIMIT 1")"
check "data files on s3://lake/lakehouse" "true" \
  "$(trino "SELECT bool_and(file_path LIKE 's3://lake/lakehouse/%') FROM lake.flink_check.\"smoke\$files\"")"

echo "Flink -> Iceberg -> Trino verified."
