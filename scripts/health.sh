#!/usr/bin/env bash
# Waits until every service in the active compose profiles is healthy.
# Usage: scripts/health.sh [timeout_seconds]
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
# shellcheck disable=SC1091
source .env
set +a

TIMEOUT="${1:-180}"
PROFILES=",${COMPOSE_PROFILES:-core,lake,stream},"
FAILED=0

has_profile() { [[ "$PROFILES" == *",$1,"* ]]; }

wait_for() {
  local name="$1"
  shift
  local start=$SECONDS
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS - start >= TIMEOUT )); then
      printf "  %-22s FAIL (timeout %ss)\n" "$name" "$TIMEOUT"
      FAILED=1
      return 0
    fi
    sleep 2
  done
  printf "  %-22s OK\n" "$name"
}

http_ok() { curl -fsS -o /dev/null --max-time 3 "$1"; }

# Lakekeeper is bootstrapped and the warehouse exists.
warehouse_exists() {
  curl -fsS --max-time 3 "http://localhost:${LAKEKEEPER_PORT}/management/v1/warehouse" \
    | grep -q "\"name\":\"$1\""
}

# Every migration file in postgres/migrations is recorded as applied.
migrations_applied() {
  local expected actual
  expected=$(find postgres/migrations -name '*.sql' | wc -l | tr -d ' ')
  actual=$(docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "select count(*) from schema_migrations" 2>/dev/null | tr -d ' ')
  [[ "$actual" == "$expected" ]]
}

# Connect worker is up and has both plugins Debezium needs.
connect_plugins_ready() {
  local plugins
  plugins=$(curl -fsS --max-time 3 "http://localhost:${CONNECT_PORT}/connector-plugins?connectorsOnly=false")
  grep -q "io.debezium.connector.postgresql.PostgresConnector" <<< "$plugins" \
    && grep -q "io.confluent.connect.avro.AvroConverter" <<< "$plugins"
}

# Debezium connector and its task are RUNNING.
connector_running() {
  curl -fsS --max-time 3 "http://localhost:${CONNECT_PORT}/connectors/$1/status" \
    | grep -o '"state":"RUNNING"' | wc -l | grep -qE '^ *[2-9]'
}

# JobManager is up and the TaskManager has registered its slots.
flink_ready() {
  curl -fsS --max-time 3 "http://localhost:${FLINK_UI_PORT}/overview" | grep -qE '"taskmanagers":[1-9]'
}

# A named Flink job is RUNNING.
flink_job_running() {
  curl -fsS --max-time 3 "http://localhost:${FLINK_UI_PORT}/jobs/overview" \
    | grep -qE "\"name\":\"$1\"[^}]*\"state\":\"RUNNING\""
}

# Trino answers and has finished starting.
trino_ready() {
  curl -fsS --max-time 3 "http://localhost:${TRINO_PORT}/v1/info" | grep -q '"starting":false'
}

# Signed HEAD on the bucket: proves credentials work and the bucket exists.
s3_bucket_exists() {
  curl -fsS -o /dev/null --max-time 3 -I \
    --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" \
    "http://localhost:${S3_API_PORT}/$1"
}

echo "Health check (profiles: ${PROFILES//,/ })"

if has_profile core; then
  wait_for "postgres" docker compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
  wait_for "oltp-migrations" migrations_applied
  wait_for "redpanda" docker compose exec -T redpanda sh -c "rpk cluster health | grep -qE 'Healthy:.+true'"
  wait_for "schema-registry" http_ok "http://localhost:${REDPANDA_SCHEMA_REGISTRY_PORT}/subjects"
  wait_for "redpanda-console" http_ok "http://localhost:${REDPANDA_CONSOLE_PORT}/"
  wait_for "kafka-connect" connect_plugins_ready
  wait_for "cdc:payments-postgres" connector_running payments-postgres
fi

if has_profile lake; then
  wait_for "seaweedfs-s3" http_ok "http://localhost:${S3_API_PORT}/healthz"
  wait_for "s3-bucket:${LAKE_BUCKET}" s3_bucket_exists "${LAKE_BUCKET}"
  wait_for "catalog-db" docker compose exec -T catalog-db pg_isready -U "$CATALOG_DB_USER" -d "$CATALOG_DB_NAME"
  wait_for "lakekeeper" http_ok "http://localhost:${LAKEKEEPER_PORT}/health"
  wait_for "warehouse:${WAREHOUSE_NAME}" warehouse_exists "${WAREHOUSE_NAME}"
  wait_for "trino" trino_ready
fi

if has_profile stream; then
  wait_for "flink" flink_ready
  wait_for "job:bronze-cdc" flink_job_running bronze-cdc
fi

if (( FAILED )); then
  echo "Unhealthy. Inspect with: make ps / make logs"
  exit 1
fi
echo "All services healthy."
