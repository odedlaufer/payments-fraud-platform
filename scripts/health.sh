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

# Redis refuses writes when full (noeviction): warn at 80% of maxmemory, fail at 90%.
redis_memory() {
  local info used max pct
  info=$(docker compose exec -T redis redis-cli info memory 2>/dev/null) || return 0
  used=$(awk -F: '/^used_memory:/ {print $2}' <<< "$info" | tr -d '\r')
  max=$(awk -F: '/^maxmemory:/ {print $2}' <<< "$info" | tr -d '\r')
  [[ -z "$used" || -z "$max" || "$max" == 0 ]] && return 0
  pct=$(( used * 100 / max ))
  if (( pct >= 90 )); then
    printf "  %-22s FAIL (%s%% of maxmemory)\n" "redis-memory" "$pct"
    FAILED=1
  elif (( pct >= 80 )); then
    printf "  %-22s WARN (%s%% of maxmemory)\n" "redis-memory" "$pct"
  else
    printf "  %-22s OK (%s%% of maxmemory)\n" "redis-memory" "$pct"
  fi
}

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

# Debezium actually holds its replication slot. Connect reported a task RUNNING while its
# Kafka clients had lost the broker and nothing streamed (2026-11-23): the slot is the truth.
cdc_slot_active() {
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "select active from pg_replication_slots where slot_name = 'payments_cdc_slot'" | grep -qx t
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

# A compose service reports healthy (its own healthcheck).
service_healthy() {
  [[ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q "$1")" 2>/dev/null)" == "healthy" ]]
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

# The memory budget (tests/test_compose.py) assumes this stack has the Docker VM to itself:
# another project's containers pushed it out of memory once (incident 2026-11-19 follow-up).
others=$(docker ps --format '{{.Label "com.docker.compose.project"}}' | grep -v '^$' \
  | grep -vx "payments-fraud-platform" | sort | uniq -c || true)
if [[ -n "$others" ]]; then
  echo "  WARN other projects share the Docker VM (stop them: docker compose -p <name> stop):"
  while read -r line; do echo "        $line"; done <<< "$others"
fi

if has_profile core; then
  wait_for "postgres" docker compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
  wait_for "oltp-migrations" migrations_applied
  wait_for "redpanda" docker compose exec -T redpanda sh -c "rpk cluster health | grep -qE 'Healthy:.+true'"
  wait_for "schema-registry" http_ok "http://localhost:${REDPANDA_SCHEMA_REGISTRY_PORT}/subjects"
  wait_for "redpanda-console" http_ok "http://localhost:${REDPANDA_CONSOLE_PORT}/"
  wait_for "kafka-connect" connect_plugins_ready
  wait_for "cdc:payments-postgres" connector_running payments-postgres
  wait_for "cdc:slot-active" cdc_slot_active
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
  wait_for "job:features-card" flink_job_running features-card
  wait_for "job:bronze-decisions" flink_job_running bronze-decisions
  wait_for "redis" docker compose exec -T redis redis-cli ping
  redis_memory
  wait_for "feature-sink" service_healthy feature-sink
  wait_for "decision-service" service_healthy decision-service
fi

# Every Prometheus scrape target up: metrics actually flow, not just containers running.
prom_targets_up() {
  curl -fsS --max-time 3 "http://localhost:${PROMETHEUS_PORT}/api/v1/targets" | python3 -c '
import json, sys
targets = json.load(sys.stdin)["data"]["activeTargets"]
down = [t["labels"]["job"] for t in targets if t["health"] != "up"]
sys.exit(0 if targets and not down else 1)'
}

# The PRD's alert rules are loaded into Prometheus.
prom_rules_loaded() {
  curl -fsS --max-time 3 "http://localhost:${PROMETHEUS_PORT}/api/v1/rules" | python3 -c '
import json, sys
groups = json.load(sys.stdin)["data"]["groups"]
sys.exit(0 if sum(len(g["rules"]) for g in groups) >= 7 else 1)'
}

if has_profile obs; then
  wait_for "prometheus" http_ok "http://localhost:${PROMETHEUS_PORT}/-/ready"
  wait_for "grafana" http_ok "http://localhost:${GRAFANA_PORT}/api/health"
  wait_for "alertmanager" http_ok "http://localhost:${ALERTMANAGER_PORT:-9093}/-/ready"
  wait_for "prometheus-targets" prom_targets_up
  wait_for "alert-rules" prom_rules_loaded
fi
if has_profile lineage; then
  wait_for "marquez-api" http_ok "http://localhost:${MARQUEZ_PORT:-5050}/api/v1/namespaces"
  wait_for "marquez-web" http_ok "http://localhost:${MARQUEZ_WEB_PORT:-3040}"
fi

if (( FAILED )); then
  echo "Unhealthy. Inspect with: make ps / make logs"
  exit 1
fi
echo "All services healthy."
