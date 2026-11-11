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
PROFILES=",${COMPOSE_PROFILES:-core,lake},"
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
  wait_for "redpanda" docker compose exec -T redpanda sh -c "rpk cluster health | grep -qE 'Healthy:.+true'"
  wait_for "schema-registry" http_ok "http://localhost:${REDPANDA_SCHEMA_REGISTRY_PORT}/subjects"
  wait_for "redpanda-console" http_ok "http://localhost:${REDPANDA_CONSOLE_PORT}/"
fi

if has_profile lake; then
  wait_for "seaweedfs-s3" http_ok "http://localhost:${S3_API_PORT}/healthz"
  wait_for "s3-bucket:${LAKE_BUCKET}" s3_bucket_exists "${LAKE_BUCKET}"
fi

if (( FAILED )); then
  echo "Unhealthy. Inspect with: make ps / make logs"
  exit 1
fi
echo "All services healthy."
