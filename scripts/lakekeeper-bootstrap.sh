#!/bin/sh
# Idempotent Lakekeeper setup: bootstrap the server once, then create the
# lakehouse warehouse on SeaweedFS if it does not exist (ADR-002).
# Runs in the one-shot `lakekeeper-init` compose service (POSIX sh + curl).
set -eu

: "${LAKEKEEPER_URL:=http://lakekeeper:8181}"
: "${WAREHOUSE_NAME:?}"
: "${LAKE_BUCKET:?}"
: "${S3_ACCESS_KEY:?}"
: "${S3_SECRET_KEY:?}"
: "${S3_ENDPOINT:=http://seaweedfs:8333}"
: "${S3_REGION:=us-east-1}"

MGMT="${LAKEKEEPER_URL}/management/v1"

echo "Waiting for Lakekeeper at ${LAKEKEEPER_URL}"
i=0
until curl -fsS -o /dev/null "${LAKEKEEPER_URL}/health"; do
  i=$((i + 1))
  [ "$i" -ge 60 ] && echo "Lakekeeper not reachable" && exit 1
  sleep 2
done

info=$(curl -fsS "${MGMT}/info")
if echo "$info" | grep -q '"bootstrapped":true'; then
  echo "Server already bootstrapped"
else
  curl -fsS -X POST "${MGMT}/bootstrap" \
    -H "Content-Type: application/json" \
    --data '{"accept-terms-of-use": true}'
  echo "Server bootstrapped"
fi

warehouses=$(curl -fsS "${MGMT}/warehouse")
if echo "$warehouses" | grep -q "\"name\":\"${WAREHOUSE_NAME}\""; then
  echo "Warehouse '${WAREHOUSE_NAME}' already exists"
  exit 0
fi

curl -fsS -X POST "${MGMT}/warehouse" \
  -H "Content-Type: application/json" \
  --data @- <<JSON
{
  "warehouse-name": "${WAREHOUSE_NAME}",
  "storage-profile": {
    "type": "s3",
    "bucket": "${LAKE_BUCKET}",
    "key-prefix": "${WAREHOUSE_NAME}",
    "endpoint": "${S3_ENDPOINT}",
    "region": "${S3_REGION}",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "${S3_ACCESS_KEY}",
    "secret-access-key": "${S3_SECRET_KEY}"
  }
}
JSON
echo
echo "Warehouse '${WAREHOUSE_NAME}' created"
