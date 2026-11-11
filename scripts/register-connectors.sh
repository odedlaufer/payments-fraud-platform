#!/bin/sh
# Registers every connector in /connectors (file name = connector name) and waits
# until each connector and its tasks are RUNNING. PUT is create-or-update, so
# re-running applies config changes from git. Runs in the `connect-init` service.
set -eu

: "${CONNECT_URL:=http://connect:8083}"
: "${CONNECTORS_DIR:=/connectors}"

for file in "${CONNECTORS_DIR}"/*.json; do
  name=$(basename "$file" .json)
  echo "Registering ${name}"
  curl -fsS -o /dev/null -X PUT -H "Content-Type: application/json" \
    --data @"$file" "${CONNECT_URL}/connectors/${name}/config"

  i=0
  while :; do
    status=$(curl -fs "${CONNECT_URL}/connectors/${name}/status" || true)
    running=$(echo "$status" | grep -o '"state":"RUNNING"' | wc -l | tr -d ' ')
    if echo "$status" | grep -q '"state":"FAILED"'; then
      echo "${name} FAILED:"
      echo "$status"
      exit 1
    fi
    # One connector state plus one state per task.
    if [ "$running" -ge 2 ]; then
      echo "${name} RUNNING"
      break
    fi
    i=$((i + 1))
    [ "$i" -ge 60 ] && echo "${name} not RUNNING after 120s: ${status}" && exit 1
    sleep 2
  done
done
