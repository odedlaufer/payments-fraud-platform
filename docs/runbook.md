# Operations runbook

How to recover the platform from the failures it is known to have. Every procedure below has
been run on this stack, during an incident or a test; each says when to use it, what to do and
how to know it worked. `make health` is always the first step: it names what is unhealthy.

| Situation | Procedure |
| --- | --- |
| Dead letters to process after a fix | [Replay dead letters](#replay-dead-letters) |
| Silver no longer matches Postgres; Dagster's gate blocks downstream | [Rebuild a silver table from bronze](#rebuild-a-silver-table-from-bronze) |
| A CDC topic is missing history, or keys | [Re-snapshot through Debezium](#re-snapshot-through-debezium) |
| A live rule version is doing harm | [Roll back a rule version](#roll-back-a-rule-version) |
| CdcStalled fires while Connect reports RUNNING | [Restart a frozen Connect task](#restart-a-frozen-connect-task) |
| Flink jobs down, FlinkUnhealthy | [Recover the Flink jobs](#recover-the-flink-jobs) |
| Redis empty after a restart; StaleFeatures | [Let the online store rebuild](#let-the-online-store-rebuild) |

## Replay dead letters

**When:** the DlqSpike alert fired, the cause is fixed (a producer bug, a missing currency) and
the rejected events should be decided after all.

1. See what is waiting, by error code: `make dlq-replay ARGS="--dry-run"`
2. Replay one code at a time: `make dlq-replay ARGS="--code currency_not_allowed"`

**Check:** replayed events are decided like any other. An event that is still invalid goes back
to the dead-letter topic with the same code, so nothing is lost by replaying too early. The
"Dead letters per minute by code" panel in Grafana shows both.

## Rebuild a silver table from bronze

**When:** `make silver-reconcile` (DQ-08) reports a difference, and Dagster's
`lake_matches_postgres` check is blocking everything downstream of silver.

1. Rebuild the table from the full change log in bronze, for example transactions:
   `cd dbt && uv run dbt build --profiles-dir . --select transactions_current --full-refresh`
2. Back in the repository root: `make silver-reconcile`

**Check:** silver equals Postgres row for row. Dagster's next silver run passes its check, and
the downstream assets build again on their schedule.

## Re-snapshot through Debezium

**When:** `make cdc-reconcile` reports keys missing from a CDC topic, for example after Kafka
retention deleted early history ([incident](incidents/2026-11-18-kafka-retention.md)).

1. Re-emit the current rows of the affected tables: `make cdc-snapshot TABLES="transactions"`
   (or leave `TABLES` out for every table)
2. When Debezium has finished: `make cdc-reconcile`, then `make bronze-reconcile`

**Check:** every Postgres row is on its topic again, and bronze holds every change event
exactly once. Consumers may log "offset out of range" while reading a topic whose early history
is gone; that is expected.

## Roll back a rule version

**Normal path, through the gate:** `make promote-rules CANDIDATE=rules/rules_vN.yml`. It
promotes only a version that keeps at least 90% of the live version's precision, with parity in
target, and records the decision in `lake.ops.rule_promotions`.

**Emergency path:** the live version is declining good customers, and the gate refuses the
previous one (it refused v1 against v2, which keeps 86% of v2's precision).

1. Point `RULES_FILE` in `rules/live.env` back at the previous file, for example
   `RULES_FILE=/app/rules/rules_v1.yml`
2. `docker compose up -d --build decision-service`
3. Commit `rules/live.env` through a pull request that says why. The rules workflow comments the
   backtest of the change, and git history records the rollback (the emergency path writes no
   row to `lake.ops.rule_promotions`).

**Check:** new decisions carry the old version. A minute after the restart:
`docker compose exec -T trino trino --execute "select rules_version, count(*) from lake.bronze.decisions where decided_ms > to_unixtime(current_timestamp - interval '5' minute) * 1000 group by 1"`

## Restart a frozen Connect task

**When:** CdcStalled fires, or `make health` reports `cdc:slot-active FAIL`, while Connect still
says the connector is RUNNING. The task has lost the broker and streams nothing; this happened
once after Docker ran out of memory.

1. `docker compose restart connect`
2. `make health`

**Check:** `cdc:slot-active OK`, then `make cdc-reconcile` passes. Postgres kept every change in
the replication slot meanwhile, so nothing is lost.

## Recover the Flink jobs

**When:** FlinkUnhealthy fires, or the TaskManager was killed or ran out of memory.

1. If the TaskManager is not running: `docker compose up -d --no-deps flink-taskmanager`
2. Wait for the jobs to restore from their last checkpoint (about 20 seconds), then
   `make health`

**Check:** `make bronze-reconcile` passes: restored from the checkpoint, nothing is lost or
written twice (failure test CH-1).

## Let the online store rebuild

**When:** Redis restarted empty and StaleFeatures fires.

Nothing to do: the feature sink rebuilds Redis from the feature topic in five to nine minutes.
Meanwhile every payment without fresh features is held for review, never approved (failure test
CH-3).

**Check:** the StaleFeatures alert clears, and `make online-check` reports the online features
equal to the newest feature records.
