# 2026-11-15: bronze stored 8,035 transaction events without status_event_ts

**Impact:** for 8 minutes (12:10:56 to 12:18:34 UTC), bronze stored `pg.public.transactions` events with `status_event_ts` dropped, although Postgres and Kafka had it. No events were lost or duplicated; one field inside them was. Repaired the same day; no downstream consumer was affected (features read Kafka, not bronze).

## Timeline (UTC)

| Time | Event |
| --- | --- |
| 12:10:47 | `make schema-evolve` (adding `transactions.status_event_ts`): the savepoint stop of `bronze-cdc` fails; the runbook aborts before migrating, as designed |
| 12:10:5x | The runbook's exit hook restarts the job supervisor with `docker compose start flink-jobs`, which also starts its dependencies, including the one-shot `postgres-migrate`: the migration is applied while bronze still runs the old schema |
| 12:10:56 | The simulator detects the new column and starts writing it; bronze silently drops it |
| 12:18:34 | A second `make schema-evolve` pauses bronze, evolves it and resumes; new events keep the field |
| 12:3x | Detected by comparing Postgres with bronze; repaired with `make bronze-replay` |

## Root cause

`docker compose start <service>` starts the service's dependency chain, re-running exited one-shot containers. The supervisor (`flink-jobs`) depends on `connect-init`, which depends on `postgres-migrate`. The same behaviour had been seen in the CH-1 chaos test and fixed there with `up -d --no-deps`, but not in the schema-change runbook.

## Repair

`make bronze-replay TABLE=transactions FIELD=status_event_ts` (`scripts/bronze_replay.py`): finds the exact offset range the old-schema job wrote (events whose Kafka value has the field, before the first offset where bronze kept it: [108752, 116787)), pauses bronze with a savepoint, deletes the range, re-ingests it from Kafka with a bounded batch Flink job using the live job's own definitions, verifies, resumes. Then `dbt run --full-refresh --select transactions_current`.

Verified: 8,035 rows restored with the field, bronze exactly once (147,508 events), silver `status_event_ts` count equal to Postgres, DQ-08 passing.

## Prevention

- The runbook restarts the supervisor with `docker compose up -d --no-deps`; no script uses `docker compose start` any more.
- The savepoint stop retries three times and reports Flink's error.
- `make bronze-replay` exists for any future dropped-field incident; Kafka retention is what makes it possible.
