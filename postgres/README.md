# postgres

OLTP source of truth for the simulator, captured by Debezium (FR-CDC1, FR-CDC3).

- `migrations/`: versioned [dbmate](https://github.com/amacneil/dbmate) migrations, applied by the `postgres-migrate` compose service on `make up`.
- Server runs with `wal_level=logical`; publication `payments_cdc` covers the six domain tables and `cdc_heartbeat`.
- `sim.sim_truth` holds ground-truth labels and is never published.

```bash
make migrate            # apply pending migrations
make migrate-status     # list applied and pending
make migrate-rollback   # roll back the latest migration
make new-migration NAME=add_x   # create a new timestamped migration file
```

## Changing the schema

Additive changes (new nullable columns) flow through every layer with one command:

```bash
make new-migration NAME=add_x     # write the migration
make schema-evolve                # apply it end to end, then commit the regenerated files
```

`make schema-evolve` pauses the bronze Flink job with a savepoint, migrates Postgres, regenerates bronze DDL, the Flink job and dbt staging from the new schema, adds the new fields to the bronze Iceberg tables, resumes the job from the savepoint and runs dbt. The pause matters: a job still running with the old schema would read the new field and silently drop it. Kafka buffers the change events while it is paused, so nothing is lost or duplicated.

Removing a column or changing its type is a breaking change and is refused (DC-3).

If step 5 fails because Flink cannot map the savepoint onto the changed job, resume without it: `docker compose run --rm --no-deps -e ONLY_JOB=bronze-cdc flink-jobs bash /submit-flink-jobs.sh`, then `docker compose start flink-jobs`. The job starts from its committed Kafka offsets; `make bronze-reconcile` confirms nothing was lost or duplicated.
