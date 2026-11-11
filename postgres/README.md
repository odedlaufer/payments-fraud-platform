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
