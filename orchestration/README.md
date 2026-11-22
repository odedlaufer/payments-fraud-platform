# orchestration

Dagster for the lakehouse (PRD 5.9). `make orchestrate` serves it at http://localhost:3070 and runs its schedule and sensor while it runs. It runs on the host, not in Docker: the Docker VM's memory budget is fully used by the stack.

- **Assets (FR-O1):** every dbt model, in two groups. `silver_dbt`: staging views and silver current-state tables. `downstream_dbt`: SCD2, features, marts, parity, ops. dbt's tests run in the same `dbt build` and appear as asset checks (FR-O4).
- **The silver gate (PRD 6.3, gate 1):** `lake_matches_postgres`, a blocking check on every silver asset, runs `bronze_reconcile` (every CDC event in bronze exactly once) and `silver_reconcile` (DQ-08). If either fails, nothing downstream of silver builds in that run.
- **Schedule and sensor (FR-O2):** `silver_job` every 15 minutes; `daily_job` (everything) whenever the simulated date advances, because the simulator's clock is accelerated.
- **One run at a time** (`dagster_home/dagster.yaml`): Trino runs one heavy model at a time, and two dbt builds would race on the same Iceberg tables.
