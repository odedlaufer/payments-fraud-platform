# orchestration

Dagster for the lakehouse (PRD 5.9). `make orchestrate` serves it at http://localhost:3070 and runs its schedule and sensor while it runs. It runs on the host, not in Docker: the Docker VM's memory budget is fully used by the stack.

- **Assets (FR-O1):** every dbt model, in two groups. `silver_dbt`: staging views and silver current-state tables. `downstream_dbt`: SCD2, features, marts, parity, ops. dbt's tests run in the same `dbt build` and appear as asset checks (FR-O4).
- **The silver gate (PRD 6.3, gate 1):** `lake_matches_postgres`, a blocking check on every silver asset, runs `bronze_reconcile` (every CDC event in bronze exactly once) and `silver_reconcile` (DQ-08). If either fails, nothing downstream of silver builds in that run.
- **More assets (FR-O1):** `ml/training_set` publishes a training set cut off at the run's simulated day (leakage and DQ-13 gates, Iceberg snapshot, tag); `marts/rule_backtests` backtests every rules version on it with a comparison; `ops/maintenance_runs` runs Iceberg maintenance.
- **Schedules (FR-O2):** `silver_job` every 15 minutes; `daily_job` (silver, gate, downstream, training set, backtests) whenever the simulated date advances, because the simulator's clock is accelerated; `maintenance_job` every night at 03:00.
- **Sensors (FR-O3):** `rules_change_sensor` runs `backtest_job` when a rules file changes; `parity_slo_sensor` raises `parity_alert_job` (a row in `lake.ops.alerts` and a failed run) when parity drops below 99.9%, once per breach.
- **One run at a time** (`dagster_home/dagster.yaml`): Trino runs one heavy model at a time, and two dbt builds would race on the same Iceberg tables.
