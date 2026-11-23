"""Dagster definitions: `make orchestrate` serves them at http://localhost:3070."""

import dagster as dg
from dagster_dbt import DbtCliResource

from orchestration.automation import (
    backtest_job,
    daily_job,
    maintenance_job,
    maintenance_schedule,
    parity_alert_job,
    parity_slo_sensor,
    record_run_failure,
    rules_change_sensor,
    silver_job,
    silver_schedule,
    simulated_day_sensor,
)
from orchestration.checks import lake_matches_postgres
from orchestration.dbt_assets import DBT, downstream_dbt, silver_dbt
from orchestration.lake_assets import iceberg_maintenance, published_training_set, rule_backtests

defs = dg.Definitions(
    assets=[
        silver_dbt,
        downstream_dbt,
        published_training_set,
        rule_backtests,
        iceberg_maintenance,
    ],
    asset_checks=[lake_matches_postgres],
    jobs=[silver_job, daily_job, backtest_job, maintenance_job, parity_alert_job],
    schedules=[silver_schedule, maintenance_schedule],
    sensors=[simulated_day_sensor, rules_change_sensor, parity_slo_sensor, record_run_failure],
    resources={"dbt": DbtCliResource(project_dir=DBT)},
)
