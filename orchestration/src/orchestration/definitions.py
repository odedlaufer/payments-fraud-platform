"""Dagster definitions: `make orchestrate` serves them at http://localhost:3070."""

import dagster as dg
from dagster_dbt import DbtCliResource

from orchestration.automation import daily_job, silver_job, silver_schedule, simulated_day_sensor
from orchestration.checks import lake_matches_postgres
from orchestration.dbt_assets import DBT, downstream_dbt, silver_dbt

defs = dg.Definitions(
    assets=[silver_dbt, downstream_dbt],
    asset_checks=[lake_matches_postgres],
    jobs=[silver_job, daily_job],
    schedules=[silver_schedule],
    sensors=[simulated_day_sensor],
    resources={"dbt": DbtCliResource(project_dir=DBT)},
)
