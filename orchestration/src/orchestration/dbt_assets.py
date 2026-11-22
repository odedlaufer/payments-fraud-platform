"""Every dbt model as a Dagster asset (FR-O1), in two groups with a gate between them.

silver: staging views and silver current-state tables (CDC applied).
downstream: SCD2, features, marts, parity, ops. They build only after the silver gate
(checks.py) passes: Postgres, bronze and silver agree (PRD 6.3, gate 1).
dbt's tests run in the same `dbt build` and become asset checks (FR-O4).
"""

from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

ROOT = Path(__file__).resolve().parents[3]
DBT = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
DBT.prepare_if_dev()

SILVER = "staging silver"  # dbt selects by folder name (fqn); path: depends on the cwd


@dbt_assets(manifest=DBT.manifest_path, select=SILVER, name="silver_dbt")
def silver_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):  # type: ignore[no-untyped-def]
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(manifest=DBT.manifest_path, exclude=SILVER, name="downstream_dbt")
def downstream_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):  # type: ignore[no-untyped-def]
    yield from dbt.cli(["build"], context=context).stream()
