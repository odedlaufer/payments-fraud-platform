"""Every dbt model as a Dagster asset (FR-O1), in two groups with a gate between them.

silver: staging views and silver current-state tables (CDC applied).
downstream: SCD2, features, marts, parity, ops. They build only after the silver gate
(checks.py) passes: Postgres, bronze and silver agree (PRD 6.3, gate 1).
dbt's tests run in the same `dbt build` and become asset checks (FR-O4).
"""

from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from orchestration import lineage

ROOT = Path(__file__).resolve().parents[3]
DBT = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
DBT.prepare_if_dev()

SILVER = "staging silver"  # dbt selects by folder name (fqn); path: depends on the cwd


# Tests spanning silver and downstream (e.g. marts cover every silver transaction) run only
# where all their parents are built: in the downstream step. dbt's default ("eager") ran
# them in the silver step against stale downstream tables, and they failed.
SILVER_BUILD = ["build", "--indirect-selection", "cautious"]


@dbt_assets(manifest=DBT.manifest_path, select=SILVER, name="silver_dbt")
def silver_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):  # type: ignore[no-untyped-def]
    invocation = dbt.cli(SILVER_BUILD, context=context)
    try:
        yield from invocation.stream()
    finally:  # a partial build still has lineage for the models it built (OBS-2)
        lineage.send_dbt(invocation.target_path)


@dbt_assets(manifest=DBT.manifest_path, exclude=SILVER, name="downstream_dbt")
def downstream_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):  # type: ignore[no-untyped-def]
    invocation = dbt.cli(["build"], context=context)
    try:
        yield from invocation.stream()
    finally:
        lineage.send_dbt(invocation.target_path)
