"""Assets beyond dbt (FR-O1): published training sets, rule backtests, Iceberg maintenance.

- ml/training_set: after the downstream models, publishes a training set cut off at the
  simulated day that triggered the run (build, leakage and DQ-13 gates, Iceberg snapshot,
  tag, reproduced through time travel: scripts/publish_training_set.py).
- marts/rule_backtests: every rules version backtested on the newest published set, with a
  side-by-side comparison (backtester).
- ops/maintenance_runs: compaction, snapshot expiry, orphan removal (FR-L5).
"""

from pathlib import Path

import dagster as dg
from dagster_dbt import get_asset_key_for_model

from orchestration import lineage
from orchestration.dbt_assets import ROOT, downstream_dbt
from orchestration.scripts import run_or_fail


def cutoff_for(context: dg.AssetExecutionContext) -> str:
    """Midnight UTC of the run's simulated day (sensor tag), else of the latest one."""
    day = context.run.tags.get("simulated_day")
    if day is None:
        from orchestration.automation import latest_simulated_day

        day = latest_simulated_day()
    if day is None:
        raise dg.Failure("no simulated day: silver.transactions_current is empty")
    return f"{day} 00:00:00+00:00"


def rules_files(root: Path = ROOT) -> list[str]:
    return [str(p.relative_to(root)) for p in sorted((root / "rules").glob("rules_v*.yml"))]


@dg.asset(
    key=["ml", "training_set"],
    deps=[get_asset_key_for_model([downstream_dbt], "training_set")],
    group_name="ml",
    description="Point-in-time training set, published and tagged (FR-PIT2-4, DQ-13)",
)
def published_training_set(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    cutoff = cutoff_for(context)
    output = run_or_fail(context.log, ["scripts/publish_training_set.py", "--cutoff", cutoff])
    lineage.emit_run(
        "dagster.publish_training_set",
        [lineage.lake("marts.training_set")],
        [lineage.lake("ml.training_set")],
    )
    staged = next((line.strip() for line in output.splitlines() if "staged:" in line), "")
    return dg.MaterializeResult(metadata={"cutoff": cutoff, "staged": staged})


@dg.asset(
    key=["marts", "rule_backtests"],
    deps=[published_training_set],
    group_name="ml",
    description="Every rules version backtested on the newest training set (FR-BT1-3)",
)
def rule_backtests(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    files = rules_files()
    args = ["-m", "backtester.cli"]
    for f in files:
        args += ["--rules", f]
    output = run_or_fail(context.log, args)
    lineage.emit_run(
        "dagster.rule_backtests",
        [lineage.lake("ml.training_set")],
        [lineage.lake("marts.rule_backtests")],
    )
    overall = [
        line
        for line in output.splitlines()
        if line.startswith("| precision") or line.startswith("| recall")
    ]
    return dg.MaterializeResult(metadata={"rules": ", ".join(files), "overall": "\n".join(overall)})


@dg.asset(
    key=["ops", "maintenance_runs"],
    group_name="maintenance",
    description="Iceberg compaction, snapshot expiry > 7 days, orphan removal (FR-L5)",
)
def iceberg_maintenance(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    output = run_or_fail(context.log, ["scripts/iceberg_maintenance.py"])
    total = next((line for line in output.splitlines() if line.startswith("total")), "")
    return dg.MaterializeResult(metadata={"total": " ".join(total.split())})
