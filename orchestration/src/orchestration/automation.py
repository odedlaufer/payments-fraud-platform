"""Jobs, schedule and sensor (FR-O2).

- silver_job every 15 minutes: staging and silver, then the gate.
- daily_job on each new simulated day: everything (silver, gate, SCD2, features, marts,
  parity). The simulator's clock is accelerated, so "daily" follows simulated time: the
  sensor watches the latest simulated date in silver and runs once when it advances (one
  run per advance, not per day skipped, and never two runs at once: dagster.yaml).
"""

from pathlib import Path
from typing import Any

import dagster as dg

from orchestration.dbt_assets import ROOT, downstream_dbt, silver_dbt

silver_job = dg.define_asset_job("silver_job", selection=dg.AssetSelection.assets(silver_dbt))
daily_job = dg.define_asset_job(
    "daily_job", selection=dg.AssetSelection.assets(silver_dbt, downstream_dbt)
)

silver_schedule = dg.ScheduleDefinition(
    job=silver_job,
    cron_schedule="*/15 * * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


def load_env(root: Path = ROOT) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (root / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def latest_simulated_day() -> str | None:
    import trino

    env = load_env()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="dagster"
    ).cursor()
    cur.execute("select cast(max(event_ts) as date) from lake.silver.transactions_current")
    (day,) = cur.fetchone()
    return None if day is None else str(day)


def next_simulated_day(cursor: str | None, latest: str | None) -> str | None:
    """The day to run for, or None: only when the simulated date moved past the cursor."""
    if latest is None:
        return None
    if cursor is not None and latest <= cursor:
        return None
    return latest


@dg.sensor(
    job=daily_job, minimum_interval_seconds=60, default_status=dg.DefaultSensorStatus.RUNNING
)
def simulated_day_sensor(context: dg.SensorEvaluationContext) -> Any:
    day = next_simulated_day(context.cursor, latest_simulated_day())
    if day is None:
        return dg.SkipReason(f"simulated day unchanged ({context.cursor})")
    context.update_cursor(day)
    return dg.RunRequest(run_key=f"simulated-day-{day}", tags={"simulated_day": day})
