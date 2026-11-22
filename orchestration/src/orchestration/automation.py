"""Jobs, schedules and sensors (FR-O2, FR-O3).

- silver_job every 15 minutes: staging and silver, then the gate.
- daily_job on each new simulated day: silver, gate, SCD2, features, marts, parity, then
  the training set published at that day and every rules version backtested on it. The
  simulator's clock is accelerated, so "daily" follows simulated time: the sensor reads the
  latest simulated date from bronze and runs once when it advances (one run per advance,
  never two runs at once).
- maintenance_job every night at 03:00: Iceberg maintenance.
- rules_change_sensor: a rules file changed -> backtest_job (FR-O3, FR-BT3).
- parity_slo_sensor: parity fell below the 99.9% SLO -> parity_alert_job, which records the
  breach in lake.ops.alerts and fails visibly; once per breach, again after recovery.
"""

import hashlib
import json
from pathlib import Path
from typing import Any

import dagster as dg

from orchestration.dbt_assets import ROOT, downstream_dbt, silver_dbt
from orchestration.lake_assets import iceberg_maintenance, published_training_set, rule_backtests

silver_job = dg.define_asset_job("silver_job", selection=dg.AssetSelection.assets(silver_dbt))
daily_job = dg.define_asset_job(
    "daily_job",
    selection=dg.AssetSelection.assets(
        silver_dbt, downstream_dbt, published_training_set, rule_backtests
    ),
)
backtest_job = dg.define_asset_job(
    "backtest_job", selection=dg.AssetSelection.assets(rule_backtests)
)
maintenance_job = dg.define_asset_job(
    "maintenance_job", selection=dg.AssetSelection.assets(iceberg_maintenance)
)

silver_schedule = dg.ScheduleDefinition(
    job=silver_job,
    cron_schedule="*/15 * * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
maintenance_schedule = dg.ScheduleDefinition(
    job=maintenance_job,
    cron_schedule="0 3 * * *",
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
    # From bronze, which lands every 10 s: silver only moves when a job builds it, so reading
    # silver delayed daily_job until the next 15-minute silver_job. The commit-time filter
    # reads only recent partitions (bronze is partitioned by day of commit).
    cur.execute(
        "select cast(max(from_iso8601_timestamp_nanos(after.event_ts)) as date) "
        "from lake.bronze.cdc_transactions "
        "where commit_ts > current_timestamp - interval '2' day and after is not null"
    )
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


def rules_hashes(root: Path = ROOT) -> dict[str, str]:
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted((root / "rules").glob("rules_v*.yml"))
    }


def changed_rules(cursor: str | None, current: dict[str, str]) -> list[str]:
    """Rules files added or changed since the cursor; nothing on the first look (baseline)."""
    if cursor is None:
        return []
    before = json.loads(cursor)
    return sorted(name for name, digest in current.items() if before.get(name) != digest)


@dg.sensor(
    job=backtest_job, minimum_interval_seconds=30, default_status=dg.DefaultSensorStatus.RUNNING
)
def rules_change_sensor(context: dg.SensorEvaluationContext) -> Any:
    current = rules_hashes()
    changed = changed_rules(context.cursor, current)
    context.update_cursor(json.dumps(current))
    if not changed:
        return dg.SkipReason("no rules file changed")
    key = hashlib.sha256(json.dumps(current, sort_keys=True).encode()).hexdigest()[:16]
    return dg.RunRequest(run_key=f"rules-{key}", tags={"changed_rules": ",".join(changed)})


PARITY_SLO = 0.999


def overall_parity() -> float | None:
    import trino

    env = load_env()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="dagster"
    ).cursor()
    cur.execute(
        "select cast(sum(matching) as double) / nullif(sum(compared), 0) "
        "from lake.marts.parity_daily where feature = '(all features)'"
    )
    (rate,) = cur.fetchone()
    return None if rate is None else float(rate)


def parity_alert(
    cursor: str | None, rate: float | None, slo: float = PARITY_SLO
) -> tuple[bool, str]:
    """(alert now?, new state): alert on entering a breach, not again until it recovers."""
    if rate is None:
        return False, cursor or "ok"
    state = "breach" if rate < slo else "ok"
    return state == "breach" and cursor != "breach", state


@dg.op
def record_parity_alert(context: dg.OpExecutionContext) -> None:
    rate = float(context.run.tags["parity"])
    import trino

    env = load_env()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="dagster"
    ).cursor()
    cur.execute(
        "create table if not exists lake.ops.alerts (raised_at timestamp(6) with time zone, "
        "alert varchar, value double, threshold double)"
    )
    cur.fetchall()
    cur.execute(
        "insert into lake.ops.alerts values "
        f"(current_timestamp, 'parity_below_slo', {rate}, {PARITY_SLO})"
    )
    cur.fetchall()
    raise dg.Failure(f"parity {rate:.4%} is below the {PARITY_SLO:.1%} SLO (DQ-14)")


@dg.job(description="Raised by parity_slo_sensor: records the breach and fails visibly")
def parity_alert_job() -> None:
    record_parity_alert()


@dg.sensor(
    job=parity_alert_job,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def parity_slo_sensor(context: dg.SensorEvaluationContext) -> Any:
    rate = overall_parity()
    alert, state = parity_alert(context.cursor, rate)
    context.update_cursor(state)
    if not alert:
        return dg.SkipReason(f"parity {rate} ({state})")
    return dg.RunRequest(run_key=f"parity-breach-{rate:.6f}", tags={"parity": str(rate)})
