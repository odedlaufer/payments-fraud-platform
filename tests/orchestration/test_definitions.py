"""Dagster orchestration (FR-O1 to FR-O4): assets, the silver gate, schedule and sensor."""

import json
import os
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import dagster as dg
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module", autouse=True)
def manifest() -> None:
    """dagster-dbt builds assets from dbt's manifest; dbt parse needs no database."""
    if (ROOT / "dbt/target/manifest.json").exists():
        return
    env = dict(os.environ)
    for line in (ROOT / ".env.example").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env.setdefault(k, v)
    subprocess.run(["dbt", "parse", "--profiles-dir", "."], cwd=ROOT / "dbt", env=env, check=True)


def keys(asset: Any) -> set[str]:
    return {k.to_user_string() for k in asset.keys}


def test_definitions_load() -> None:
    from orchestration.definitions import defs

    dg.Definitions.validate_loadable(defs)


def test_two_groups_partition_every_dbt_model() -> None:
    from orchestration.dbt_assets import downstream_dbt, silver_dbt

    silver, downstream = keys(silver_dbt), keys(downstream_dbt)
    assert "silver/transactions_current" in silver and "staging/stg_cdc__transactions" in silver
    assert "marts/fct_transactions" in downstream and "marts/training_set" in downstream
    assert not silver & downstream
    nodes = json.loads((ROOT / "dbt/target/manifest.json").read_text())["nodes"].values()
    models = {n["name"] for n in nodes if n["resource_type"] in ("model", "snapshot")}
    assert {k.split("/")[-1] for k in silver | downstream} == models


def test_the_gate_blocks_on_every_silver_asset_only() -> None:
    from orchestration.checks import CHECK, lake_matches_postgres
    from orchestration.dbt_assets import downstream_dbt, silver_dbt

    specs = list(lake_matches_postgres.check_specs)
    assert all(s.name == CHECK and s.blocking for s in specs)
    gated = {s.asset_key.to_user_string() for s in specs}
    assert gated == keys(silver_dbt) and not gated & keys(downstream_dbt)


def gate_results() -> list[dg.AssetCheckResult]:
    import orchestration.checks as checks

    produced = checks.lake_matches_postgres(dg.build_asset_check_context())
    return list(cast(Iterable[dg.AssetCheckResult], produced))


def test_one_failed_reconciliation_fails_the_gate_everywhere(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import orchestration.checks as checks
    from orchestration.dbt_assets import silver_dbt

    outcomes = {"scripts/bronze_reconcile.py": 0, "scripts/silver_reconcile.py": 1}
    monkeypatch.setattr(checks, "run_script", lambda path: (outcomes[path], f"{path}: done"))
    results = gate_results()
    assert len(results) == len(silver_dbt.keys)
    assert all(not r.passed for r in results)
    outcomes["scripts/silver_reconcile.py"] = 0
    assert all(r.passed for r in gate_results())


def test_simulated_day_runs_once_per_advance() -> None:
    from orchestration.automation import next_simulated_day

    assert next_simulated_day(None, "2026-03-30") == "2026-03-30"  # first look: catch up
    assert next_simulated_day("2026-03-30", "2026-03-30") is None
    assert next_simulated_day("2026-03-30", "2026-04-02") == "2026-04-02"  # one run, not three
    assert next_simulated_day("2026-03-30", None) is None


def test_schedule_and_jobs() -> None:
    from orchestration.automation import daily_job, silver_job, silver_schedule

    assert silver_schedule.cron_schedule == "*/15 * * * *"
    assert silver_schedule.job_name == silver_job.name
    assert {silver_job.name, daily_job.name} == {"silver_job", "daily_job"}


def test_one_run_at_a_time() -> None:
    instance = yaml.safe_load((ROOT / "orchestration/dagster_home/dagster.yaml").read_text())
    assert instance["run_queue"]["max_concurrent_runs"] == 1
