"""Training-set, backtest and maintenance assets; the rules and parity sensors (FR-O1, O3)."""

import json
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest


def selected(job: Any) -> set[str]:
    from orchestration.definitions import defs

    resolved = defs.resolve_job_def(job.name).asset_layer.selected_asset_keys
    return {k.to_user_string() for k in resolved}


def test_daily_job_publishes_and_backtests_after_the_models() -> None:
    from orchestration.automation import backtest_job, daily_job, maintenance_job

    daily = selected(daily_job)
    assert {"ml/training_set", "marts/rule_backtests", "marts/training_set"} <= daily
    assert "ops/maintenance_runs" not in daily
    assert selected(backtest_job) == {"marts/rule_backtests"}
    assert selected(maintenance_job) == {"ops/maintenance_runs"}


def test_rules_sensor_reports_changed_files_after_a_baseline() -> None:
    from orchestration.automation import changed_rules

    now = {"rules_v1.yml": "a", "rules_v2.yml": "b"}
    assert changed_rules(None, now) == []  # first look records the baseline
    assert changed_rules(json.dumps(now), now) == []
    assert changed_rules(json.dumps(now), {**now, "rules_v2.yml": "c"}) == ["rules_v2.yml"]
    assert changed_rules(json.dumps(now), {**now, "rules_v3.yml": "d"}) == ["rules_v3.yml"]


def test_parity_alerts_once_per_breach() -> None:
    from orchestration.automation import parity_alert

    assert parity_alert(None, 1.0) == (False, "ok")
    assert parity_alert("ok", 0.998) == (True, "breach")  # entering a breach
    assert parity_alert("breach", 0.997) == (False, "breach")  # still in it: no repeat
    assert parity_alert("breach", 0.9995) == (False, "ok")  # recovered: re-armed
    assert parity_alert("ok", None) == (False, "ok")  # no data: no change


def test_training_set_cutoff_is_the_runs_simulated_day() -> None:
    from orchestration.lake_assets import cutoff_for

    context = SimpleNamespace(run=SimpleNamespace(tags={"simulated_day": "2026-04-02"}))
    assert cutoff_for(context) == "2026-04-02 00:00:00+00:00"  # type: ignore[arg-type]


def test_assets_call_the_project_scripts(monkeypatch: pytest.MonkeyPatch) -> None:
    import orchestration.lake_assets as la

    calls: list[list[str]] = []

    def fake_run(log: Any, args: list[str]) -> str:
        calls.append(args)
        return "  staged: 10 transactions"

    monkeypatch.setattr(la, "run_or_fail", fake_run)
    monkeypatch.setattr(la, "cutoff_for", lambda context: "2026-04-02 00:00:00+00:00")
    la.published_training_set(dg.build_asset_context())
    la.rule_backtests(dg.build_asset_context())
    la.iceberg_maintenance(dg.build_asset_context())
    assert calls[0] == ["scripts/publish_training_set.py", "--cutoff", "2026-04-02 00:00:00+00:00"]
    assert calls[1][:2] == ["-m", "backtester.cli"]
    assert calls[1][2:] == [x for f in la.rules_files() for x in ("--rules", f)]
    assert "rules/rules_v1.yml" in calls[1] and "rules/rules_v2.yml" in calls[1]
    assert calls[2] == ["scripts/iceberg_maintenance.py"]


def test_a_failing_script_fails_the_step(monkeypatch: pytest.MonkeyPatch) -> None:
    import orchestration.scripts as scripts

    monkeypatch.setattr(scripts, "run_script", lambda args: (1, "REFUSED: leakage"))
    with pytest.raises(dg.Failure, match="exited 1"):
        scripts.run_or_fail(dg.build_asset_context().log, ["scripts/publish_training_set.py"])
