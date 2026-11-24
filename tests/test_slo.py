"""The SLO run and its report (PRD section 2, Phase 7 gate)."""

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / f"scripts/{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


report = load("slo_report")
run = load("slo_run")
LATENCY = next(s for s in report.SLOS if s.name == "Decision latency p95")
PARITY = next(s for s in report.SLOS if s.name == "Online/offline parity")


def test_minutes_without_traffic_are_skipped_not_counted() -> None:
    s = report.summarize([1.0, float("nan"), 6.0, 2.0], LATENCY)
    assert s["minutes"] == 3 and s["held"] == pytest.approx(2 / 3)
    assert s["median"] == 2.0 and s["worst"] == 6.0  # worst latency is the highest
    assert report.summarize([float("nan")], LATENCY) == {"minutes": 0}


def test_worst_parity_is_the_lowest() -> None:
    s = report.summarize([1.0, 0.9995, 0.998], PARITY)
    assert s["worst"] == 0.998 and s["held"] == pytest.approx(2 / 3)


def test_alert_minutes_per_alert() -> None:
    series = [
        {"metric": {"alertname": "StaleFeatures"}, "values": [[1, "1"], [61, "1"]]},
        {"metric": {"alertname": "StaleFeatures"}, "values": [[900, "1"]]},
        {"metric": {"alertname": "CdcStalled"}, "values": [[5, "1"]]},
    ]
    assert report.alert_minutes(series) == {"StaleFeatures": 3, "CdcStalled": 1}


def sample(**extra: Any) -> str:
    record = {
        "outcome": "completed",
        "hours": 8.2,
        "sim_days": 49,
        "tps": 20,
        "sim_start": "2026-04-05T00:00:00+00:00",
        "sim_end": "2026-05-24T00:00:00+00:00",
    }
    slos = [(LATENCY, report.summarize([1.0, 1.2], LATENCY)), (PARITY, {"minutes": 0})]
    base = {
        "decisions": 590000,
        "stale_share": 0.0012,
        "dead_letters": 0,
        "alerts": {},
        "checks": {"CDC complete": (True, "CDC complete")},
    }
    text: str = report.render("20261125T2200Z", record, slos, {**base, **extra})
    return text


def test_the_report_states_the_run_and_every_slo() -> None:
    text = sample()
    assert "8.2 hours of real time covering 49 simulated days (2026-04-05 to 2026-05-24)" in text
    assert "| Decision latency p95 | under 5 s | 2 | 100.00% | 1.10 s | 1.20 s |" in text
    assert "| Online/offline parity | at least 99.9% | 0 | no data | | |" in text
    assert "- **Alerts:** none fired" in text and "- **CDC complete:** passed" in text


def test_the_report_names_alerts_and_failed_checks() -> None:
    text = sample(
        alerts={"StaleFeatures": 12}, checks={"Bronze exactly once": (False, "3 missing")}
    )
    assert (
        "StaleFeatures (12 min)" in text and "- **Bronze exactly once:** FAILED (3 missing)" in text
    )


def test_the_newest_run_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "20261124T2200Z.json").write_text(json.dumps({"sim_days": 1}))
    (tmp_path / "20261125T2200Z.json").write_text(json.dumps({"sim_days": 49}))
    monkeypatch.setattr(report, "RUNS", tmp_path)
    assert report.latest_run() == ("20261125T2200Z", {"sim_days": 49})


def test_the_default_run_is_seven_accelerated_weeks() -> None:
    assert run.MINUTES_PER_SIM_DAY == 10
    assert pytest.approx(8.17, abs=0.01) == 49 * run.MINUTES_PER_SIM_DAY / 60


def test_a_checks_verdict_is_not_a_kafka_client_log_line(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    out = "CDC complete\n%4|1795526989.851|OFFSET|rdkafka#consumer-1| offset reset\n"
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, out, "")
    )
    assert report.check("cdc_reconcile.py") == (True, "CDC complete")
