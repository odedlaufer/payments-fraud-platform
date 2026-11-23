"""Every PRD alert fires when its condition holds and stays quiet when healthy (promtool).

Each scenario feeds synthetic series to Prometheus's own rule engine. Expected labels and
annotations come from the rule file, so tests and rules cannot drift apart. promtool runs
from PATH (or PROMTOOL), else through the pinned Prometheus image.
"""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
RULES = ROOT / "observability/prometheus/rules/alerts.yml"
IMAGE = "prom/prometheus:v3.5.0"
# series, by short name
ACTIVE = 'pg_replication_slot_active{slot="payments_cdc_slot"}'
LAG = 'pg_replication_slot_lag_bytes{slot="payments_cdc_slot"}'
TASK = 'connect_task_running{connector="payments-postgres",task="0"}'
B = 'decision_latency_seconds_bucket{le="%s"}'
STALE = 'decisions_total{decision="review",reason="stale_features"}'
OK = 'decisions_total{decision="approve",reason="no_rule_fired"}'
RUN, RESTARTS = (
    'flink_job_running{flink_job="bronze-cdc"}',
    'flink_job_restarts{flink_job="bronze-cdc"}',
)
FAILED, DONE = (
    'flink_checkpoints_failed{flink_job="bronze-cdc"}',
    'flink_checkpoints_completed{flink_job="bronze-cdc"}',
)
GATE = 'lake_alerts_recent{alert="dagster_run_failed:daily_job"}'
UP = 'up{job="lake-metrics",instance="lake-metrics:9109"}'
DEAD = 'dead_lettered_total{code="amount_not_positive"}'
READ = 'transaction_events_consumed_total{topic="pg.public.transactions"}'
COMMIT = 'lake_table_last_commit_timestamp_seconds{table="cdc_transactions"}'
SLOW = {B % "1": "0x20", B % "5": "0x20", B % "10": "0+60x20", B % "+Inf": "0+60x20"}
FAST = {B % "1": "0+60x20", B % "5": "0+60x20", B % "10": "0+60x20", B % "+Inf": "0+60x20"}

# alert, input series {series: values}, eval time, fires?
SCENARIOS: list[tuple[str, dict[str, str], str, bool]] = [
    ("CdcStalled", {ACTIVE: "1 1 0x8", LAG: "0x10", TASK: "1x10"}, "3m", False),  # not 2 min yet
    ("CdcStalled", {ACTIVE: "1 1 0x8", LAG: "0x10", TASK: "1x10"}, "5m", True),  # slot inactive
    ("CdcStalled", {ACTIVE: "1x10", LAG: "200000000x10", TASK: "1x10"}, "5m", True),  # > 100 MB
    ("CdcStalled", {ACTIVE: "1x10", LAG: "0x10", TASK: "0x10"}, "5m", True),  # task down
    ("CdcStalled", {ACTIVE: "1x10", LAG: "5000x10", TASK: "1x10"}, "8m", False),  # healthy
    ("DecisionsSlow", SLOW, "12m", True),
    ("DecisionsSlow", FAST, "12m", False),
    ("StaleFeatures", {STALE: "0+2x20", OK: "0+98x20"}, "6m", True),  # 2% stale
    ("StaleFeatures", {STALE: "0+0x20", OK: "0+98x20"}, "6m", False),
    ("ParityBreach", {"lake_parity_ratio": "0.9985x5"}, "2m", True),
    ("ParityBreach", {"lake_parity_ratio": "1x5"}, "2m", False),
    ("GateFailed", {GATE: "1x5"}, "2m", True),
    ("GateFailed", {GATE: "0x5"}, "2m", False),
    ("FlinkUnhealthy", {RUN: "1 1 0x8"}, "5m", True),  # job down
    ("FlinkUnhealthy", {RUN: "1x10", RESTARTS: "0 0 1x8"}, "4m", True),  # restarted
    ("FlinkUnhealthy", {RUN: "1x15", FAILED: "0+1x15", DONE: "7x15"}, "12m", True),  # failing
    (
        "FlinkUnhealthy",
        {RUN: "1x15", RESTARTS: "0x15", FAILED: "0x15", DONE: "0+1x15"},
        "12m",
        False,
    ),
    ("DlqSpike", {DEAD: "0+1x20", READ: "0+600x20"}, "6m", True),  # 1 in 600: 0.17%
    ("DlqSpike", {DEAD: "0+1x20", READ: "0+6000x20"}, "6m", False),  # 1 in 6,000: 0.017%
    ("LakehouseStale", {COMMIT: "0x10"}, "7m", True),  # last commit at 0, 7 min ago
    ("LakehouseStale", {COMMIT: "0+60x10"}, "7m", False),  # a commit every minute
    ("MetricsTargetDown", {UP: "1 0x6"}, "4m", True),
    ("MetricsTargetDown", {UP: "1x6"}, "4m", False),
]


def rules() -> dict[str, dict[str, Any]]:
    groups = yaml.safe_load(RULES.read_text())["groups"]
    return {r["alert"]: r for g in groups for r in g["rules"]}


# Alerts that keep labels of the series that fired them (their expressions do not aggregate)
KEPT_LABELS = {
    "LakehouseStale": {"table": "cdc_transactions"},
    "MetricsTargetDown": {"job": "lake-metrics", "instance": "lake-metrics:9109"},
}


def expected(alert: str, series: dict[str, str]) -> dict[str, Any]:
    rule = rules()[alert]
    labels = {**rule.get("labels", {}), **KEPT_LABELS.get(alert, {})}
    annotations = dict(rule.get("annotations", {}))
    if alert == "MetricsTargetDown":  # the summary is templated with the target's job
        annotations["summary"] = annotations["summary"].replace("{{ $labels.job }}", "lake-metrics")
    return {"exp_labels": labels, "exp_annotations": annotations}


def promtool() -> list[str] | None:
    if found := os.environ.get("PROMTOOL") or shutil.which("promtool"):
        return [found]
    if shutil.which("docker"):
        return [
            "docker",
            "run",
            "--rm",
            "-v",
            "{dir}:/t",
            "-w",
            "/t",
            "--entrypoint",
            "promtool",
            IMAGE,
        ]
    return None


def test_every_prd_alert_has_scenarios() -> None:
    tested = {alert for alert, *_ in SCENARIOS}
    assert tested == set(rules())
    for alert in tested:  # each alert both fires and stays quiet
        assert {fires for a, _, _, fires in SCENARIOS if a == alert} == {True, False}


def test_alert_rules_with_promtool(tmp_path: Path) -> None:
    command = promtool()
    if command is None:
        pytest.skip("neither promtool nor docker available")
    shutil.copy(RULES, tmp_path / "alerts.yml")
    tests = []
    for alert, series, at, fires in SCENARIOS:
        exp = [expected(alert, series)] if fires else []
        tests.append(
            {
                "interval": "1m",
                "input_series": [{"series": s, "values": v} for s, v in series.items()],
                "alert_rule_test": [{"eval_time": at, "alertname": alert, "exp_alerts": exp}],
            }
        )
    (tmp_path / "alerts.test.yml").write_text(
        yaml.safe_dump(
            {"rule_files": ["alerts.yml"], "evaluation_interval": "1m", "tests": tests},
            sort_keys=False,
        )
    )
    argv = [a.replace("{dir}", str(tmp_path)) for a in command]
    run = subprocess.run(
        [*argv, "test", "rules", "alerts.test.yml"], cwd=tmp_path, capture_output=True, text=True
    )
    assert run.returncode == 0, run.stdout + run.stderr
