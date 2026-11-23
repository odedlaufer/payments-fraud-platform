"""Observability (OBS-1, PRD sections 2 and 7): the SLO dashboard, scrape targets, exporters."""

import importlib.util
import json
from pathlib import Path
from typing import Any

import yaml
from prometheus_client import REGISTRY

ROOT = Path(__file__).resolve().parents[1]
SERVICES = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
DASHBOARD = json.loads((ROOT / "observability/grafana/dashboards/platform-slos.json").read_text())
PROMETHEUS = yaml.safe_load((ROOT / "observability/prometheus/prometheus.yml").read_text())

# PRD section 2: metric behind each SLO and its threshold
SLOS = {
    "cdc_lag_seconds": 2,
    "decision_latency_seconds": 5,
    "feature_age_seconds": 5,
    "lake_table_last_commit_timestamp_seconds": 300,
    "lake_parity_ratio": 0.999,
}


def stat_panels() -> list[dict[str, Any]]:
    return [p for p in DASHBOARD["panels"] if p["type"] == "stat"]


def test_dashboard_has_a_live_panel_per_slo_with_the_prd_threshold() -> None:
    for metric, limit in SLOS.items():
        (panel,) = [p for p in stat_panels() if metric in p["targets"][0]["expr"]]
        steps = panel["fieldConfig"]["defaults"]["thresholds"]["steps"]
        assert limit in [s["value"] for s in steps], f"{panel['title']}: threshold {limit}"


def test_dashboard_uses_the_provisioned_datasource() -> None:
    provisioned = yaml.safe_load(
        (ROOT / "observability/grafana/provisioning/datasources/prometheus.yml").read_text()
    )["datasources"][0]
    uids = {p["datasource"]["uid"] for p in DASHBOARD["panels"]}
    assert uids == {provisioned["uid"]}
    assert provisioned["url"] == "http://prometheus:9090"


def test_every_scrape_target_is_a_compose_service_on_its_metrics_port() -> None:
    ports = {
        "decision-service": SERVICES["decision-service"]["environment"]["METRICS_PORT"],
        "lake-metrics": SERVICES["lake-metrics"]["environment"]["METRICS_PORT"],
        "redpanda": "9644",
    }
    targets = [
        t
        for job in PROMETHEUS["scrape_configs"]
        for c in job["static_configs"]
        for t in c["targets"]
    ]
    assert sorted(targets) == sorted(f"{name}:{port}" for name, port in ports.items())


def test_observability_runs_in_its_own_profile_with_health_checks() -> None:
    for name in ("prometheus", "grafana", "lake-metrics"):
        assert SERVICES[name]["profiles"] == ["obs"]
        assert "healthcheck" in SERVICES[name]
    assert "8d" in " ".join(SERVICES["prometheus"]["command"])  # 7-day SLO run fits


def sample(name: str, labels: dict[str, str] | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def test_decision_metrics_measure_the_slo_durations() -> None:
    from decision_service import metrics

    before = (
        sample("cdc_lag_seconds_sum"),
        sample("decision_latency_seconds_sum"),
        sample("feature_age_seconds_sum"),
    )
    metrics.observe_consumed((1, 10_750), commit_ms=10_000)  # 0.75 s in CDC
    metrics.observe_consumed((0, -1), commit_ms=10_000)  # no Kafka timestamp: not observed
    record = {
        "latency_ms": 1_200,
        "decision": "approve",
        "reason": "no_rule_fired",
        "decided_ms": 11_200,
    }
    metrics.observe_decision(record, features_written_ms=10_900)  # features 0.3 s old
    metrics.observe_decision({**record, "reason": "stale_features"}, features_written_ms=None)
    after = (
        sample("cdc_lag_seconds_sum"),
        sample("decision_latency_seconds_sum"),
        sample("feature_age_seconds_sum"),
    )
    assert [round(a - b, 3) for a, b in zip(after, before, strict=True)] == [0.75, 2.4, 0.3]
    assert sample("decisions_total", {"decision": "approve", "reason": "stale_features"}) >= 1


def lake_metrics() -> Any:
    spec = importlib.util.spec_from_file_location(
        "lake_metrics", ROOT / "observability/lake_metrics/lake_metrics.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeCursor:
    def __init__(self, fail_on: str = "") -> None:
        self.fail_on, self.sql = fail_on, ""

    def execute(self, sql: str) -> None:
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("table missing")
        self.sql = sql

    def fetchone(self) -> tuple[Any, ...]:
        return (1_700_000_000.0,) if "$snapshots" in self.sql else (999, 1000)


def test_lake_metrics_set_freshness_and_parity_and_count_errors() -> None:
    lm = lake_metrics()
    assert lm.refresh(FakeCursor()) is True
    assert lm.LAST_COMMIT.labels(table="cdc_transactions")._value.get() == 1_700_000_000.0
    assert lm.PARITY._value.get() == 0.999
    assert lm.refresh(FakeCursor(fail_on="cdc_merchants")) is False  # one table fails
    assert lm.ERRORS.labels(query="commit:cdc_merchants")._value.get() == 1
    assert (
        lm.LAST_COMMIT.labels(table="cdc_cards")._value.get() == 1_700_000_000.0
    )  # others still set
