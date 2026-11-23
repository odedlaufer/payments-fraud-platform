"""Observability (OBS-1, PRD sections 2 and 7): the SLO dashboard, scrape targets, exporters."""

import functools
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
        "stream-metrics": SERVICES["stream-metrics"]["environment"]["METRICS_PORT"],
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
    for name in ("prometheus", "grafana", "lake-metrics", "stream-metrics", "alertmanager"):
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


@functools.cache
def load(name: str) -> Any:
    """Each exporter module once: its metrics register in Prometheus's global registry."""
    spec = importlib.util.spec_from_file_location(name, ROOT / f"observability/{name}/{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def lake_metrics() -> Any:
    return load("lake_metrics")


class FakeCursor:
    def __init__(self, fail_on: str = "", alerts: list[tuple[str, int]] | None = None) -> None:
        self.fail_on, self.sql = fail_on, ""
        self.alerts = alerts if alerts is not None else [("dagster_run_failed:daily_job", 2)]

    def execute(self, sql: str) -> None:
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("table missing")
        self.sql = sql

    def fetchone(self) -> tuple[Any, ...]:
        if "$snapshots" in self.sql:
            return (1_700_000_000.0,)
        if "information_schema" in self.sql:
            return (1,)
        return (999, 1000)

    def fetchall(self) -> list[tuple[str, int]]:
        return self.alerts


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


def test_lake_metrics_count_recent_alerts_and_reset_old_ones() -> None:
    lm = lake_metrics()
    assert lm.refresh(FakeCursor()) is True
    assert lm.ALERTS_RECENT.labels(alert="dagster_run_failed:daily_job")._value.get() == 2
    assert lm.refresh(FakeCursor(alerts=[])) is True  # an hour later: nothing recent
    assert lm.ALERTS_RECENT.labels(alert="dagster_run_failed:daily_job")._value.get() == 0


def test_prometheus_loads_the_rules_and_sends_to_alertmanager() -> None:
    assert PROMETHEUS["rule_files"] == ["/etc/prometheus/rules/alerts.yml"]
    (am,) = PROMETHEUS["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"]
    assert am == "alertmanager:9093"
    mounts = SERVICES["prometheus"]["volumes"]
    assert "./observability/prometheus/rules:/etc/prometheus/rules:ro" in mounts


def test_dashboard_shows_firing_alerts_first() -> None:
    first = DASHBOARD["panels"][0]
    assert first["title"] == "Firing alerts" and "ALERTS" in first["targets"][0]["expr"]


def stream_metrics(monkeypatch: Any, responses: dict[str, Any]) -> Any:
    module = load("stream_metrics")

    def fake_get(url: str) -> Any:
        for suffix, value in responses.items():
            if url.endswith(suffix):
                if isinstance(value, Exception):
                    raise value
                return value
        raise OSError(f"unexpected {url}")

    monkeypatch.setattr(module, "get_json", fake_get)
    return module


def test_stream_metrics_count_the_running_flink_run(monkeypatch: Any) -> None:
    overview = {"jobs": [
        {"name": "bronze-cdc", "jid": "old", "state": "CANCELED"},
        {"name": "bronze-cdc", "jid": "new", "state": "RUNNING"},
        {"name": "features-card", "jid": "f", "state": "RUNNING"},
    ]}  # fmt: skip
    sm = stream_metrics(monkeypatch, {
        "/jobs/overview": overview,
        "/checkpoints": {"counts": {"completed": 40, "failed": 1}},
        "numRestarts": [{"id": "numRestarts", "value": "0"}],
    })  # fmt: skip
    sm.read_flink("http://flink")
    assert (
        sm.FLINK_RUNNING.labels(flink_job="bronze-cdc")._value.get() == 1
    )  # the cancelled run is ignored
    assert (
        sm.FLINK_RUNNING.labels(flink_job="bronze-decisions")._value.get() == 0
    )  # not listed: down
    assert sm.CKPT_DONE.labels(flink_job="features-card")._value.get() == 40


def test_stream_metrics_unreachable_services_read_as_down(monkeypatch: Any) -> None:
    sm = stream_metrics(
        monkeypatch, {"/jobs/overview": OSError("refused"), "/status": OSError("refused")}
    )
    sm.read_flink("http://flink")
    sm.read_connect("http://connect")
    assert all(sm.FLINK_RUNNING.labels(flink_job=j)._value.get() == 0 for j in sm.FLINK_JOBS)
    assert sm.CONNECT_TASK.labels(connector="payments-postgres", task="0")._value.get() == 0
