"""Demo console: health, the decision feed's verdicts, controls, auto-play and the API."""

from typing import Any

import pytest
from fastapi.testclient import TestClient

from demo import status
from demo.control import Chaos, Traffic
from demo.feed import DecisionFeed, classify
from demo.story import Story


def test_health_is_ok_down_or_unknown_never_a_false_green() -> None:
    answers = {q: 1.0 for q in status.HEALTH.values()}
    answers[status.HEALTH["debezium"]] = 0.0
    answers[status.HEALTH["redpanda"]] = None  # type: ignore[assignment]
    snap = status.snapshot(lambda q: answers.get(q, 0.5), postgres=lambda: True)
    assert snap["health"]["decision_service"] == "ok"
    assert snap["health"]["debezium"] == "down"
    assert snap["health"]["redpanda"] == "unknown"
    assert snap["health"]["postgres"] == "ok"

    def unreachable(q: str) -> float | None:
        raise ConnectionError("Prometheus down")

    snap = status.snapshot(unreachable, postgres=lambda: False)
    assert set(snap["health"].values()) == {"unknown", "down"}  # never ok without evidence
    assert all(v is None for v in snap["numbers"].values())


def test_attack_verdicts() -> None:
    assert classify("decline", "rules", "card_testing") == "caught"
    assert classify("review", "rules", "account_takeover") == "caught"
    assert (
        classify("review", "stale_features", "impossible_travel") == "held"
    )  # safe, but not detection
    assert classify("approve", "no_rule_fired", "card_testing") == "missed"
    assert classify("decline", "rules", None) is None  # legitimate payments are not scored


def decision(txn: int, outcome: str, reason: str, at: int) -> dict[str, Any]:
    return {
        "transaction_id": txn,
        "card_id": 9,
        "decision": outcome,
        "reason": reason,
        "fired_rules": ["card_testing"] if reason == "rules" else [],
        "decided_ms": at,
        "latency_ms": 900,
    }


def test_the_feed_scores_attacks_and_counts_approvals() -> None:
    feed = DecisionFeed()
    payments = {
        1: ("2.10", "USD", "online", "card_testing"),
        2: ("40.00", "EUR", "card_present", None),
        3: ("3.50", "USD", "online", "card_testing"),
    }
    feed.add(
        [
            decision(1, "decline", "rules", 1000),
            decision(2, "approve", "no_rule_fired", 2000),
            decision(3, "approve", "no_rule_fired", 3000),
        ],
        payments,
    )
    view = feed.view()
    assert view["attacks"] == {"card_testing": {"caught": 1, "missed": 1}}
    assert view["outcomes"] == {"decline": 1, "approve": 2}
    assert view["items"][0]["transaction_id"] == 3  # newest first
    assert view["items"][2]["amount"] == "2.10 USD" and view["items"][2]["verdict"] == "caught"
    assert feed.approvals_between(1500, 2500) == 1


def test_an_attack_rides_the_next_traffic_chunk() -> None:
    traffic = Traffic(tps=20)
    assert "--attack" not in traffic.command(None)
    assert traffic.command("impossible_travel")[-2:] == ["--attack", "impossible_travel"]


def test_one_failure_at_a_time_and_only_known_ones() -> None:
    chaos = Chaos(DecisionFeed())
    assert not chaos.run("delete_production")
    chaos.state = {"kind": "kill_flink", "phase": "recovering"}
    assert not chaos.run("pause_features")


def test_an_attack_step_waits_until_the_attack_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    feed = DecisionFeed()
    traffic = Traffic()
    launched: list[str] = []
    monkeypatch.setattr(traffic, "attack", launched.append)
    story = Story(traffic, Chaos(feed), feed)
    steps = story.steps()
    assert len(steps) == 8 and steps[0].title == "Live payments"
    step = next(s for s in steps if s.title == "Attack: impossible travel")
    assert step.action is not None and step.done is not None
    step.action()
    assert launched == ["impossible_travel"] and not step.done()
    feed.add(
        [decision(7, "decline", "rules", 1)],
        {7: ("900.00", "USD", "card_present", "impossible_travel")},
    )
    assert step.done()


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    from demo import app as console

    monkeypatch.setattr(console.feed, "start", lambda: None)  # no Kafka in tests
    monkeypatch.setattr(status, "snapshot", lambda: {"numbers": {}, "health": {}})
    console._cache.update(at=0.0, status=None)
    return TestClient(console.app)


def test_the_api_serves_the_page_and_the_state(client: TestClient) -> None:
    with client:
        assert "Payments fraud platform" in client.get("/").text
        state = client.get("/api/state").json()
        assert {
            "health",
            "numbers",
            "feed",
            "traffic",
            "chaos",
            "story",
            "attacks",
            "failures",
            "links",
        } <= set(state)
        assert "merchant_bust_out" not in state["attacks"]  # no rule catches it yet: no button
        assert client.post("/api/attack/bank_heist").status_code == 404
        assert client.post("/api/chaos/delete_production").status_code == 404


def test_feature_store_light_with_prometheus_itself(tmp_path: Any) -> None:
    """Evaluated by promtool: green with fresh features, green with no traffic, red when stale.
    A plain `rate == 0` returns the value 0 and showed an idle feature store as down."""
    import subprocess

    import yaml

    from tests.test_alert_rules import promtool

    command = promtool()
    if command is None:
        pytest.skip("neither promtool nor docker available")
    expr = status.HEALTH["feature_store"]
    fresh = {
        'feature_age_seconds_bucket{le="0.05"}': "0+5x40",
        'feature_age_seconds_bucket{le="+Inf"}': "0+5x40",
    }
    stale = {
        'feature_age_seconds_bucket{le="5.0"}': "0x40",
        'feature_age_seconds_bucket{le="10.0"}': "0+5x40",
        'feature_age_seconds_bucket{le="+Inf"}': "0+5x40",
    }
    cases = [
        (
            "fresh",
            {**fresh, 'decisions_total{decision="approve",reason="no_rule_fired"}': "0+5x40"},
            1,
        ),
        (
            "idle",  # nothing moves: no feature updates, no decisions
            {
                'feature_age_seconds_bucket{le="+Inf"}': "100x40",
                'decisions_total{decision="approve",reason="no_rule_fired"}': "100x40",
            },
            1,
        ),
        (
            "stale",
            {**stale, 'decisions_total{decision="review",reason="stale_features"}': "0+5x40"},
            0,
        ),
    ]
    tests = [
        {
            "interval": "15s",  # as Prometheus scrapes: a 1m rate needs two samples
            "input_series": [{"series": k, "values": v} for k, v in series.items()],
            "promql_expr_test": [
                {"expr": expr, "eval_time": "8m", "exp_samples": [{"labels": "{}", "value": want}]}
            ],
        }
        for _, series, want in cases
    ]
    (tmp_path / "health.test.yml").write_text(
        yaml.safe_dump({"rule_files": [], "tests": tests}, sort_keys=False)
    )
    argv = [a.replace("{dir}", str(tmp_path)) for a in command]
    run = subprocess.run(
        [*argv, "test", "rules", "health.test.yml"], cwd=tmp_path, capture_output=True, text=True
    )
    assert run.returncode == 0, run.stdout + run.stderr
