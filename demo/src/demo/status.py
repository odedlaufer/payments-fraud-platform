"""Headline numbers and component health, from Prometheus (the platform's own metrics)."""

import json
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from demo.config import port

P95 = "histogram_quantile(0.95, sum by (le) (rate({m}_bucket[5m])))"

SLOT = 'pg_replication_slot_active{slot="payments_cdc_slot"}'
BRONZE_AGE = 'time() - lake_table_last_commit_timestamp_seconds{table="cdc_transactions"}'
RATE = "sum(rate(decisions_total{sel}[1m]))"

HEADLINE = {
    "decision_p95_s": P95.format(m="decision_latency_seconds"),
    "cdc_lag_p95_s": P95.format(m="cdc_lag_seconds"),
    "feature_age_p95_s": P95.format(m="feature_age_seconds"),
    "decisions_per_min": RATE.format(sel="") + " * 60",
    "declines_per_min": RATE.format(sel='{decision="decline"}') + " * 60",
    "stale_share": RATE.format(sel='{reason="stale_features"}') + " / " + RATE.format(sel=""),
    "parity": "lake_parity_ratio",
    "lake_freshness_s": BRONZE_AGE,
    # none firing is 0, not "no data"
    "firing_alerts": 'count(ALERTS{alertstate="firing"}) or vector(0)',
}

# component -> query: 1 healthy, 0 down, no data unknown (Postgres is checked directly).
# Comparisons use bool: plain `x == 0` returns x's value (0), which would read as down.
HEALTH = {
    "debezium": f"min(connect_task_running) * max({SLOT})",
    "redpanda": 'max(up{job="redpanda"})',
    "flink_bronze": 'max(flink_job_running{flink_job="bronze-cdc"})',
    "flink_features": 'max(flink_job_running{flink_job="features-card"})',
    # fresh features, or no traffic needing them: 1 if either holds
    "feature_store": "clamp_max(max(("
    + P95.format(m="feature_age_seconds")
    + " < bool 5) or vector(0)) + ("
    + RATE.format(sel="")
    + " == bool 0), 1)",
    "decision_service": 'max(up{job="decision-service"})',
    "flink_decisions": 'max(flink_job_running{flink_job="bronze-decisions"})',
    "lakehouse": f"{BRONZE_AGE} < bool 300",
    "monitoring": 'min(up{job=~"lake-metrics|stream-metrics"})',
}


def prometheus(query: str) -> float | None:
    url = (
        f"http://localhost:{port('PROMETHEUS_PORT', '9095')}/api/v1/query?"
        + urllib.parse.urlencode({"query": query})
    )
    with urllib.request.urlopen(url, timeout=3) as response:
        result = json.load(response)["data"]["result"]
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if value != value else value  # NaN: no traffic in the window


def postgres_ok() -> bool:
    import psycopg

    from demo.config import dsn

    try:
        with psycopg.connect(dsn()) as conn:
            conn.execute("select 1")
        return True
    except Exception:
        return False


def snapshot(
    query: Callable[[str], float | None] = prometheus, postgres: Callable[[], bool] = postgres_ok
) -> dict[str, Any]:
    """Headline numbers and per-component health: ok, down, or unknown (no data)."""
    numbers: dict[str, float | None] = {}
    for name, q in HEADLINE.items():
        try:
            numbers[name] = query(q)
        except Exception:
            numbers[name] = None
    health: dict[str, str] = {}
    for name, q in HEALTH.items():
        try:
            value = query(q)
        except Exception:
            value = None
        health[name] = "unknown" if value is None else ("ok" if value >= 1 else "down")
    health["postgres"] = "ok" if postgres() else "down"
    return {"numbers": numbers, "health": health}
