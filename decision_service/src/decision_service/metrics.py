"""Prometheus metrics of the decision service (OBS-1), served on METRICS_PORT (9108).

Three PRD section 2 SLOs are measured where their timestamps meet:
- cdc_lag_seconds: Postgres commit (Debezium source.ts_ms) -> the event in Redpanda (the
  Kafka message timestamp, set when Connect produced it). SLO: p95 < 2 s.
- decision_latency_seconds: Postgres commit -> decision published. SLO: p95 < 5 s.
- feature_age_seconds: decision time - when the card's features were written to Redis.
  SLO: < 5 s. Decisions without features are counted in decisions_total{reason=...}.
"""

from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

SECONDS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 30.0, 60.0)

CDC_LAG = Histogram(
    "cdc_lag_seconds", "Postgres commit to the event in Redpanda (SLO p95 < 2 s)", buckets=SECONDS
)
DECISION_LATENCY = Histogram(
    "decision_latency_seconds",
    "Postgres commit to decision published (SLO p95 < 5 s)",
    buckets=SECONDS,
)
FEATURE_AGE = Histogram(
    "feature_age_seconds",
    "Age of the card's online features at decision (SLO < 5 s)",
    buckets=SECONDS,
)
DECISIONS = Counter("decisions", "Decisions by outcome", ["decision", "reason"])
PENDING = Gauge("decision_pending_transactions", "Transactions waiting for their features")
CONSUMED = Counter("transaction_events_consumed", "Change events read, by topic", ["topic"])
DEAD_LETTERED = Counter(
    "dead_lettered", "Events sent to dlq.decision-service, by code (DQ-01, DQ-03)", ["code"]
)


def serve(port: int, topics: tuple[str, ...] = (), codes: tuple[str, ...] = ()) -> None:
    """Starts the endpoint with every known label already at 0: a labelled counter that first
    appears at 20 has no visible increase, so rate() and the DlqSpike alert would miss it."""
    for topic in topics:
        CONSUMED.labels(topic=topic)
    for code in codes:
        DEAD_LETTERED.labels(code=code)
    start_http_server(port)


def observe_consumed(kafka_timestamp: tuple[int, int], commit_ms: int) -> None:
    """kafka_timestamp: confluent-kafka's (type, ms); type 0 means not available."""
    kind, ms = kafka_timestamp
    if kind != 0 and ms > 0:
        CDC_LAG.observe(max(0.0, (ms - commit_ms) / 1000))


def observe_decision(record: dict[str, Any], features_written_ms: int | None) -> None:
    DECISION_LATENCY.observe(max(0.0, record["latency_ms"] / 1000))
    DECISIONS.labels(decision=record["decision"], reason=record["reason"]).inc()
    if features_written_ms is not None:
        FEATURE_AGE.observe(max(0.0, (record["decided_ms"] - features_written_ms) / 1000))
