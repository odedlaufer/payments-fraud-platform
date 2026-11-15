"""The decision contract (FR-D1, FR-D3, FR-D4), without Kafka or Redis."""

import io
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import fastavro
import pytest

from decision_service.context import FLINK_FEATURES, RULE_NAMES, SNAPSHOT_FEATURES
from decision_service.decision import STALE, Transaction, avro_schema, decide
from decision_service.online_store import CardFeatures
from rules.engine import RuleSet

ROOT = Path(__file__).resolve().parents[2]
RULES = RuleSet.load(ROOT / "rules/rules_v1.yml", RULE_NAMES)


def event(
    op: str = "c", risk: object = None, ts: str = "2026-01-04T10:00:00.123999Z"
) -> dict[str, Any]:
    after = {"transaction_id": 7, "card_id": 3, "merchant_id": 9, "amount": Decimal("4.50"),
             "channel": "online", "device_risk_score": risk, "event_ts": ts}  # fmt: skip
    return {"op": op, "after": after, "source": {"ts_ms": 1_000}}


def parse(raw: dict[str, Any]) -> Transaction:
    txn = Transaction.from_debezium(raw)
    assert txn is not None
    return txn


TXN = parse(event())


def snapshot(txn_id: int = 7, **values: object) -> CardFeatures:
    base: dict[str, object] = {name: None for name in FLINK_FEATURES}
    base.update(txn_count_1m=1, txn_count_1h=1, txn_count_24h=1, distinct_merchants_1h=1,
                txn_amount_sum_1h=Decimal("4.50"), txn_amount_sum_24h=Decimal("4.50"))  # fmt: skip
    base.update(values)
    return CardFeatures(3, txn_id, 0, 1, "2026-01-04T10:00:00.500Z", 0, base)


CUSTOMER = {"new_device_24h": None, "profile_change_24h": False}


def run(snap: CardFeatures | None, customer: dict[str, Any] | None = CUSTOMER) -> dict[str, Any]:
    ready = snap is not None and customer is not None
    return decide(TXN, snap, customer, RULES, consumed_ms=1_100,
                  features_ms=1_300 if ready else None, decided_ms=1_400)  # fmt: skip


def test_parses_only_creations() -> None:
    assert TXN.event_ms == 1767520800123  # microseconds truncated to milliseconds
    assert (TXN.commit_ms, TXN.amount, TXN.device_risk_score) == (1_000, Decimal("4.50"), None)
    assert TXN.session_id is None
    assert Transaction.from_debezium(event(op="u")) is None
    assert parse(event(risk=Decimal("0.75"))).device_risk_score == Decimal("0.75")


def test_no_snapshot_is_review_stale_never_approve() -> None:
    record = run(None)
    assert (record["decision"], record["reason"], record["fired_rules"]) == ("review", STALE, [])
    assert all(v is None for v in record["features"].values())
    assert set(record["features"]) == set(SNAPSHOT_FEATURES)
    assert record["features_ms"] is None and record["definitions_version"] is None


def test_card_snapshot_without_customer_context_is_still_stale() -> None:
    record = run(snapshot(), customer=None)
    assert (record["decision"], record["reason"]) == ("review", STALE)


def test_customer_features_are_logged_and_reach_the_rules() -> None:
    record = run(snapshot(), customer={"new_device_24h": True, "profile_change_24h": True})
    assert record["features"]["new_device_24h"] is True
    assert record["features"]["profile_change_24h"] is True
    assert record["features"]["txn_count_1h"] == 1


def test_rules_decide_on_the_snapshot() -> None:
    record = run(snapshot(txn_count_1h=6, distinct_merchants_1h=5))
    assert (record["decision"], record["score"]) == ("decline", 90)
    assert record["fired_rules"] == ["card_testing"]
    assert record["reason"] == "rules" and record["rules_version"] == 1


def test_nothing_fired_is_approve() -> None:
    record = run(snapshot())
    assert (record["decision"], record["reason"], record["score"]) == (
        "approve",
        "no_rule_fired",
        0,
    )


def test_snapshot_of_another_transaction_is_refused() -> None:
    with pytest.raises(ValueError, match="snapshot of transaction 8"):
        run(snapshot(txn_id=8))


def test_latency_fields() -> None:
    record = run(snapshot())
    stamps = ("commit_ms", "consumed_ms", "features_ms", "decided_ms", "latency_ms")
    assert tuple(record[k] for k in stamps) == (1_000, 1_100, 1_300, 1_400, 400)


@pytest.mark.parametrize("snap", [None, snapshot(amount_vs_avg_30d=0.5, km_from_last_txn=None)])
def test_records_match_the_avro_schema(snap: CardFeatures | None) -> None:
    schema = fastavro.parse_schema(avro_schema())
    record = run(snap)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    back = cast(dict[str, Any], fastavro.schemaless_reader(buf, schema, schema))
    assert back == record
    if snap is not None:
        assert back["features"]["txn_amount_sum_1h"] == "4.50"  # decimals stay exact
