"""Dead-letter queue (PRD 6.4; DQ-01, DQ-03): what is rejected, with which code, and replay."""

import importlib.util
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from decision_service import dead_letter
from decision_service.dead_letter import RejectedEventError, check
from decision_service.service import _transaction

ROOT = Path(__file__).resolve().parents[2]


def row(**changes: Any) -> dict[str, Any]:
    base = {"transaction_id": 7, "card_id": 3, "merchant_id": 9, "amount": Decimal("4.50"),
            "currency": "USD", "channel": "online", "device_risk_score": None,
            "event_ts": "2026-01-04T10:00:00.123Z"}  # fmt: skip
    return {**base, **changes}


class Msg:
    def __init__(
        self, value: bytes | None = b"payload", topic: str = "pg.public.transactions"
    ) -> None:
        self._value, self._topic = value, topic

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return self._topic


def msg(value: bytes | None = b"payload", topic: str = "pg.public.transactions") -> Any:
    """A stand-in for confluent_kafka's Message (value and topic are all _transaction reads)."""
    return Msg(value, topic)


def decoded(event: Any) -> Any:
    return lambda value, context: event


def test_valid_rows_pass_and_each_rule_has_its_code() -> None:
    assert check(row()) is None
    assert check(row(card_id=None)) == "card_id_missing"
    assert check(row(amount=Decimal("0"))) == "amount_not_positive"
    assert check(row(amount=Decimal("-5.00"))) == "amount_not_positive"
    assert check(row(amount="not a number")) == "amount_not_positive"
    assert check(row(currency="XXX")) == "currency_not_allowed"
    assert check(row(currency=None)) == "currency_not_allowed"


def test_every_currency_the_payments_app_uses_is_allowed() -> None:
    from simulator.engine import CURRENCY_BY_COUNTRY

    assert set(CURRENCY_BY_COUNTRY.values()) | {"USD"} <= dead_letter.ALLOWED_CURRENCIES


def test_undecodable_events_are_rejected_not_fatal() -> None:
    def broken(value: bytes, context: Any) -> Any:
        raise ValueError("Unknown magic byte")

    with pytest.raises(RejectedEventError) as err:
        _transaction(msg(b"garbage"), broken)
    assert err.value.code == "deserialize_failed" and "magic byte" in err.value.detail
    with pytest.raises(RejectedEventError) as err:
        _transaction(msg(), decoded("not an event"))
    assert err.value.code == "malformed_event"


def test_invalid_transactions_are_rejected_with_their_code() -> None:
    event = {"op": "c", "after": row(amount=Decimal("-5.00")), "source": {"ts_ms": 1}}
    with pytest.raises(RejectedEventError) as err:
        _transaction(msg(), decoded(event))
    assert err.value.code == "amount_not_positive" and "transaction_id=7" in err.value.detail
    missing = row()
    del missing["merchant_id"]
    with pytest.raises(RejectedEventError) as err:
        _transaction(msg(), decoded({"op": "c", "after": missing, "source": {"ts_ms": 1}}))
    assert err.value.code == "malformed_event"


def test_valid_and_other_events_pass_as_before() -> None:
    txn = _transaction(msg(), decoded({"op": "c", "after": row(), "source": {"ts_ms": 1}}))
    assert txn is not None and txn.transaction_id == 7
    assert (
        _transaction(
            msg(), decoded({"op": "u", "after": row(amount=Decimal("-1")), "source": {"ts_ms": 1}})
        )
        is None
    )
    assert _transaction(msg(None), decoded(None)) is None  # tombstone


def test_dead_letters_say_why_where_and_who() -> None:
    rejected = RejectedEventError("amount_not_positive", "transaction_id=7")
    headers = dict(dead_letter.headers(rejected, "retry.decision-service", 0, 42))
    assert headers["dlq.code"] == b"amount_not_positive"
    assert headers["dlq.source.topic"] == b"retry.decision-service"
    assert headers["dlq.source.offset"] == b"42"
    assert headers["dlq.consumer.version"] == dead_letter.VERSION.encode()


def replay() -> Any:
    sys.path.insert(0, str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("dlq_replay", ROOT / "scripts/dlq_replay.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_replay_reads_codes_in_either_header_form_and_records_its_origin() -> None:
    r = replay()
    listed = [("dlq.code", b"currency_not_allowed"), ("dlq.source.offset", b"3")]
    assert r.code_of(r.pairs(listed)) == "currency_not_allowed"
    assert r.code_of(r.pairs({"dlq.code": b"deserialize_failed"})) == "deserialize_failed"
    assert r.code_of(r.pairs(None)) == "unknown"
    assert r.retry_headers(r.pairs(listed), 12)[-1] == ("dlq.replayed_from_offset", b"12")
    assert r.DLQ == dead_letter.DLQ and r.RETRY == dead_letter.RETRY


def test_every_code_raised_is_a_known_code() -> None:
    """Known codes start at 0 in Prometheus, so DlqSpike sees the first dead letters."""
    import inspect

    from decision_service import service

    raised = set(re.findall(r'RejectedEventError\("(\w+)"', inspect.getsource(service)))
    raised |= set(re.findall(r'return "(\w+)"', inspect.getsource(dead_letter.check)))
    assert raised == set(dead_letter.CODES)


def test_known_labels_start_at_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    from prometheus_client import REGISTRY

    from decision_service import metrics

    monkeypatch.setattr(metrics, "start_http_server", lambda port: None)
    metrics.serve(0, topics=("retry.decision-service",), codes=dead_letter.CODES)
    for code in dead_letter.CODES:
        assert REGISTRY.get_sample_value("dead_lettered_total", {"code": code}) is not None
    assert (
        REGISTRY.get_sample_value(
            "transaction_events_consumed_total", {"topic": "retry.decision-service"}
        )
        is not None
    )
