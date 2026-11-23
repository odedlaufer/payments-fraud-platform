"""Dead-letter queue of the decision service (PRD 6.4; DQ-01, DQ-03).

An event the service cannot or must not decide goes to dlq.decision-service instead of
stopping the service: the original bytes and key unchanged, and headers saying why
(dlq.code, dlq.detail), where it came from (topic, partition, offset) and which consumer
version rejected it. make dlq-replay sends records back through retry.decision-service, a
topic only this service reads, so bronze and Flink never see a replayed event twice.

Codes:
- deserialize_failed: not decodable against the registered Avro schema (DQ-01)
- malformed_event: decodable, but a required field is missing or of the wrong type
- card_id_missing, amount_not_positive, currency_not_allowed: stream checks (DQ-03)
"""

import time
from decimal import Decimal, InvalidOperation
from typing import Any

CODES = (
    "deserialize_failed", "malformed_event", "card_id_missing", "amount_not_positive",
    "currency_not_allowed",
)  # fmt: skip
DLQ = "dlq.decision-service"
RETRY = "retry.decision-service"
CONSUMER = "decision-service"
VERSION = "0.2.0"

# Every currency the simulated payments app settles in (simulator.engine CURRENCY_BY_COUNTRY,
# checked by a test). A new one is a contract change, made here on purpose.
ALLOWED_CURRENCIES = frozenset(
    {"AED", "AUD", "BRL", "CAD", "EUR", "GBP", "ILS", "INR", "JPY", "KRW", "MXN", "NGN",
     "SGD", "USD", "ZAR"}
)  # fmt: skip


class RejectedEventError(Exception):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code, self.detail = code, detail


def check(row: dict[str, Any]) -> str | None:
    """DQ-03 on a transaction row: the error code, or None when it passes."""
    if row.get("card_id") is None:
        return "card_id_missing"
    try:
        if Decimal(str(row.get("amount"))) <= 0:
            return "amount_not_positive"
    except InvalidOperation:
        return "amount_not_positive"
    if row.get("currency") not in ALLOWED_CURRENCIES:
        return "currency_not_allowed"
    return None


def headers(
    rejected: RejectedEventError, topic: str, partition: int, offset: int
) -> list[tuple[str, str | bytes | None]]:
    return [
        ("dlq.code", rejected.code.encode()),
        ("dlq.detail", rejected.detail[:500].encode()),
        ("dlq.source.topic", topic.encode()),
        ("dlq.source.partition", str(partition).encode()),
        ("dlq.source.offset", str(offset).encode()),
        ("dlq.consumer", CONSUMER.encode()),
        ("dlq.consumer.version", VERSION.encode()),
        ("dlq.failed_at_ms", str(int(time.time() * 1000)).encode()),
    ]
