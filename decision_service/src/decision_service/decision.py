"""One decision: a transaction, its exact feature snapshot (or none), and the rules.

Pure logic with no Kafka or Redis, so the decision contract is unit-tested directly:

- the features must be this transaction's own snapshot (FR-D1, ADR-003 parity);
- no snapshot in time means `review` with reason `stale_features`, never an approve (FR-D4);
- the record logs rules version, fired rules, the full snapshot and latency stamps (FR-D3).
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from decision_service.context import CARD_FEATURES
from decision_service.online_store import CardFeatures
from features.model import load
from features.timeparse import iso_to_ms
from rules.engine import RuleSet

DEFS = load()
STALE = "stale_features"


@dataclass(frozen=True)
class Transaction:
    transaction_id: int
    card_id: int
    merchant_id: int
    amount: Decimal
    channel: str
    device_risk_score: Decimal | None
    event_ms: int
    commit_ms: int  # Debezium source.ts_ms: when Postgres committed it

    @classmethod
    def from_debezium(cls, event: dict[str, Any]) -> "Transaction | None":
        """A transaction creation, or None for any other change event."""
        if event.get("op") != "c" or event.get("after") is None:
            return None
        row = event["after"]
        return cls(
            transaction_id=int(row["transaction_id"]),
            card_id=int(row["card_id"]),
            merchant_id=int(row["merchant_id"]),
            amount=Decimal(row["amount"]),
            channel=str(row["channel"]),
            device_risk_score=(
                None if row.get("device_risk_score") is None else Decimal(row["device_risk_score"])
            ),
            event_ms=iso_to_ms(str(row["event_ts"])),
            commit_ms=int(event["source"]["ts_ms"]),
        )


def _snapshot_value(name: str, value: object) -> object:
    """Avro form of a feature value: decimals as exact strings."""
    if value is None:
        return None
    if DEFS.by_name(name).dtype.startswith("decimal"):
        return str(value)
    return value


def decide(
    txn: Transaction,
    snapshot: CardFeatures | None,
    ruleset: RuleSet,
    *,
    consumed_ms: int,
    features_ms: int | None,
    decided_ms: int,
) -> dict[str, Any]:
    if snapshot is not None and snapshot.transaction_id != txn.transaction_id:
        raise ValueError(
            f"snapshot of transaction {snapshot.transaction_id} used for {txn.transaction_id}"
        )
    fired: tuple[str, ...] = ()
    if snapshot is None:
        outcome, score, reason = "review", 0, STALE
        values: dict[str, object] = {name: None for name in CARD_FEATURES}
    else:
        context = {
            **snapshot.values,
            "amount": txn.amount,
            "channel": txn.channel,
            "device_risk_score": txn.device_risk_score,
        }
        evaluation = ruleset.evaluate(context)
        outcome, score, fired = evaluation.outcome, evaluation.score, evaluation.fired
        reason = "rules" if fired else "no_rule_fired"
        values = snapshot.values
    return {
        "transaction_id": txn.transaction_id,
        "card_id": txn.card_id,
        "merchant_id": txn.merchant_id,
        "event_ms": txn.event_ms,
        "decision": outcome,
        "score": score,
        "fired_rules": list(fired),
        "rules_version": ruleset.version,
        "reason": reason,
        "definitions_version": None if snapshot is None else snapshot.definitions_version,
        "features": {name: _snapshot_value(name, values[name]) for name in CARD_FEATURES},
        "commit_ms": txn.commit_ms,
        "consumed_ms": consumed_ms,
        "features_ms": features_ms,
        "decided_ms": decided_ms,
        "latency_ms": decided_ms - txn.commit_ms,
    }


_AVRO = {"bigint": "long", "double": "double", "boolean": "boolean"}


def avro_schema() -> dict[str, Any]:
    """Schema of the decisions topic; the feature snapshot follows definitions.yml."""
    snapshot_fields = [
        {
            "name": f.name,
            "type": ["null", "string" if f.dtype.startswith("decimal") else _AVRO[f.dtype]],
            "default": None,
        }
        for f in DEFS.for_entity("card")
    ]
    return {
        "type": "record",
        "name": "Decision",
        "namespace": "payments.decisions",
        "fields": [
            {"name": "transaction_id", "type": "long"},
            {"name": "card_id", "type": "long"},
            {"name": "merchant_id", "type": "long"},
            {"name": "event_ms", "type": "long"},
            {"name": "decision", "type": "string"},
            {"name": "score", "type": "int"},
            {"name": "fired_rules", "type": {"type": "array", "items": "string"}},
            {"name": "rules_version", "type": "int"},
            {"name": "reason", "type": "string"},
            {"name": "definitions_version", "type": ["null", "int"], "default": None},
            {
                "name": "features",
                "type": {
                    "type": "record",
                    "name": "CardFeatureSnapshot",
                    "fields": snapshot_fields,
                },
            },
            {"name": "commit_ms", "type": "long"},
            {"name": "consumed_ms", "type": "long"},
            {"name": "features_ms", "type": ["null", "long"], "default": None},
            {"name": "decided_ms", "type": "long"},
            {"name": "latency_ms", "type": "long"},
        ],
    }
