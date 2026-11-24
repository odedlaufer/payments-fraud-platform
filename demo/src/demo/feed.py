"""The live decision feed: every decision from the moment the console starts, with its
payment (amount, channel) and the simulator's ground truth (sim.sim_truth) for attacks.

An attack transaction is caught (a rule fired: declined or reviewed), held (reviewed only
because its features were stale: safe, but not detection) or missed (approved).
"""

import threading
import time
import uuid
from collections import Counter, deque
from datetime import UTC, datetime
from typing import Any

from demo.config import dsn, port


def classify(decision: str, reason: str, pattern: str | None) -> str | None:
    """caught / held / missed for an attack transaction; None for a legitimate one."""
    if pattern is None:
        return None
    if decision == "approve":
        return "missed"
    return "caught" if reason == "rules" else "held"


class DecisionFeed:
    def __init__(self, size: int = 80) -> None:
        self.items: deque[dict[str, Any]] = deque(maxlen=size)
        self.outcomes: Counter[str] = Counter()
        self.attacks: dict[str, Counter[str]] = {}
        self.approvals_ms: deque[int] = deque(maxlen=20000)
        self.lock = threading.Lock()
        self.error = ""
        self._stop = threading.Event()

    def start(self) -> None:
        threading.Thread(target=self._run, name="decision-feed", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        from confluent_kafka import Consumer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        consumer = Consumer(
            {
                "bootstrap.servers": f"localhost:{port('REDPANDA_KAFKA_PORT', '19092')}",
                "group.id": f"demo-console-{uuid.uuid4().hex[:8]}",  # its own position: from now
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        registry = SchemaRegistryClient(
            {"url": f"http://localhost:{port('REDPANDA_SCHEMA_REGISTRY_PORT', '18081')}"}
        )
        deserialize = AvroDeserializer(registry)
        consumer.subscribe(["decisions"])
        while not self._stop.is_set():
            try:
                batch = consumer.consume(num_messages=500, timeout=1.0)
                records = [
                    deserialize(m.value(), SerializationContext("decisions", MessageField.VALUE))
                    for m in batch
                    if m.error() is None and m.value()
                ]
                if records:
                    self.add(records, self.lookup([r["transaction_id"] for r in records]))
                self.error = ""
            except Exception as exc:
                self.error = str(exc)[:200]
                time.sleep(2)
        consumer.close()

    def lookup(self, ids: list[int]) -> dict[int, tuple[Any, ...]]:
        """transaction_id -> (amount, currency, channel, attack pattern or None)."""
        import psycopg

        with psycopg.connect(dsn()) as conn:
            rows = conn.execute(
                "select t.transaction_id, t.amount, t.currency, t.channel, "
                "case when s.is_fraud then s.pattern end "
                "from transactions t "
                "left join sim.sim_truth s on s.transaction_id = t.transaction_id "
                "where t.transaction_id = any(%s)",
                (ids,),
            ).fetchall()
        return {int(r[0]): (r[1], r[2], r[3], r[4]) for r in rows}

    def add(self, records: list[dict[str, Any]], payments: dict[int, tuple[Any, ...]]) -> None:
        with self.lock:
            for r in records:
                amount, currency, channel, pattern = payments.get(
                    r["transaction_id"], (None, None, None, None)
                )
                verdict = classify(r["decision"], r["reason"], pattern)
                self.outcomes[r["decision"]] += 1
                if r["decision"] == "approve":
                    self.approvals_ms.append(int(r["decided_ms"]))
                if pattern is not None and verdict is not None:
                    self.attacks.setdefault(pattern, Counter())[verdict] += 1
                self.items.appendleft(
                    {
                        "time": datetime.fromtimestamp(r["decided_ms"] / 1000, UTC).strftime(
                            "%H:%M:%S"
                        ),
                        "transaction_id": r["transaction_id"],
                        "card_id": r["card_id"],
                        "amount": None if amount is None else f"{amount} {currency}",
                        "channel": channel,
                        "decision": r["decision"],
                        "reason": r["reason"],
                        "rules": list(r.get("fired_rules") or []),
                        "latency_ms": r.get("latency_ms"),
                        "attack": pattern,
                        "verdict": verdict,
                    }
                )

    def approvals_between(self, lo_ms: int, hi_ms: int) -> int:
        with self.lock:
            return sum(1 for t in self.approvals_ms if lo_ms <= t <= hi_ms)

    def view(self, limit: int = 40) -> dict[str, Any]:
        with self.lock:
            return {
                "items": list(self.items)[:limit],
                "outcomes": dict(self.outcomes),
                "attacks": {p: dict(c) for p, c in self.attacks.items()},
                "error": self.error,
            }
