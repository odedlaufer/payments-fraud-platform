"""decision-service: pg.public.transactions in, decisions out (FR-D1 to FR-D5).

Each new transaction waits in a pending set until its exact feature snapshot is in Redis
(written by feature-sink), or until its deadline, when it becomes `review` with reason
`stale_features`. All pending transactions are checked in one Redis round trip per pass,
so a slow one never delays the others. Redis errors count as "no snapshot": an outage
turns decisions into reviews, never silent approvals (CH-3).

Delivery: publish, flush, mark `decided:{id}` in Redis, then commit the Kafka offset up to
the oldest transaction still pending. Redelivered transactions that were already decided
are skipped (FR-D5); consumers of `decisions` still key on transaction_id.
"""

import json
import os
import signal
import statistics
import sys
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import Any, cast

import redis
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from decision_service.context import RULE_NAMES
from decision_service.decision import Transaction, avro_schema, decide
from decision_service.online_store import CardFeatures, OnlineStore
from features.model import load
from rules.engine import RuleSet

SOURCE = "pg.public.transactions"
SINK = "decisions"
HEARTBEAT = Path("/tmp/decision-service.alive")


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Pending:
    txn: Transaction
    partition: int
    offset: int
    consumed_ms: int
    deadline_ms: int


@dataclass
class Resolver:
    """Pending transactions and which are ready to decide. No Kafka, so it is testable."""

    store: OnlineStore
    wait_ms: int
    pending: dict[int, Pending] = field(default_factory=dict)
    skipped_duplicates: int = 0

    def add(self, p: Pending) -> None:
        self.pending[p.txn.transaction_id] = p

    def resolve(self, now: int) -> list[tuple[Pending, CardFeatures | None, int | None]]:
        """(pending, snapshot or None, features_ms) for every transaction ready to decide."""
        if not self.pending:
            return []
        ids = list(self.pending)
        try:
            for txn_id in self.store.decided(ids):
                del self.pending[txn_id]
                self.skipped_duplicates += 1
            snapshots = self.store.get_transactions(list(self.pending))
        except redis.RedisError:
            snapshots = {}
        ready: list[tuple[Pending, CardFeatures | None, int | None]] = []
        for txn_id, p in list(self.pending.items()):
            snap = snapshots.get(txn_id)
            if snap is not None:
                ready.append((p, snap, now))
            elif now >= p.deadline_ms:
                ready.append((p, None, None))
            else:
                continue
            del self.pending[txn_id]
        return ready

    def committable(self, consumed: dict[int, int]) -> dict[int, int]:
        """Per partition: the next offset that is safe to commit (all before it decided)."""
        safe = {part: last + 1 for part, last in consumed.items()}
        for p in self.pending.values():
            safe[p.partition] = min(safe.get(p.partition, p.offset), p.offset)
        return safe


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    rules_file = Path(os.environ.get("RULES_FILE", "/app/rules/rules_v1.yml"))
    wait_ms = int(os.environ.get("FEATURE_WAIT_MS", "3000"))

    ruleset = RuleSet.load(rules_file, RULE_NAMES)
    store = OnlineStore(redis.Redis.from_url(redis_url, decode_responses=True), load())
    registry = SchemaRegistryClient({"url": registry_url})
    deserialize = AvroDeserializer(registry)
    serialize = AvroSerializer(registry, json.dumps(avro_schema()))
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "decision-service",
            # First start decides new transactions only; afterwards, committed offsets.
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer(
        {"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5}
    )
    consumer.subscribe([SOURCE])
    resolver = Resolver(store, wait_ms)
    consumed: dict[int, int] = {}

    running = True

    def stop(signum: int, frame: FrameType | None) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    outcomes: Counter[str] = Counter()
    latencies: list[int] = []
    last_report = time.monotonic()
    print(f"decision-service: {SOURCE} -> {SINK}, rules v{ruleset.version}, "
          f"wait {wait_ms} ms", flush=True)  # fmt: skip
    while running:
        HEARTBEAT.touch()
        for msg in consumer.consume(500, timeout=0.02):
            if msg.error() is not None:
                continue
            part, offset = cast(int, msg.partition()), cast(int, msg.offset())
            consumed[part] = max(consumed.get(part, -1), offset)
            txn = _transaction(msg, deserialize)
            if txn is not None:
                resolver.add(Pending(txn, part, offset, now_ms(), now_ms() + wait_ms))
        ready = resolver.resolve(now_ms())
        decided_ids = []
        for p, snapshot, features_ms in ready:
            record = decide(p.txn, snapshot, ruleset, consumed_ms=p.consumed_ms,
                            features_ms=features_ms, decided_ms=now_ms())  # fmt: skip
            producer.produce(
                SINK,
                key=str(p.txn.transaction_id).encode(),
                value=serialize(record, SerializationContext(SINK, MessageField.VALUE)),
            )
            decided_ids.append(p.txn.transaction_id)
            outcomes[f"{record['decision']}:{record['reason']}"] += 1
            latencies.append(record["latency_ms"])
        if decided_ids:
            producer.flush()
            try:
                store.mark_decided(decided_ids)
            except redis.RedisError as exc:
                print(f"decision-service: could not mark decided: {exc}", file=sys.stderr)
        safe = resolver.committable(consumed)
        if safe and (decided_ids or not resolver.pending):
            consumer.commit(
                offsets=[TopicPartition(SOURCE, part, off) for part, off in safe.items()],
                asynchronous=False,
            )
            consumed.clear()
        if time.monotonic() - last_report >= 30:
            _report(outcomes, latencies, resolver)
            outcomes.clear()
            latencies.clear()
            last_report = time.monotonic()
    producer.flush()
    consumer.close()
    return 0


def _transaction(msg: Message, deserialize: Callable[..., Any]) -> Transaction | None:
    value = msg.value()
    if value is None:  # tombstone
        return None
    event = deserialize(value, SerializationContext(SOURCE, MessageField.VALUE))
    return Transaction.from_debezium(cast(dict[str, Any], event))


def _report(outcomes: Counter[str], latencies: list[int], resolver: Resolver) -> None:
    if latencies:
        cuts = statistics.quantiles(latencies, n=20) if len(latencies) > 1 else latencies * 19
        p50, p95 = int(statistics.median(latencies)), int(cuts[18])
    else:
        p50 = p95 = 0
    print(
        f"decision-service: {sum(outcomes.values())} decisions {dict(outcomes)} "
        f"latency p50={p50}ms p95={p95}ms pending={len(resolver.pending)} "
        f"duplicates_skipped={resolver.skipped_duplicates}",
        flush=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
