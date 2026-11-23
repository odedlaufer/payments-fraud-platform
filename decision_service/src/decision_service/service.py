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
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from decision_service import dead_letter, metrics
from decision_service.context import RULE_NAMES
from decision_service.customer_features import Ask, customer_values
from decision_service.dead_letter import RejectedEventError
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
    topic: str = SOURCE  # or the retry topic (dead_letter.RETRY)


Ready = tuple[Pending, CardFeatures | None, dict[str, Any] | None, int | None]


@dataclass
class Resolver:
    """Pending transactions and which are ready to decide. No Kafka, so it is testable.

    Ready means: the card feature snapshot has arrived and the customer context is
    known (card owner, and the session for an online transaction).
    """

    store: OnlineStore
    wait_ms: int
    customers: Callable[[list[Ask]], dict[int, dict[str, Any]]]
    pending: dict[int, Pending] = field(default_factory=dict)
    skipped_duplicates: int = 0

    def add(self, p: Pending) -> None:
        self.pending[p.txn.transaction_id] = p

    def resolve(self, now: int) -> list[Ready]:
        """(pending, snapshot, customer values, features_ms) for every transaction ready to
        decide; snapshot or customer values are None when the deadline passed without them."""
        if not self.pending:
            return []
        ids = list(self.pending)
        try:
            for txn_id in self.store.decided(ids):
                del self.pending[txn_id]
                self.skipped_duplicates += 1
            snapshots = self.store.get_transactions(list(self.pending))
            asks = [
                Ask(i, p.txn.card_id, p.txn.session_id, p.txn.event_ms)
                for i, p in self.pending.items()
                if i in snapshots
            ]
            customers = self.customers(asks) if asks else {}
        except redis.RedisError:
            snapshots, customers = {}, {}
        ready: list[Ready] = []
        for txn_id, p in list(self.pending.items()):
            snap, cust = snapshots.get(txn_id), customers.get(txn_id)
            if snap is not None and cust is not None:
                ready.append((p, snap, cust, now))
            elif now >= p.deadline_ms:
                ready.append((p, snap, cust, None))
            else:
                continue
            del self.pending[txn_id]
        return ready

    def committable(self, consumed: dict[tuple[str, int], int]) -> dict[tuple[str, int], int]:
        """Per (topic, partition): the next offset safe to commit (all before it decided)."""
        safe = {key: last + 1 for key, last in consumed.items()}
        for p in self.pending.values():
            key = (p.topic, p.partition)
            safe[key] = min(safe.get(key, p.offset), p.offset)
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
    ensure_topics(bootstrap, [dead_letter.DLQ, dead_letter.RETRY])
    consumer.subscribe([SOURCE, dead_letter.RETRY])
    defs = load()
    resolver = Resolver(store, wait_ms, lambda asks: customer_values(defs, store, asks))
    consumed: dict[tuple[str, int], int] = {}

    running = True

    def stop(signum: int, frame: FrameType | None) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    metrics.serve(
        int(os.environ.get("METRICS_PORT", "9108")),
        topics=(SOURCE, dead_letter.RETRY),
        codes=dead_letter.CODES,
    )
    outcomes: Counter[str] = Counter()
    latencies: list[int] = []
    last_report = time.monotonic()
    print(f"decision-service: {SOURCE} -> {SINK}, rules v{ruleset.version}, "
          f"wait {wait_ms} ms", flush=True)  # fmt: skip
    while running:
        HEARTBEAT.touch()
        dead_lettered = False
        for msg in consumer.consume(500, timeout=0.02):
            if msg.error() is not None:
                continue
            topic, part = cast(str, msg.topic()), cast(int, msg.partition())
            offset = cast(int, msg.offset())
            consumed[(topic, part)] = max(consumed.get((topic, part), -1), offset)
            metrics.CONSUMED.labels(topic=topic).inc()
            try:
                txn = _transaction(msg, deserialize)
            except RejectedEventError as rejected:
                producer.produce(
                    dead_letter.DLQ,
                    key=msg.key(),
                    value=msg.value(),
                    headers=dead_letter.headers(rejected, topic, part, offset),
                )
                metrics.DEAD_LETTERED.labels(code=rejected.code).inc()
                dead_lettered = True
                continue
            if txn is not None:
                metrics.observe_consumed(msg.timestamp(), txn.commit_ms)
                resolver.add(Pending(txn, part, offset, now_ms(), now_ms() + wait_ms, topic))
        ready = resolver.resolve(now_ms())
        metrics.PENDING.set(len(resolver.pending))
        decided_ids = []
        for p, snapshot, customer, features_ms in ready:
            record = decide(p.txn, snapshot, customer, ruleset, consumed_ms=p.consumed_ms,
                            features_ms=features_ms, decided_ms=now_ms())  # fmt: skip
            producer.produce(
                SINK,
                key=str(p.txn.transaction_id).encode(),
                value=serialize(record, SerializationContext(SINK, MessageField.VALUE)),
            )
            decided_ids.append(p.txn.transaction_id)
            metrics.observe_decision(record, snapshot.written_at_ms if snapshot else None)
            outcomes[f"{record['decision']}:{record['reason']}"] += 1
            latencies.append(record["latency_ms"])
        if decided_ids or dead_lettered:
            producer.flush()  # decisions and dead letters are durable before offsets commit
            try:
                store.mark_decided(decided_ids)
            except redis.RedisError as exc:
                print(f"decision-service: could not mark decided: {exc}", file=sys.stderr)
        safe = resolver.committable(consumed)
        if safe and (decided_ids or dead_lettered or not resolver.pending):
            consumer.commit(
                offsets=[TopicPartition(t, part, off) for (t, part), off in safe.items()],
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
    """A transaction creation to decide, None for other change events; RejectedEventError
    for an event that must go to the dead-letter queue (DQ-01, DQ-03)."""
    value = msg.value()
    if value is None:  # tombstone
        return None
    try:
        context = SerializationContext(cast(str, msg.topic()), MessageField.VALUE)
        event = deserialize(value, context)
    except Exception as exc:
        raise RejectedEventError("deserialize_failed", str(exc)) from exc
    if not isinstance(event, dict):
        raise RejectedEventError("malformed_event", f"not a change event: {type(event).__name__}")
    after = event.get("after")
    if event.get("op") == "c" and isinstance(after, dict):
        code = dead_letter.check(after)
        if code is not None:
            raise RejectedEventError(code, f"transaction_id={after.get('transaction_id')}")
    try:
        return Transaction.from_debezium(cast(dict[str, Any], event))
    except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
        raise RejectedEventError("malformed_event", repr(exc)) from exc


def ensure_topics(bootstrap: str, topics: list[str]) -> None:
    """Creates missing topics now: a consumer only notices a topic created later at its next
    metadata refresh (up to 5 minutes), which would delay replays."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [
        NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in existing
    ]
    if not missing:
        return
    for topic, future in admin.create_topics(missing).items():
        try:
            future.result()
        except Exception as exc:
            print(f"decision-service: topic {topic}: {exc}", file=sys.stderr)


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
