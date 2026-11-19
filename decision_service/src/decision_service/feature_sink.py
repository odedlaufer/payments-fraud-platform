"""feature-sink: features.card and customer CDC (Kafka) -> Redis online state (FR-F3, ADR-007).

The online store is derived data and Redis keeps no persistence. The sink records in
Redis which Redis instance (run_id) its state was built in:

- same instance (the sink restarted, Redis did not): resume from the committed offsets of
  its consumer group; only records since the last commit are re-applied, and every write
  is idempotent (newer-only card hashes, set/min/zadd customer context);
- otherwise (Redis restarted empty, or first start): rebuild from the beginning of every
  topic. A full replay grows with history, so it is reserved for this case.

The sink writes /tmp/feature-sink.ready once every partition has caught up; the
healthcheck needs it, so nothing waits on a cold store. If Redis restarts while the sink
runs, the sink exits (run_id watch) and Docker restarts it into a rebuild.
"""

import contextlib
import os
import signal
import sys
import time
from pathlib import Path
from types import FrameType
from typing import Any, cast

import redis
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from decision_service.customer_sink import CDC_TOPICS, apply_cdc
from decision_service.online_store import OnlineStore
from features.model import load

TOPIC = "features.card"
HEARTBEAT = Path("/tmp/feature-sink.alive")
READY = Path("/tmp/feature-sink.ready")  # written once caught up; the healthcheck needs it
GROUP = "feature-sink"
BUILT_FOR = "sink:built_for"  # Redis key: run_id of the Redis instance this state was built in
COMMIT_EVERY_S = 1.0
READY_CHECK_EVERY_S = 2.0


def start_mode(built_for: str | None, run_id: str) -> str:
    """'resume' from committed offsets when Redis still holds the state this sink built
    (same Redis run); 'rebuild' from the beginning when Redis lost it or never had it."""
    return "resume" if built_for == run_id else "rebuild"


def read_positions(
    mode: str, positions: dict[tuple[str, int], int], committed: dict[tuple[str, int], int]
) -> dict[tuple[str, int], int]:
    """Where each partition stands. Kafka reports no position (negative) until a message is
    consumed after assignment, so an idle partition resumed exactly at its end would never
    look caught up: when resuming, it stands at its committed offset. Not when rebuilding,
    where an old committed offset could look caught up before the replay has started."""
    if mode != "resume":
        return positions
    return {tp: (pos if pos >= 0 else committed.get(tp, pos)) for tp, pos in positions.items()}


def caught_up(
    positions: dict[tuple[str, int], int], marks: dict[tuple[str, int], tuple[int, int]]
) -> bool:
    """Every assigned partition read to its end (or empty). positions: next offset to read
    (negative before the first fetch); marks: (low, high) watermarks per partition."""
    return all(high <= low or positions.get(tp, -1) >= high for tp, (low, high) in marks.items())


# Write a per-transaction snapshot only for transactions committed this recently: the
# decision service needs it within seconds, and replays of history must not flood Redis.
FRESH_MS = 60_000


def redis_run_id(client: redis.Redis) -> str:
    return str(client.info("server")["run_id"])


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")

    client = redis.Redis.from_url(redis_url, decode_responses=True)
    defs = load()
    store = OnlineStore(client, defs)
    run_id = redis_run_id(client)
    built_for = client.get(BUILT_FOR)
    mode = start_mode(None if built_for is None else str(built_for), run_id)
    READY.unlink(missing_ok=True)
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": registry}))
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            # One stable group: a restart resumes from the committed offsets while Redis keeps
            # its state; only an empty Redis needs the full replay (rebuild).
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "topic.metadata.refresh.interval.ms": 5000,
        }
    )

    def on_assign(c: Consumer, partitions: list[TopicPartition]) -> None:
        if mode == "rebuild":
            for p in partitions:
                p.offset = OFFSET_BEGINNING
        c.assign(partitions)

    consumer.subscribe([TOPIC, *CDC_TOPICS], on_assign=on_assign)
    if mode == "rebuild":
        # Recorded as the rebuild starts: a sink that crashes mid-rebuild resumes it.
        client.set(BUILT_FOR, run_id)

    running = True

    def stop(signum: int, frame: FrameType | None) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        return _run(
            consumer, client, store, defs, deserialize, mode, run_id, redis_url, lambda: running
        )
    except redis.exceptions.OutOfMemoryError as exc:
        # noeviction: Redis refuses writes when full. Exit loudly; offsets are committed only
        # after records are applied, so the restart resumes without losing anything.
        print(
            f"feature-sink: Redis is full (maxmemory), cannot write: {exc}",
            file=sys.stderr,
            flush=True,
        )
        READY.unlink(missing_ok=True)
        consumer.close()
        return 1


def _run(
    consumer: Consumer,
    client: redis.Redis,
    store: OnlineStore,
    defs: Any,
    deserialize: Any,
    mode: str,
    run_id: str,
    redis_url: str,
    is_running: Any,
) -> int:
    applied = skipped = 0
    started = last_report = last_commit = last_ready_check = time.monotonic()
    ready = False
    print(f"feature-sink: {TOPIC} -> {redis_url} ({mode})", flush=True)
    while is_running():
        HEARTBEAT.touch()
        if redis_run_id(client) != run_id:
            print("feature-sink: Redis restarted (empty); exiting to rebuild", flush=True)
            consumer.close()
            return 1
        # Return quickly: consume() waits for the full timeout unless 500 messages arrive,
        # and every millisecond here is decision latency.
        batch = consumer.consume(500, timeout=0.05)
        for msg in batch:
            error = msg.error()
            if error is not None:
                if error.code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"feature-sink: {error}", file=sys.stderr, flush=True)
                continue
            value, topic = msg.value(), msg.topic()
            if value is None or topic is None:
                continue
            record = cast(
                dict[str, Any], deserialize(value, SerializationContext(topic, MessageField.VALUE))
            )
            if topic != TOPIC:
                apply_cdc(defs, store, topic, record)  # customer context (ADR-007)
                continue
            now_ms = int(time.time() * 1000)
            commit_ms = record.get("commit_ms")
            if commit_ms is not None and now_ms - int(commit_ms) < FRESH_MS:
                store.put_transaction(record, now_ms)  # exact snapshot for its decision
            store.upsert_merchant(record)
            if store.upsert_card(record):
                applied += 1
            else:
                skipped += 1
        now = time.monotonic()
        if batch and now - last_commit >= COMMIT_EVERY_S:
            consumer.commit(asynchronous=True)
            last_commit = now
        if not ready and now - last_ready_check >= READY_CHECK_EVERY_S:
            last_ready_check = now
            assignment = consumer.assignment()
            if assignment:
                positions = {
                    (p.topic, p.partition): p.offset for p in consumer.position(assignment)
                }
                committed = {
                    (p.topic, p.partition): p.offset
                    for p in consumer.committed(assignment, timeout=2)
                }
                positions = read_positions(mode, positions, committed)
                marks = {
                    (p.topic, p.partition): consumer.get_watermark_offsets(p, timeout=2)
                    for p in assignment
                }
                if caught_up(positions, marks):
                    READY.touch()
                    ready = True
                    print(f"feature-sink: {mode} caught up in {now - started:.0f}s", flush=True)
        if now - last_report >= 30:
            print(f"feature-sink: applied={applied} skipped_older={skipped}", flush=True)
            last_report = now
    with contextlib.suppress(KafkaException):  # nothing consumed since the last commit
        consumer.commit(asynchronous=False)
    consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
