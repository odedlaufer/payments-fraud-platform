"""feature-sink: features.card (Kafka) -> Redis card hashes (FR-F3).

Reads the topic from the beginning on every start: the online store is derived data
and Redis keeps no persistence, so a restart rebuilds it. The newer-only write makes
replays and duplicate deliveries harmless.

If Redis itself restarts (empty), the client would reconnect silently and only new
records would be written. The sink watches Redis's run_id and exits when it changes;
Docker restarts the sink, which rebuilds the whole store.
"""

import os
import signal
import sys
import time
import uuid
from pathlib import Path
from types import FrameType
from typing import Any, cast

import redis
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from decision_service.online_store import OnlineStore
from features.model import load

TOPIC = "features.card"
HEARTBEAT = Path("/tmp/feature-sink.alive")
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
    store = OnlineStore(client, load())
    run_id = redis_run_id(client)
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": registry}))
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            # A fresh group each start: always rebuild from the beginning of the topic.
            "group.id": f"feature-sink-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "topic.metadata.refresh.interval.ms": 5000,
        }
    )
    consumer.subscribe([TOPIC])

    running = True

    def stop(signum: int, frame: FrameType | None) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    applied = skipped = 0
    last_report = time.monotonic()
    print(f"feature-sink: {TOPIC} -> {redis_url}", flush=True)
    while running:
        HEARTBEAT.touch()
        if redis_run_id(client) != run_id:
            print("feature-sink: Redis restarted (empty); exiting to rebuild", flush=True)
            consumer.close()
            return 1
        # Return quickly: consume() waits for the full timeout unless 500 messages arrive,
        # and every millisecond here is decision latency.
        for msg in consumer.consume(500, timeout=0.05):
            error = msg.error()
            if error is not None:
                if error.code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"feature-sink: {error}", file=sys.stderr, flush=True)
                continue
            value = msg.value()
            if value is None:
                continue
            record = cast(
                dict[str, Any], deserialize(value, SerializationContext(TOPIC, MessageField.VALUE))
            )
            now_ms = int(time.time() * 1000)
            commit_ms = record.get("commit_ms")
            if commit_ms is not None and now_ms - int(commit_ms) < FRESH_MS:
                store.put_transaction(record, now_ms)  # exact snapshot for its decision
            if store.upsert_card(record):
                applied += 1
            else:
                skipped += 1
        if time.monotonic() - last_report >= 30:
            print(f"feature-sink: applied={applied} skipped_older={skipped}", flush=True)
            last_report = time.monotonic()
    consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
