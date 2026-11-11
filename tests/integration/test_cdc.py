"""End-to-end CDC: Postgres -> Debezium -> Redpanda as Avro (FR-CDC1 to FR-CDC4, DC-1)."""

import json
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[tuple[object, ...]]
TIMEOUT_S = 60


@pytest.fixture(scope="module")
def registry_url(env: dict[str, str]) -> str:
    return f"http://localhost:{env.get('REDPANDA_SCHEMA_REGISTRY_PORT', '18081')}"


@pytest.fixture(scope="module")
def bootstrap(env: dict[str, str]) -> str:
    return f"localhost:{env.get('REDPANDA_KAFKA_PORT', '19092')}"


def read_events(
    bootstrap: str, registry_url: str, topic: str, key_field: str, key_value: object, count: int
) -> list[dict[str, Any] | None]:
    """Reads `count` records for one row key from the start of `topic`; None = tombstone."""
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": registry_url}))
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"it-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "topic.metadata.refresh.interval.ms": 1000,
        }
    )
    consumer.subscribe([topic])
    events: list[dict[str, Any] | None] = []
    deadline = time.monotonic() + TIMEOUT_S
    try:
        while len(events) < count and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = deserialize(msg.key(), SerializationContext(topic, MessageField.KEY))
            if not key or key.get(key_field) != key_value:
                continue
            raw = msg.value()
            events.append(
                None
                if raw is None
                else deserialize(raw, SerializationContext(topic, MessageField.VALUE))
            )
    finally:
        consumer.close()
    return events


def test_insert_update_delete_arrive_in_order_with_tombstone(
    oltp_autocommit: Conn, bootstrap: str, registry_url: str
) -> None:
    email = f"cdc-{uuid.uuid4().hex[:8]}@test.invalid"
    row = oltp_autocommit.execute(
        "insert into customers (full_name, email, phone, address_line, city, country_code, "
        "home_lat, home_lon, event_ts) values ('CDC Test', %s, '1', 'x', 'y', 'IL', 32.0, 34.8, "
        "%s) returning customer_id",
        (email, datetime(2026, 1, 1, tzinfo=UTC)),
    ).fetchone()
    assert row is not None
    customer_id = row[0]
    oltp_autocommit.execute(
        "update customers set risk_tier = 'high' where customer_id = %s", (customer_id,)
    )
    oltp_autocommit.execute("delete from customers where customer_id = %s", (customer_id,))

    events = read_events(
        bootstrap, registry_url, "pg.public.customers", "customer_id", customer_id, 4
    )

    assert len(events) == 4, f"expected c, u, d, tombstone; got {len(events)} events"
    created, updated, deleted, tombstone = events
    assert created is not None and updated is not None and deleted is not None
    assert [created["op"], updated["op"], deleted["op"]] == ["c", "u", "d"]
    assert tombstone is None, "delete must be followed by a tombstone (FR-CDC4)"

    assert created["after"]["email"] == email
    # Replica identity full: updates and deletes carry the complete prior row.
    assert updated["before"]["risk_tier"] == "low"
    assert updated["after"]["risk_tier"] == "high"
    assert deleted["before"]["email"] == email
    assert deleted["after"] is None
    assert created["source"]["ts_ms"] > 0


def test_schema_registry_enforces_backward_compatibility(registry_url: str) -> None:
    """DC-1: incompatible schema changes are rejected at produce time."""
    with urllib.request.urlopen(f"{registry_url}/config", timeout=5) as resp:
        config = json.load(resp)
    assert config["compatibilityLevel"] == "BACKWARD"


def test_customers_subjects_registered(registry_url: str) -> None:
    with urllib.request.urlopen(f"{registry_url}/subjects", timeout=5) as resp:
        subjects = set(json.load(resp))
    assert {"pg.public.customers-key", "pg.public.customers-value"} <= subjects


def test_no_topic_for_sim_schema(bootstrap: str) -> None:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-{uuid.uuid4()}"})
    try:
        topics = consumer.list_topics(timeout=10).topics
    finally:
        consumer.close()
    assert not [t for t in topics if t.startswith("pg.sim")]


def test_replication_slot_active(oltp: Conn) -> None:
    row = oltp.execute(
        "select active, plugin from pg_replication_slots where slot_name = 'payments_cdc_slot'"
    ).fetchone()
    assert row == (True, "pgoutput")


def test_heartbeat_advances(oltp_autocommit: Conn) -> None:
    """FR-CDC3: Debezium updates the heartbeat row every 10 s, keeping the slot moving."""
    first = oltp_autocommit.execute("select beat_at from cdc_heartbeat").fetchone()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        time.sleep(2)
        now = oltp_autocommit.execute("select beat_at from cdc_heartbeat").fetchone()
        if now != first:
            return
    pytest.fail("heartbeat row did not change within 30 s")
