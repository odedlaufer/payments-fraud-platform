"""CDC completeness check (Phase 1 gate, later DQ-08).

Rebuilds every table from its Debezium topic (create/read/update set the row,
delete removes it, a tombstone must follow each delete) and compares the result
with Postgres, key by key and column by column. Exits 1 on any difference.

    uv run python scripts/cdc_reconcile.py [--settle-seconds 6]
"""

import argparse
import os
import sys
import time
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

# table -> (primary key, columns compared with Postgres)
TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "customers": ("customer_id", ("email", "phone", "risk_tier")),
    "cards": ("card_id", ("status", "credit_limit")),
    "merchants": ("merchant_id", ("mcc", "category", "status")),
    "sessions": ("session_id", ("device_id", "ip_country")),
    "transactions": ("transaction_id", ("status", "amount", "currency")),
    "chargebacks": ("chargeback_id", ("transaction_id", "amount")),
}


@dataclass
class Replay:
    """Current state of one table rebuilt from its changelog."""

    rows: dict[Any, dict[str, Any]] = field(default_factory=dict)
    events: int = 0
    deletes: int = 0
    tombstones: int = 0
    orphan_tombstones: int = 0
    deletes_without_tombstone: int = 0
    _awaiting_tombstone: set[Any] = field(default_factory=set)

    def apply(self, key: Any, value: dict[str, Any] | None) -> None:
        self.events += 1
        if value is None:
            self.tombstones += 1
            if key in self._awaiting_tombstone:
                self._awaiting_tombstone.discard(key)
            else:
                self.orphan_tombstones += 1
            return
        op = value["op"]
        if op in ("c", "r", "u"):
            self.rows[key] = value["after"]
        elif op == "d":
            self.deletes += 1
            self.rows.pop(key, None)
            self._awaiting_tombstone.add(key)
        else:
            raise ValueError(f"unexpected op {op!r}")

    def finish(self) -> None:
        self.deletes_without_tombstone = len(self._awaiting_tombstone)


def replay(events: Iterable[tuple[Any, dict[str, Any] | None]]) -> Replay:
    result = Replay()
    for key, value in events:
        result.apply(key, value)
    result.finish()
    return result


def normalise(value: Any) -> Any:
    if isinstance(value, Decimal):
        return value.normalize()
    if isinstance(value, str):
        return value.rstrip()  # char(n) padding
    return value


@dataclass
class TableReport:
    table: str
    pg_rows: int
    cdc_rows: int
    missing_in_cdc: int
    extra_in_cdc: int
    value_mismatches: int
    replay: Replay
    examples: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        r = self.replay
        return (
            self.missing_in_cdc == 0
            and self.extra_in_cdc == 0
            and self.value_mismatches == 0
            and r.deletes == r.tombstones
            and r.orphan_tombstones == 0
            and r.deletes_without_tombstone == 0
        )


def compare(
    table: str, pk: str, columns: tuple[str, ...], pg: dict[Any, dict[str, Any]], cdc: Replay
) -> TableReport:
    missing = pg.keys() - cdc.rows.keys()
    extra = cdc.rows.keys() - pg.keys()
    mismatches = 0
    examples = [f"missing {pk}={k}" for k in sorted(missing)[:3]]
    examples += [f"extra {pk}={k}" for k in sorted(extra)[:3]]
    for key in pg.keys() & cdc.rows.keys():
        for col in columns:
            a, b = normalise(pg[key][col]), normalise(cdc.rows[key].get(col))
            if a != b:
                mismatches += 1
                if len(examples) < 6:
                    examples.append(f"{pk}={key} {col}: postgres={a!r} cdc={b!r}")
    return TableReport(
        table, len(pg), len(cdc.rows), len(missing), len(extra), mismatches, cdc, examples
    )


# ------------------------------------------------------------------------- I/O


def load_env() -> dict[str, str]:
    values: dict[str, str] = {}
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                values[k.strip()] = v.strip()
    values.update(os.environ)
    return values


def kafka_clients(env: dict[str, str]) -> tuple[Consumer, AvroDeserializer]:
    consumer = Consumer(
        {
            "bootstrap.servers": f"127.0.0.1:{env.get('REDPANDA_KAFKA_PORT', '19092')}",
            "group.id": f"reconcile-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    registry = SchemaRegistryClient(
        {"url": f"http://localhost:{env.get('REDPANDA_SCHEMA_REGISTRY_PORT', '18081')}"}
    )
    return consumer, AvroDeserializer(registry)


def high_watermarks(consumer: Consumer, topics: list[str]) -> dict[str, int]:
    marks = {}
    metadata = consumer.list_topics(timeout=10).topics
    for topic in topics:
        if topic not in metadata:
            marks[topic] = 0
            continue
        total = 0
        for p in metadata[topic].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            total += high
        marks[topic] = total
    return marks


def wait_until_settled(consumer: Consumer, topics: list[str], settle_s: float) -> dict[str, int]:
    """Waits until no topic grows for settle_s seconds, i.e. Debezium has caught up."""
    last = high_watermarks(consumer, topics)
    stable_since = time.monotonic()
    while time.monotonic() - stable_since < settle_s:
        time.sleep(1)
        now = high_watermarks(consumer, topics)
        if now != last:
            last, stable_since = now, time.monotonic()
    return last


def read_records(
    consumer: Consumer, deserialize: AvroDeserializer, topic: str, pk: str
) -> Iterator[tuple[int, int, Any, dict[str, Any] | None]]:
    """Yields (partition, offset, primary key, value) from the start of `topic` to its end."""
    metadata = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if metadata is None or not metadata.partitions:
        return
    ends = {}
    for p in metadata.partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if high > 0:
            ends[p] = high
    if not ends:
        return
    consumer.assign([TopicPartition(topic, p, 0) for p in ends])
    remaining = dict(ends)
    last_progress = time.monotonic()
    while remaining:
        if time.monotonic() - last_progress > 30:
            raise RuntimeError(f"{topic}: no progress for 30s, still missing {remaining}")
        for msg in consumer.consume(num_messages=1000, timeout=5):
            last_progress = time.monotonic()
            if msg.error():
                raise RuntimeError(str(msg.error()))
            p, offset = msg.partition(), msg.offset()
            if p is None or offset is None or p not in remaining or offset >= remaining[p]:
                continue
            key_ctx = SerializationContext(topic, MessageField.KEY)
            key = cast(dict[str, Any], deserialize(msg.key(), key_ctx))
            raw = msg.value()
            value = (
                None
                if raw is None
                else cast(
                    dict[str, Any],
                    deserialize(raw, SerializationContext(topic, MessageField.VALUE)),
                )
            )
            yield p, offset, key[pk], value
            if offset + 1 >= remaining[p]:
                del remaining[p]
    consumer.unassign()


def read_topic(
    consumer: Consumer, deserialize: AvroDeserializer, topic: str, pk: str
) -> Iterator[tuple[Any, dict[str, Any] | None]]:
    for _, _, key, value in read_records(consumer, deserialize, topic, pk):
        yield key, value


def postgres_rows(
    conn: psycopg.Connection[Any], table: str, pk: str, cols: tuple[str, ...]
) -> dict[Any, dict[str, Any]]:
    sql = f"select {pk}, {', '.join(cols)} from public.{table}"
    return {r[0]: dict(zip(cols, r[1:], strict=True)) for r in conn.execute(sql).fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settle-seconds", type=float, default=6.0)
    args = parser.parse_args()
    env = load_env()

    consumer, deserialize = kafka_clients(env)
    topics = [f"pg.public.{t}" for t in TABLES]

    print(f"waiting for CDC to settle ({args.settle_seconds:.0f}s without new events)...")
    wait_until_settled(consumer, topics, args.settle_seconds)

    dsn = (
        f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
        f"user={env['POSTGRES_USER']} password={env.get('POSTGRES_PASSWORD', '')}"
    )
    reports = []
    with psycopg.connect(dsn) as conn:
        for table, (pk, cols) in TABLES.items():
            cdc = replay(read_topic(consumer, deserialize, f"pg.public.{table}", pk))
            reports.append(compare(table, pk, cols, postgres_rows(conn, table, pk, cols), cdc))
    consumer.close()

    header = (
        f"{'table':<13}{'postgres':>9}{'cdc':>9}{'events':>9}{'deletes':>9}{'tombst.':>9}  result"
    )
    print(header)
    print("-" * len(header))
    for r in reports:
        print(
            f"{r.table:<13}{r.pg_rows:>9}{r.cdc_rows:>9}{r.replay.events:>9}"
            f"{r.replay.deletes:>9}{r.replay.tombstones:>9}  {'OK' if r.ok else 'FAIL'}"
        )
        for example in r.examples:
            print(f"    {example}")
    total = sum(r.pg_rows for r in reports)
    events = sum(r.replay.events for r in reports)
    passed = all(r.ok for r in reports)
    print(
        f"\n{total} rows in Postgres, {events} CDC events replayed: "
        f"{'CDC complete' if passed else 'CDC INCOMPLETE'}"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
