"""Re-emits current rows of CDC tables as snapshot ('r') events: a Debezium incremental
snapshot, triggered through the signaling table while streaming continues.

    uv run python scripts/cdc_snapshot.py                     # all six domain tables
    uv run python scripts/cdc_snapshot.py cards sessions      # some of them

Use when a topic no longer holds every row's history (e.g. after retention deleted early
events): consumers that rebuild state from the start of a topic get every current row
again. The Flink feature job and the decision service ignore 'r' events; bronze appends
them; silver, SCD2 and the offline model treat them as the current state.

Done when, for each table, the topic has gained at least as many 'r' events as the table
had rows when the snapshot was requested.
"""

import json
import sys
import time
import uuid
from typing import Any, cast

import psycopg
from confluent_kafka import TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext

from cdc_reconcile import TABLES, kafka_clients, load_env


def dsn(env: dict[str, str]) -> str:
    return (
        f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
        f"user={env['POSTGRES_USER']} password={env.get('POSTGRES_PASSWORD', '')}"
    )


def main() -> int:
    tables = sys.argv[1:] or list(TABLES)
    unknown = set(tables) - set(TABLES)
    if unknown:
        sys.exit(f"unknown tables: {sorted(unknown)}; choose from {list(TABLES)}")
    env = load_env()
    consumer, deserialize = kafka_clients(env)
    start = {
        t: consumer.get_watermark_offsets(TopicPartition(f"pg.public.{t}", 0), timeout=10)[1]
        for t in tables
    }
    rows: dict[str, int] = {}
    with psycopg.connect(dsn(env), autocommit=True) as conn:
        for t in tables:
            row = conn.execute(f"select count(*) from public.{t}").fetchone()
            rows[t] = int(row[0]) if row else 0
        data = {"data-collections": [f"public.{t}" for t in tables], "type": "incremental"}
        conn.execute(
            "insert into public.debezium_signal (id, type, data) "
            "values (%s, 'execute-snapshot', %s)",
            (str(uuid.uuid4()), json.dumps(data)),
        )
    print("snapshot requested: " + ", ".join(f"{t} ({rows[t]} rows)" for t in tables))

    consumer.assign([TopicPartition(f"pg.public.{t}", 0, start[t]) for t in tables])
    seen = dict.fromkeys(tables, 0)
    deadline = time.monotonic() + 1800
    last_report = time.monotonic()
    while any(seen[t] < rows[t] for t in tables):
        if time.monotonic() > deadline:
            print("TIMED OUT: " + ", ".join(f"{t} {seen[t]}/{rows[t]}" for t in tables))
            return 1
        msg = consumer.poll(1.0)
        topic = msg.topic() if msg is not None and not msg.error() else None
        value = msg.value() if msg is not None else None
        if topic is not None and value is not None:
            event = cast(
                dict[str, Any] | None,
                deserialize(value, SerializationContext(topic, MessageField.VALUE)),
            )
            if event and event.get("op") == "r":
                seen[topic.removeprefix("pg.public.")] += 1
        if time.monotonic() - last_report > 15:
            print("  progress: " + ", ".join(f"{t} {seen[t]}/{rows[t]}" for t in tables))
            last_report = time.monotonic()
    consumer.close()
    print("snapshot complete: " + ", ".join(f"{t} {seen[t]} 'r' events" for t in tables))
    return 0


if __name__ == "__main__":
    sys.exit(main())
