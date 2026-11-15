"""Live feature equality (PRD 8.4 on real data): every record on features.card must equal
the reference implementation recomputed from Postgres, feature by feature.

    uv run python scripts/features_check.py
"""

import math
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any, cast

import psycopg
from confluent_kafka import TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext

from cdc_reconcile import kafka_clients, load_env
from features.model import load
from features.reference import Txn, card_features

TOPIC = "features.card"


def read_features(env: dict[str, str]) -> dict[int, dict[str, Any]]:
    consumer, deserialize = kafka_clients(env)
    meta = consumer.list_topics(TOPIC, timeout=10).topics.get(TOPIC)
    if meta is None or not meta.partitions:
        raise SystemExit(f"{TOPIC} does not exist yet: is the features-card job running?")
    ends = {}
    for p in meta.partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
        if high:
            ends[p] = high
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in ends])
    records: dict[int, dict[str, Any]] = {}
    while ends:
        for msg in consumer.consume(1000, timeout=5):
            p, off = msg.partition(), msg.offset()
            if msg.error() or p is None or off is None or p not in ends:
                continue
            value = cast(
                dict[str, Any],
                deserialize(msg.value(), SerializationContext(TOPIC, MessageField.VALUE)),
            )
            records[value["transaction_id"]] = value  # at-least-once: last copy wins
            if off + 1 >= ends[p]:
                del ends[p]
    consumer.close()
    return records


def card_history(env: dict[str, str], cards: set[int]) -> list[Txn]:
    dsn = (
        f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
        f"user={env['POSTGRES_USER']} password={env.get('POSTGRES_PASSWORD', '')}"
    )
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "select transaction_id, card_id, merchant_id, amount, channel, lat, lon, "
            "(extract(epoch from date_trunc('second', event_ts))::bigint * 1000 "
            " + floor(extract(microseconds from event_ts)::bigint %% 1000000 / 1000))::bigint "
            "from transactions where card_id = any(%s) order by transaction_id",
            (sorted(cards),),
        ).fetchall()
    return [
        Txn(int(r[0]), int(r[1]), int(r[2]), Decimal(r[3]), str(r[4]), r[5], r[6], int(r[7]))
        for r in rows
    ]


def equal(got: object, want: object) -> bool:
    if isinstance(want, float) or isinstance(got, float):
        if got is None or want is None:
            return got is want
        return math.isclose(
            float(cast(float, got)), float(cast(float, want)), rel_tol=1e-9, abs_tol=1e-9
        )
    return got == want


def main() -> int:
    env = load_env()
    defs = load()
    records = read_features(env)
    history = card_history(env, {int(r["card_id"]) for r in records.values()})
    expected = card_features(defs, history)
    names = [f.name for f in defs.for_entity("card")]
    mismatches: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    for txn_id, record in records.items():
        want = expected.get(txn_id)
        if want is None:
            mismatches["<not in postgres>"] += 1
            continue
        for name in names:
            if not equal(record[name], want[name]):
                mismatches[name] += 1
                if len(examples[name]) < 3:
                    examples[name].append(
                        f"txn {txn_id}: flink={record[name]!r} reference={want[name]!r}"
                    )
    print(f"{len(records)} transactions x {len(names)} card features compared with the reference")
    for name in names:
        print(f"  {name:<24} {'OK' if not mismatches[name] else f'{mismatches[name]} mismatches'}")
        for line in examples[name]:
            print(f"      {line}")
    if mismatches["<not in postgres>"]:
        print(f"  {mismatches['<not in postgres>']} feature records for unknown transactions")
    ok = not mismatches
    print("online features match the definitions" if ok else "FEATURE MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
