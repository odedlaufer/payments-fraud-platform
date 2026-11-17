"""The decisions topic vs lake.bronze.decisions: every decision exactly once (FR-F5).

Snapshots the topic's end offsets first, waits for bronze to reach them, then compares the
exact set of (partition, offset) pairs below them: missing, extra and duplicated rows.
"""

import sys
import time
from collections import Counter

import trino
from confluent_kafka import TopicPartition

from cdc_reconcile import kafka_clients, load_env

TOPIC = "decisions"


def topic_offsets(consumer: object, end: dict[int, int]) -> set[tuple[int, int]]:
    """(partition, offset) of every decision below the snapshot end offsets."""
    c = consumer  # confluent_kafka.Consumer
    c.assign([TopicPartition(TOPIC, p, 0) for p in end])  # type: ignore[attr-defined]
    seen: set[tuple[int, int]] = set()
    remaining = {p for p, e in end.items() if e > 0}
    while remaining:
        msg = c.poll(5.0)  # type: ignore[attr-defined]
        if msg is None:
            break
        if msg.error():
            continue
        p, o = msg.partition(), msg.offset()
        if o < end[p] and msg.value() is not None:
            seen.add((p, o))
        if o >= end[p] - 1:
            remaining.discard(p)
    return seen


def main() -> int:
    env = load_env()
    consumer, _ = kafka_clients(env)
    meta = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
    end = {
        p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1]
        for p in meta.partitions
    }
    expected = topic_offsets(consumer, end)
    consumer.close()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="reconcile"
    ).cursor()
    upto = (
        " or ".join(f"(kafka_partition = {p} and kafka_offset < {e})" for p, e in end.items())
        or "false"
    )
    deadline = time.monotonic() + 90
    while True:  # bronze commits on checkpoints: wait until it has everything below `end`
        cur.execute(
            "select count(distinct (kafka_partition, kafka_offset)) "
            f"from lake.bronze.decisions where {upto}"
        )
        if cur.fetchone()[0] >= len(expected) or time.monotonic() > deadline:
            break
        time.sleep(3)
    cur.execute(f"select kafka_partition, kafka_offset from lake.bronze.decisions where {upto}")
    rows = Counter((int(p), int(o)) for p, o in cur.fetchall())
    missing = expected - set(rows)
    extra = set(rows) - expected
    duplicates = sum(n - 1 for n in rows.values() if n > 1)
    print(f"{len(expected)} decisions on the topic, {sum(rows.values())} bronze rows: "
          f"missing={len(missing)} extra={len(extra)} duplicates={duplicates}")  # fmt: skip
    ok = not missing and not extra and not duplicates
    print("decisions bronze complete, exactly once" if ok else "DECISIONS BRONZE INCOMPLETE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
