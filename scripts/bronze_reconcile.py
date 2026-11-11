"""Bronze completeness check (FR-F5): every CDC event is in bronze exactly once.

For each table, compares the topic with lake.bronze.cdc_{table}:

- the set of (partition, offset) of non-tombstone events must equal bronze's set,
  so nothing is missing, nothing is extra and nothing is duplicated;
- counts per Debezium op (c, r, u, d) must match.

Tombstones are not stored in bronze (the preceding op='d' event records the delete).
Bronze commits on Flink checkpoints, so the check waits for it to catch up.

    uv run python scripts/bronze_reconcile.py [--timeout 240]
"""

import argparse
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import trino

from cdc_reconcile import TABLES, kafka_clients, load_env, read_records, wait_until_settled

Offset = tuple[int, int]


@dataclass
class Side:
    offsets: set[Offset] = field(default_factory=set)
    ops: Counter[str] = field(default_factory=Counter)
    rows: int = 0  # bronze row count, including any duplicates
    end: dict[int, int] = field(default_factory=dict)  # topic: next offset per partition


@dataclass
class BronzeReport:
    table: str
    topic: Side
    bronze: Side

    @property
    def missing(self) -> int:
        return len(self.topic.offsets - self.bronze.offsets)

    @property
    def extra(self) -> int:
        return len(self.bronze.offsets - self.topic.offsets)

    @property
    def duplicates(self) -> int:
        return self.bronze.rows - len(self.bronze.offsets)

    @property
    def ok(self) -> bool:
        return (
            self.missing == 0
            and self.extra == 0
            and self.duplicates == 0
            and self.topic.ops == self.bronze.ops
        )


def topic_side(records: Any) -> Side:
    side = Side()
    for partition, offset, _key, value in records:
        side.end[partition] = max(side.end.get(partition, 0), offset + 1)
        if value is None:  # tombstone: not stored in bronze
            continue
        side.offsets.add((partition, offset))
        side.ops[value["op"]] += 1
    side.rows = len(side.offsets)
    return side


def bronze_side(cur: Any, table: str, end: dict[int, int]) -> Side:
    """Bronze rows up to the topic snapshot's end offsets: a consistent point-in-time view,
    so events Debezium emits after the snapshot cannot show up as `extra`."""
    side = Side()
    if not end:
        return side
    upto = " or ".join(f"(kafka_partition = {p} and kafka_offset < {o})" for p, o in end.items())
    cur.execute(
        f"select kafka_partition, kafka_offset, op from lake.bronze.cdc_{table} where {upto}"
    )
    for partition, offset, op in cur.fetchall():
        side.rows += 1
        side.offsets.add((int(partition), int(offset)))
        side.ops[str(op)] += 1
    return side


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=240.0, help="seconds to wait for bronze")
    parser.add_argument("--settle-seconds", type=float, default=6.0)
    args = parser.parse_args()
    env = load_env()

    consumer, deserialize = kafka_clients(env)
    topics = [f"pg.public.{t}" for t in TABLES]
    print(f"waiting for CDC to settle ({args.settle_seconds:.0f}s without new events)...")
    wait_until_settled(consumer, topics, args.settle_seconds)
    expected = {
        table: topic_side(read_records(consumer, deserialize, f"pg.public.{table}", pk))
        for table, (pk, _) in TABLES.items()
    }
    consumer.close()

    conn = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="bronze-reconcile"
    )
    cur = conn.cursor()
    deadline = time.monotonic() + args.timeout
    print(f"waiting for bronze to catch up (commits every 60s, up to {args.timeout:.0f}s)...")
    while True:
        reports = [
            BronzeReport(t, expected[t], bronze_side(cur, t, expected[t].end)) for t in TABLES
        ]
        caught_up = all(r.bronze.rows >= r.topic.rows for r in reports)
        if caught_up or time.monotonic() > deadline:
            break
        time.sleep(10)

    header = f"{'table':<13}{'topic':>9}{'bronze':>9}{'missing':>9}{'extra':>8}{'dups':>7}  result"
    print(header)
    print("-" * len(header))
    for r in reports:
        print(
            f"{r.table:<13}{r.topic.rows:>9}{r.bronze.rows:>9}{r.missing:>9}{r.extra:>8}"
            f"{r.duplicates:>7}  {'OK' if r.ok else 'FAIL'}"
        )
        if r.topic.ops != r.bronze.ops:
            print(f"    ops topic={dict(r.topic.ops)} bronze={dict(r.bronze.ops)}")
    passed = all(r.ok for r in reports)
    events = sum(r.topic.rows for r in reports)
    verdict = "bronze complete, exactly once" if passed else "BRONZE MISMATCH"
    print(f"\n{events} change events: {verdict}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
