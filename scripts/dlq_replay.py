"""Replay dead-lettered events after a fix (PRD 6.4).

    make dlq-replay                                  # every dead letter not yet replayed
    make dlq-replay ARGS="--code currency_not_allowed"
    make dlq-replay ARGS="--dry-run"                 # count by code; send and commit nothing

Reads dlq.decision-service as consumer group dlq-replay (each record is replayed once) and
sends the original bytes and key to retry.decision-service, which only the decision service
reads: bronze and Flink never see a replayed event twice, and decisions are idempotent per
transaction (FR-D5). An event that is still invalid simply returns to the DLQ.
"""

import argparse
import sys
from collections import Counter
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition

from cdc_reconcile import load_env

DLQ = "dlq.decision-service"
RETRY = "retry.decision-service"


Headers = list[tuple[str, str | bytes | None]]


def pairs(headers: Headers | dict[str, str | bytes | None] | None) -> Headers:
    """Message headers as (name, value) pairs, whichever form the client returns."""
    if headers is None:
        return []
    return list(headers.items()) if isinstance(headers, dict) else list(headers)


def code_of(headers: Headers) -> str:
    value = next((v for k, v in headers if k == "dlq.code"), None)
    return value.decode() if isinstance(value, bytes) else str(value or "unknown")


def retry_headers(headers: Headers, dlq_offset: int) -> Headers:
    """The dead letter's own headers, plus where in the DLQ the replay came from."""
    return [*headers, ("dlq.replayed_from_offset", str(dlq_offset).encode())]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--code", help="only dead letters with this dlq.code")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = load_env()
    bootstrap = f"localhost:{env.get('REDPANDA_KAFKA_PORT', '19092')}"
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "dlq-replay",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    _, high = consumer.get_watermark_offsets(TopicPartition(DLQ, 0), timeout=10)
    consumer.assign([TopicPartition(DLQ, 0, -1001)])  # the group's committed offset, else earliest
    seen: Counter[str] = Counter()
    sent = 0
    last: Any = None
    while True:
        msg = consumer.poll(5.0)
        if msg is None or msg.error() is not None:
            break
        last = msg
        offset = int(msg.offset() or 0)
        headers = pairs(msg.headers())
        code = code_of(headers)
        seen[code] += 1
        if not args.dry_run and (args.code is None or code == args.code):
            producer.produce(
                RETRY,
                key=msg.key(),
                value=msg.value(),
                headers=retry_headers(headers, offset),
            )
            sent += 1
        if offset + 1 >= high:
            break
    producer.flush()
    if last is not None and not args.dry_run:
        consumer.commit(
            offsets=[TopicPartition(DLQ, 0, int(last.offset()) + 1)], asynchronous=False
        )
    consumer.close()
    by_code = ", ".join(f"{c} {n}" for c, n in sorted(seen.items())) or "none"
    print(f"dead letters read: {sum(seen.values())} ({by_code})")
    print("dry run: nothing sent" if args.dry_run else f"replayed to {RETRY}: {sent}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
