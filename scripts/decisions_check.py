"""Decision service check (FR-D1 to FR-D5, Phase 3 gate):

- every transaction created since the first decision has exactly one decision;
- every logged feature snapshot equals that transaction's features.card record;
- latency from Postgres commit to decision: p50, p95, p99 (gate: p95 < 5 s).

    uv run python scripts/decisions_check.py [--since-minutes 10]
"""

import argparse
import statistics
import sys
from collections import Counter
from decimal import Decimal
from typing import Any, cast

import psycopg
from confluent_kafka import TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext

from cdc_reconcile import kafka_clients, load_env
from features.model import load
from features_check import equal, read_features

TOPIC = "decisions"


def read_decisions(env: dict[str, str]) -> list[dict[str, Any]]:
    consumer, deserialize = kafka_clients(env)
    meta = consumer.list_topics(TOPIC, timeout=10).topics.get(TOPIC)
    if meta is None or not meta.partitions:
        raise SystemExit("no decisions yet: is decision-service running?")
    ends = {}
    for p in meta.partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
        if high:
            ends[p] = high
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in ends])
    out: list[dict[str, Any]] = []
    while ends:
        for msg in consumer.consume(1000, timeout=5):
            p, off = msg.partition(), msg.offset()
            if msg.error() or p is None or off is None or p not in ends:
                continue
            ctx = SerializationContext(TOPIC, MessageField.VALUE)
            out.append(cast(dict[str, Any], deserialize(msg.value(), ctx)))
            if off + 1 >= ends[p]:
                del ends[p]
    consumer.close()
    return out


def pct(values: list[int], q: int) -> int:
    if len(values) < 2:
        return values[0] if values else 0
    return int(statistics.quantiles(values, n=100)[q - 1])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-minutes", type=float, default=None,
                        help="only decisions committed in the last N minutes")  # fmt: skip
    args = parser.parse_args()
    env = load_env()
    decisions = read_decisions(env)
    if args.since_minutes is not None:
        cutoff = max(d["decided_ms"] for d in decisions) - int(args.since_minutes * 60_000)
        decisions = [d for d in decisions if d["commit_ms"] >= cutoff]
    ids = [d["transaction_id"] for d in decisions]
    duplicates = len(ids) - len(set(ids))
    first, last = min(ids), max(ids)

    dsn = (
        f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
        f"user={env['POSTGRES_USER']} password={env.get('POSTGRES_PASSWORD', '')}"
    )
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "select transaction_id from transactions where transaction_id between %s and %s",
            (first, last),
        ).fetchall()
    created = {int(r[0]) for r in rows}
    missing = sorted(created - set(ids))

    names = [f.name for f in load().for_entity("card")]
    dtypes = {f.name: f.dtype for f in load().for_entity("card")}
    features = read_features(env)
    snapshot_mismatches = 0
    for d in decisions:
        if d["reason"] == "stale_features":
            continue
        record = features.get(d["transaction_id"])
        for name in names:
            got = d["features"][name]
            if got is not None and dtypes[name].startswith("decimal"):
                got = Decimal(got)
            if record is None or not equal(got, record[name]):
                snapshot_mismatches += 1
                break

    outcomes = Counter(f"{d['decision']}/{d['reason']}" for d in decisions)
    fired = Counter(r for d in decisions for r in d["fired_rules"])
    latency = [int(d["latency_ms"]) for d in decisions]
    features_wait = [int(d["features_ms"]) - int(d["consumed_ms"]) for d in decisions
                     if d["features_ms"] is not None]  # fmt: skip
    stale = outcomes.get("review/stale_features", 0)
    print(f"{len(decisions)} decisions for transactions {first}..{last}")
    print(f"  missing={len(missing)} duplicates={duplicates} "
          f"snapshot_mismatches={snapshot_mismatches}")  # fmt: skip
    print(f"  outcomes: {dict(sorted(outcomes.items()))}")
    print(f"  fired rules: {dict(fired.most_common())}")
    print(f"  stale_features: {stale} ({100 * stale / len(decisions):.2f}%)")
    print(f"  latency commit->decision ms: p50={pct(latency, 50)} p95={pct(latency, 95)} "
          f"p99={pct(latency, 99)} max={max(latency)}")  # fmt: skip
    if features_wait:
        print(f"  wait for features ms: p50={pct(features_wait, 50)} p95={pct(features_wait, 95)}")
    ok = not missing and not duplicates and not snapshot_mismatches
    gate = pct(latency, 95) < 5000
    print("decisions complete, exact snapshots" if ok else "DECISION CHECK FAILED")
    print(f"gate: p95 {'<' if gate else '>='} 5000 ms")
    return 0 if ok and gate else 1


if __name__ == "__main__":
    sys.exit(main())
