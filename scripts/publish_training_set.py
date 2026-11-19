"""Publishes a point-in-time training set (FR-PIT4; PRD 6.3 gate 2).

    uv run python scripts/publish_training_set.py --cutoff "2026-03-30 00:00:00+00:00"
    uv run python scripts/publish_training_set.py --verify      # re-check all published sets

1. builds marts.training_set for the cutoff with its tests (the leakage test, FR-PIT3, among
   them): any failure and nothing is published;
2. DQ-13: refuses when the fraud rate is outside +-50% of the average of earlier sets;
3. publishes into lake.ml.training_set (one partition per cutoff; a republish replaces it)
   and records the Iceberg snapshot id in lake.ops.training_sets with rows, positives and a
   fingerprint (an order-independent checksum over every column);
4. proves the set reproduces: re-read through time travel (FOR VERSION AS OF the snapshot)
   must give the same fingerprint.

Run make dbt-build first so the upstream models are current.
"""

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any

import trino

from cdc_reconcile import load_env
from gen_bronze import ROOT

STAGING = "lake.marts.training_set"
PUBLISHED = "lake.ml.training_set"
REGISTRY = "lake.ops.training_sets"
DQ13_BAND = 0.5


def cursor(env: dict[str, str]) -> Any:
    return trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="training-set"
    ).cursor()


def fetch(cur: Any, sql: str) -> list[Any]:
    cur.execute(sql)
    return list(cur.fetchall())


def fingerprint(
    cur: Any, table: str, cutoff: str, version: int | None = None
) -> tuple[int, int, str]:
    """(rows, positives, checksum over every column) for one cutoff of a table."""
    schema, name = table.split(".")[1:]
    cols = [
        r[0]
        for r in fetch(
            cur,
            (
                "select column_name from lake.information_schema.columns "
                f"where table_schema = '{schema}' and table_name = '{name}' "
                "order by ordinal_position"
            ),
        )
    ]
    at = f" for version as of {version}" if version is not None else ""
    (rows, positives, digest) = fetch(
        cur,
        (
            f"select count(*), coalesce(sum(label), 0), to_hex(checksum(row({', '.join(cols)}))) "
            f"from {table}{at} where cutoff = timestamp '{cutoff}'"
        ),
    )[0]
    return int(rows), int(positives), str(digest)


def build(cutoff: str) -> bool:
    run = subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "build",
            "--profiles-dir",
            ".",
            "--select",
            "training_set+",
            "--vars",
            json.dumps({"training_cutoff": cutoff}),
        ],
        cwd=ROOT / "dbt",
        capture_output=True,
        text=True,
    )
    for line in run.stdout.splitlines():
        if any(k in line for k in ("PASS", "FAIL", "ERROR", "Done.")) and "START" not in line:
            print("  " + line.split("  ", 1)[-1].strip())
    return run.returncode == 0


def history_rate(cur: Any, cutoff: str) -> float | None:
    if not fetch(
        cur,
        "select 1 from lake.information_schema.tables "
        "where table_schema = 'ops' and table_name = 'training_sets'",
    ):
        return None
    rows = fetch(
        cur, f"select avg(fraud_rate) from {REGISTRY} where cutoff != timestamp '{cutoff}'"
    )
    return None if rows[0][0] is None else float(rows[0][0])


def publish(cur: Any, cutoff: str) -> int:
    fetch(cur, "create schema if not exists lake.ml")
    fetch(
        cur,
        (
            f"create table if not exists {PUBLISHED} with (partitioning = ARRAY['day(cutoff)']) "
            f"as select * from {STAGING} with no data"
        ),
    )
    fetch(cur, f"delete from {PUBLISHED} where cutoff = timestamp '{cutoff}'")
    fetch(cur, f"insert into {PUBLISHED} select * from {STAGING}")
    return int(
        fetch(
            cur,
            (
                'select snapshot_id from lake.ml."training_set$snapshots" '
                "order by committed_at desc limit 1"
            ),
        )[0][0]
    )


def record(cur: Any, cutoff: str, snapshot: int, rows: int, positives: int, digest: str) -> None:
    fetch(cur, "create schema if not exists lake.ops")
    fetch(
        cur,
        (
            f"create table if not exists {REGISTRY} (cutoff timestamp(6) with time zone, "
            "published_at timestamp(6) with time zone, snapshot_id bigint, rows bigint, "
            "positives bigint, fraud_rate double, fingerprint varchar, definitions_version integer)"
        ),
    )
    version = fetch(cur, f"select max(definitions_version) from {STAGING}")[0][0]
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    rate = positives / rows if rows else 0.0
    fetch(
        cur,
        (
            f"insert into {REGISTRY} values (timestamp '{cutoff}', timestamp '{now}', {snapshot}, "
            f"{rows}, {positives}, {rate}, '{digest}', {version})"
        ),
    )


def verify_all(cur: Any) -> int:
    ok = True
    for cutoff, snapshot, rows, positives, digest in fetch(
        cur,
        (
            "select cast(cutoff as varchar), snapshot_id, rows, positives, fingerprint "
            f"from {REGISTRY} "
            "order by published_at"
        ),
    ):
        c = str(cutoff).replace(" UTC", "+00:00")
        again = fingerprint(cur, PUBLISHED, c, int(snapshot))
        same = again == (int(rows), int(positives), str(digest))
        ok = ok and same
        print(
            f"  cutoff {cutoff}  snapshot {snapshot}: "
            + ("reproduces" if same else f"DIFFERS {again}")
        )
    print("every published training set reproduces" if ok else "A TRAINING SET DOES NOT REPRODUCE")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--cutoff", help='e.g. "2026-03-30 00:00:00+00:00"')
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    cur = cursor(load_env())
    if args.verify:
        return verify_all(cur)
    if not args.cutoff:
        parser.error("--cutoff is required (or --verify)")
    cutoff = args.cutoff

    print(f"== 1. build and test the training set for cutoff {cutoff}")
    if not build(cutoff):
        print(
            "REFUSED: the build or a test failed (leakage test FR-PIT3 among them); "
            "nothing published"
        )
        return 1
    rows, positives, digest = fingerprint(cur, STAGING, cutoff)
    rate = positives / rows if rows else 0.0
    print(f"  staged: {rows} transactions, {positives} positive ({rate:.3%})")

    print("== 2. DQ-13: fraud rate against earlier sets")
    previous = history_rate(cur, cutoff)
    if previous is None:
        print("  no earlier sets: nothing to compare (first publication)")
    elif not (1 - DQ13_BAND) * previous <= rate <= (1 + DQ13_BAND) * previous:
        print(
            f"REFUSED: fraud rate {rate:.3%} is outside +-50% of the earlier average {previous:.3%}"
        )
        return 1
    else:
        print(f"  {rate:.3%} within +-50% of the earlier average {previous:.3%}")

    print("== 3. publish and record the Iceberg snapshot")
    snapshot = publish(cur, cutoff)
    published = fingerprint(cur, PUBLISHED, cutoff)
    if published != (rows, positives, digest):
        print(f"FAILED: published set differs from staging: {published}")
        return 1
    record(cur, cutoff, snapshot, rows, positives, digest)
    print(f"  {PUBLISHED}: cutoff {cutoff}, snapshot {snapshot}, fingerprint {digest[:16]}...")

    print("== 4. reproduce through time travel")
    again = fingerprint(cur, PUBLISHED, cutoff, snapshot)
    if again != (rows, positives, digest):
        print(f"FAILED: time travel to {snapshot} gives {again}")
        return 1
    print(f"  select * from {PUBLISHED} for version as of {snapshot} -> identical")
    print("training set published")
    return 0


if __name__ == "__main__":
    sys.exit(main())
