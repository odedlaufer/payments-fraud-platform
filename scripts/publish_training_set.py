"""Publishes a point-in-time training set (FR-PIT4; PRD 6.3 gate 2).

    uv run python scripts/publish_training_set.py --cutoff "2026-03-30 00:00:00+00:00"
    uv run python scripts/publish_training_set.py --verify      # re-check all published sets
    uv run python scripts/publish_training_set.py --protect     # tag every published set

1. builds marts.training_set for the cutoff with its tests (the leakage test, FR-PIT3, among
   them): any failure and nothing is published;
2. DQ-13: refuses when the fraud rate is outside +-50% of the average of earlier sets;
3. publishes into lake.ml.training_set (one partition per cutoff; a republish replaces it)
   and records the Iceberg snapshot id in lake.ops.training_sets with rows, positives and a
   fingerprint (an order-independent checksum over every column);
4. proves the set reproduces: re-read through time travel (FOR VERSION AS OF the snapshot)
   must give the same fingerprint;
5. tags the snapshot (lake_refs.py): snapshot expiry (FR-L5) never removes a tagged one.

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
from lake_refs import catalog, protect, refs, tag_name, unprotected

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


def columns(cur: Any, relation: str) -> list[str]:
    """Columns of a relation; with FOR VERSION AS OF, the schema as of that snapshot."""
    cur.execute(f"select * from {relation} limit 0")
    cur.fetchall()
    return [d[0] for d in cur.description]


def fingerprint(
    cur: Any,
    table: str,
    cutoff: str,
    version: int | None = None,
    order: list[str] | None = None,
) -> tuple[int, int, str]:
    """(rows, positives, checksum over every column) for one cutoff of a table. At a snapshot,
    over the columns that existed then (later columns would not exist in it). The checksum
    hashes a row of the columns, so order matters: `order` gives another table's order (an
    evolved table appends new columns at the end, a fresh one has them where they belong)."""
    relation = f"{table} for version as of {version}" if version is not None else table
    cols = order if order is not None else columns(cur, relation)
    (rows, positives, digest) = fetch(
        cur,
        f"select count(*), coalesce(sum(label), 0), to_hex(checksum(row({', '.join(cols)}))) "
        f"from {relation} where cutoff = timestamp '{cutoff}'",
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


def history_rate(cur: Any, cutoff: str, set_version: int) -> float | None:
    """Average fraud rate of earlier sets built by the same training-set definition."""
    if not fetch(
        cur,
        "select 1 from lake.information_schema.tables "
        "where table_schema = 'ops' and table_name = 'training_sets'",
    ):
        return None
    ensure_registry(cur)
    rows = fetch(
        cur,
        f"select avg(fraud_rate) from {REGISTRY} where cutoff != timestamp '{cutoff}' "
        f"and coalesce(training_set_version, 1) = {set_version}",
    )
    return None if rows[0][0] is None else float(rows[0][0])


def current_snapshot(cur: Any) -> int | None:
    rows = fetch(
        cur,
        'select snapshot_id from lake.ml."training_set$snapshots" '
        "order by committed_at desc limit 1",
    )
    return int(rows[0][0]) if rows else None


def rollback(cur: Any, snapshot: int | None) -> None:
    """Undoes a publish that failed verification: the table as it was before."""
    if snapshot is None:
        fetch(cur, f"delete from {PUBLISHED} where true")
    else:
        fetch(cur, f"alter table {PUBLISHED} execute rollback_to_snapshot({snapshot})")


def publish(cur: Any, cutoff: str) -> int:
    fetch(cur, "create schema if not exists lake.ml")
    fetch(
        cur,
        f"create table if not exists {PUBLISHED} with (partitioning = ARRAY['day(cutoff)']) "
        f"as select * from {STAGING} with no data",
    )
    staged, published = columns(cur, STAGING), columns(cur, PUBLISHED)
    types = dict(
        fetch(
            cur,
            (
                "select column_name, data_type from lake.information_schema.columns "
                "where table_schema = 'marts' and table_name = 'training_set'"
            ),
        )
    )
    for col in staged:
        if col not in published:  # the definition gained a column: evolve the published table
            fetch(cur, f"alter table {PUBLISHED} add column {col} {types[col]}")
    fetch(cur, f"delete from {PUBLISHED} where cutoff = timestamp '{cutoff}'")
    names = ", ".join(staged)
    fetch(cur, f"insert into {PUBLISHED} ({names}) select {names} from {STAGING}")
    return int(
        fetch(
            cur,
            'select snapshot_id from lake.ml."training_set$snapshots" '
            "order by committed_at desc limit 1",
        )[0][0]
    )


def ensure_registry(cur: Any) -> None:
    fetch(cur, "create schema if not exists lake.ops")
    fetch(
        cur,
        f"create table if not exists {REGISTRY} (cutoff timestamp(6) with time zone, "
        "published_at timestamp(6) with time zone, snapshot_id bigint, rows bigint, "
        "positives bigint, fraud_rate double, fingerprint varchar, definitions_version integer)",
    )
    fetch(cur, f"alter table {REGISTRY} add column if not exists training_set_version integer")


def record(cur: Any, cutoff: str, snapshot: int, rows: int, positives: int, digest: str) -> None:
    ensure_registry(cur)
    definitions, set_version = fetch(
        cur, f"select max(definitions_version), max(training_set_version) from {STAGING}"
    )[0]
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    rate = positives / rows if rows else 0.0
    fetch(
        cur,
        f"insert into {REGISTRY} (cutoff, published_at, snapshot_id, rows, positives, fraud_rate, "
        "fingerprint, definitions_version, training_set_version) values "
        f"(timestamp '{cutoff}', timestamp '{now}', {snapshot}, {rows}, {positives}, {rate}, "
        f"'{digest}', {definitions}, {set_version})",
    )


def registered(cur: Any) -> list[tuple[Any, int]]:
    return [(c, int(snap)) for c, snap in fetch(cur, f"select cutoff, snapshot_id from {REGISTRY}")]


def protect_all(cur: Any, env: dict[str, str]) -> int:
    cat = catalog(env)
    added = 0
    for cutoff, snapshot in registered(cur):
        name = tag_name(cutoff, snapshot)
        if protect(cat, "ml.training_set", snapshot, name):
            added += 1
            print(f"  tagged {name}")
    missing = unprotected(registered(cur), refs(cat, "ml.training_set"))
    print(f"{added} tag(s) added; {len(missing)} published set(s) unprotected")
    return 1 if missing else 0


def verify_all(cur: Any, env: dict[str, str] | None = None) -> int:
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
    if env is not None:
        missing = unprotected(registered(cur), refs(catalog(env), "ml.training_set"))
        print(f"  {len(missing)} published set(s) without a tag (make protect-training-sets)")
        ok = ok and not missing
    print("every published training set reproduces" if ok else "A TRAINING SET DOES NOT REPRODUCE")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--cutoff", help='e.g. "2026-03-30 00:00:00+00:00"')
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--protect", action="store_true", help="tag every published set")
    args = parser.parse_args()
    env = load_env()
    cur = cursor(env)
    if args.protect:
        return protect_all(cur, env)
    if args.verify:
        return verify_all(cur, env)
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
    set_version = int(fetch(cur, f"select max(training_set_version) from {STAGING}")[0][0])
    previous = history_rate(cur, cutoff, set_version)
    if previous is None:
        print(f"  no earlier sets of training-set version {set_version}: nothing to compare")
    elif not (1 - DQ13_BAND) * previous <= rate <= (1 + DQ13_BAND) * previous:
        print(
            f"REFUSED: fraud rate {rate:.3%} is outside +-50% of the earlier average {previous:.3%}"
        )
        return 1
    else:
        print(f"  {rate:.3%} within +-50% of the earlier average {previous:.3%}")

    print("== 3. publish and record the Iceberg snapshot")
    fetch(cur, "create schema if not exists lake.ml")
    exists = fetch(
        cur,
        "select 1 from lake.information_schema.tables "
        "where table_schema = 'ml' and table_name = 'training_set'",
    )
    before = current_snapshot(cur) if exists else None
    snapshot = publish(cur, cutoff)
    order = columns(cur, PUBLISHED)
    published = fingerprint(cur, PUBLISHED, cutoff)
    expected = fingerprint(cur, STAGING, cutoff, order=order)
    if published != expected:
        rollback(cur, before)
        print(f"FAILED: published {published} differs from staging {expected}; rolled back")
        return 1
    rows, positives, digest = published
    record(cur, cutoff, snapshot, rows, positives, digest)
    print(f"  {PUBLISHED}: cutoff {cutoff}, snapshot {snapshot}, fingerprint {digest[:16]}...")

    print("== 4. reproduce through time travel")
    again = fingerprint(cur, PUBLISHED, cutoff, snapshot)
    if again != (rows, positives, digest):
        print(f"FAILED: time travel to {snapshot} gives {again}")
        return 1
    print(f"  select * from {PUBLISHED} for version as of {snapshot} -> identical")
    print("== 5. protect the snapshot from expiry")
    name = tag_name(cutoff, snapshot)
    protect(catalog(env), "ml.training_set", snapshot, name)
    print(f"  tagged {name}")
    print("training set published")
    return 0


if __name__ == "__main__":
    sys.exit(main())
