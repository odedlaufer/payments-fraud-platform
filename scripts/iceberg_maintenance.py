"""Iceberg table maintenance (FR-L5): compaction, snapshot expiry, orphan-file removal.

    uv run python scripts/iceberg_maintenance.py               # every table in the lake
    uv run python scripts/iceberg_maintenance.py --dry-run     # counts only

Per base table (views are skipped): optimize (merge small files, e.g. bronze's one file per
checkpoint), expire_snapshots older than 7 days, remove_orphan_files older than 7 days. Data
files and snapshots are counted before and after, and each run is recorded in
lake.ops.maintenance_runs.

Guard: published training sets must stay reproducible (FR-PIT4). Their snapshots are
tagged and expiry never removes a tagged snapshot; ml.training_set is not expired at all
while any registered set lacks a tag.
"""

import argparse
import sys
import time
from datetime import UTC, datetime
from typing import Any

import trino
import trino.exceptions

from cdc_reconcile import load_env
from lake_refs import catalog, refs, unprotected

RETENTION = "7d"
TRAINING_SETS = ("ml", "training_set")


def fetch(cur: Any, sql: str) -> list[Any]:
    cur.execute(sql)
    return list(cur.fetchall())


def tables(cur: Any) -> list[tuple[str, str]]:
    return [
        (schema, name)
        for schema, name in fetch(
            cur,
            "select table_schema, table_name from lake.information_schema.tables "
            "where table_type = 'BASE TABLE' "
            # system: Trino's virtual tables; dq_failures: rebuilt by every dbt build
            "and table_schema not in ('information_schema', 'system', 'dq_failures') "
            "order by 1, 2",
        )
    ]


def counts(cur: Any, schema: str, name: str) -> tuple[int, int]:
    files = fetch(cur, f'select count(*) from lake.{schema}."{name}$files"')[0][0]
    snapshots = fetch(cur, f'select count(*) from lake.{schema}."{name}$snapshots"')[0][0]
    return int(files), int(snapshots)


def guard(cur: Any, env: dict[str, str]) -> list[int]:
    """Registered training-set snapshots without a tag (they would be expirable)."""
    if not fetch(
        cur,
        "select 1 from lake.information_schema.tables "
        "where table_schema = 'ops' and table_name = 'training_sets'",
    ):
        return []
    registered = [
        (c, int(s)) for c, s in fetch(cur, "select cutoff, snapshot_id from lake.ops.training_sets")
    ]
    return unprotected(registered, refs(catalog(env), "ml.training_set"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = load_env()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="maintenance"
    ).cursor()
    exposed = guard(cur, env)
    if exposed:
        print(
            f"GUARD: {len(exposed)} published training set(s) untagged {exposed}: "
            "ml.training_set will not be expired (make protect-training-sets)"
        )
    results = []
    failures: list[str] = []
    print(f"{'table':<32}{'files':>16}{'snapshots':>16}{'seconds':>9}")
    for schema, name in tables(cur):
        start = time.monotonic()
        files_before, snaps_before = counts(cur, schema, name)
        error = ""
        if not args.dry_run:
            statements = [f"alter table lake.{schema}.{name} execute optimize"]
            if not ((schema, name) == TRAINING_SETS and exposed):
                statements.append(
                    f"alter table lake.{schema}.{name} execute "
                    f"expire_snapshots(retention_threshold => '{RETENTION}')"
                )
            statements.append(
                f"alter table lake.{schema}.{name} execute "
                f"remove_orphan_files(retention_threshold => '{RETENTION}')"
            )
            for sql in statements:
                try:  # streaming writers commit meanwhile: a conflict fails one statement
                    fetch(cur, sql)
                except trino.exceptions.TrinoQueryError as exc:
                    error = f"{sql.split('execute ')[1].split('(')[0]}: {exc.message[:120]}"
                    break
        files_after, snaps_after = counts(cur, schema, name)
        seconds = time.monotonic() - start
        results.append(
            (schema, name, files_before, files_after, snaps_before, snaps_after, seconds)
        )
        if error:
            failures.append(f"{schema}.{name}: {error}")
        print(
            f"{schema + '.' + name:<32}{files_before:>7} -> {files_after:<6}"
            f"{snaps_before:>7} -> {snaps_after:<6}{seconds:>8.1f}"
        )
    total = [sum(int(r[i]) for r in results) for i in (2, 3, 4, 5)]
    print(f"{'total':<32}{total[0]:>7} -> {total[1]:<6}{total[2]:>7} -> {total[3]:<6}")
    if not args.dry_run:
        fetch(cur, "create schema if not exists lake.ops")
        fetch(
            cur,
            "create table if not exists lake.ops.maintenance_runs ("
            "run_at timestamp(6) with time zone, "
            "table_name varchar, files_before bigint, files_after bigint, snapshots_before bigint, "
            "snapshots_after bigint, seconds double)",
        )
        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
        values = ", ".join(
            f"(timestamp '{now}', '{s}.{n}', {fb}, {fa}, {sb}, {sa}, {sec:.1f})"
            for s, n, fb, fa, sb, sa, sec in results
        )
        fetch(cur, f"insert into lake.ops.maintenance_runs values {values}")
        print("recorded in lake.ops.maintenance_runs")
    for f in failures:
        print(f"FAILED {f}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
