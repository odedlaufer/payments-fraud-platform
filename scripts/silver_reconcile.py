"""DQ-08: Postgres and silver agree, row for row (FR-L1).

For each table, compares live Postgres rows with silver's non-deleted rows by
primary key and column values, as of silver's watermark (the commit time of the
latest change merged into silver):

- Every row is compared. A missing or different row is only tolerated (counted
  as in flight, checked again next run) if Postgres changed it after the
  watermark minus a safety margin.
- A key live in silver but absent from Postgres is only accepted if bronze holds
  a delete for it after the watermark (deleted, not merged yet).

Every run appends one row per table to lake.ops.dq_results.

    uv run python scripts/silver_reconcile.py
"""

import argparse
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import trino

from cdc_reconcile import TABLES, load_env, normalise

# Change-time column in Postgres used to recognise in-flight rows.
CHANGE_TS = {
    "customers": "updated_at",
    "cards": "updated_at",
    "merchants": "updated_at",
    "sessions": "created_at",
    "transactions": "updated_at",
    "chargebacks": "created_at",
}
MARGIN = timedelta(seconds=5)

OPS_DDL = [
    "CREATE SCHEMA IF NOT EXISTS lake.ops",
    """CREATE TABLE IF NOT EXISTS lake.ops.dq_results (
        run_id varchar,
        check_id varchar,
        table_name varchar,
        checked_at timestamp(6) with time zone,
        passed boolean,
        compared bigint,
        missing_in_silver bigint,
        extra_in_silver bigint,
        value_mismatches bigint,
        in_flight bigint,
        watermark timestamp(6) with time zone,
        details varchar
    ) WITH (format = 'PARQUET', partitioning = ARRAY['day(checked_at)'])""",
]


@dataclass
class SilverReport:
    table: str
    watermark: datetime | None
    compared: int = 0
    in_flight: int = 0
    missing: list[Any] = field(default_factory=list)
    extra: list[Any] = field(default_factory=list)
    excused_deletes: int = 0
    mismatches: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.missing or self.extra or self.mismatches)


def compare(
    table: str,
    pk: str,
    cols: tuple[str, ...],
    pg: dict[Any, dict[str, Any]],
    pg_in_flight: set[Any],
    silver: dict[Any, dict[str, Any]],
    deleted_after_watermark: set[Any],
    watermark: datetime | None,
) -> SilverReport:
    report = SilverReport(table, watermark)
    for key in sorted(silver.keys() - pg.keys()):
        if key in deleted_after_watermark:
            report.excused_deletes += 1
        else:
            report.extra.append(key)
    for key in sorted(pg.keys()):
        if key not in silver:
            if key in pg_in_flight:
                report.in_flight += 1
            else:
                report.missing.append(key)
            continue
        diffs = [
            f"{pk}={key} {col}: postgres={normalise(pg[key][col])!r} "
            f"silver={normalise(silver[key][col])!r}"
            for col in cols
            if normalise(pg[key][col]) != normalise(silver[key][col])
        ]
        if diffs and key in pg_in_flight:
            report.in_flight += 1
        else:
            report.compared += 1
            report.mismatches.extend(diffs)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-record", action="store_true", help="do not write ops.dq_results")
    args = parser.parse_args()
    env = load_env()

    tconn = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="silver-reconcile"
    )
    cur = tconn.cursor()
    dsn = (
        f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
        f"user={env['POSTGRES_USER']} password={env.get('POSTGRES_PASSWORD', '')}"
    )
    reports = []
    with psycopg.connect(dsn) as pconn:
        for table, (pk, cols) in TABLES.items():
            silver_table = f"lake.silver.{table}_current"
            cur.execute(f"select max(_commit_ts) from {silver_table}")
            watermark = cur.fetchone()[0]

            select = f"select {pk}, {', '.join(cols)}"
            cur.execute(f"{select} from {silver_table} where not is_deleted")
            silver = {r[0]: dict(zip(cols, r[1:], strict=True)) for r in cur.fetchall()}

            rows = pconn.execute(f"{select}, {CHANGE_TS[table]} from public.{table}").fetchall()
            pg = {r[0]: dict(zip(cols, r[1:-1], strict=True)) for r in rows}
            cutoff = watermark - MARGIN if watermark else None
            in_flight = {r[0] for r in rows if cutoff is not None and r[-1] > cutoff}

            deleted_after: set[Any] = set()
            if watermark is not None:
                cur.execute(
                    f"select distinct before.{pk} from lake.bronze.cdc_{table} "
                    "where op = 'd' and commit_ts > ?",
                    (watermark,),
                )
                deleted_after = {r[0] for r in cur.fetchall()}
            reports.append(
                compare(table, pk, cols, pg, in_flight, silver, deleted_after, watermark)
            )

    header = (
        f"{'table':<13}{'compared':>9}{'missing':>9}{'extra':>7}{'values':>8}"
        f"{'in-flight':>11}  watermark                         result"
    )
    print(header)
    print("-" * len(header))
    for r in reports:
        mark = r.watermark.isoformat(timespec="seconds") if r.watermark else "-"
        print(
            f"{r.table:<13}{r.compared:>9}{len(r.missing):>9}{len(r.extra):>7}"
            f"{len(r.mismatches):>8}{r.in_flight:>11}  {mark:<32}  {'OK' if r.ok else 'FAIL'}"
        )
        for line in (
            [f"missing {k}" for k in r.missing[:3]]
            + [f"extra {k}" for k in r.extra[:3]]
            + r.mismatches[:3]
        ):
            print(f"    {line}")

    if not args.no_record:
        for statement in OPS_DDL:
            cur.execute(statement)
            cur.fetchall()
        run_id, now = str(uuid.uuid4()), datetime.now(UTC)
        for r in reports:
            details = "; ".join(
                [f"missing {k}" for k in r.missing[:5]]
                + [f"extra {k}" for k in r.extra[:5]]
                + r.mismatches[:5]
            )
            cur.execute(
                "insert into lake.ops.dq_results values (?, 'DQ-08', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    r.table,
                    now,
                    r.ok,
                    r.compared,
                    len(r.missing),
                    len(r.extra),
                    len(r.mismatches),
                    r.in_flight,
                    r.watermark,
                    details or None,
                ),
            )
            cur.fetchall()
        print(f"\nrecorded run {run_id} in lake.ops.dq_results")

    passed = all(r.ok for r in reports)
    total = sum(r.compared for r in reports)
    print(f"{total} rows compared: {'silver matches Postgres' if passed else 'SILVER MISMATCH'}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
