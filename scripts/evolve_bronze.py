"""Applies additive Postgres schema changes to the bronze Iceberg tables (DC-4).

Bronze tables keep each change event's row in two row-typed columns, `before`
and `after`. A column added in Postgres becomes a new field in both. This compares
the live Postgres schema with each bronze table and adds missing fields with
ALTER TABLE ... ADD COLUMN. Anything other than an added column (a removed
column, or a changed type) is refused: that is a breaking change (DC-3).

    uv run python scripts/evolve_bronze.py            # apply
    uv run python scripts/evolve_bronze.py --dry-run  # show what would change
"""

import argparse
import sys
from typing import Any

import trino

from cdc_reconcile import load_env
from gen_bronze import TABLES, Column, dsn_from_env, load_columns


def split_top_level(text: str) -> list[str]:
    """Splits on commas that are not inside parentheses: 'a bigint, b decimal(12,2)'."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return parts


def row_fields(trino_type: str) -> dict[str, str]:
    """'row(a bigint, b decimal(12,2))' -> {'a': 'bigint', 'b': 'decimal(12,2)'}."""
    text = trino_type.strip()
    if not (text.startswith("row(") and text.endswith(")")):
        raise ValueError(f"not a row type: {trino_type}")
    fields = {}
    for part in split_top_level(text[4:-1]):
        name, _, ftype = part.partition(" ")
        fields[name.strip('"')] = ftype.strip().replace(" ", "")
    return fields


def plan(table: str, current: dict[str, str], desired: list[Column]) -> list[str]:
    """ALTER statements that add missing fields; raises on non-additive differences."""
    wanted = {c.name: c.trino_type().replace(" ", "") for c in desired}
    removed = sorted(current.keys() - wanted.keys())
    changed = sorted(k for k in current.keys() & wanted.keys() if current[k] != wanted[k])
    if removed or changed:
        raise SystemExit(
            f"cdc_{table}: breaking change refused (removed={removed}, changed type={changed})"
        )
    statements = []
    for column in desired:
        if column.name not in current:
            for side in ("before", "after"):
                statements.append(
                    f"ALTER TABLE lake.bronze.cdc_{table} "
                    f"ADD COLUMN {side}.{column.name} {column.trino_type()}"
                )
    return statements


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = load_env()
    desired = load_columns(dsn_from_env())
    conn = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="evolve-bronze"
    )
    cur: Any = conn.cursor()
    applied = 0
    for table in TABLES:
        cur.execute(
            "select data_type from lake.information_schema.columns "
            "where table_schema = 'bronze' and table_name = ? and column_name = 'after'",
            (f"cdc_{table}",),
        )
        row = cur.fetchone()
        if row is None:
            print(f"cdc_{table}: table not found (make up creates it)")
            return 1
        for statement in plan(table, row_fields(row[0]), desired[table]):
            print(f"{'would run' if args.dry_run else 'running'}: {statement}")
            if not args.dry_run:
                cur.execute(statement)
                cur.fetchall()
            applied += 1
    print(f"{applied} field(s) {'to add' if args.dry_run else 'added'}; bronze matches Postgres")
    return 0


if __name__ == "__main__":
    sys.exit(main())
