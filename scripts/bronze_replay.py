"""Repairs a range of bronze rows by re-ingesting them from Kafka (incident tool).

Use when bronze stored events with a field missing that Kafka still has, e.g. a column
added in Postgres while the bronze job was still running on the old schema:

    uv run python scripts/bronze_replay.py transactions --dropped-field status_event_ts
    uv run python scripts/bronze_replay.py transactions --dropped-field status_event_ts --dry-run

1. finds the damaged range: events whose Kafka value has the field, before the first
   offset where bronze kept it (exactly what the old-schema job wrote);
2. pauses bronze-cdc with a savepoint (the job supervisor stopped first);
3. deletes that offset range from lake.bronze.cdc_<table>;
4. re-ingests exactly those offsets with a bounded batch Flink job, using the live job's
   own source and insert definitions with the current schema;
5. checks the rows are back, field included; resumes bronze-cdc from the savepoint.
Bronze is resumed and the supervisor restarted even if a step fails; rerunning is safe.
"""

import argparse
import subprocess
import sys
from typing import Any

import trino

from cdc_reconcile import kafka_clients, load_env, read_records
from gen_bronze import CATALOG, ROOT, bronze_insert, dsn_from_env, kafka_source, load_columns

REPAIR_DIR = ROOT / "flink/sql/repair"


def damaged_range(env: dict[str, str], cur: Any, table: str, field: str) -> tuple[int, int, int]:
    """(start, end, events): offsets [start, end) on partition 0 to re-ingest."""
    cur.execute(
        f"select min(kafka_offset) from lake.bronze.cdc_{table} "
        f"where after.{field} is not null or before.{field} is not null"
    )
    end = cur.fetchone()[0]
    if end is None:
        sys.exit(f"bronze has no {field} at all yet: nothing to compare against")
    consumer, deserialize = kafka_clients(env)
    carrying = []
    events = 0
    pk = {"transactions": "transaction_id"}.get(table, f"{table[:-1]}_id")
    for partition, offset, _, value in read_records(
        consumer, deserialize, f"pg.public.{table}", pk
    ):
        if partition != 0 or offset >= end or value is None:
            continue
        after, before = value.get("after") or {}, value.get("before") or {}
        if after.get(field) is not None or before.get(field) is not None:
            carrying.append(offset)
    consumer.close()
    if not carrying:
        return end, end, 0
    start = min(carrying)
    events = len(carrying)
    return start, end, events


def replay_sql(table: str, columns: list[Any], start: int, end: int) -> str:
    options = f"""\
    'connector' = 'kafka',
    'topic' = 'pg.public.{table}',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'bronze-replay-{table}',
    'scan.startup.mode' = 'specific-offsets',
    'scan.startup.specific-offsets' = 'partition:0,offset:{start}',
    'scan.bounded.mode' = 'specific-offsets',
    'scan.bounded.specific-offsets' = 'partition:0,offset:{end}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://redpanda:8081'"""
    where = f"\nWHERE `kafka_offset` >= {start} AND `kafka_offset` < {end}"
    return (
        f"-- Replay of pg.public.{table} offsets [{start}, {end}) into bronze (bronze_replay.py).\n"
        f"SET 'pipeline.name' = 'bronze-replay-{table}';\n"
        "SET 'execution.runtime-mode' = 'batch';\n"
        "SET 'table.dml-sync' = 'true';\n"
        + CATALOG
        + kafka_source(table, columns, options)
        + "\n"
        + bronze_insert(table, where)
    )


def sh(command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, shell=True, cwd=ROOT, capture_output=True, text=True)


def count_rows(cur: Any, table: str, field: str, start: int, end: int) -> tuple[int, int]:
    cur.execute(
        f"select count(*), count_if(after.{field} is not null or before.{field} is not null) "
        f"from lake.bronze.cdc_{table} where kafka_partition = 0 "
        f"and kafka_offset >= {start} and kafka_offset < {end}"
    )
    rows, with_field = cur.fetchone()
    return int(rows), int(with_field)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("table")
    parser.add_argument("--dropped-field", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = load_env()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="bronze-replay"
    ).cursor()

    start, end, events = damaged_range(env, cur, args.table, args.dropped_field)
    rows, kept = count_rows(cur, args.table, args.dropped_field, start, end)
    print(f"damaged range: pg.public.{args.table} offsets [{start}, {end}): {events} events "
          f"carry {args.dropped_field} in Kafka; bronze has {rows} rows there, "
          f"{kept} with it")  # fmt: skip
    if events == 0 or args.dry_run:
        return 0

    sql = replay_sql(args.table, load_columns(dsn_from_env())[args.table], start, end)
    REPAIR_DIR.mkdir(parents=True, exist_ok=True)
    (REPAIR_DIR / f"replay_{args.table}.sql").write_text(sql)

    sh("docker compose stop flink-jobs")
    stopped = sh("scripts/flink-savepoint-stop.sh bronze-cdc")
    savepoint = stopped.stdout.strip() if stopped.returncode == 0 else ""
    if stopped.returncode == 1:
        sh("docker compose up -d --no-deps flink-jobs")
        sys.exit(f"could not pause bronze-cdc; nothing was changed\n{stopped.stderr}")
    print(f"paused bronze-cdc: {savepoint or 'it was not running'}")
    ok = False
    try:
        cur.execute(
            f"delete from lake.bronze.cdc_{args.table} where kafka_partition = 0 "
            f"and kafka_offset >= {start} and kafka_offset < {end}"
        )
        cur.fetchall()
        left = count_rows(cur, args.table, args.dropped_field, start, end)[0]
        print(f"deleted the damaged rows: {left} left")
        run = sh(
            "docker compose exec -T flink-jobmanager timeout 600 ./bin/sql-client.sh "
            f"-f /opt/flink/sql/repair/replay_{args.table}.sql"
        )
        if run.returncode != 0 or "[ERROR]" in run.stdout:
            print(run.stdout[-2000:], run.stderr[-2000:], file=sys.stderr)
            sys.exit("replay failed; rerun this command (it deletes the range again first)")
        rows, kept = count_rows(cur, args.table, args.dropped_field, start, end)
        print(f"replayed: {rows} rows in [{start}, {end}), {kept} with {args.dropped_field}")
        ok = rows == end - start - tombstones(env, args.table, start, end) and kept == events
    finally:
        resume = sh(
            f"docker compose run --rm --no-deps -e RESTORE_PATH='{savepoint}' "
            "-e ONLY_JOB=bronze-cdc flink-jobs bash /submit-flink-jobs.sh"
        )
        state = "RUNNING" if "RUNNING" in resume.stdout else resume.stdout[-500:]
        print(f"resumed bronze-cdc: {state}")
        sh("docker compose up -d --no-deps flink-jobs")
    print("bronze repaired" if ok else "REPAIR INCOMPLETE")
    return 0 if ok else 1


def tombstones(env: dict[str, str], table: str, start: int, end: int) -> int:
    """Tombstones in the range: bronze never stores them."""
    consumer, deserialize = kafka_clients(env)
    pk = {"transactions": "transaction_id"}.get(table, f"{table[:-1]}_id")
    n = sum(
        1
        for partition, offset, _, value in read_records(
            consumer, deserialize, f"pg.public.{table}", pk
        )
        if partition == 0 and start <= offset < end and value is None
    )
    consumer.close()
    return n


if __name__ == "__main__":
    sys.exit(main())
