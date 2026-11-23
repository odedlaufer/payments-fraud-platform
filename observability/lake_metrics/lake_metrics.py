"""Lakehouse metrics for Prometheus (OBS-1), served on port 9109; refreshed every minute.

- lake_table_last_commit_timestamp_seconds{table}: the latest Iceberg commit of each bronze
  table, read from its snapshots metadata table (no data files are scanned). Lakehouse
  freshness (PRD section 2, SLO < 5 min) is time() minus this.
- lake_parity_ratio / lake_parity_compared: online/offline feature parity over all
  decisions compared (marts.parity_daily, "(all features)"). SLO >= 99.9%.
- lake_metrics_errors{query}: failed queries; lake_metrics_last_success_timestamp_seconds.
"""

import os
import time
from typing import Any

import trino
from prometheus_client import Counter, Gauge, start_http_server

BRONZE = (
    "cdc_customers", "cdc_cards", "cdc_merchants", "cdc_sessions",
    "cdc_transactions", "cdc_chargebacks", "decisions",
)  # fmt: skip

LAST_COMMIT = Gauge(
    "lake_table_last_commit_timestamp_seconds", "Latest Iceberg commit of a bronze table", ["table"]
)
PARITY = Gauge("lake_parity_ratio", "Online/offline feature parity over all compared decisions")
COMPARED = Gauge("lake_parity_compared", "Decisions compared for parity")
ERRORS = Counter("lake_metrics_errors", "Failed lake queries", ["query"])
LAST_SUCCESS = Gauge("lake_metrics_last_success_timestamp_seconds", "Last complete refresh")


def one(cur: Any, sql: str) -> Any:
    cur.execute(sql)
    row = cur.fetchone()
    return None if row is None else row[0]


def refresh(cur: Any) -> bool:
    ok = True
    for table in BRONZE:
        try:
            ts = one(
                cur,
                f'select to_unixtime(max(committed_at)) from lake.bronze."{table}$snapshots"',
            )
            if ts is not None:
                LAST_COMMIT.labels(table=table).set(float(ts))
        except Exception:
            ERRORS.labels(query=f"commit:{table}").inc()
            ok = False
    try:
        cur.execute(
            "select sum(matching), sum(compared) from lake.marts.parity_daily "
            "where feature = '(all features)'"
        )
        matching, compared = cur.fetchone()
        if compared:
            PARITY.set(float(matching) / float(compared))
            COMPARED.set(float(compared))
    except Exception:
        ERRORS.labels(query="parity").inc()
        ok = False
    if ok:
        LAST_SUCCESS.set(time.time())
    return ok


def main() -> None:
    start_http_server(int(os.environ.get("METRICS_PORT", "9109")))
    host, port = os.environ.get("TRINO_HOST", "trino"), int(os.environ.get("TRINO_PORT", "8080"))
    interval = int(os.environ.get("REFRESH_SECONDS", "60"))
    while True:
        try:
            cur = trino.dbapi.connect(host=host, port=port, user="lake-metrics").cursor()  # type: ignore[no-untyped-call]
            refresh(cur)
        except Exception:
            ERRORS.labels(query="connect").inc()
        time.sleep(interval)


if __name__ == "__main__":
    main()
