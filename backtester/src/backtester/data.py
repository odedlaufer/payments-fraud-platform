"""Training sets from the lake through DuckDB's Iceberg extension (FR-BT1).

DuckDB attaches the Lakekeeper REST catalog (no authentication in this local stack) and
reads data files from SeaweedFS's S3 API; a training set is read at the Iceberg snapshot it
was published at, so the same snapshot always yields the same rows.
"""

from typing import Any

import duckdb

WAREHOUSE_ALIAS = "lake"


def connect(env: dict[str, str]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # report and store times in UTC
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute(
        "CREATE SECRET lake_s3 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', "
        "USE_SSL false, REGION 'us-east-1')",
        [env["S3_ACCESS_KEY"], env["S3_SECRET_KEY"], f"localhost:{env.get('S3_API_PORT', '8333')}"],
    )
    endpoint = f"http://localhost:{env.get('LAKEKEEPER_PORT', '8181')}/catalog"
    warehouse = env.get("WAREHOUSE_NAME", "lakehouse")
    con.execute(
        f"ATTACH '{warehouse}' AS {WAREHOUSE_ALIAS} (TYPE iceberg, ENDPOINT '{endpoint}', "
        "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
    )
    return con


def published(
    con: duckdb.DuckDBPyConnection, registry: str = "lake.ops.training_sets"
) -> list[dict[str, Any]]:
    """Published training sets, newest first: snapshot_id, cutoff, rows, positives."""
    rows = con.execute(
        f"select snapshot_id, cutoff, rows, positives, published_at from {registry} "
        "order by published_at desc"
    ).fetchall()
    keys = ("snapshot_id", "cutoff", "rows", "positives", "published_at")
    return [dict(zip(keys, r, strict=True)) for r in rows]


def training_rows(
    con: duckdb.DuckDBPyConnection,
    source: str,
    cutoff: Any,
    columns: list[str],
    window: tuple[str | None, str | None] = (None, None),
) -> list[dict[str, Any]]:
    """Rows of one training set (one cutoff) from `source`, a table expression: in the lake,
    `lake.ml.training_set AT (VERSION => <snapshot_id>)`. window: event_ts bounds (dates)."""
    wanted = list(dict.fromkeys(["transaction_id", "event_ts", "label", "amount", *columns]))
    where = ["cutoff = ?"]
    params: list[Any] = [cutoff]
    if window[0]:
        where.append("event_ts >= cast(? as timestamptz)")
        params.append(window[0])
    if window[1]:
        where.append("event_ts < cast(? as timestamptz) + interval 1 day")
        params.append(window[1])
    cur = con.execute(
        f"select {', '.join(wanted)} from {source} where {' and '.join(where)} "
        "order by transaction_id",
        params,
    )
    return [dict(zip(wanted, r, strict=True)) for r in cur.fetchall()]
