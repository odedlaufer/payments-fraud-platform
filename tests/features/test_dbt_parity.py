"""Parity models (FR-PAR2, FR-PAR3) on DuckDB with handcrafted decisions, one per rule."""

import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from features.dbt import ParityModel
from features.model import load

DEFS = load()
DBT = Path(__file__).resolve().parents[2] / "dbt"
DAY = datetime(2026, 1, 3, tzinfo=UTC)
NAMES = [f.name for f in DEFS.features]
AVRO_TYPE = {"bigint": "bigint", "boolean": "boolean", "double": "double"}  # decimals: varchar


def render(sql: str) -> str:
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*source\('(\w+)',\s*'(\w+)'\)\s*\}\}", r"\1__\2", sql)
    sql = re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"\1", sql)
    return re.sub(r"\{\{\s*var\(\"\w+\",\s*([\d.]+)\)\s*\}\}", r"\1", sql)


def offline(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "txn_count_1m": 1,
        "txn_count_1h": 3,
        "txn_count_24h": 5,
        "txn_amount_sum_1h": Decimal("12.50"),
        "txn_amount_sum_24h": Decimal("80.25"),
        "distinct_merchants_1h": 2,
        "amount_vs_avg_30d": 1.25,
        "km_from_last_txn": None,
        "minutes_since_last_txn": None,
        "new_device_24h": None,
        "profile_change_24h": False,
        "merchant_refund_rate_7d": 0.01,
    }
    row.update(overrides)
    return row


def logged(row: dict[str, Any]) -> dict[str, Any]:
    """What a decision logs: decimals as exact strings."""
    return {
        k: (None if v is None else str(v)) if isinstance(v, Decimal) else v for k, v in row.items()
    }


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("set timezone = 'UTC'")
    types = {f.name: (AVRO_TYPE.get(f.dtype, "varchar")) for f in DEFS.features}
    struct = "struct(" + ", ".join(f"{n} {t}" for n, t in types.items()) + ")"
    c.execute(
        "create table decision_log__decisions (transaction_id bigint, decision varchar, "
        f"reason varchar, definitions_version integer, features {struct}, "
        "decided_at timestamptz, kafka_offset bigint)"
    )
    cols = ", ".join(f"{f.name} {f.dtype}" for f in DEFS.features)
    c.execute(
        "create table card_offline (transaction_id bigint, card_id bigint, merchant_id bigint, "
        f"event_ms bigint, {cols}, definitions_version integer)"
    )
    c.execute(
        "create table transactions_current "
        "(transaction_id bigint, event_ts timestamptz, is_deleted boolean)"
    )
    c.execute(
        "create table stg_cdc__transactions "
        "(transaction_id bigint, _op varchar, _kafka_offset bigint)"
    )

    # txn: (card, merchant, event offset ms, arrival, offline features, logged features,
    #       reason, definitions version)
    base = offline()
    null_snapshot = dict.fromkeys(NAMES)
    cases: dict[int, tuple[int, int, int, int, dict[str, Any], dict[str, Any], str, int]] = {
        1: (
            10,
            100,
            0,
            1,
            base,
            logged({**base, "amount_vs_avg_30d": 1.25 * (1 + 1e-9)}),
            "no_rule_fired",
            1,
        ),
        2: (
            11,
            101,
            60_000,
            2,
            offline(txn_count_1h=4),
            logged(offline(txn_count_1h=3)),
            "no_rule_fired",
            1,
        ),
        3: (12, 102, 0, 3, base, null_snapshot, "stale_features", 1),
        4: (
            13,
            103,
            0,
            4,
            base,
            logged({**base, "new_device_24h": None, "profile_change_24h": None}),
            "no_rule_fired",
            1,
        ),
        5: (
            11,
            101,
            30_000,
            9,
            base,
            logged(base),
            "no_rule_fired",
            1,
        ),  # earlier event, arrived after txn 2
        6: (
            14,
            104,
            0,
            6,
            offline(km_from_last_txn=100.0),
            logged(offline(km_from_last_txn=100.01)),
            "no_rule_fired",
            1,
        ),
        7: (
            15,
            105,
            0,
            7,
            offline(txn_count_24h=6),
            logged(offline(txn_count_24h=5)),
            "no_rule_fired",
            0,
        ),
        8: (16, 106, 0, 8, offline(minutes_since_last_txn=4.5), logged(base), "no_rule_fired", 1),
    }
    t0 = int(DAY.timestamp() * 1000)
    for txn, (card, merchant, dt, arrival, off, log, reason, version) in cases.items():
        c.execute(
            "insert into card_offline values (?, ?, ?, ?, "
            + ", ".join("?" for _ in NAMES)
            + ", 1)",
            [txn, card, merchant, t0 + dt, *[off[n] for n in NAMES]],
        )
        c.execute("insert into transactions_current values (?, ?, false)", [txn, DAY])
        c.execute("insert into stg_cdc__transactions values (?, 'c', ?)", [txn, arrival])
        c.execute(
            "insert into decision_log__decisions values (?, ?, ?, ?, ?, ?, ?)",
            [
                txn,
                "approve" if reason != "stale_features" else "review",
                reason,
                version,
                log,
                DAY,
                100 + txn,
            ],
        )
    # a stale duplicate of decision 1 at an earlier offset: the latest one must win
    c.execute(
        "insert into decision_log__decisions values (1, 'review', 'stale_features', 1, ?, ?, 1)",
        [null_snapshot, DAY],
    )
    c.execute("create table parity_joined as " + render(ParityModel(DEFS).joined_sql()))
    c.execute("create table parity_comparisons as " + render(ParityModel(DEFS).sql()))
    c.execute(
        "create table parity_daily as "
        + render((DBT / "models/marts/parity_daily.sql").read_text())
    )
    c.execute(
        "create table parity_mismatches as "
        + render((DBT / "models/ops/parity_mismatches.sql").read_text())
    )
    return c


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return con.execute(sql).fetchall()


def test_one_row_per_decision_and_feature_latest_decision_wins(
    con: duckdb.DuckDBPyConnection,
) -> None:
    assert rows(con, "select count(*), count(distinct transaction_id) from parity_comparisons") == [
        (8 * len(NAMES), 8)
    ]
    assert rows(con, "select distinct reason from parity_comparisons where transaction_id = 1") == [
        ("no_rule_fired",)
    ]


def test_matches_follow_the_type_rules(con: duckdb.DuckDBPyConnection) -> None:
    mismatched = rows(
        con,
        "select transaction_id, feature from parity_comparisons "
        "where compared and not matches order by 1, 2",
    )
    assert mismatched == [
        (2, "txn_count_1h"),  # a count off by one
        (6, "km_from_last_txn"),  # 1e-4 relative: outside tolerance
        (7, "txn_count_24h"),
        (8, "minutes_since_last_txn"),  # logged null, offline 4.5
    ]  # txn 1: decimal strings equal, double within 1e-6 relative, nulls equal


def test_stale_and_not_yet_logged_groups_are_not_compared(con: duckdb.DuckDBPyConnection) -> None:
    assert rows(
        con, "select bool_or(compared) from parity_comparisons where transaction_id = 3"
    ) == [(False,)]
    by_entity = dict(
        rows(
            con,
            "select entity, bool_or(compared) from parity_comparisons "
            "where transaction_id = 4 group by 1",
        )
    )
    assert by_entity == {"card": True, "customer": False, "merchant": True}


def test_daily_parity_per_feature_and_per_decision(con: duckdb.DuckDBPyConnection) -> None:
    (all_features,) = rows(
        con,
        "select compared, matching, not_compared, match_rate from parity_daily "
        "where feature = '(all features)'",
    )
    assert all_features == (7, 3, 1, pytest.approx(3 / 7))  # 3, 1, 4, 5 match; 2, 6, 7, 8 do not
    (count_1h,) = rows(
        con, "select compared, matching from parity_daily where feature = 'txn_count_1h'"
    )
    assert count_1h == (7, 6)


def test_mismatch_causes(con: duckdb.DuckDBPyConnection) -> None:
    causes = dict(rows(con, "select transaction_id, cause from parity_mismatches"))
    assert causes == {2: "late_event", 6: "unexplained", 7: "definition_drift", 8: "unexplained"}


def test_slo_test_fires_below_99_9(con: duckdb.DuckDBPyConnection) -> None:
    sql = render((DBT / "tests/assert_parity_meets_slo.sql").read_text())
    assert len(rows(con, sql)) == 1


def test_generated_parity_models_are_up_to_date() -> None:
    model = ParityModel(DEFS)
    assert (DBT / "models/marts/parity_joined.sql").read_text() == model.joined_sql()
    assert (DBT / "models/marts/parity_comparisons.sql").read_text() == model.sql()


def test_the_join_runs_once_not_once_per_feature() -> None:
    """Each comparison branch scans parity_joined; none re-runs the join (it once took Trino
    down: twelve concurrent three-way joins over every decision)."""
    sql = ParityModel(DEFS).sql()
    assert sql.count("ref('parity_joined')") == len(DEFS.features)
    assert "ref('card_offline')" not in sql and "source(" not in sql


def test_matching_booleans_differ_by_zero_even_when_both_are_null(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A card-present purchase has no device: new_device_24h is null on both sides, which is
    a match, and its difference must be 0, not 1."""
    booleans = [f.name for f in DEFS.features if f.dtype == "boolean"]
    names = ", ".join(f"'{n}'" for n in booleans)
    sql = f"select max(abs_diff) from parity_comparisons where feature in ({names}) and matches"
    assert rows(con, sql) == [(0.0,)]
