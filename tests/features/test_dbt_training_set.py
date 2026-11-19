"""Training set (FR-PIT2, FR-PIT3) on DuckDB: point-in-time rows and labels for a cutoff, and
leakage tests that catch each kind of leak."""

import re
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import jinja2
import pytest

DBT = Path(__file__).resolve().parents[2] / "dbt"
CUTOFF = "2026-03-30 00:00:00+00:00"


def ts(month: int, day: int) -> datetime:
    return datetime(2026, month, day, tzinfo=UTC)


def render(path: Path, sql_override: str | None = None) -> str:
    env = jinja2.Environment()
    template = env.from_string(sql_override if sql_override is not None else path.read_text())
    sql = template.render(
        ref=lambda name: name,
        var=lambda name, default=None: {"training_cutoff": CUTOFF}.get(name, default),
        epoch_ms=lambda column: f"epoch_ms({column})",
    )
    return re.sub(r"timestamp\s*\(6\)\s*with time zone", "timestamptz", sql)


# transaction: event, chargeback filed (or None), card dimension valid_from
TXNS: dict[int, tuple[datetime, datetime | None, datetime]] = {
    1: (ts(1, 10), ts(2, 1), ts(1, 1)),  # A: mature, chargeback before the cutoff
    2: (ts(1, 20), ts(4, 5), ts(1, 1)),  # B: mature, chargeback only after the cutoff
    3: (ts(3, 1), None, ts(1, 1)),  # C: not mature at the cutoff
    4: (ts(1, 25), None, ts(1, 1)),  # D: mature, never disputed
    5: (ts(1, 2), None, ts(1, 1)),  # E: before label coverage (first label known: A, Jan 10)
    6: (ts(1, 5), ts(4, 2), ts(1, 1)),  # F: labelled, but only after the cutoff: no coverage yet
}


def build(
    con: duckdb.DuckDBPyConnection, txns: dict[int, tuple[datetime, datetime | None, datetime]]
) -> None:
    for t in ("fct_labels", "card_offline", "fct_transactions", "training_set"):
        con.execute(f"drop table if exists {t}")
    con.execute(
        "create table fct_labels (transaction_id bigint, event_ts timestamptz, label integer, "
        "label_known_at timestamptz, as_of_ts timestamptz)"
    )
    con.execute(
        "create table card_offline (transaction_id bigint, card_id bigint, merchant_id bigint, "
        "event_ms bigint, txn_count_24h bigint, definitions_version integer)"
    )
    cols = (
        "transaction_id bigint, amount decimal(12,2), currency varchar, channel varchar, "
        "device_risk_score decimal(5,4), card_type varchar, card_status_at_event varchar, "
        "card_credit_limit_at_event decimal(12,2), customer_risk_tier_at_event varchar, "
        "customer_country varchar, merchant_mcc_at_event varchar, "
        "merchant_category_at_event varchar, merchant_country varchar, "
        "merchant_status_at_event varchar, card_version_from timestamptz, "
        "customer_version_from timestamptz, merchant_version_from timestamptz, status varchar"
    )
    con.execute(f"create table fct_transactions ({cols})")
    now = ts(4, 10)
    for txn, (event, filed, version) in txns.items():
        con.execute(
            "insert into fct_labels values (?, ?, ?, ?, ?)",
            [txn, event, 1 if filed else 0, filed, now],
        )
        con.execute(
            "insert into card_offline values (?, 10, 100, ?, 3, 1)",
            [txn, int(event.timestamp() * 1000)],
        )
        con.execute(
            "insert into fct_transactions values (?, 10, 'USD', 'online', null, 'debit', 'active', "
            "500, 'low', 'IL', '5411', 'grocery', 'IL', 'active', ?, ?, ?, 'reversed')",
            [txn, version, version, version],
        )


def run(con: duckdb.DuckDBPyConnection, model_sql: str | None = None) -> dict[str, int]:
    model = DBT / "models/marts/training_set.sql"
    con.execute("create table training_set as " + render(model, model_sql))
    return {
        t: len(con.execute(render(DBT / f"tests/{t}.sql")).fetchall())
        for t in ("assert_training_set_has_no_leakage", "assert_training_labels_as_known_at_cutoff")
    }


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("set timezone = 'UTC'")
    build(c, TXNS)
    return c


def test_mature_rows_and_labels_as_known_at_the_cutoff(con: duckdb.DuckDBPyConnection) -> None:
    failures = run(con)
    rows = con.execute(
        "select transaction_id, label, label_known_at is not null from training_set order by 1"
    ).fetchall()
    assert rows == [(1, 1, True), (2, 0, False), (4, 0, False)]  # 3 is not mature yet
    # one row per transaction (an explicit cutoff once multiplied rows by the label count)
    assert con.execute(
        "select count(*) = count(distinct transaction_id) from training_set"
    ).fetchone() == (True,)
    assert failures == {
        "assert_training_set_has_no_leakage": 0,
        "assert_training_labels_as_known_at_cutoff": 0,
    }


def leak(old: str, new: str) -> str:
    sql = (DBT / "models/marts/training_set.sql").read_text()
    assert old in sql, old
    return sql.replace(old, new)


def test_a_label_filed_after_the_cutoff_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    sql = leak(
        "case when e.label = 1 and e.label_known_at <= e.cutoff then 1 else 0 end as label",
        "e.label as label",
    )
    sql = sql.replace(
        "case when e.label = 1 and e.label_known_at <= e.cutoff then e.label_known_at end",
        "e.label_known_at",
    )
    failures = run(con, sql)
    assert failures["assert_training_set_has_no_leakage"] > 0
    assert failures["assert_training_labels_as_known_at_cutoff"] > 0


def test_an_immature_transaction_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    failures = run(con, leak("k.cutoff - interval '{{ maturity }}' day", "k.cutoff"))
    assert failures["assert_training_set_has_no_leakage"] > 0


def test_a_dimension_version_from_after_the_event_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    build(con, {**TXNS, 1: (ts(1, 10), ts(2, 1), ts(1, 15))})  # version starts after the event
    assert run(con)["assert_training_set_has_no_leakage"] > 0


def test_the_current_status_is_never_a_training_column() -> None:
    """status (and when it changed) is set after the event, e.g. a reversal days later."""
    sql = (DBT / "models/marts/training_set.sql").read_text()
    code = "\n".join(line.split("--")[0] for line in sql.splitlines())
    assert not re.search(r"\bstatus\b(?!_at_event)", code.replace("status_at_event", "")), code


def test_default_cutoff_is_the_simulation_now(con: duckdb.DuckDBPyConnection) -> None:
    env = jinja2.Environment()
    sql = env.from_string((DBT / "models/marts/training_set.sql").read_text()).render(
        ref=lambda name: name, var=lambda name, default=None: default, epoch_ms=lambda c: c
    )
    sql = re.sub(r"timestamp\s*\(6\)\s*with time zone", "timestamptz", sql)
    con.execute("create table training_set as " + sql)
    (cutoff,) = con.execute("select distinct cutoff from training_set").fetchone()  # type: ignore[misc]
    assert cutoff == ts(4, 10)  # max(as_of_ts) of fct_labels


def test_a_cutoff_in_the_future_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    """With the simulation at April 10, a June cutoff admits transactions whose chargebacks
    have not happened yet: every row looks fine, the labels are silently incomplete."""
    global CUTOFF
    saved, CUTOFF = CUTOFF, "2026-06-01 00:00:00+00:00"
    try:
        assert run(con)["assert_training_set_has_no_leakage"] > 0
    finally:
        CUTOFF = saved


def test_rows_before_label_coverage_are_excluded(con: duckdb.DuckDBPyConnection) -> None:
    run(con)
    ids = [
        r[0] for r in con.execute("select transaction_id from training_set order by 1").fetchall()
    ]
    assert 5 not in ids and 6 not in ids  # coverage starts Jan 10 (A), known at the cutoff
    (coverage,) = con.execute("select distinct label_coverage_from from training_set").fetchone()  # type: ignore[misc]
    assert coverage == ts(1, 10)


def test_coverage_from_labels_known_only_later_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    """Choosing coverage with every label known today (F's April chargeback) lets the future
    decide the window: F enters the set."""
    sql = leak("and l.label_known_at <= k.cutoff\n", "\n")
    assert run(con, sql)["assert_training_set_has_no_leakage"] > 0


def test_no_coverage_filter_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    sql = leak("        and l.event_ts >= c.label_coverage_from\n", "")
    assert run(con, sql)["assert_training_set_has_no_leakage"] > 0
