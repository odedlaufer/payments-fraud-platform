"""OLTP schema and logical replication setup (FR-CDC1, FR-CDC3, PRD section 3)."""

from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[tuple[object, ...]]

ROOT = Path(__file__).resolve().parents[2]
DOMAIN_TABLES = ["customers", "cards", "merchants", "sessions", "transactions", "chargebacks"]
PUBLISHED_TABLES = sorted([*DOMAIN_TABLES, "cdc_heartbeat"])
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def scalar(conn: Conn, sql: str, params: tuple[object, ...] = ()) -> object:
    row = conn.execute(sql, params).fetchone()
    assert row is not None
    return row[0]


def test_wal_level_is_logical(oltp: Conn) -> None:
    assert scalar(oltp, "show wal_level") == "logical"


def test_slot_wal_retention_is_capped(oltp: Conn) -> None:
    assert scalar(oltp, "show max_slot_wal_keep_size") == "1GB"


def test_all_migrations_applied(oltp: Conn) -> None:
    files = sorted(p.name.split("_", 1)[0] for p in (ROOT / "postgres/migrations").glob("*.sql"))
    rows = oltp.execute("select version from schema_migrations order by version").fetchall()
    assert [r[0] for r in rows] == files


def test_publication_covers_exactly_domain_tables_and_heartbeat(oltp: Conn) -> None:
    rows = oltp.execute(
        "select schemaname, tablename from pg_publication_tables "
        "where pubname = 'payments_cdc' order by tablename"
    ).fetchall()
    assert rows == [("public", t) for t in PUBLISHED_TABLES]


def test_sim_truth_exists_and_is_never_published(oltp: Conn) -> None:
    assert scalar(oltp, "select to_regclass('sim.sim_truth') is not null") is True
    published = scalar(oltp, "select count(*) from pg_publication_tables where schemaname = 'sim'")
    assert published == 0


@pytest.mark.parametrize("table", DOMAIN_TABLES)
def test_replica_identity_full(oltp: Conn, table: str) -> None:
    identity = scalar(oltp, "select relreplident from pg_class where oid = %s::regclass", (table,))
    assert identity == "f"


def test_heartbeat_has_single_row(oltp: Conn) -> None:
    assert scalar(oltp, "select count(*) from public.cdc_heartbeat") == 1


def _customer(conn: Conn) -> object:
    return scalar(
        conn,
        "insert into customers (full_name, email, phone, address_line, city, country_code, "
        "home_lat, home_lon, event_ts) values ('A B', 'a@b.c', '1', 'x', 'y', 'IL', 32.0, 34.8, "
        "%s) returning customer_id",
        (NOW,),
    )


def test_updated_at_moves_on_update(oltp: Conn) -> None:
    customer_id = _customer(oltp)
    oltp.commit()  # new transaction so now() differs
    try:
        oltp.execute(
            "update customers set risk_tier = 'high' where customer_id = %s", (customer_id,)
        )
        changed = scalar(
            oltp,
            "select updated_at > created_at from customers where customer_id = %s",
            (customer_id,),
        )
        assert changed is True
    finally:
        oltp.rollback()
        oltp.execute("delete from customers where customer_id = %s", (customer_id,))
        oltp.commit()


@pytest.mark.parametrize(
    ("sql", "constraint"),
    [
        ("update customers set risk_tier = 'extreme'", "customers_risk_tier_valid"),
        (
            "insert into transactions (card_id, merchant_id, amount, currency, channel, event_ts) "
            "values (1, 1, 0, 'USD', 'online', now())",
            "transactions_amount_check",
        ),
        (
            "insert into transactions (card_id, merchant_id, amount, currency, channel, event_ts) "
            "values (1, 1, 5, 'USD', 'card_present', now())",
            "transactions_card_present_has_location",
        ),
    ],
)
def test_check_constraints_reject_bad_rows(oltp: Conn, sql: str, constraint: str) -> None:
    _customer(oltp)
    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        oltp.execute(sql)
    assert exc.value.diag.constraint_name == constraint


def test_card_requires_existing_customer(oltp: Conn) -> None:
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        oltp.execute(
            "insert into cards (customer_id, pan_last4, card_type, credit_limit, expires_on, "
            "event_ts) values (-1, '1234', 'credit', 100, '2030-01-01', %s)",
            (NOW,),
        )


def test_merchant_can_be_deleted_while_transactions_reference_it(oltp: Conn) -> None:
    """Merchants are hard-deleted (FR-SIM5); their transactions must remain."""
    customer_id = _customer(oltp)
    card_id = scalar(
        oltp,
        "insert into cards (customer_id, pan_last4, card_type, credit_limit, expires_on, "
        "event_ts) values (%s, '1234', 'credit', 100, '2030-01-01', %s) returning card_id",
        (customer_id, NOW),
    )
    merchant_id = scalar(
        oltp,
        "insert into merchants (name, mcc, category, country_code, lat, lon, event_ts) "
        "values ('M', '5411', 'grocery', 'IL', 32.0, 34.8, %s) returning merchant_id",
        (NOW,),
    )
    oltp.execute(
        "insert into transactions (card_id, merchant_id, amount, currency, channel, event_ts) "
        "values (%s, %s, 10, 'USD', 'online', %s)",
        (card_id, merchant_id, NOW),
    )
    oltp.execute("delete from merchants where merchant_id = %s", (merchant_id,))
    remaining = scalar(
        oltp, "select count(*) from transactions where merchant_id = %s", (merchant_id,)
    )
    assert remaining == 1


@pytest.mark.parametrize(
    ("is_fraud", "pattern", "valid"),
    [
        (False, None, True),
        (True, "card_testing", True),
        (False, "friendly_fraud", True),
        (True, None, False),
        (False, "card_testing", False),
        (True, "friendly_fraud", False),
    ],
)
def test_sim_truth_label_matches_pattern(
    oltp: Conn, is_fraud: bool, pattern: str | None, valid: bool
) -> None:
    customer_id = _customer(oltp)
    card_id = scalar(
        oltp,
        "insert into cards (customer_id, pan_last4, card_type, credit_limit, expires_on, "
        "event_ts) values (%s, '1234', 'credit', 100, '2030-01-01', %s) returning card_id",
        (customer_id, NOW),
    )
    txn_id = scalar(
        oltp,
        "insert into transactions (card_id, merchant_id, amount, currency, channel, event_ts) "
        "values (%s, 1, 10, 'USD', 'online', %s) returning transaction_id",
        (card_id, NOW),
    )
    insert = "insert into sim.sim_truth (transaction_id, is_fraud, pattern) values (%s, %s, %s)"
    if valid:
        oltp.execute(insert, (txn_id, is_fraud, pattern))
    else:
        with pytest.raises(psycopg.errors.CheckViolation):
            oltp.execute(insert, (txn_id, is_fraud, pattern))


def test_debezium_role_is_least_privilege(oltp: Conn, env: dict[str, str]) -> None:
    """FR-CDC2: replication + read on published tables; no sim schema, no writes."""
    attrs = oltp.execute(
        "select rolreplication, rolcanlogin, rolsuper from pg_roles where rolname = 'debezium'"
    ).fetchone()
    assert attrs == (True, True, False)

    dsn = (
        f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
        f"user=debezium password={env['DEBEZIUM_DB_PASSWORD']}"
    )
    with psycopg.connect(dsn, connect_timeout=3) as dbz:
        assert dbz.execute("select count(*) >= 0 from public.transactions").fetchone() == (True,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            dbz.execute("select count(*) from sim.sim_truth")
        dbz.rollback()
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            dbz.execute("delete from public.customers")
