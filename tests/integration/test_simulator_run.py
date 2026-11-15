"""The engine against real Postgres: every write passes the schema constraints."""

from datetime import timedelta

import psycopg
import pytest

from simulator.cli import DEFAULT_SIM_START
from simulator.engine import Engine
from simulator.rates import Rates
from simulator.runner import StopAt, run
from simulator.store import PostgresStore

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[tuple[object, ...]]


def count(conn: Conn, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    assert row is not None
    return int(str(row[0]))


def test_one_simulated_hour_writes_valid_rows(oltp: Conn) -> None:
    """Rolled back afterwards; requires seeded reference data (`make seed`)."""
    store = PostgresStore(oltp)
    world = store.load_world()
    if not world.customers:
        pytest.skip("no reference data: run `make seed`")
    tx_before = count(oltp, "select count(*) from transactions")
    start = (world.last_event_ts or DEFAULT_SIM_START) + timedelta(seconds=1)
    engine = Engine(
        world,
        store,
        seed=7,
        tx_per_sim_second=20 / 144,
        rates=Rates().scaled(50).with_fraud(0),
        start=start,
    )

    run(
        engine,
        store,
        commit=lambda: None,
        stop=StopAt(sim_until=start + timedelta(hours=1)),
        clock=None,
        fast_step=timedelta(seconds=10),
    )

    inserted = store.ops["transactions", "insert"]
    assert inserted == pytest.approx(500, rel=0.02)
    assert count(oltp, "select count(*) from transactions") - tx_before == inserted
    assert count(oltp, "select count(*) from transactions where status = 'pending'") == 0
    assert store.ops["customers", "update"] > 0
    assert store.ops["merchants", "delete"] > 0


def test_ground_truth_is_written_to_sim_only(oltp: psycopg.Connection[tuple[object, ...]]) -> None:
    """FR-SIM3: injected fraud is labelled in sim.sim_truth (never captured by CDC)."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from simulator.store import NewTransaction

    store = PostgresStore(oltp)
    world = store.load_world()
    card = next(iter(world.cards.values()))
    merchant = next(iter(world.merchants.values()))
    txn_id = store.insert_transaction(
        NewTransaction(
            card.card_id,
            merchant.merchant_id,
            None,
            Decimal("1.50"),
            "USD",
            "online",
            None,
            None,
            datetime(2026, 1, 1, tzinfo=UTC),
        )  # fmt: skip
    )
    store.insert_truth(txn_id, "card_testing")
    row = oltp.execute(
        "select is_fraud, pattern from sim.sim_truth where transaction_id = %s", (txn_id,)
    ).fetchone()
    assert row == (True, "card_testing")
