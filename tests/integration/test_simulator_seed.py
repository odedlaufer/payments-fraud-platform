"""Simulator reference data lands in Postgres with valid relationships (FR-SIM1)."""

from datetime import UTC, datetime

import psycopg
import pytest

from simulator.db import insert_population
from simulator.entities import generate_population

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[tuple[object, ...]]
START = datetime(2026, 1, 1, tzinfo=UTC)


def test_population_inserts_with_valid_foreign_keys(oltp: Conn) -> None:
    """Runs inside the rolled-back test transaction, so nothing is left behind."""
    population = generate_population(seed=99, customers=200, merchants=40, at=START)
    before = oltp.execute("select count(*) from customers").fetchone()
    assert before is not None

    counts = insert_population(oltp, population)

    assert counts == {
        "customers": 200,
        "cards": len(population.cards),
        "merchants": 40,
    }
    after = oltp.execute("select count(*) from customers").fetchone()
    assert after is not None
    assert int(str(after[0])) - int(str(before[0])) == 200

    # Every new card belongs to one of the new customers, matching the generator.
    rows = oltp.execute(
        "select c.email, count(k.card_id) from customers c join cards k using (customer_id) "
        "where c.event_ts = %s and c.email = any(%s) group by c.email",
        (START, [c.email for c in population.customers]),
    ).fetchall()
    assert len(rows) == 200
    assert sum(int(str(n)) for _, n in rows) == len(population.cards)
