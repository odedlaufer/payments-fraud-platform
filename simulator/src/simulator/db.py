"""Postgres access for the simulator."""

import os
from dataclasses import astuple
from pathlib import Path

import psycopg

from simulator.entities import Population

Conn = psycopg.Connection[tuple[object, ...]]


def dsn_from_env(env_file: Path = Path(".env")) -> str:
    """Builds a DSN from POSTGRES_* in the environment, falling back to .env."""
    values: dict[str, str] = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    values.update(os.environ)
    return (
        f"host={values.get('POSTGRES_HOST', 'localhost')} "
        f"port={values.get('POSTGRES_PORT', '5432')} "
        f"dbname={values['POSTGRES_DB']} "
        f"user={values['POSTGRES_USER']} "
        f"password={values.get('POSTGRES_PASSWORD', '')}"
    )


def count_customers(conn: Conn) -> int:
    row = conn.execute("select count(*) from customers").fetchone()
    assert row is not None
    return int(str(row[0]))


def insert_population(conn: Conn, population: Population) -> dict[str, int]:
    """Inserts customers, their cards and merchants. Caller owns the transaction."""
    with conn.cursor() as cur:
        cur.executemany(
            "insert into customers (full_name, email, phone, address_line, city, country_code, "
            "home_lat, home_lon, risk_tier, event_ts) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning customer_id",
            [astuple(c) for c in population.customers],
            returning=True,
        )
        customer_ids: list[object] = []
        while True:
            row = cur.fetchone()
            assert row is not None
            customer_ids.append(row[0])
            if not cur.nextset():
                break

        cur.executemany(
            "insert into cards (customer_id, pan_last4, card_type, status, credit_limit, "
            "expires_on, event_ts) values (%s, %s, %s, %s, %s, %s, %s)",
            [(customer_ids[c.customer_index], *astuple(c)[1:]) for c in population.cards],
        )
        cur.executemany(
            "insert into merchants (name, mcc, category, country_code, lat, lon, status, "
            "event_ts) values (%s, %s, %s, %s, %s, %s, %s, %s)",
            [astuple(m) for m in population.merchants],
        )
    return {
        "customers": len(population.customers),
        "cards": len(population.cards),
        "merchants": len(population.merchants),
    }
