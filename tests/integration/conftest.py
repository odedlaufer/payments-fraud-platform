"""Fixtures for tests that run against the live stack started by `make up`."""

import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[2]


def _env() -> dict[str, str]:
    """Values from .env, overridden by the process environment."""
    values: dict[str, str] = {}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    values.update(os.environ)
    return values


@pytest.fixture(scope="session")
def oltp_dsn() -> str:
    env = _env()
    return (
        f"host={env.get('POSTGRES_HOST', 'localhost')} "
        f"port={env.get('POSTGRES_PORT', '5432')} "
        f"dbname={env['POSTGRES_DB']} "
        f"user={env['POSTGRES_USER']} "
        f"password={env.get('POSTGRES_PASSWORD', '')}"
    )


@pytest.fixture
def oltp(oltp_dsn: str) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    """A connection whose work is always rolled back."""
    try:
        conn = psycopg.connect(oltp_dsn, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.fail(f"OLTP Postgres not reachable (run `make up`): {exc}")
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
