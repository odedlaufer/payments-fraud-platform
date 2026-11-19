"""The dbt offline features model equals the reference implementation (ADR-003, FR-PAR1).

The generated SQL runs on DuckDB over a randomized lake (same-millisecond ties whose arrival
order differs from id order, sub-millisecond times, reversals, repeated devices, changes to
unwatched columns); every feature of every transaction must equal features.reference.
"""

import random
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import duckdb
import pytest

from features.dbt import OfflineModel
from features.model import load
from features.reference import (
    CustomerTxn,
    MerchantTxn,
    ProfileChange,
    Session,
    StatusChange,
    Txn,
    card_features,
    customer_features,
    merchant_features,
)
from features.timeparse import dt_to_ms

DEFS = load()
TXN_COLUMNS = {
    "transaction_id": "bigint",
    "card_id": "bigint",
    "merchant_id": "bigint",
    "session_id": "bigint",
    "amount": "decimal(12,2)",
    "channel": "varchar",
    "lat": "double",
    "lon": "double",
    "status": "varchar",
    "event_ts": "timestamptz",
    "status_event_ts": "timestamptz",
    "is_deleted": "boolean",
}
STG_TXN_COLUMNS = {"transaction_id": "bigint", "_op": "varchar", "_kafka_offset": "bigint"}
SESSION_COLUMNS = {
    "session_id": "bigint",
    "customer_id": "bigint",
    "device_id": "varchar",
    "event_ts": "timestamptz",
    "is_deleted": "boolean",
}
CARD_COLUMNS = {"card_id": "bigint", "customer_id": "bigint", "is_deleted": "boolean"}
CUSTOMER_COLUMNS = {
    "customer_id": "bigint",
    "_op": "varchar",
    "_kafka_offset": "bigint",
    "event_ts": "timestamptz",
    "email": "varchar",
    "phone": "varchar",
    "address_line": "varchar",
    "risk_tier": "varchar",
}


def table(
    con: duckdb.DuckDBPyConnection, name: str, columns: dict[str, str], rows: list[dict[str, Any]]
) -> None:
    """Creates a DuckDB table standing in for a lake model, and fills it."""
    con.execute(f"create table {name} ({', '.join(f'{c} {t}' for c, t in columns.items())})")
    marks = ", ".join("?" for _ in columns)
    con.executemany(f"insert into {name} values ({marks})", [[r[c] for c in columns] for r in rows])


T0 = datetime(2026, 1, 1, tzinfo=UTC)
WATCHED = ("email", "phone", "address_line")


def duck_sql() -> str:
    sql = OfflineModel(DEFS).sql()
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"\1", sql)
    return re.sub(r"\{\{\s*epoch_ms\('([\w.]+)'\)\s*\}\}", r"epoch_ms(\1)", sql)


def scenario(seed: int) -> tuple[duckdb.DuckDBPyConnection, dict[int, dict[str, object]]]:
    rng = random.Random(seed)
    cards = {c: c % 6 for c in range(1, 15)}  # card -> customer
    txns: list[dict[str, Any]] = []
    t = T0
    for i in range(1, 700):
        if rng.random() > 0.15:  # 15%: same instant as the previous one
            t += timedelta(
                seconds=rng.choice([1, 7, 45, 300, 3600, 5 * 3600]),
                microseconds=rng.randrange(1000),
            )
        online = rng.random() < 0.5
        txns.append(
            {
                "transaction_id": i,
                "card_id": rng.choice(list(cards)),
                "merchant_id": rng.randrange(1, 6),
                "session_id": rng.randrange(1, 40) if online and rng.random() < 0.8 else None,
                "amount": Decimal(rng.randrange(50, 90000)) / 100,
                "channel": "online" if online else "card_present",
                "lat": None if online else rng.uniform(-60, 60),
                "lon": None if online else rng.uniform(-170, 170),
                "event_ts": t,
                "status": "approved",
                "status_event_ts": t,
            }
        )
    for row in rng.sample(txns, 60):  # reversals, some on another creation's millisecond
        row["status"] = "reversed"
        row["status_event_ts"] = (
            rng.choice(txns)["event_ts"]
            if rng.random() < 0.3
            else row["event_ts"] + timedelta(hours=rng.uniform(0, 72))
        )
    arrival = list(range(len(txns)))
    for a in range(len(txns) - 1):  # same-instant transactions arrive in random order
        if txns[a]["event_ts"] == txns[a + 1]["event_ts"] and rng.random() < 0.5:
            arrival[a], arrival[a + 1] = arrival[a + 1], arrival[a]
    sessions: list[dict[str, Any]] = [
        {
            "session_id": s,
            "customer_id": c,
            "device_id": f"d{c}-{rng.randrange(3)}",
            "event_ts": T0 + timedelta(hours=rng.uniform(0, 300), microseconds=rng.randrange(1000)),
        }
        for s in range(1, 40)
        for c in [rng.randrange(6)]
    ]
    events: list[dict[str, Any]] = []
    for c in range(6):
        state = {"email": f"e{c}", "phone": f"p{c}", "address_line": f"a{c}", "risk_tier": "low"}
        events.append({"customer_id": c, "_op": "c", "event_ts": T0, **state})
        when = T0
        for _ in range(rng.randrange(2, 8)):
            when += timedelta(hours=rng.uniform(1, 80))
            col = rng.choice([*WATCHED, "risk_tier"])
            state = {**state, col: f"{col}-{rng.randrange(10**6)}"}
            events.append({"customer_id": c, "_op": "u", "event_ts": when, **state})
    events.sort(key=lambda e: e["event_ts"])
    for offset, e in enumerate(events):
        e["_kafka_offset"] = offset

    con = duckdb.connect()
    con.execute("set timezone = 'UTC'")
    table(con, "transactions_current", TXN_COLUMNS, [{**r, "is_deleted": False} for r in txns])
    creations = [
        {"transaction_id": r["transaction_id"], "_op": "c", "_kafka_offset": arrival[k] * 2}
        for k, r in enumerate(txns)
    ]
    updates = [
        {"transaction_id": r["transaction_id"], "_op": "u", "_kafka_offset": 10_000 + k}
        for k, r in enumerate(txns)
    ]
    table(con, "stg_cdc__transactions", STG_TXN_COLUMNS, creations + updates)
    table(con, "sessions_current", SESSION_COLUMNS, [{**x, "is_deleted": False} for x in sessions])
    owners = [{"card_id": c, "customer_id": u, "is_deleted": False} for c, u in cards.items()]
    table(con, "cards_current", CARD_COLUMNS, owners)
    table(con, "stg_cdc__customers", CUSTOMER_COLUMNS, events)

    by_arrival = [txns[k] for k in sorted(range(len(txns)), key=lambda k: arrival[k])]
    want = card_features(
        DEFS,
        [
            Txn(
                r["transaction_id"],
                r["card_id"],
                r["merchant_id"],
                r["amount"],
                r["channel"],
                r["lat"],
                r["lon"],
                dt_to_ms(r["event_ts"]),
            )
            for r in by_arrival
        ],
    )
    m_txns = [
        MerchantTxn(r["transaction_id"], r["merchant_id"], dt_to_ms(r["event_ts"])) for r in txns
    ]
    statuses = [
        StatusChange(
            r["transaction_id"], r["merchant_id"], r["status"], dt_to_ms(r["status_event_ts"])
        )
        for r in txns
    ]
    changes: list[ProfileChange] = []
    previous: dict[int, dict[str, Any]] = {}
    for e in events:
        prev = previous.get(e["customer_id"])
        if e["_op"] == "u" and prev is not None:
            changed = frozenset(c for c in (*WATCHED, "risk_tier") if e[c] != prev[c])
            changes.append(ProfileChange(e["customer_id"], changed, dt_to_ms(e["event_ts"])))
        previous[e["customer_id"]] = e
    c_txns = [
        CustomerTxn(r["transaction_id"], r["card_id"], r["session_id"], dt_to_ms(r["event_ts"]))
        for r in txns
    ]
    c_sessions = [
        Session(s["session_id"], s["customer_id"], s["device_id"], dt_to_ms(s["event_ts"]))
        for s in sessions
    ]
    for source in (
        merchant_features(DEFS, m_txns, statuses),
        customer_features(DEFS, c_txns, c_sessions, changes, cards),
    ):
        for txn_id, values in source.items():
            want[txn_id].update(values)
    return con, want


def close(a: object, b: object) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        return abs(float(a) - float(b)) <= 1e-9 * max(1.0, abs(float(b)))  # type: ignore[arg-type]
    return a == b


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_offline_model_equals_reference(seed: int) -> None:
    con, want = scenario(seed)
    names = [f.name for f in DEFS.features]
    rows = con.execute(f"select transaction_id, {', '.join(names)} from ({duck_sql()})").fetchall()
    assert len(rows) == len(want)
    bad = [
        (row[0], name, got, want[row[0]][name])
        for row in rows
        for name, got in zip(names, row[1:], strict=True)
        if not close(got, want[row[0]][name])
    ]
    assert not bad, bad[:5]
    # the scenario exercised the interesting cases
    values = list(want.values())
    assert any(v["km_from_last_txn"] is not None for v in values)
    assert any(v["merchant_refund_rate_7d"] for v in values)
    assert any(v["new_device_24h"] for v in values) and any(v["profile_change_24h"] for v in values)


def test_generated_model_is_up_to_date() -> None:
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "dbt/models/features/card_offline.sql"
    assert path.read_text() == OfflineModel(DEFS).sql(), "run `uv run features dbt`"


def test_no_division_by_a_decimal_literal() -> None:
    """Trino types 60000.0 as DECIMAL and rounds the quotient to the literal's scale, which
    DuckDB does not: every division in the model must be in floating point."""
    sql = OfflineModel(DEFS).sql()
    assert not re.search(r"/\s*\d+\.\d+", sql), re.findall(r"/\s*\d+\.\d+", sql)
