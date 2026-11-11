"""Where the engine's writes go: Postgres in production, memory in unit tests."""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import psycopg

from simulator.world import CardState, CustomerState, MerchantState, World

Conn = psycopg.Connection[tuple[object, ...]]


@dataclass(frozen=True)
class NewTransaction:
    card_id: int
    merchant_id: int
    session_id: int | None
    amount: Decimal
    currency: str
    channel: str
    lat: float | None
    lon: float | None
    event_ts: datetime


class Store(Protocol):
    """Every write is counted per (table, op) so a run can be reconciled against CDC."""

    ops: Counter[tuple[str, str]]

    def load_world(self) -> World: ...
    def insert_session(
        self, customer_id: int, device_id: str, ip: str, ip_country: str, at: datetime
    ) -> int: ...
    def insert_transaction(self, txn: NewTransaction) -> int: ...
    def update_transaction_status(self, transaction_id: int, status: str, at: datetime) -> None: ...
    def update_customer(
        self, customer_id: int, column: str, value: object, at: datetime
    ) -> None: ...
    def update_card(self, card_id: int, column: str, value: object, at: datetime) -> None: ...
    def update_merchant(
        self, merchant_id: int, changes: dict[str, object], at: datetime
    ) -> None: ...
    def delete_merchant(self, merchant_id: int) -> None: ...


CUSTOMER_COLUMNS = {"email", "phone", "address_line", "risk_tier"}
CARD_COLUMNS = {"status", "credit_limit"}
MERCHANT_COLUMNS = {"mcc", "category", "status"}


class PostgresStore:
    """Writes through one connection; the caller decides when to commit."""

    def __init__(self, conn: Conn) -> None:
        self.conn = conn
        self.ops: Counter[tuple[str, str]] = Counter()

    def load_world(self) -> World:
        c = self.conn
        customers = {
            int(str(r[0])): CustomerState(
                int(str(r[0])), str(r[1]), float(str(r[2])), float(str(r[3]))
            )
            for r in c.execute(
                "select customer_id, country_code, home_lat, home_lon from customers"
            ).fetchall()
        }
        for customer_id, device_id in c.execute(
            "select customer_id, device_id from sessions group by customer_id, device_id"
        ).fetchall():
            cust = customers.get(int(str(customer_id)))
            if cust is not None:
                cust.devices.append(str(device_id))
        cards = {
            int(str(r[0])): CardState(
                int(str(r[0])),
                int(str(r[1])),
                str(r[2]),
                str(r[3]),
                r[4],  # type: ignore[arg-type]
            )
            for r in c.execute(
                "select card_id, customer_id, card_type, status, expires_on from cards"
            ).fetchall()
        }
        merchants = {
            int(str(r[0])): MerchantState(
                int(str(r[0])), str(r[1]), str(r[2]), float(str(r[3])), float(str(r[4])), str(r[5])
            )
            for r in c.execute(
                "select merchant_id, mcc, country_code, lat, lon, status from merchants"
            ).fetchall()
        }
        row = c.execute(
            "select max(ts) from ("
            "select max(event_ts) as ts from customers "
            "union all select max(event_ts) from cards "
            "union all select max(event_ts) from merchants "
            "union all select max(event_ts) from sessions "
            "union all select max(event_ts) from transactions) t"
        ).fetchone()
        last = row[0] if row else None
        pending = [
            (int(str(r[0])), r[1])
            for r in c.execute(
                "select transaction_id, event_ts from transactions where status = 'pending'"
            ).fetchall()
        ]
        return World(
            customers=customers,
            cards=cards,
            merchants=merchants,
            last_event_ts=last if isinstance(last, datetime) else None,
            pending_transactions=pending,  # type: ignore[arg-type]
        )

    def _returning_id(self, sql: str, params: tuple[object, ...]) -> int:
        row = self.conn.execute(sql, params).fetchone()
        assert row is not None
        return int(str(row[0]))

    def insert_session(
        self, customer_id: int, device_id: str, ip: str, ip_country: str, at: datetime
    ) -> int:
        self.ops["sessions", "insert"] += 1
        return self._returning_id(
            "insert into sessions (customer_id, device_id, ip_address, ip_country, event_ts) "
            "values (%s, %s, %s, %s, %s) returning session_id",
            (customer_id, device_id, ip, ip_country, at),
        )

    def insert_transaction(self, txn: NewTransaction) -> int:
        self.ops["transactions", "insert"] += 1
        return self._returning_id(
            "insert into transactions (card_id, merchant_id, session_id, amount, currency, "
            "channel, lat, lon, event_ts) values (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "returning transaction_id",
            (
                txn.card_id,
                txn.merchant_id,
                txn.session_id,
                txn.amount,
                txn.currency,
                txn.channel,
                txn.lat,
                txn.lon,
                txn.event_ts,
            ),
        )

    def update_transaction_status(self, transaction_id: int, status: str, at: datetime) -> None:
        self.ops["transactions", "update"] += 1
        # event_ts is the authorization time and never changes: feature windows key on it.
        # When the status changed is visible in updated_at and the CDC commit timestamp.
        self.conn.execute(
            "update transactions set status = %s where transaction_id = %s",
            (status, transaction_id),
        )

    def update_customer(self, customer_id: int, column: str, value: object, at: datetime) -> None:
        assert column in CUSTOMER_COLUMNS
        self.ops["customers", "update"] += 1
        self.conn.execute(
            f"update customers set {column} = %s, event_ts = %s where customer_id = %s",
            (value, at, customer_id),
        )

    def update_card(self, card_id: int, column: str, value: object, at: datetime) -> None:
        assert column in CARD_COLUMNS
        self.ops["cards", "update"] += 1
        self.conn.execute(
            f"update cards set {column} = %s, event_ts = %s where card_id = %s",
            (value, at, card_id),
        )

    def update_merchant(self, merchant_id: int, changes: dict[str, object], at: datetime) -> None:
        assert set(changes) <= MERCHANT_COLUMNS
        self.ops["merchants", "update"] += 1
        assignments = ", ".join(f"{k} = %s" for k in changes)
        self.conn.execute(
            f"update merchants set {assignments}, event_ts = %s where merchant_id = %s",
            (*changes.values(), at, merchant_id),
        )

    def delete_merchant(self, merchant_id: int) -> None:
        self.ops["merchants", "delete"] += 1
        self.conn.execute("delete from merchants where merchant_id = %s", (merchant_id,))


class MemoryStore:
    """Records writes in memory for unit tests; assigns ids like identity columns."""

    def __init__(self, world: World) -> None:
        self.world = world
        self.ops: Counter[tuple[str, str]] = Counter()
        self.transactions: dict[int, dict[str, object]] = {}
        self.sessions: dict[int, dict[str, object]] = {}
        self.log: list[tuple[str, str, int, object]] = []
        self._next = {"transactions": 1, "sessions": 1}

    def _id(self, table: str) -> int:
        value = self._next[table]
        self._next[table] += 1
        return value

    def load_world(self) -> World:
        return self.world

    def insert_session(
        self, customer_id: int, device_id: str, ip: str, ip_country: str, at: datetime
    ) -> int:
        self.ops["sessions", "insert"] += 1
        sid = self._id("sessions")
        self.sessions[sid] = {
            "customer_id": customer_id,
            "device_id": device_id,
            "ip_country": ip_country,
            "event_ts": at,
        }
        return sid

    def insert_transaction(self, txn: NewTransaction) -> int:
        self.ops["transactions", "insert"] += 1
        tid = self._id("transactions")
        self.transactions[tid] = {**txn.__dict__, "status": "pending", "history": ["pending"]}
        self.log.append(("transactions", "insert", tid, txn.event_ts))
        return tid

    def update_transaction_status(self, transaction_id: int, status: str, at: datetime) -> None:
        self.ops["transactions", "update"] += 1
        row = self.transactions[transaction_id]
        row["status"] = status
        history = row["history"]
        assert isinstance(history, list)
        history.append((status, at))
        self.log.append(("transactions", status, transaction_id, at))

    def update_customer(self, customer_id: int, column: str, value: object, at: datetime) -> None:
        assert column in CUSTOMER_COLUMNS
        self.ops["customers", "update"] += 1
        self.log.append(("customers", column, customer_id, value))

    def update_card(self, card_id: int, column: str, value: object, at: datetime) -> None:
        assert column in CARD_COLUMNS
        self.ops["cards", "update"] += 1
        self.log.append(("cards", column, card_id, value))

    def update_merchant(self, merchant_id: int, changes: dict[str, object], at: datetime) -> None:
        assert set(changes) <= MERCHANT_COLUMNS
        self.ops["merchants", "update"] += 1
        self.log.append(("merchants", "update", merchant_id, changes))

    def delete_merchant(self, merchant_id: int) -> None:
        self.ops["merchants", "delete"] += 1
        self.log.append(("merchants", "delete", merchant_id, None))
