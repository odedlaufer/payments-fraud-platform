"""On-demand customer features == the reference (ADR-007), through a real Redis.

A randomized scenario goes through exactly what feature-sink does (CDC events, delivered
twice and shuffled), then customer_values() must equal features.reference on every
transaction. Uses Redis database 15.
"""

import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis

from decision_service.customer_features import Ask, customer_values
from decision_service.customer_sink import apply_cdc
from decision_service.online_store import OnlineStore
from features.model import load
from features.reference import CustomerTxn, ProfileChange, Session, customer_features

pytestmark = pytest.mark.integration
T0 = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def store(env: dict[str, str]) -> Iterator[OnlineStore]:
    client = redis.Redis(
        host="127.0.0.1", port=int(env.get("REDIS_PORT", "6379")), db=15, decode_responses=True
    )
    client.flushdb()
    yield OnlineStore(client, load())
    client.flushdb()


def iso(ms: int) -> str:
    return (T0 + timedelta(milliseconds=ms)).isoformat().replace("+00:00", "Z")


def test_on_demand_equals_reference(store: OnlineStore) -> None:
    rng = random.Random(3)
    defs = load()
    t0 = int(T0.timestamp() * 1000)
    owners = {card: card % 5 for card in range(1, 13)}  # 12 cards, 5 customers
    sessions, changes, txns, events = [], [], [], []
    for card, customer in owners.items():
        events.append(
            ("pg.public.cards", {"op": "c", "after": {"card_id": card, "customer_id": customer}})
        )
    for sid in range(1, 80):
        customer, ms = rng.randrange(5), t0 + rng.randrange(0, 5 * 86_400_000)
        device = f"dev-{customer}-{rng.randrange(3)}"
        sessions.append(Session(sid, customer, device, ms))
        after = {"session_id": sid, "customer_id": customer, "device_id": device,
                 "event_ts": iso(ms - t0)}  # fmt: skip
        events.append(("pg.public.sessions", {"op": "c", "after": after}))
    for _ in range(25):
        customer, ms = rng.randrange(5), t0 + rng.randrange(0, 5 * 86_400_000)
        column = rng.choice(["email", "phone", "address_line", "risk_tier"])
        before = {"customer_id": customer, "email": "e", "phone": "p", "address_line": "a",
                  "risk_tier": "low", "event_ts": iso(ms - t0 - 1)}  # fmt: skip
        after = {**before, column: "changed", "event_ts": iso(ms - t0)}
        changes.append(ProfileChange(customer, frozenset({column}), ms))
        events.append(("pg.public.customers", {"op": "u", "before": before, "after": after}))
    for txn_id in range(1, 300):
        if rng.random() < 0.5:
            s = rng.choice(sessions)
            card = rng.choice([c for c, o in owners.items() if o == s.customer_id] or [1])
            ms = s.event_ms + rng.randrange(0, 2 * 3_600_000)
            txns.append(CustomerTxn(txn_id, card, s.session_id, ms))
        else:
            txns.append(CustomerTxn(txn_id, rng.choice(list(owners)), None,
                                    t0 + rng.randrange(0, 5 * 86_400_000)))  # fmt: skip
    deliveries = events + events
    rng.shuffle(deliveries)
    for topic, event in deliveries:
        apply_cdc(defs, store, topic, event)

    asks = [Ask(t.transaction_id, t.card_id, t.session_id, t.event_ms) for t in txns]
    got = customer_values(defs, store, asks)
    want = customer_features(defs, txns, sessions, changes, owners)
    assert set(got) == set(want), "every transaction's context is ready"
    mismatches: list[tuple[int, Any, Any]] = [
        (i, got[i], want[i]) for i in want if got[i] != want[i]
    ]
    assert not mismatches, mismatches[:3]
    assert any(v["new_device_24h"] for v in want.values())
    assert any(v["profile_change_24h"] for v in want.values())


def test_waits_for_an_unseen_session(store: OnlineStore) -> None:
    defs = load()
    store.set_owner(1, 7)
    asks = [Ask(1, 1, 55, 1000), Ask(2, 1, None, 1000), Ask(3, 2, None, 1000)]
    got = customer_values(defs, store, asks)
    assert set(got) == {2}  # 1: session 55 not seen yet; 3: card owner unknown
    assert got[2] == {"new_device_24h": None, "profile_change_24h": False}
