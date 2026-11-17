"""Travel realism and injected fraud (FR-SIM3), on simulated days in memory."""

from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Any, cast

import pytest

from simulator.engine import FAR_KM, HIGH_VALUE_MCCS, Engine
from simulator.geo import CITIES, haversine_km
from simulator.rates import Rates
from simulator.store import MemoryStore
from tests.simulator.test_engine import simulate

Rows = dict[int, dict[str, Any]]


def tx(store: MemoryStore) -> Rows:
    """MemoryStore rows, typed for the assertions below."""
    return cast(Rows, store.transactions)


@pytest.fixture(scope="module")
def three_days() -> tuple[Engine, MemoryStore]:
    return simulate(72, rates=Rates().with_fraud(5))


def card_present_legs(rows: Rows) -> list[tuple[int, int, float]]:
    """(earlier id, later id, km/h) for consecutive card-present purchases of each card."""
    by_card: dict[int, list[tuple[Any, int]]] = defaultdict(list)
    for txn_id, r in rows.items():
        if r["channel"] == "card_present":
            by_card[r["card_id"]].append((r["event_ts"], txn_id))
    legs = []
    for seq in by_card.values():
        seq.sort()
        for (t1, a), (t2, b) in pairwise(seq):
            km = haversine_km(rows[a]["lat"], rows[a]["lon"], rows[b]["lat"], rows[b]["lon"])
            hours = max((t2 - t1).total_seconds() / 3600, 1e-9)
            legs.append((a, b, km / hours))
    return legs


def test_legitimate_travel_never_beats_an_airliner(three_days: tuple[Engine, MemoryStore]) -> None:
    _, store = three_days
    legit = {i: r for i, r in tx(store).items() if i not in store.truth}
    legs = card_present_legs(legit)
    assert len(legs) > 1000
    fastest = max(speed for _, _, speed in legs if speed < float("inf"))

    def km(a: int, b: int) -> float:
        return haversine_km(legit[a]["lat"], legit[a]["lon"], legit[b]["lat"], legit[b]["lon"])

    long_legs = [s for a, b, s in legs if km(a, b) > 500]
    assert fastest < 900 or all(s < 900 for s in long_legs), f"legit speed {fastest:.0f} km/h"


def test_customers_take_trips(three_days: tuple[Engine, MemoryStore]) -> None:
    engine, store = three_days
    abroad = 0
    for r in tx(store).values():
        if r["channel"] != "card_present":
            continue
        cust = engine.world.customers[engine.world.cards[r["card_id"]].customer_id]
        home = CITIES[cust.home_city]
        if haversine_km(home.lat, home.lon, r["lat"], r["lon"]) > 500:
            abroad += 1
    assert abroad > 0


def test_every_pattern_is_injected_and_labelled(three_days: tuple[Engine, MemoryStore]) -> None:
    _, store = three_days
    patterns = set(store.truth.values())
    assert patterns == {
        "card_testing", "impossible_travel", "account_takeover", "merchant_bust_out",
        "friendly_fraud",  # legitimate, disputed anyway: label noise, not an attack
    }  # fmt: skip
    assert all(i in tx(store) for i in store.truth)


def test_card_testing_is_small_online_payments_at_many_merchants(
    three_days: tuple[Engine, MemoryStore],
) -> None:
    _, store = three_days
    by_card: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for i, pattern in store.truth.items():
        if pattern == "card_testing":
            by_card[tx(store)[i]["card_id"]].append(tx(store)[i])
    bursts: list[list[dict[str, Any]]] = []
    for rows in by_card.values():  # a card can be hit twice: split on gaps over 10 minutes
        rows.sort(key=lambda r: r["event_ts"])
        bursts.append([rows[0]])
        for prev, row in pairwise(rows):
            if row["event_ts"] - prev["event_ts"] > timedelta(minutes=10):
                bursts.append([])
            bursts[-1].append(row)
    assert bursts
    for burst in bursts:
        assert len(burst) >= 6
        assert all(r["channel"] == "online" and r["amount"] < Decimal("5") for r in burst)
        assert len({r["merchant_id"] for r in burst}) == len(burst)


def test_impossible_travel_follows_a_legit_purchase_far_away(
    three_days: tuple[Engine, MemoryStore],
) -> None:
    _, store = three_days
    legs = {b: (a, speed) for a, b, speed in card_present_legs(tx(store))}
    frauds = [i for i, p in store.truth.items() if p == "impossible_travel"]
    assert frauds
    for i in frauds:
        assert i in legs, "the clone purchase follows a card-present purchase"
        a, speed = legs[i]
        r, prev = tx(store)[i], tx(store)[a]
        assert haversine_km(prev["lat"], prev["lon"], r["lat"], r["lon"]) >= FAR_KM - 50
        assert speed > 900


def test_account_takeover_spends_big_from_a_new_device(
    three_days: tuple[Engine, MemoryStore],
) -> None:
    engine, store = three_days
    frauds = [tx(store)[i] for i, p in store.truth.items() if p == "account_takeover"]
    assert frauds
    for r in frauds:
        assert r["channel"] == "online" and r["device_risk_score"] >= Decimal("0.8")
        assert str(store.sessions[int(r["session_id"])]["device_id"]).startswith("dev-ato-")
        assert engine.world.merchants[r["merchant_id"]].mcc in HIGH_VALUE_MCCS


def test_fraud_can_be_turned_off() -> None:
    """No attacks; friendly fraud (a legitimate dispute) can still happen."""
    _, store = simulate(6, rates=Rates().with_fraud(0))
    assert set(store.truth.values()) <= {"friendly_fraud"}


def test_trips_have_transit_without_card_present() -> None:
    engine, _ = simulate(1, rates=Rates().with_fraud(0))
    cust = next(iter(engine.world.customers.values()))
    start = engine.now
    engine.rates = Rates(trip_start=0)
    cust.trip_city = None
    dest = next(i for i in range(len(CITIES)) if i != cust.home_city)
    cust.trip_city, cust.trip_arrive = dest, start + timedelta(hours=3)
    cust.trip_leave, cust.trip_home = start + timedelta(days=2), start + timedelta(days=2, hours=3)
    assert engine.current_city(cust, start + timedelta(hours=1)) is None
    assert engine.current_city(cust, start + timedelta(days=1)) == dest
    assert engine.current_city(cust, start + timedelta(days=2, hours=1)) is None
    assert engine.current_city(cust, start + timedelta(days=3)) == cust.home_city
    assert cust.trip_city is None
