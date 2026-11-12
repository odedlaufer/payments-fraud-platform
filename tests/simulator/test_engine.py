"""Behaviour of the simulation engine, using the in-memory store (FR-SIM2, FR-SIM5)."""

import random
import statistics
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from simulator.engine import Engine, poisson
from simulator.entities import generate_population
from simulator.rates import Rates
from simulator.store import MemoryStore
from simulator.world import CardState, CustomerState, MerchantState, World

START = datetime(2026, 1, 1, tzinfo=UTC)
TX_PER_SIM_SECOND = 20 / 144  # default 20 tx/s at 1 sim day = 10 real minutes


def make_world(seed: int = 3, customers: int = 300, merchants: int = 60) -> World:
    pop = generate_population(seed, customers, merchants, START)
    world = World(customers={}, cards={}, merchants={}, last_event_ts=START)
    for i, c in enumerate(pop.customers, start=1):
        world.customers[i] = CustomerState(i, c.country_code, c.home_lat, c.home_lon)
    for i, k in enumerate(pop.cards, start=1):
        world.cards[i] = CardState(i, k.customer_index + 1, k.card_type, k.status, k.expires_on)
    for i, m in enumerate(pop.merchants, start=1):
        world.merchants[i] = MerchantState(i, m.mcc, m.country_code, m.lat, m.lon, m.status)
    return world


def simulate(
    hours: float, *, seed: int = 11, rates: Rates | None = None, world: World | None = None
) -> tuple[Engine, MemoryStore]:
    world = world or make_world()
    store = MemoryStore(world)
    engine = Engine(
        world, store, seed=seed, tx_per_sim_second=TX_PER_SIM_SECOND, rates=rates, start=START
    )
    end = START + timedelta(hours=hours)
    while engine.now < end:
        engine.advance(engine.now + timedelta(seconds=30))
    # Drain like the runner does: no new arrivals, let pending statuses resolve.
    engine.tx_per_sim_second = 0
    engine.advance(engine.now + timedelta(seconds=15))
    return engine, store


@pytest.fixture(scope="module")
def day() -> tuple[Engine, MemoryStore]:
    return simulate(24, rates=Rates().scaled(30))


def test_poisson_mean_and_edges() -> None:
    rng = random.Random(1)
    assert poisson(rng, 0) == 0
    for lam in (0.5, 4.0, 80.0):
        mean = statistics.fmean(poisson(rng, lam) for _ in range(4000))
        assert mean == pytest.approx(lam, rel=0.08)


def test_transaction_volume_matches_rate(day: tuple[Engine, MemoryStore]) -> None:
    _, store = day
    expected = TX_PER_SIM_SECOND * 86_400
    assert store.ops["transactions", "insert"] == pytest.approx(expected, rel=0.01)


def test_every_transaction_starts_pending_then_resolves(day: tuple[Engine, MemoryStore]) -> None:
    _, store = day
    final: Counter[str] = Counter()
    for row in store.transactions.values():
        history = row["history"]
        assert isinstance(history, list)
        assert history[0] == "pending"
        resolved_status, resolved_at = history[1]
        assert resolved_status in {"approved", "declined"}
        delay = (resolved_at - row["event_ts"]).total_seconds()
        assert 1 <= delay <= 10, "status update within simulated seconds (FR-SIM2)"
        final[str(row["status"])] += 1
    total = sum(final.values())
    assert final["approved"] / total > 0.9
    assert final["declined"] > 0


def test_reversals_only_follow_approvals(day: tuple[Engine, MemoryStore]) -> None:
    _, store = day
    reversed_rows = [r for r in store.transactions.values() if r["status"] == "reversed"]
    assert reversed_rows, "expect some reversals in a simulated day"
    for row in reversed_rows:
        history = row["history"]
        assert isinstance(history, list)
        assert [h[0] for h in history[1:]] == ["approved", "reversed"]


def test_channels_and_locations(day: tuple[Engine, MemoryStore]) -> None:
    _, store = day
    channels = Counter(r["channel"] for r in store.transactions.values())
    assert channels["online"] > 0 and channels["card_present"] > 0
    for row in store.transactions.values():
        if row["channel"] == "card_present":
            assert row["lat"] is not None and row["lon"] is not None
            assert row["session_id"] is None
        else:
            assert row["session_id"] in store.sessions


def test_amounts_positive_two_decimals(day: tuple[Engine, MemoryStore]) -> None:
    _, store = day
    for row in store.transactions.values():
        amount = row["amount"]
        assert amount > 0  # type: ignore[operator]
        assert amount.as_tuple().exponent == -2  # type: ignore[attr-defined]


def test_reference_data_changes_happen(day: tuple[Engine, MemoryStore]) -> None:
    _, store = day
    assert store.ops["customers", "update"] > 0
    assert store.ops["cards", "update"] > 0
    assert store.ops["merchants", "update"] > 0
    assert store.ops["merchants", "delete"] > 0


def test_no_transactions_for_deleted_merchants(day: tuple[Engine, MemoryStore]) -> None:
    _, store = day
    deleted_at = {
        int(str(merchant_id)): i
        for i, (t, op, merchant_id, _) in enumerate(store.log)
        if (t, op) == ("merchants", "delete")
    }
    for i, (table, op, tid, _) in enumerate(store.log):
        if (table, op) == ("transactions", "insert"):
            merchant = int(str(store.transactions[tid]["merchant_id"]))
            assert not (merchant in deleted_at and i > deleted_at[merchant])


def test_blocked_cards_are_declined() -> None:
    world = make_world()
    for card in world.cards.values():
        card.status = "blocked"
    _, store = simulate(1, world=world, rates=Rates().scaled(0))
    statuses = {r["status"] for r in store.transactions.values()}
    assert statuses == {"declined"}


def test_same_seed_same_run() -> None:
    _, a = simulate(2, seed=5)
    _, b = simulate(2, seed=5)
    assert a.log == b.log


def test_different_seed_different_run() -> None:
    _, a = simulate(2, seed=5)
    _, b = simulate(2, seed=6)
    assert a.log != b.log


def test_pending_transactions_from_previous_run_are_finalised() -> None:
    world = make_world()
    world.pending_transactions = [(1, START)]
    store = MemoryStore(world)
    store.transactions[1] = {"status": "pending", "history": ["pending"], "event_ts": START}
    engine = Engine(world, store, seed=1, tx_per_sim_second=0, start=START)
    engine.advance(START + timedelta(seconds=1))
    assert store.transactions[1]["status"] in {"approved", "declined"}


def test_cards_expire_when_their_date_passes() -> None:
    world = make_world()
    card = next(iter(world.cards.values()))
    card.expires_on = (START + timedelta(days=1)).date()
    simulate(30, world=world, rates=Rates().scaled(0))
    assert card.status == "expired"


def test_rejects_naive_start() -> None:
    world = make_world()
    with pytest.raises(ValueError):
        Engine(world, MemoryStore(world), seed=1, tx_per_sim_second=1, start=datetime(2026, 1, 1))


def test_device_risk_score_on_online_transactions_only(day: tuple[Engine, MemoryStore]) -> None:
    """FR-SIM7 column: online transactions carry the session device's risk, card-present none."""
    _, store = day
    online = [r for r in store.transactions.values() if r["channel"] == "online"]
    present = [r for r in store.transactions.values() if r["channel"] == "card_present"]
    assert all(r["device_risk_score"] is None for r in present)
    scores = [Decimal(str(r["device_risk_score"])) for r in online]
    assert all(0 <= s <= 1 for s in scores)
    high = sum(1 for s in scores if s >= Decimal("0.6"))
    assert 0 < high < len(scores) * 0.2, "new devices are rare and score high"
