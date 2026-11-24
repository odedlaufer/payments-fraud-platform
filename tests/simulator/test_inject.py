"""On-demand attacks for the demo console (simulator run --attack)."""

from collections import Counter
from datetime import timedelta
from decimal import Decimal

import pytest

from simulator.engine import Engine
from simulator.rates import Rates
from simulator.store import MemoryStore
from tests.simulator.test_engine import simulate


def engine() -> tuple[Engine, MemoryStore]:
    """10 simulated minutes of normal traffic, random fraud off."""
    return simulate(10 / 60, rates=Rates().with_fraud(0))


@pytest.mark.parametrize("pattern", ["card_testing", "account_takeover"])
def test_an_injected_attack_produces_its_fraud(pattern: str) -> None:
    e, store = engine()
    assert not store.truth  # random fraud is off
    assert e.inject(pattern)
    e.advance(e.now + timedelta(hours=2))
    assert store.truth and set(store.truth.values()) == {pattern}


def test_card_testing_is_a_burst_of_small_payments() -> None:
    e, store = engine()
    assert e.inject("card_testing")
    e.advance(e.now + timedelta(hours=1))
    attacked = [t for t in store.truth if store.truth[t] == "card_testing"]
    assert 6 <= len(attacked) <= 15
    cards = Counter(store.transactions[t]["card_id"] for t in attacked)
    assert len(cards) == 1  # one stolen card, many merchants
    assert all(Decimal(str(store.transactions[t]["amount"])) < 5 for t in attacked)


def test_unknown_attacks_are_refused() -> None:
    e, _ = engine()
    with pytest.raises(ValueError, match="unknown attack"):
        e.inject("bank_heist")
