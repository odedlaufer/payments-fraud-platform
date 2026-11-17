"""Delayed labels (FR-SIM4) and the merchant bust-out pattern, in memory."""

from datetime import timedelta
from typing import Any, cast

import pytest

from simulator.engine import Engine
from simulator.rates import Rates
from simulator.store import MemoryStore
from tests.simulator.test_engine import TX_PER_SIM_SECOND, simulate

Rows = dict[int, dict[str, Any]]
DAY = timedelta(days=1)


@pytest.fixture(scope="module")
def ninety_days() -> tuple[Engine, MemoryStore]:
    """Long enough for every chargeback of the first weeks to fall due (max 60 days)."""
    rates = Rates(friendly_fraud_probability=0.03).with_fraud(5)
    # A tenth of the normal arrival rate: labels need weeks of simulated time, not volume.
    engine, store = simulate(24 * 90, rates=rates, tx_per_sim_second=TX_PER_SIM_SECOND / 10)
    return engine, store


def rows(store: MemoryStore) -> Rows:
    return cast(Rows, store.transactions)


def test_chargebacks_arrive_1_to_60_days_later(ninety_days: tuple[Engine, MemoryStore]) -> None:
    _, store = ninety_days
    assert len(store.chargebacks) > 50
    for txn_id, (_, filed_at) in store.chargebacks.items():
        approved_at = next(
            at for status, at in rows(store)[txn_id]["history"][1:] if status == "approved"
        )
        assert DAY <= filed_at - approved_at <= 60 * DAY + timedelta(minutes=1)


def test_only_approved_transactions_are_disputed(ninety_days: tuple[Engine, MemoryStore]) -> None:
    _, store = ninety_days
    for txn_id in store.chargebacks:
        assert rows(store)[txn_id]["status"] == "approved"  # never declined, never refunded


def test_fraud_is_disputed_far_more_often_than_legitimate(
    ninety_days: tuple[Engine, MemoryStore],
) -> None:
    _, store = ninety_days
    fraud = {i for i, p in store.truth.items() if p != "friendly_fraud"}
    friendly = {i for i, p in store.truth.items() if p == "friendly_fraud"}
    approved = {i for i, r in rows(store).items() if r["status"] == "approved"}
    disputed_fraud = len(fraud & set(store.chargebacks)) / len(fraud & approved)
    legit = approved - fraud
    disputed_legit = len(legit & set(store.chargebacks)) / len(legit)
    assert disputed_fraud > 0.5 > 0.02 > disputed_legit
    assert set(store.chargebacks) & legit <= friendly  # a legit chargeback is friendly fraud


def test_chargebacks_still_due_are_kept_for_the_next_run() -> None:
    engine, store = simulate(24 * 3, rates=Rates(friendly_fraud_probability=0.05).with_fraud(5))
    assert store.pending_chargebacks, "some chargebacks fall due after the run"
    for _, file_at in store.pending_chargebacks.values():
        assert file_at > engine.now - timedelta(minutes=1)


def test_a_restarted_engine_files_pending_chargebacks() -> None:
    first, store = simulate(24 * 3, rates=Rates(friendly_fraud_probability=0.05).with_fraud(5))
    pending = dict(store.pending_chargebacks)
    world = first.world
    world.pending_chargebacks = [(i, reason, at) for i, (reason, at) in pending.items()]
    quiet = Rates(friendly_fraud_probability=0).with_fraud(0)  # nothing new is disputed
    restarted = Engine(world, store, seed=2, tx_per_sim_second=0, rates=quiet, start=first.now)
    restarted.advance(first.now + 61 * DAY)
    assert not set(pending) & set(store.pending_chargebacks)
    filed_or_refunded = {
        i for i in pending if i in store.chargebacks or rows(store)[i]["status"] != "approved"
    }
    assert filed_or_refunded == set(pending)


def test_bust_out_spikes_refunds_then_the_merchant_disappears(
    ninety_days: tuple[Engine, MemoryStore],
) -> None:
    engine, store = ninety_days
    by_merchant: dict[int, list[int]] = {}
    for i, p in store.truth.items():
        if p == "merchant_bust_out":
            by_merchant.setdefault(int(rows(store)[i]["merchant_id"]), []).append(i)
    assert by_merchant
    for merchant_id, ids in by_merchant.items():
        assert len(ids) >= 30
        refunded = sum(rows(store)[i]["status"] == "reversed" for i in ids)
        assert refunded / len(ids) > 0.15  # vs about 1% normally
        assert merchant_id not in engine.world.merchants  # gone after the burst
