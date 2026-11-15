"""The reference implementation against values computed by hand (it is the oracle)."""

from decimal import Decimal

import pytest

from features.model import load
from features.reference import Txn, card_features, haversine_km
from simulator.geo import haversine_km as simulator_haversine

H = 3_600_000  # one hour in ms
TLV, NYC = (32.0853, 34.7818), (40.7128, -74.0060)


def txn(
    i: int,
    ms: int,
    amount: str = "10.00",
    *,
    card: int = 1,
    merchant: int = 1,
    channel: str = "online",
    loc: tuple[float, float] | None = None,
) -> Txn:
    lat, lon = loc or (None, None)
    return Txn(i, card, merchant, Decimal(amount), channel, lat, lon, ms)


def features(*txns: Txn) -> dict[int, dict[str, object]]:
    return card_features(load(), txns)


def test_window_lower_bound_is_inclusive() -> None:
    out = features(txn(1, 0), txn(2, H), txn(3, H + 1))
    assert out[2]["txn_count_1h"] == 2  # [0, 1h] includes the transaction at 0
    assert out[3]["txn_count_1h"] == 2  # [1ms, 1h+1ms] excludes it


def test_current_transaction_and_ties_are_counted() -> None:
    out = features(txn(1, 5), txn(2, 5))
    assert out[1]["txn_count_1m"] == 2 and out[2]["txn_count_1m"] == 2


def test_other_cards_are_ignored() -> None:
    out = features(txn(1, 0), txn(2, 1, card=2))
    assert out[2]["txn_count_1h"] == 1


def test_sums_and_distinct_merchants() -> None:
    out = features(
        txn(1, 0, "10.00", merchant=1), txn(2, 1, "2.50", merchant=1), txn(3, 2, "7.25", merchant=2)
    )
    assert out[3]["txn_amount_sum_1h"] == Decimal("19.75")
    assert out[3]["distinct_merchants_1h"] == 2


def test_amount_vs_prior_mean() -> None:
    out = features(txn(1, 0, "10.00"), txn(2, 1, "30.00"), txn(3, 2, "40.00"))
    assert out[1]["amount_vs_avg_30d"] is None  # no earlier transaction
    assert out[2]["amount_vs_avg_30d"] == pytest.approx(3.0)  # 30 / 10
    assert out[3]["amount_vs_avg_30d"] == pytest.approx(2.0)  # 40 / mean(10, 30)


def test_previous_card_present_only() -> None:
    out = features(
        txn(1, 0, channel="card_present", loc=TLV),
        txn(2, H, channel="online"),
        txn(3, 2 * H, channel="card_present", loc=NYC),
    )
    assert out[1]["km_from_last_txn"] is None  # first card-present
    assert out[2]["km_from_last_txn"] is None  # online
    assert out[3]["minutes_since_last_txn"] == pytest.approx(120.0)  # skips the online one
    assert out[3]["km_from_last_txn"] == pytest.approx(9130, rel=0.01)  # Tel Aviv -> New York


def test_haversine_matches_the_simulator() -> None:
    assert haversine_km(*TLV, *NYC) == simulator_haversine(*TLV, *NYC)
