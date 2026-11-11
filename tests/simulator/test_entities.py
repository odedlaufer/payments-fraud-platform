import re
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from simulator.catalog import CATEGORIES
from simulator.entities import RISK_TIERS, Population, generate_population
from simulator.geo import CITIES, haversine_km

START = datetime(2026, 1, 1, tzinfo=UTC)
MCCS = {c.mcc for c in CATEGORIES}
COUNTRIES = {c.country_code for c in CITIES}


@pytest.fixture(scope="module")
def population() -> Population:
    return generate_population(seed=7, customers=3000, merchants=400, at=START)


def test_same_seed_same_population() -> None:
    a = generate_population(seed=42, customers=50, merchants=10, at=START)
    b = generate_population(seed=42, customers=50, merchants=10, at=START)
    assert a == b


def test_different_seed_different_population() -> None:
    a = generate_population(seed=1, customers=50, merchants=10, at=START)
    b = generate_population(seed=2, customers=50, merchants=10, at=START)
    assert a.customers != b.customers


def test_sizes(population: Population) -> None:
    assert len(population.customers) == 3000
    assert len(population.merchants) == 400


def test_every_customer_has_one_to_three_cards(population: Population) -> None:
    per_customer = Counter(card.customer_index for card in population.cards)
    assert set(per_customer) == set(range(3000))
    assert set(per_customer.values()) <= {1, 2, 3}


def test_customer_fields_satisfy_db_constraints(population: Population) -> None:
    for c in population.customers:
        assert c.risk_tier in RISK_TIERS
        assert c.country_code in COUNTRIES
        assert -90 <= c.home_lat <= 90 and -180 <= c.home_lon <= 180
        assert "@" in c.email and c.event_ts == START


def test_card_fields_satisfy_db_constraints(population: Population) -> None:
    for card in population.cards:
        assert re.fullmatch(r"[0-9]{4}", card.pan_last4)
        assert card.card_type in {"credit", "debit"}
        assert card.status == "active"
        assert card.credit_limit >= 0
        if card.card_type == "debit":
            assert card.credit_limit == Decimal("0.00")
        assert card.expires_on > START.date()


def test_merchant_fields_satisfy_db_constraints(population: Population) -> None:
    for m in population.merchants:
        assert re.fullmatch(r"[0-9]{4}", m.mcc) and m.mcc in MCCS
        assert m.status == "active"
        assert m.country_code in COUNTRIES


def test_risk_tier_mix_is_realistic(population: Population) -> None:
    share = Counter(c.risk_tier for c in population.customers)
    assert 0.80 < share["low"] / 3000 < 0.90
    assert share["high"] > 0


def test_customers_live_near_their_city(population: Population) -> None:
    centres = {c.name: c for c in CITIES}
    for c in population.customers:
        city = centres[c.city]
        assert haversine_km(c.home_lat, c.home_lon, city.lat, city.lon) < 10


def test_haversine_known_distance() -> None:
    """Tel Aviv to New York is about 9,130 km."""
    assert haversine_km(32.0853, 34.7818, 40.7128, -74.0060) == pytest.approx(9130, rel=0.01)


def test_rejects_empty_population() -> None:
    with pytest.raises(ValueError):
        generate_population(seed=1, customers=0, merchants=1, at=START)
