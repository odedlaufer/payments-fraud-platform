"""Seeded generators for reference data: customers, cards, merchants (FR-SIM1).

Pure functions of (seed, sizes, sim start): no I/O, so they are unit-testable and
the same seed always yields the same population.
"""

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from faker import Faker

from simulator.catalog import CATEGORIES, Category
from simulator.geo import CITIES, HOME_WEIGHTS, City

RISK_TIERS = ("low", "medium", "high")
RISK_WEIGHTS = (0.85, 0.12, 0.03)
CARDS_PER_CUSTOMER = (1, 2, 3)
CARDS_PER_CUSTOMER_WEIGHTS = (0.70, 0.25, 0.05)
CREDIT_LIMITS = (1000, 2500, 5000, 7500, 10000, 15000, 20000)


@dataclass(frozen=True)
class Customer:
    full_name: str
    email: str
    phone: str
    address_line: str
    city: str
    country_code: str
    home_lat: float
    home_lon: float
    risk_tier: str
    event_ts: datetime


@dataclass(frozen=True)
class Card:
    customer_index: int  # position in the customers list, resolved to an id on insert
    pan_last4: str
    card_type: str
    status: str
    credit_limit: Decimal
    expires_on: date
    event_ts: datetime


@dataclass(frozen=True)
class Merchant:
    name: str
    mcc: str
    category: str
    country_code: str
    lat: float
    lon: float
    status: str
    event_ts: datetime


@dataclass(frozen=True)
class Population:
    customers: list[Customer]
    cards: list[Card]
    merchants: list[Merchant]


def _jitter(rng: random.Random, value: float, spread: float = 0.05) -> float:
    """Spread points around a city centre (about +/- 5 km)."""
    return round(value + rng.uniform(-spread, spread), 6)


def generate_customers(rng: random.Random, fake: Faker, count: int, at: datetime) -> list[Customer]:
    customers = []
    for _ in range(count):
        city: City = rng.choices(CITIES, weights=HOME_WEIGHTS)[0]
        first, last = fake.first_name(), fake.last_name()
        customers.append(
            Customer(
                full_name=f"{first} {last}",
                email=f"{first}.{last}.{rng.randrange(10**6):06d}@example.com".lower(),
                phone=fake.numerify("+###########"),
                address_line=fake.street_address(),
                city=city.name,
                country_code=city.country_code,
                home_lat=_jitter(rng, city.lat),
                home_lon=_jitter(rng, city.lon),
                risk_tier=rng.choices(RISK_TIERS, weights=RISK_WEIGHTS)[0],
                event_ts=at,
            )
        )
    return customers


def generate_cards(rng: random.Random, customer_count: int, at: datetime) -> list[Card]:
    cards = []
    for index in range(customer_count):
        n = rng.choices(CARDS_PER_CUSTOMER, weights=CARDS_PER_CUSTOMER_WEIGHTS)[0]
        for _ in range(n):
            card_type = "credit" if rng.random() < 0.6 else "debit"
            limit = Decimal(rng.choice(CREDIT_LIMITS)) if card_type == "credit" else Decimal(0)
            expires = (at + timedelta(days=rng.randint(365, 5 * 365))).date().replace(day=1)
            cards.append(
                Card(
                    customer_index=index,
                    pan_last4=f"{rng.randrange(10_000):04d}",
                    card_type=card_type,
                    status="active",
                    credit_limit=limit.quantize(Decimal("0.01")),
                    expires_on=expires,
                    event_ts=at,
                )
            )
    return cards


def generate_merchants(rng: random.Random, fake: Faker, count: int, at: datetime) -> list[Merchant]:
    weights = [c.weight for c in CATEGORIES]
    merchants = []
    for _ in range(count):
        category: Category = rng.choices(CATEGORIES, weights=weights)[0]
        city: City = rng.choice(CITIES)
        merchants.append(
            Merchant(
                name=fake.company(),
                mcc=category.mcc,
                category=category.name,
                country_code=city.country_code,
                lat=_jitter(rng, city.lat),
                lon=_jitter(rng, city.lon),
                status="active",
                event_ts=at,
            )
        )
    return merchants


def generate_population(seed: int, customers: int, merchants: int, at: datetime) -> Population:
    """Same seed, sizes and start time -> identical population."""
    if customers < 1 or merchants < 1:
        raise ValueError("need at least one customer and one merchant")
    rng = random.Random(seed)
    fake = Faker("en_US")
    fake.seed_instance(seed)
    customer_rows = generate_customers(rng, fake, customers, at)
    return Population(
        customers=customer_rows,
        cards=generate_cards(rng, len(customer_rows), at),
        merchants=generate_merchants(rng, fake, merchants, at),
    )
