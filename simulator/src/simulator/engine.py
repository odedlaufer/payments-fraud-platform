"""Event-driven simulation of the payments app (FR-SIM2, FR-SIM5).

The engine advances on simulated time only. Callers decide how simulated time
relates to real time (paced by SimClock, or stepped as fast as possible).
"""

import heapq
import math
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from faker import Faker

from simulator.catalog import CATEGORIES, Category
from simulator.geo import CITIES
from simulator.rates import Rates
from simulator.store import NewTransaction, Store
from simulator.world import CustomerState, MerchantState, World

CATEGORY_BY_MCC: dict[str, Category] = {c.mcc: c for c in CATEGORIES}
CURRENCY_BY_COUNTRY: dict[str, str] = {}
for _city in CITIES:
    CURRENCY_BY_COUNTRY.setdefault(_city.country_code, _city.currency)
COUNTRIES = sorted(CURRENCY_BY_COUNTRY)
DAY_S = 86_400.0


def poisson(rng: random.Random, lam: float) -> int:
    """Poisson sample; Knuth for small means, normal approximation above 30."""
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, round(rng.gauss(lam, math.sqrt(lam))))
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= threshold:
            return k
        k += 1


@dataclass(order=True)
class _Scheduled:
    due: datetime
    seq: int
    action: Callable[[datetime], None] = field(compare=False)


class Engine:
    def __init__(
        self,
        world: World,
        store: Store,
        *,
        seed: int,
        tx_per_sim_second: float,
        rates: Rates | None = None,
        start: datetime,
    ) -> None:
        if start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        self.world = world
        self.store = store
        self.rates = rates or Rates()
        self.tx_per_sim_second = tx_per_sim_second
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)
        self.now = start
        self._queue: list[_Scheduled] = []
        self._seq = 0
        self._tx_tokens = 0.0
        self._last_expiry_check = start
        self._merchants_by_country: dict[str, list[int]] = {}
        for m in world.merchants.values():
            self._merchants_by_country.setdefault(m.country_code, []).append(m.merchant_id)
        for cust in world.customers.values():
            if not cust.devices:
                cust.devices.append(self._device_id(cust.customer_id, 0))
        # Transactions left pending by a previous run are finalised promptly.
        for transaction_id, _ in world.pending_transactions:
            self._schedule(start, self._finalise(transaction_id))
        world.pending_transactions.clear()

    # ------------------------------------------------------------------ scheduling

    def _schedule(self, due: datetime, action: Callable[[datetime], None]) -> None:
        self._seq += 1
        heapq.heappush(self._queue, _Scheduled(due, self._seq, action))

    def pending_actions(self) -> int:
        return len(self._queue)

    def advance(self, to: datetime) -> None:
        """Runs everything that happens in (self.now, to], in simulated-time order."""
        if to <= self.now:
            return
        start = self.now
        dt = (to - start).total_seconds()

        # New transactions, spread evenly across the interval.
        self._tx_tokens += self.tx_per_sim_second * dt
        count = int(self._tx_tokens)
        self._tx_tokens -= count
        arrivals = [start + timedelta(seconds=dt * (i + 1) / (count + 1)) for i in range(count)]

        # Reference-data changes as Poisson processes over the interval.
        days = dt / DAY_S
        changes: list[tuple[datetime, Callable[[datetime], None]]] = []
        processes: tuple[tuple[float, int, Callable[[datetime], None]], ...] = (
            (self.rates.customer_profile_change, len(self.world.customers), self._change_profile),
            (self.rates.card_block, len(self.world.cards), self._block_card),
            (self.rates.card_limit_change, len(self.world.cards), self._change_limit),
            (self.rates.merchant_update, len(self.world.merchants), self._update_merchant),
            (self.rates.merchant_delete, len(self.world.merchants), self._delete_merchant),
        )
        for rate, pool, action in processes:
            for _ in range(poisson(self.rng, rate * pool * days)):
                changes.append((start + timedelta(seconds=self.rng.uniform(0, dt)), action))

        for at in arrivals:
            self._schedule(at, self._new_transaction)
        for at, action in changes:
            self._schedule(at, action)

        while self._queue and self._queue[0].due <= to:
            item = heapq.heappop(self._queue)
            self.now = max(self.now, item.due)
            item.action(item.due)
        self.now = to
        self._expire_cards(to)

    # ---------------------------------------------------------------- transactions

    def _device_id(self, customer_id: int, n: int) -> str:
        return f"dev-{customer_id}-{n}"

    def _ip(self) -> str:
        r = self.rng
        return f"{r.randint(1, 223)}.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}"

    def _session_for(self, cust: CustomerState, at: datetime) -> int:
        reuse = timedelta(seconds=self.rates.session_reuse_s)
        if cust.last_session_id and cust.last_session_at and at - cust.last_session_at < reuse:
            return cust.last_session_id
        new_device = self.rng.random() < self.rates.new_device_probability
        if new_device:
            cust.devices.append(self._device_id(cust.customer_id, len(cust.devices)))
            device = cust.devices[-1]
        else:
            device = self.rng.choice(cust.devices)
        low, high = (0.6, 0.95) if new_device else (0.01, 0.3)
        cust.last_session_risk = Decimal(str(round(self.rng.uniform(low, high), 4)))
        foreign = self.rng.random() < self.rates.foreign_ip_probability
        ip_country = self.rng.choice(COUNTRIES) if foreign else cust.country_code
        session_id = self.store.insert_session(cust.customer_id, device, self._ip(), ip_country, at)
        cust.last_session_id, cust.last_session_at = session_id, at
        return session_id

    def _pick_merchant(self, cust: CustomerState) -> MerchantState | None:
        local = self._merchants_by_country.get(cust.country_code)
        if local and self.rng.random() < self.rates.home_country_share:
            return self.world.merchants[self.rng.choice(local)]
        if not self.world.merchants:
            return None
        return self.world.merchants[self.rng.choice(list(self.world.merchants))]

    def _amount(self, category: Category) -> Decimal:
        value = category.amount_median * math.exp(self.rng.gauss(0, 0.6))
        return Decimal(str(max(0.5, round(value, 2)))).quantize(Decimal("0.01"))

    def _new_transaction(self, at: datetime) -> None:
        if not self.world.cards:
            return
        card = self.world.cards[self.rng.choice(list(self.world.cards))]
        cust = self.world.customers[card.customer_id]
        merchant = self._pick_merchant(cust)
        if merchant is None:
            return
        category = CATEGORY_BY_MCC.get(merchant.mcc, CATEGORIES[0])
        online = category.online and self.rng.random() < 0.8
        session_id = self._session_for(cust, at) if online else None
        jitter = self.rng.uniform(-0.001, 0.001)
        txn = NewTransaction(
            card_id=card.card_id,
            merchant_id=merchant.merchant_id,
            session_id=session_id,
            amount=self._amount(category),
            currency=CURRENCY_BY_COUNTRY.get(merchant.country_code, "USD"),
            channel="online" if online else "card_present",
            lat=None if online else round(merchant.lat + jitter, 6),
            lon=None if online else round(merchant.lon + jitter, 6),
            event_ts=at,
            device_risk_score=cust.last_session_risk if online else None,
        )
        transaction_id = self.store.insert_transaction(txn)
        delay = self.rng.uniform(*self.rates.status_delay_s)
        self._schedule(at + timedelta(seconds=delay), self._finalise(transaction_id, card.card_id))

    def _finalise(
        self, transaction_id: int, card_id: int | None = None
    ) -> Callable[[datetime], None]:
        def run(at: datetime) -> None:
            card = self.world.cards.get(card_id) if card_id is not None else None
            usable = card is None or card.status == "active"
            approved = usable and self.rng.random() < self.rates.approve_probability
            self.store.update_transaction_status(
                transaction_id, "approved" if approved else "declined", at
            )
            if approved and self.rng.random() < self.rates.reversal_probability:
                delay = self.rng.uniform(*self.rates.reversal_delay_s)
                self._schedule(at + timedelta(seconds=delay), self._reverse(transaction_id))

        return run

    def _reverse(self, transaction_id: int) -> Callable[[datetime], None]:
        def run(at: datetime) -> None:
            self.store.update_transaction_status(transaction_id, "reversed", at)

        return run

    # ------------------------------------------------------------ reference data

    def _change_profile(self, at: datetime) -> None:
        if not self.world.customers:
            return
        customer_id = self.rng.choice(list(self.world.customers))
        column = self.rng.choice(["email", "phone", "address_line", "risk_tier"])
        value: object
        if column == "email":
            value = f"{self.fake.user_name()}.{self.rng.randrange(10**6):06d}@example.com"
        elif column == "phone":
            value = self.fake.numerify("+###########")
        elif column == "address_line":
            value = self.fake.street_address()
        else:
            value = self.rng.choice(["low", "medium", "high"])
        self.store.update_customer(customer_id, column, value, at)

    def _block_card(self, at: datetime) -> None:
        active = [c for c in self.world.cards.values() if c.status == "active"]
        if not active:
            return
        card = self.rng.choice(active)
        card.status = "blocked"
        self.store.update_card(card.card_id, "status", "blocked", at)

    def _change_limit(self, at: datetime) -> None:
        credit = [c for c in self.world.cards.values() if c.card_type == "credit"]
        if not credit:
            return
        card = self.rng.choice(credit)
        limit = Decimal(self.rng.choice([1000, 2500, 5000, 7500, 10000, 15000, 20000, 30000]))
        self.store.update_card(card.card_id, "credit_limit", limit.quantize(Decimal("0.01")), at)

    def _expire_cards(self, at: datetime) -> None:
        if at - self._last_expiry_check < timedelta(hours=1):
            return
        self._last_expiry_check = at
        for card in self.world.cards.values():
            if card.status == "active" and card.expires_on <= at.date():
                card.status = "expired"
                self.store.update_card(card.card_id, "status", "expired", at)

    def _update_merchant(self, at: datetime) -> None:
        if not self.world.merchants:
            return
        merchant = self.world.merchants[self.rng.choice(list(self.world.merchants))]
        if self.rng.random() < 0.5:
            merchant.status = "suspended" if merchant.status == "active" else "active"
            self.store.update_merchant(merchant.merchant_id, {"status": merchant.status}, at)
        else:
            category = self.rng.choice(CATEGORIES)
            merchant.mcc = category.mcc
            self.store.update_merchant(
                merchant.merchant_id, {"mcc": category.mcc, "category": category.name}, at
            )

    def _delete_merchant(self, at: datetime) -> None:
        if len(self.world.merchants) <= 1:
            return
        merchant_id = self.rng.choice(list(self.world.merchants))
        merchant = self.world.merchants.pop(merchant_id)
        self._merchants_by_country[merchant.country_code].remove(merchant_id)
        self.store.delete_merchant(merchant_id, at)
