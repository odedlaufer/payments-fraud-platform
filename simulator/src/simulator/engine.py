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
from simulator.geo import CITIES, haversine_km, nearest_city
from simulator.rates import Rates
from simulator.store import NewTransaction, Store
from simulator.world import CustomerState, MerchantState, World

CATEGORY_BY_MCC: dict[str, Category] = {c.mcc: c for c in CATEGORIES}
CURRENCY_BY_COUNTRY: dict[str, str] = {}
for _city in CITIES:
    CURRENCY_BY_COUNTRY.setdefault(_city.country_code, _city.currency)
COUNTRIES = sorted(CURRENCY_BY_COUNTRY)
HIGH_VALUE_MCCS = ("5732", "4829", "4511", "7011")  # electronics, transfers, airlines, hotels
FAR_KM = 3_000.0  # an impossible-travel attack lands at least this far from the card

# FR-SIM4: how likely an approved fraudulent transaction is disputed. Tiny card-testing
# payments often go unnoticed; stolen or hijacked spend almost always comes back.
CHARGEBACK_PROBABILITY = {
    "card_testing": 0.3,
    "impossible_travel": 0.9,
    "account_takeover": 0.9,
    "merchant_bust_out": 0.8,
}
# Card-network style reason codes: fraud card-absent, counterfeit card-present, dispute.
REASON_FRAUD_ONLINE, REASON_FRAUD_PRESENT, REASON_FRIENDLY = "10.4", "10.2", "13.1"
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
        self._disputed: set[int] = set()  # charged back: a refund can no longer happen
        self._seq = 0
        self._tx_tokens = 0.0
        self._last_expiry_check = start
        self._merchants_by_city: dict[int, list[int]] = {}
        self._online_merchants: list[int] = []
        for m in world.merchants.values():
            self._index_merchant(m)
        for cust in world.customers.values():
            if not cust.devices:
                cust.devices.append(self._device_id(cust.customer_id, 0))
            cust.home_city = nearest_city(cust.home_lat, cust.home_lon)
        self._resume_locations(start)
        # Transactions left pending by a previous run are finalised promptly.
        for transaction_id, _ in world.pending_transactions:
            self._schedule(start, self._finalise(transaction_id))
        world.pending_transactions.clear()
        # Chargebacks decided by earlier runs are filed when they fall due (FR-SIM4).
        for transaction_id, reason, file_at in world.pending_chargebacks:
            self._schedule(max(file_at, start), self._file_chargeback(transaction_id, reason))
        world.pending_chargebacks.clear()

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
            (self.rates.trip_start, len(self.world.customers), self._start_trip),
            (self.rates.card_testing_attacks, 1, self._attack_card_testing),
            (self.rates.impossible_travel_attacks, 1, self._attack_impossible_travel),
            (self.rates.account_takeover_attacks, 1, self._attack_account_takeover),
            (self.rates.merchant_bust_out_attacks, 1, self._attack_merchant_bust_out),
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

    # ------------------------------------------------------------------- places

    def _index_merchant(self, m: MerchantState) -> None:
        self._merchants_by_city.setdefault(nearest_city(m.lat, m.lon), []).append(m.merchant_id)
        if CATEGORY_BY_MCC.get(m.mcc, CATEGORIES[0]).online:
            self._online_merchants.append(m.merchant_id)

    def _live(self, ids: list[int]) -> list[int]:
        return [i for i in ids if i in self.world.merchants]

    def current_city(self, cust: CustomerState, at: datetime) -> int | None:
        """Where the customer is: a city index, or None while travelling (online only)."""
        if cust.trip_city is None:
            return cust.home_city
        assert cust.trip_arrive and cust.trip_leave and cust.trip_home
        if at < cust.trip_arrive or cust.trip_leave <= at < cust.trip_home:
            return None
        if at < cust.trip_leave:
            return cust.trip_city
        cust.trip_city = cust.trip_arrive = cust.trip_leave = cust.trip_home = None
        return cust.home_city

    def _transit(self, a: int, b: int) -> timedelta:
        km = haversine_km(CITIES[a].lat, CITIES[a].lon, CITIES[b].lat, CITIES[b].lon)
        return timedelta(hours=km / self.rates.travel_kmh + self.rates.transit_buffer_h)

    def _resume_locations(self, start: datetime) -> None:
        """Puts customers where their last legitimate in-person purchase was. Trips live in
        memory only: without this, everyone abroad at the end of the previous run would be
        home at the start of this one, teleporting (impossible travel on a legitimate card).
        Someone last seen away travels home now, taking the real transit time."""
        for customer_id, (lat, lon, at) in self.world.last_seen.items():
            cust = self.world.customers.get(customer_id)
            if cust is None:
                continue
            city = nearest_city(lat, lon)
            if city == cust.home_city:
                continue
            cust.trip_city = city
            cust.trip_arrive = at
            cust.trip_leave = max(at, start)
            cust.trip_home = cust.trip_leave + self._transit(city, cust.home_city)

    def _start_trip(self, at: datetime) -> None:
        cust = self.world.customers[self.rng.choice(list(self.world.customers))]
        if cust.trip_city is not None or self.current_city(cust, at) is None:
            return
        dest = self.rng.choice([i for i in range(len(CITIES)) if i != cust.home_city])
        stay = timedelta(days=self.rng.uniform(*self.rates.trip_days))
        cust.trip_city = dest
        cust.trip_arrive = at + self._transit(cust.home_city, dest)
        cust.trip_leave = cust.trip_arrive + stay
        cust.trip_home = cust.trip_leave + self._transit(dest, cust.home_city)

    def _pick_merchant(
        self, cust: CustomerState, at: datetime
    ) -> tuple[MerchantState, bool] | None:
        """A merchant and whether the purchase is online. Card-present purchases only happen
        in the city the customer is in, so legitimate travel never beats an airliner."""
        city = self.current_city(cust, at)
        local = self._live(self._merchants_by_city.get(city, [])) if city is not None else []
        if local and self.rng.random() < self.rates.local_share:
            merchant = self.world.merchants[self.rng.choice(local)]
            category = CATEGORY_BY_MCC.get(merchant.mcc, CATEGORIES[0])
            return merchant, category.online and self.rng.random() < 0.5
        online = self._live(self._online_merchants)
        if not online:
            return None
        return self.world.merchants[self.rng.choice(online)], True

    def _amount(self, category: Category) -> Decimal:
        value = category.amount_median * math.exp(self.rng.gauss(0, 0.6))
        return Decimal(str(max(0.5, round(value, 2)))).quantize(Decimal("0.01"))

    def _new_transaction(self, at: datetime) -> None:
        if not self.world.cards:
            return
        card = self.world.cards[self.rng.choice(list(self.world.cards))]
        cust = self.world.customers[card.customer_id]
        picked = self._pick_merchant(cust, at)
        if picked is None:
            return
        merchant, online = picked
        category = CATEGORY_BY_MCC.get(merchant.mcc, CATEGORIES[0])
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
        self._schedule(
            at + timedelta(seconds=delay),
            self._finalise(transaction_id, card.card_id, online=online),
        )

    def _finalise(
        self,
        transaction_id: int,
        card_id: int | None = None,
        *,
        online: bool = False,
        pattern: str | None = None,
        reversal_probability: float | None = None,
    ) -> Callable[[datetime], None]:
        def run(at: datetime) -> None:
            card = self.world.cards.get(card_id) if card_id is not None else None
            usable = card is None or card.status == "active"
            approved = usable and self.rng.random() < self.rates.approve_probability
            self.store.update_transaction_status(
                transaction_id, "approved" if approved else "declined", at
            )
            if not approved:
                return
            refund = (
                self.rates.reversal_probability
                if reversal_probability is None
                else reversal_probability
            )
            if self.rng.random() < refund:
                delay = self.rng.uniform(*self.rates.reversal_delay_s)
                self._schedule(at + timedelta(seconds=delay), self._reverse(transaction_id))
            if pattern is not None:
                if self.rng.random() < CHARGEBACK_PROBABILITY[pattern]:
                    reason = REASON_FRAUD_ONLINE if online else REASON_FRAUD_PRESENT
                    self._promise_chargeback(transaction_id, reason, at)
            elif online and self.rng.random() < self.rates.friendly_fraud_probability:
                # Legitimate, but the cardholder disputes it anyway: label noise.
                self.store.insert_truth(transaction_id, "friendly_fraud")
                self._promise_chargeback(transaction_id, REASON_FRIENDLY, at)

        return run

    def _promise_chargeback(self, transaction_id: int, reason: str, at: datetime) -> None:
        low, high, mode = self.rates.chargeback_delay_days
        file_at = at + timedelta(days=self.rng.triangular(low, high, mode))
        self.store.schedule_chargeback(transaction_id, reason, file_at)
        self._schedule(file_at, self._file_chargeback(transaction_id, reason))

    def _file_chargeback(self, transaction_id: int, reason: str) -> Callable[[datetime], None]:
        def run(at: datetime) -> None:
            if self.store.file_chargeback(transaction_id, reason, at):
                self._disputed.add(transaction_id)

        return run

    def _reverse(self, transaction_id: int) -> Callable[[datetime], None]:
        def run(at: datetime) -> None:
            if transaction_id in self._disputed:
                return  # the cardholder was already refunded through the chargeback
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
        self._remove_merchant(self.rng.choice(list(self.world.merchants)), at)

    def _remove_merchant(self, merchant_id: int, at: datetime) -> None:
        if merchant_id in self.world.merchants and len(self.world.merchants) > 1:
            self.world.merchants.pop(merchant_id)
            self.store.delete_merchant(merchant_id, at)

    # --------------------------------------------------------------- fraud (FR-SIM3)

    def _fraud(
        self,
        card_id: int,
        merchant_id: int,
        amount: Decimal,
        pattern: str,
        *,
        session_id: int | None = None,
        risk: Decimal | None = None,
        card_present: bool = False,
        reversal_probability: float | None = None,
    ) -> Callable[[datetime], None]:
        """A labelled fraudulent transaction, created when its scheduled time comes."""

        def run(at: datetime) -> None:
            merchant = self.world.merchants.get(merchant_id)
            if merchant is None or card_id not in self.world.cards:
                return
            jitter = self.rng.uniform(-0.001, 0.001)
            txn = NewTransaction(
                card_id=card_id,
                merchant_id=merchant_id,
                session_id=session_id,
                amount=amount,
                currency=CURRENCY_BY_COUNTRY.get(merchant.country_code, "USD"),
                channel="card_present" if card_present else "online",
                lat=round(merchant.lat + jitter, 6) if card_present else None,
                lon=round(merchant.lon + jitter, 6) if card_present else None,
                event_ts=at,
                device_risk_score=None if card_present else risk,
            )
            transaction_id = self.store.insert_transaction(txn)
            self.store.insert_truth(transaction_id, pattern)
            delay = self.rng.uniform(*self.rates.status_delay_s)
            finalise = self._finalise(
                transaction_id,
                card_id,
                online=not card_present,
                pattern=pattern,
                reversal_probability=reversal_probability,
            )
            self._schedule(at + timedelta(seconds=delay), finalise)

        return run

    def _active_card(self) -> int | None:
        active = [c.card_id for c in self.world.cards.values() if c.status == "active"]
        return self.rng.choice(active) if active else None

    def _attack_card_testing(self, at: datetime) -> None:
        """Stolen card numbers checked with tiny online payments at many merchants."""
        card_id = self._active_card()
        online = self._live(self._online_merchants)
        if card_id is None or len(online) < 6:
            return
        when = at
        for merchant_id in self.rng.sample(online, min(len(online), self.rng.randint(6, 15))):
            when += timedelta(seconds=self.rng.uniform(10, 90))
            amount = Decimal(str(round(self.rng.uniform(0.5, 4.99), 2))).quantize(Decimal("0.01"))
            self._schedule(when, self._fraud(card_id, merchant_id, amount, "card_testing"))

    def _attack_impossible_travel(self, at: datetime) -> None:
        """A cloned card used in person far away, minutes after the owner used it at home."""
        card_id = self._active_card()
        if card_id is None:
            return
        cust = self.world.customers[self.world.cards[card_id].customer_id]
        city = self.current_city(cust, at)
        if city is None:
            return
        home = [
            m
            for m in self._live(self._merchants_by_city.get(city, []))
            if not CATEGORY_BY_MCC.get(self.world.merchants[m].mcc, CATEGORIES[0]).online
        ]
        far = [i for i in range(len(CITIES)) if self._km(city, i) >= FAR_KM]
        far_merchants = [m for i in far for m in self._live(self._merchants_by_city.get(i, []))]
        if not home or not far_merchants:
            return
        legit = self.world.merchants[self.rng.choice(home)]
        self._schedule(at, self._card_present(card_id, legit.merchant_id))
        clone = self.world.merchants[self.rng.choice(far_merchants)]
        category = CATEGORY_BY_MCC.get(clone.mcc, CATEGORIES[0])
        when = at + timedelta(minutes=self.rng.uniform(5, 90))
        self._schedule(
            when,
            self._fraud(
                card_id,
                clone.merchant_id,
                self._amount(category),
                "impossible_travel",
                card_present=True,
            ),  # fmt: skip
        )

    def _km(self, a: int, b: int) -> float:
        return haversine_km(CITIES[a].lat, CITIES[a].lon, CITIES[b].lat, CITIES[b].lon)

    def _card_present(self, card_id: int, merchant_id: int) -> Callable[[datetime], None]:
        """A legitimate in-person purchase by the card owner (not labelled)."""

        def run(at: datetime) -> None:
            merchant = self.world.merchants.get(merchant_id)
            if merchant is None or card_id not in self.world.cards:
                return
            category = CATEGORY_BY_MCC.get(merchant.mcc, CATEGORIES[0])
            jitter = self.rng.uniform(-0.001, 0.001)
            transaction_id = self.store.insert_transaction(
                NewTransaction(
                    card_id=card_id,
                    merchant_id=merchant_id,
                    session_id=None,
                    amount=self._amount(category),
                    currency=CURRENCY_BY_COUNTRY.get(merchant.country_code, "USD"),
                    channel="card_present",
                    lat=round(merchant.lat + jitter, 6),
                    lon=round(merchant.lon + jitter, 6),
                    event_ts=at,
                )
            )
            delay = self.rng.uniform(*self.rates.status_delay_s)
            self._schedule(at + timedelta(seconds=delay), self._finalise(transaction_id, card_id))

        return run

    def _attack_account_takeover(self, at: datetime) -> None:
        """Contact details changed, a new device logs in from abroad, then big online spend."""
        card_id = self._active_card()
        high_value = [m for m in self._live(self._online_merchants)
                      if self.world.merchants[m].mcc in HIGH_VALUE_MCCS]  # fmt: skip
        if card_id is None or not high_value:
            return
        cust = self.world.customers[self.world.cards[card_id].customer_id]
        email = f"{self.fake.user_name()}.{self.rng.randrange(10**6):06d}@example.net"
        self.store.update_customer(cust.customer_id, "email", email, at)
        login = at + timedelta(minutes=self.rng.uniform(10, 120))
        abroad = self.rng.choice([c for c in COUNTRIES if c != cust.country_code])
        device = f"dev-ato-{cust.customer_id}-{self.rng.randrange(10**6)}"
        risk = Decimal(str(round(self.rng.uniform(0.8, 0.99), 4)))

        def take_over(when: datetime) -> None:
            session_id = self.store.insert_session(
                cust.customer_id, device, self._ip(), abroad, when
            )
            spend = when
            # Choose merchants at login: one picked earlier may have closed meanwhile.
            still_open = self._live(high_value)
            if not still_open:
                return
            for merchant_id in self.rng.sample(
                still_open, min(len(still_open), self.rng.randint(2, 4))
            ):
                spend += timedelta(minutes=self.rng.uniform(1, 20))
                category = CATEGORY_BY_MCC.get(self.world.merchants[merchant_id].mcc, CATEGORIES[0])
                amount = Decimal(str(round(category.amount_median * self.rng.uniform(3, 8), 2)))
                self._schedule(
                    spend,
                    self._fraud(
                        card_id,
                        merchant_id,
                        amount.quantize(Decimal("0.01")),
                        "account_takeover",
                        session_id=session_id,
                        risk=risk,
                    ),  # fmt: skip
                )

        self._schedule(login, take_over)

    def _attack_merchant_bust_out(self, at: datetime) -> None:
        """A merchant takes a burst of high-ticket charges on many cards over 1 to 3 days,
        refunds a large share of them (the refund-rate spike), then disappears."""
        online = self._live(self._online_merchants)
        cards = [c.card_id for c in self.world.cards.values() if c.status == "active"]
        if not online or not cards:
            return
        merchant_id = self.rng.choice(online)
        category = CATEGORY_BY_MCC.get(self.world.merchants[merchant_id].mcc, CATEGORIES[0])
        span = timedelta(days=self.rng.uniform(1, 3))
        for _ in range(self.rng.randint(30, 80)):
            when = at + span * self.rng.random()
            amount = Decimal(str(round(category.amount_median * self.rng.uniform(2, 5), 2)))
            charge = self._fraud(
                self.rng.choice(cards),
                merchant_id,
                amount.quantize(Decimal("0.01")),
                "merchant_bust_out",
                reversal_probability=0.4,
            )
            self._schedule(when, charge)
        self._schedule(
            at + span + timedelta(hours=1), lambda when: self._remove_merchant(merchant_id, when)
        )
