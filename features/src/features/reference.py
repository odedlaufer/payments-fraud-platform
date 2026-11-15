"""Reference implementation of the features, straight from the definitions.

Plain Python with no streaming concerns: the oracle that the generated Flink job
(and later the dbt offline model) must agree with, feature by feature (PRD 8.4).
"""

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from features.model import Definitions, Feature

EARTH_RADIUS_KM = 6371.0088  # same constant as the simulator and the generated SQL


@dataclass(frozen=True)
class Txn:
    """One transaction at creation. event_ms: event_ts truncated to milliseconds."""

    transaction_id: int
    card_id: int
    merchant_id: int
    amount: Decimal
    channel: str
    lat: float | None
    lon: float | None
    event_ms: int


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    a = (
        math.sin(math.radians(lat2 - lat1) / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _window_ms(feature: Feature) -> int:
    delta = feature.window_delta
    assert delta is not None
    return int(delta.total_seconds() * 1000)


def card_features(defs: Definitions, txns: Iterable[Txn]) -> dict[int, dict[str, object]]:
    """Feature values per transaction_id. Input order = arrival order (ties in ROWS frames).

    Work is grouped by card, so cost grows with transactions per card, not in total.
    """
    by_card: dict[int, list[Txn]] = defaultdict(list)
    for t in txns:
        by_card[t.card_id].append(t)
    result: dict[int, dict[str, object]] = {}
    for card_txns in by_card.values():
        for i, t in enumerate(card_txns):
            values: dict[str, object] = {}
            for f in defs.for_entity("card"):
                window: list[Txn] = []
                if f.window is not None:
                    lo = t.event_ms - _window_ms(f)
                    window = [o for o in card_txns if lo <= o.event_ms <= t.event_ms]
                if f.kind == "window_count":
                    values[f.name] = len(window)
                elif f.kind == "window_sum":
                    total = sum((getattr(o, str(f.field)) for o in window), Decimal(0))
                    values[f.name] = Decimal(total).quantize(Decimal("0.01"))
                elif f.kind == "window_count_distinct":
                    values[f.name] = len({getattr(o, str(f.field)) for o in window})
                elif f.kind == "ratio_to_prior_mean":
                    prior = [o for o in window if o is not t]
                    if prior:
                        mean = sum(float(getattr(o, str(f.field))) for o in prior) / len(prior)
                        values[f.name] = float(getattr(t, str(f.field))) / mean
                    else:
                        values[f.name] = None
                elif f.kind in ("distance_from_previous", "minutes_since_previous"):
                    values[f.name] = _previous_card_present(f, i, card_txns)
                else:
                    raise NotImplementedError(f"{f.name}: kind {f.kind} is not a card feature")
            result[t.transaction_id] = values
    return result


def _previous_card_present(f: Feature, i: int, card_txns: list[Txn]) -> float | None:
    t = card_txns[i]
    if t.channel != "card_present":
        return None
    # The card's previous card-present transaction: latest by event time, ties by arrival.
    earlier = [
        (o.event_ms, j)
        for j, o in enumerate(card_txns)
        if j != i
        and o.channel == "card_present"
        and (o.event_ms < t.event_ms or (o.event_ms == t.event_ms and j < i))
    ]
    if not earlier:
        return None
    prev = card_txns[max(earlier)[1]]
    if f.kind == "minutes_since_previous":
        return (t.event_ms - prev.event_ms) / 60000.0
    assert t.lat is not None and t.lon is not None and prev.lat is not None and prev.lon is not None
    return haversine_km(prev.lat, prev.lon, t.lat, t.lon)


# ------------------------------------------------------------------ customer features


@dataclass(frozen=True)
class CustomerTxn:
    """What the customer features need of a transaction. event_ms: truncated event time."""

    transaction_id: int
    card_id: int
    session_id: int | None
    event_ms: int


@dataclass(frozen=True)
class Session:
    session_id: int
    customer_id: int
    device_id: str
    event_ms: int


@dataclass(frozen=True)
class ProfileChange:
    """An update to a customer row; `columns` are the ones whose value changed."""

    customer_id: int
    columns: frozenset[str]
    event_ms: int


def customer_features(
    defs: Definitions,
    txns: Iterable[CustomerTxn],
    sessions: Iterable[Session],
    changes: Iterable[ProfileChange],
    card_owner: dict[int, int],
) -> dict[int, dict[str, object]]:
    """Customer feature values per transaction_id (ADR-007 semantics, inclusive windows)."""
    by_id = {s.session_id: s for s in sessions}
    first_seen: dict[tuple[int, str], int] = {}
    for s in by_id.values():
        key = (s.customer_id, s.device_id)
        first_seen[key] = min(first_seen.get(key, s.event_ms), s.event_ms)
    change_list = list(changes)
    result: dict[int, dict[str, object]] = {}
    for t in txns:
        values: dict[str, object] = {}
        for f in defs.for_entity("customer"):
            lo = t.event_ms - _window_ms(f)
            if f.kind == "first_seen_within":
                session = by_id.get(t.session_id) if t.session_id is not None else None
                if session is None:
                    values[f.name] = None  # card-present, or no customer session
                else:
                    seen = first_seen[(session.customer_id, session.device_id)]
                    values[f.name] = lo <= seen <= t.event_ms
            elif f.kind == "changed_within":
                customer = card_owner.get(t.card_id)
                watched = frozenset(f.columns)
                values[f.name] = (
                    None
                    if customer is None
                    else any(
                        c.customer_id == customer
                        and c.columns & watched
                        and lo <= c.event_ms <= t.event_ms
                        for c in change_list
                    )
                )
            else:
                raise NotImplementedError(f"{f.name}: kind {f.kind} is not a customer feature")
        result[t.transaction_id] = values
    return result
