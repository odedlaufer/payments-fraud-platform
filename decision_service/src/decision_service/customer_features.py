"""Customer features computed on demand at decision time (ADR-007).

They are lookups over online state rather than aggregations: the first time a device was
seen for a customer, and the times a customer's watched profile columns changed. The
feature sink keeps that state in Redis from CDC (cards, sessions, customers); this module
turns it into feature values for one transaction, with the same inclusive windows as the
reference implementation (features.reference.customer_features).

A transaction is ready when its card owner is known and, for an online transaction, its
session has been seen. Until then the decision waits, like it waits for card features.
"""

from dataclasses import dataclass
from typing import Any, Protocol

from features.model import Definitions, Feature

DAY_MS = 86_400_000


class CustomerState(Protocol):
    def owners(self, card_ids: list[int]) -> dict[int, int]: ...
    def sessions(self, session_ids: list[int]) -> dict[int, tuple[int, str, int]]: ...
    def first_seen(self, feature: str, pairs: list[tuple[int, str]]) -> list[int | None]: ...
    def changes_between(self, feature: str, queries: list[tuple[int, int, int]]) -> list[int]: ...


@dataclass(frozen=True)
class Ask:
    transaction_id: int
    card_id: int
    session_id: int | None
    event_ms: int


def _window_ms(f: Feature) -> int:
    delta = f.window_delta
    assert delta is not None
    return int(delta.total_seconds() * 1000)


def customer_values(
    defs: Definitions, state: CustomerState, asks: list[Ask]
) -> dict[int, dict[str, Any]]:
    """Feature values for every transaction whose customer context is ready (two round trips)."""
    features = defs.for_entity("customer")
    owners = state.owners([a.card_id for a in asks])
    sessions = state.sessions([a.session_id for a in asks if a.session_id is not None])
    ready = [
        a
        for a in asks
        if a.card_id in owners and (a.session_id is None or a.session_id in sessions)
    ]
    out: dict[int, dict[str, Any]] = {a.transaction_id: {} for a in ready}
    for f in features:
        lo = {a.transaction_id: a.event_ms - _window_ms(f) for a in ready}
        if f.kind == "first_seen_within":
            with_session = [a for a in ready if a.session_id is not None]
            pairs = [(sessions[a.session_id][0], sessions[a.session_id][1]) for a in with_session]  # type: ignore[index]
            for a, seen in zip(with_session, state.first_seen(f.name, pairs), strict=True):
                # Never None in practice: the sink records the device before the session.
                out[a.transaction_id][f.name] = (
                    seen is not None and lo[a.transaction_id] <= seen <= a.event_ms
                )
            for a in ready:
                if a.session_id is None:
                    out[a.transaction_id][f.name] = None
        elif f.kind == "changed_within":
            queries = [(owners[a.card_id], lo[a.transaction_id], a.event_ms) for a in ready]
            for a, n in zip(ready, state.changes_between(f.name, queries), strict=True):
                out[a.transaction_id][f.name] = n > 0
        else:
            raise NotImplementedError(f"{f.name}: kind {f.kind}")
    return out


# Change times are pruned relative to the newest one recorded. Replays deliver events in
# any order, so the margin must cover any backlog a decision could still be asked about.
RETAIN_MS = 30 * DAY_MS


def keep_ms(f: Feature) -> int:
    """How long the sink keeps change times for a changed_within feature."""
    return _window_ms(f) + RETAIN_MS
