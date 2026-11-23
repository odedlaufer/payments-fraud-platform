"""Pending transactions: decided when their snapshot arrives or at their deadline."""

from dataclasses import replace
from decimal import Decimal
from typing import Any

import redis

from decision_service.context import CARD_FEATURES
from decision_service.decision import Transaction
from decision_service.online_store import CardFeatures
from decision_service.service import Pending, Resolver


class FakeStore:
    def __init__(self) -> None:
        self.snapshots: dict[int, CardFeatures] = {}
        self.decided_ids: set[int] = set()
        self.down = False

    def decided(self, ids: list[int]) -> set[int]:
        if self.down:
            raise redis.ConnectionError("down")
        return {i for i in ids if i in self.decided_ids}

    def get_transactions(self, ids: list[int]) -> dict[int, CardFeatures]:
        if self.down:
            raise redis.ConnectionError("down")
        return {i: self.snapshots[i] for i in ids if i in self.snapshots}


def pending(txn_id: int, offset: int, deadline: int = 3_000) -> Pending:
    txn = Transaction(txn_id, 1, 1, Decimal("1"), "online", None, 0, 0)
    return Pending(txn, 0, offset, 0, deadline)


def snap(txn_id: int) -> CardFeatures:
    return CardFeatures(1, txn_id, 0, 1, "", 0, {n: None for n in CARD_FEATURES})


CUSTOMER = {"new_device_24h": None, "profile_change_24h": False}


def resolver(store: FakeStore, customer_ready: bool = True) -> Resolver:
    def customers(asks: list[Any]) -> dict[int, dict[str, Any]]:
        if store.down:
            raise redis.ConnectionError("down")
        return {a.transaction_id: CUSTOMER for a in asks} if customer_ready else {}

    return Resolver(store, 3_000, customers)  # type: ignore[arg-type]


def test_waits_then_decides_when_the_snapshot_arrives() -> None:
    store = FakeStore()
    r = resolver(store)
    r.add(pending(1, 10))
    assert r.resolve(now=100) == []
    store.snapshots[1] = snap(1)
    [(p, s, c, features_ms)] = r.resolve(now=200)
    assert (p.txn.transaction_id, s, c, features_ms) == (1, store.snapshots[1], CUSTOMER, 200)
    assert not r.pending


def test_deadline_without_snapshot_is_stale() -> None:
    r = resolver(FakeStore())
    r.add(pending(1, 10, deadline=3_000))
    assert r.resolve(now=2_999) == []
    [(_, s, c, features_ms)] = r.resolve(now=3_000)
    assert s is None and c is None and features_ms is None


def test_a_slow_transaction_does_not_hold_back_others() -> None:
    store = FakeStore()
    r = resolver(store)
    r.add(pending(1, 10))
    r.add(pending(2, 11))
    store.snapshots[2] = snap(2)
    [(p, _, _, _)] = r.resolve(now=100)
    assert p.txn.transaction_id == 2 and list(r.pending) == [1]


def test_already_decided_is_skipped() -> None:
    store = FakeStore()
    store.decided_ids.add(1)
    r = resolver(store)
    r.add(pending(1, 10))
    assert r.resolve(now=100) == [] and not r.pending and r.skipped_duplicates == 1


def test_redis_down_means_stale_at_the_deadline_not_a_crash() -> None:
    store = FakeStore()
    store.down = True
    r = resolver(store)
    r.add(pending(1, 10))
    assert r.resolve(now=100) == []
    [(_, s, _, _)] = r.resolve(now=3_000)
    assert s is None


def test_offsets_are_committed_only_up_to_the_oldest_pending() -> None:
    r = resolver(FakeStore())
    source = ("pg.public.transactions", 0)
    consumed: dict[tuple[str, int], int] = {source: 14}
    assert r.committable(consumed) == {source: 15}
    r.add(pending(1, 12))
    r.add(pending(2, 13))
    assert r.committable(consumed) == {source: 12}


def test_offsets_are_tracked_per_topic() -> None:
    """A replayed event pending on the retry topic holds back only the retry topic."""
    r = resolver(FakeStore())
    source, retry = ("pg.public.transactions", 0), ("retry.decision-service", 0)
    consumed = {source: 20, retry: 5}
    r.add(replace(pending(1, 4), topic=retry[0]))
    assert r.committable(consumed) == {source: 21, retry: 4}


def test_waits_for_customer_context_then_stale_at_the_deadline() -> None:
    store = FakeStore()
    store.snapshots[1] = snap(1)
    r = resolver(store, customer_ready=False)
    r.add(pending(1, 10))
    assert r.resolve(now=100) == []  # card snapshot alone is not enough
    [(_, s, c, features_ms)] = r.resolve(now=3_000)
    assert s is not None and c is None and features_ms is None
