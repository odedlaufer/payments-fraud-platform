"""Pending transactions: decided when their snapshot arrives or at their deadline."""

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


def resolver(store: FakeStore) -> Resolver:
    return Resolver(store, 3_000)  # type: ignore[arg-type]


def test_waits_then_decides_when_the_snapshot_arrives() -> None:
    store = FakeStore()
    r = resolver(store)
    r.add(pending(1, 10))
    assert r.resolve(now=100) == []
    store.snapshots[1] = snap(1)
    [(p, s, features_ms)] = r.resolve(now=200)
    assert (p.txn.transaction_id, s, features_ms) == (1, store.snapshots[1], 200)
    assert not r.pending


def test_deadline_without_snapshot_is_stale() -> None:
    r = resolver(FakeStore())
    r.add(pending(1, 10, deadline=3_000))
    assert r.resolve(now=2_999) == []
    [(_, s, features_ms)] = r.resolve(now=3_000)
    assert s is None and features_ms is None


def test_a_slow_transaction_does_not_hold_back_others() -> None:
    store = FakeStore()
    r = resolver(store)
    r.add(pending(1, 10))
    r.add(pending(2, 11))
    store.snapshots[2] = snap(2)
    [(p, _, _)] = r.resolve(now=100)
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
    [(_, s, _)] = r.resolve(now=3_000)
    assert s is None


def test_offsets_are_committed_only_up_to_the_oldest_pending() -> None:
    r = resolver(FakeStore())
    consumed: dict[int, Any] = {0: 14}
    assert r.committable(consumed) == {0: 15}
    r.add(pending(1, 12))
    r.add(pending(2, 13))
    assert r.committable(consumed) == {0: 12}
