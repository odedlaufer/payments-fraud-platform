"""Online store against a real Redis: older records never win, replays repair (FR-F3).

Uses database 15 so the live store (database 0) is never touched.
"""

import random
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest
import redis

from decision_service.online_store import OnlineStore, card_key
from features.model import load

pytestmark = pytest.mark.integration


@pytest.fixture
def store(env: dict[str, str]) -> Iterator[OnlineStore]:
    client = redis.Redis(
        host="127.0.0.1", port=int(env.get("REDIS_PORT", "6379")), db=15, decode_responses=True
    )
    client.flushdb()
    yield OnlineStore(client, load())
    client.flushdb()


def record(txn: int, ms: int, count_1m: int = 1, ratio: float | None = None) -> dict[str, Any]:
    values: dict[str, Any] = {f.name: None for f in load().for_entity("card")}
    values.update(
        txn_count_1m=count_1m,
        txn_count_1h=count_1m,
        txn_count_24h=count_1m,
        txn_amount_sum_1h=Decimal("10.00"),
        txn_amount_sum_24h=Decimal("10.00"),
        distinct_merchants_1h=1,
        amount_vs_avg_30d=ratio,
    )
    return {
        "card_id": 42,
        "transaction_id": txn,
        "event_ms": ms,
        "definitions_version": 1,
        "computed_at": "2026-01-01T00:00:00.000Z",
        **values,
    }


def test_first_write_is_stored_and_read_back(store: OnlineStore) -> None:
    assert store.upsert_card(record(1, 1000, count_1m=3, ratio=1 / 3), written_at_ms=5)
    card = store.get_card(42)
    assert card is not None
    assert (card.transaction_id, card.event_ms, card.written_at_ms) == (1, 1000, 5)
    assert card.values["txn_count_1m"] == 3
    assert card.values["amount_vs_avg_30d"] == 1 / 3  # exact float round trip
    assert card.values["km_from_last_txn"] is None
    assert card.values["txn_amount_sum_1h"] == Decimal("10.00")


@pytest.mark.parametrize(
    ("second", "applied"),
    [
        ((1, 1000), True),  # same transaction again: the last delivery wins
        ((0, 999), False),  # older event time (replay)
        ((2, 1001), True),  # newer event time
        ((2, 1000), True),  # same millisecond, later transaction
        ((0, 1000), False),  # same millisecond, earlier transaction
    ],
)
def test_older_records_never_replace_the_stored_one(
    store: OnlineStore, second: tuple[int, int], applied: bool
) -> None:
    store.upsert_card(record(1, 1000, count_1m=1))
    txn, ms = second
    assert store.upsert_card(record(txn, ms, count_1m=9)) is applied
    card = store.get_card(42)
    assert card is not None
    assert card.values["txn_count_1m"] == (9 if applied else 1)


def test_any_replay_order_ends_with_the_newest(store: OnlineStore) -> None:
    records = [record(i, 1000 + i, count_1m=i) for i in range(1, 51)]
    random.Random(7).shuffle(records)
    for r in records + records:  # shuffled, then everything delivered twice
        store.upsert_card(r)
    card = store.get_card(42)
    assert card is not None
    assert (card.transaction_id, card.values["txn_count_1m"]) == (50, 50)


def test_missing_card_is_none(store: OnlineStore) -> None:
    assert store.get_card(999) is None
    assert store.client.exists(card_key(999)) == 0


def test_a_recomputed_copy_repairs_an_earlier_wrong_one(store: OnlineStore) -> None:
    """A feature job that lost its state emits a wrong record; its replay emits the same
    transaction again with correct values. The store must end with the correct copy."""
    store.upsert_card(record(7, 5000, count_1m=1))  # computed without history
    store.upsert_card(record(7, 5000, count_1m=4))  # replayed with history
    card = store.get_card(42)
    assert card is not None and card.values["txn_count_1m"] == 4


def test_transaction_snapshots_expire_and_are_read_in_one_round_trip(store: OnlineStore) -> None:
    store.put_transaction(record(7, 5000, count_1m=2, ratio=0.25), written_at_ms=9)
    store.put_transaction(record(8, 5001, count_1m=3))
    found = store.get_transactions([7, 8, 999])
    assert sorted(found) == [7, 8]
    assert found[7].transaction_id == 7 and found[7].values["txn_count_1m"] == 2
    assert found[7].values["amount_vs_avg_30d"] == 0.25 and found[7].card_id == 42
    ttl = store.client.ttl("txn:7")
    assert 0 < int(str(ttl)) <= 120


def test_decided_markers(store: OnlineStore) -> None:
    assert store.decided([1, 2]) == set()
    store.mark_decided([1, 2])
    store.mark_decided([2])  # idempotent
    assert store.decided([1, 2, 3]) == {1, 2}
