"""Redis online feature store: one hash per card, `card:{card_id}` (FR-F3).

Writes go through an atomic Lua script that ignores a record older than what the hash
holds, by (event_ms, transaction_id). The feature job re-emits old records when it
rebuilds state, so most writes during a replay are no-ops by design.

For the same transaction, the last delivery wins: a later copy is either an identical
duplicate (at-least-once) or a recomputation by a newer job, which must replace an
earlier copy so that a replay can repair the store.
"""

import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

import redis

from features.model import Definitions

# KEYS[1] = card:{id}; ARGV[1] = event_ms; ARGV[2] = transaction_id; ARGV[3..] = field, value
NEWER_ONLY = """
local current_ms = tonumber(redis.call('HGET', KEYS[1], 'event_ms') or '-1')
local current_tx = tonumber(redis.call('HGET', KEYS[1], 'transaction_id') or '-1')
local ms, tx = tonumber(ARGV[1]), tonumber(ARGV[2])
if ms < current_ms or (ms == current_ms and tx < current_tx) then
  return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 3))
return 1
"""

META = ("transaction_id", "event_ms", "definitions_version", "computed_at", "written_at_ms")


# Per-transaction snapshots live just long enough for the decision service to read them.
TRANSACTION_TTL_S = 120
DECIDED_TTL_S = 600
SESSION_TTL_S = 3 * 86_400  # a session is only looked up by transactions made during it

# KEYS[1] = first:{feature}:{customer}; ARGV[1] = device; ARGV[2] = event_ms. Keeps the minimum.
EARLIEST = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if (not current) or tonumber(ARGV[2]) < tonumber(current) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
return 0
"""


def card_key(card_id: int) -> str:
    return f"card:{card_id}"


def transaction_key(transaction_id: int) -> str:
    return f"txn:{transaction_id}"


def decided_key(transaction_id: int) -> str:
    return f"decided:{transaction_id}"


def encode(value: object) -> str:
    """Hash values are strings; '' means null. repr() keeps floats exact."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return repr(value)
    return str(value)


def decode(text: str, dtype: str) -> object:
    if text == "":
        return None
    if dtype == "bigint":
        return int(text)
    if dtype == "double":
        return float(text)
    if dtype == "boolean":
        return text == "true"
    if dtype.startswith("decimal"):
        return Decimal(text)
    raise ValueError(f"unknown dtype {dtype}")


@dataclass(frozen=True)
class CardFeatures:
    card_id: int
    transaction_id: int  # the latest transaction these features include
    event_ms: int
    definitions_version: int
    computed_at: str
    written_at_ms: int
    values: dict[str, object]


class OnlineStore:
    def __init__(self, client: redis.Redis, defs: Definitions) -> None:
        self.client = client
        self.features = defs.for_entity("card")
        self._newer_only = client.register_script(NEWER_ONLY)
        self._earliest = client.register_script(EARLIEST)

    def fields(self, record: dict[str, Any], written_at_ms: int) -> list[str]:
        pairs: list[str] = []
        for f in self.features:
            pairs += [f.name, encode(record[f.name])]
        pairs += [
            "transaction_id", encode(record["transaction_id"]),
            "event_ms", encode(record["event_ms"]),
            "definitions_version", encode(record["definitions_version"]),
            "computed_at", encode(record["computed_at"]),
            "written_at_ms", str(written_at_ms),
        ]  # fmt: skip
        return pairs

    def upsert_card(self, record: dict[str, Any], written_at_ms: int | None = None) -> bool:
        """True if the record is stored (not older than the current one)."""
        stamp = written_at_ms if written_at_ms is not None else int(time.time() * 1000)
        result = self._newer_only(
            keys=[card_key(int(record["card_id"]))],
            args=[record["event_ms"], record["transaction_id"], *self.fields(record, stamp)],
        )
        return bool(result)

    def put_transaction(self, record: dict[str, Any], written_at_ms: int | None = None) -> None:
        """The exact feature snapshot of one transaction, for its decision (no race with
        later transactions on the same card, which replace card:{id})."""
        stamp = written_at_ms if written_at_ms is not None else int(time.time() * 1000)
        pairs = self.fields(record, stamp)
        payload = {
            **dict(zip(pairs[::2], pairs[1::2], strict=True)),
            "card_id": str(record["card_id"]),
        }
        self.client.set(
            transaction_key(int(record["transaction_id"])),
            json.dumps(payload),
            ex=TRANSACTION_TTL_S,
        )

    def get_transactions(self, transaction_ids: Iterable[int]) -> dict[int, CardFeatures]:
        """Snapshots that have arrived, by transaction_id (one round trip)."""
        ids = list(transaction_ids)
        if not ids:
            return {}
        found: dict[int, CardFeatures] = {}
        raws = cast(list[str | None], self.client.mget([transaction_key(i) for i in ids]))
        for txn_id, raw in zip(ids, raws, strict=True):
            if raw is not None:
                found[txn_id] = self._features(json.loads(raw), int(json.loads(raw)["card_id"]))
        return found

    def decided(self, transaction_ids: Iterable[int]) -> set[int]:
        ids = list(transaction_ids)
        if not ids:
            return set()
        marks = self.client.mget([decided_key(i) for i in ids])
        return {i for i, mark in zip(ids, marks, strict=True) if mark is not None}

    def mark_decided(self, transaction_ids: Iterable[int]) -> None:
        pipe = self.client.pipeline(transaction=False)
        for i in transaction_ids:
            pipe.set(decided_key(i), "1", ex=DECIDED_TTL_S, nx=True)
        pipe.execute()

    def _features(self, raw: dict[str, str], card_id: int) -> CardFeatures:
        return CardFeatures(
            card_id=card_id,
            transaction_id=int(raw["transaction_id"]),
            event_ms=int(raw["event_ms"]),
            definitions_version=int(raw["definitions_version"]),
            computed_at=raw["computed_at"],
            written_at_ms=int(raw["written_at_ms"]),
            values={f.name: decode(raw.get(f.name, ""), f.dtype) for f in self.features},
        )

    # ------------------------------------------------- customer context (ADR-007)

    def set_owner(self, card_id: int, customer_id: int) -> None:
        self.client.set(f"owner:{card_id}", str(customer_id))

    def put_session(self, session_id: int, customer_id: int, device_id: str, event_ms: int) -> None:
        key = f"session:{session_id}"
        pipe = self.client.pipeline(transaction=False)
        pipe.hset(key, mapping={"customer": customer_id, "device": device_id, "ms": event_ms})
        pipe.expire(key, SESSION_TTL_S)
        pipe.execute()

    def see_device(self, feature: str, customer_id: int, device_id: str, event_ms: int) -> None:
        self._earliest(keys=[f"first:{feature}:{customer_id}"], args=[device_id, event_ms])

    def add_change(self, feature: str, customer_id: int, event_ms: int, keep_ms: int) -> None:
        key = f"chg:{feature}:{customer_id}"
        pipe = self.client.pipeline(transaction=False)
        pipe.zadd(key, {str(event_ms): event_ms})
        pipe.zremrangebyscore(key, "-inf", event_ms - keep_ms)  # older changes can't matter
        pipe.execute()

    def owners(self, card_ids: list[int]) -> dict[int, int]:
        if not card_ids:
            return {}
        raws = self.client.mget([f"owner:{c}" for c in card_ids])
        return {c: int(r) for c, r in zip(card_ids, raws, strict=True) if r is not None}

    def sessions(self, session_ids: list[int]) -> dict[int, tuple[int, str, int]]:
        """session_id -> (customer_id, device_id, event_ms), for sessions already seen."""
        pipe = self.client.pipeline(transaction=False)
        for sid in session_ids:
            pipe.hgetall(f"session:{sid}")
        found = {}
        for sid, raw in zip(session_ids, pipe.execute(), strict=True):
            if raw:
                found[sid] = (int(raw["customer"]), str(raw["device"]), int(raw["ms"]))
        return found

    def first_seen(self, feature: str, pairs: list[tuple[int, str]]) -> list[int | None]:
        pipe = self.client.pipeline(transaction=False)
        for customer_id, device_id in pairs:
            pipe.hget(f"first:{feature}:{customer_id}", device_id)
        return [None if v is None else int(v) for v in pipe.execute()]

    def changes_between(self, feature: str, queries: list[tuple[int, int, int]]) -> list[int]:
        """For each (customer_id, lo_ms, hi_ms): changes with lo <= event_ms <= hi."""
        pipe = self.client.pipeline(transaction=False)
        for customer_id, lo, hi in queries:
            pipe.zcount(f"chg:{feature}:{customer_id}", lo, hi)
        return [int(n) for n in pipe.execute()]

    def get_card(self, card_id: int) -> CardFeatures | None:
        raw = cast(dict[str, str], self.client.hgetall(card_key(card_id)))
        return self._features(raw, card_id) if raw else None
