"""Redis online feature store: one hash per card, `card:{card_id}` (FR-F3).

Writes go through an atomic Lua script that ignores a record older than what the hash
holds, by (event_ms, transaction_id). The feature job re-emits old records when it
rebuilds state, so most writes during a replay are no-ops by design.

For the same transaction, the last delivery wins: a later copy is either an identical
duplicate (at-least-once) or a recomputation by a newer job, which must replace an
earlier copy so that a replay can repair the store.
"""

import time
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


def card_key(card_id: int) -> str:
    return f"card:{card_id}"


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

    def get_card(self, card_id: int) -> CardFeatures | None:
        raw = cast(dict[str, str], self.client.hgetall(card_key(card_id)))
        if not raw:
            return None
        return CardFeatures(
            card_id=card_id,
            transaction_id=int(raw["transaction_id"]),
            event_ms=int(raw["event_ms"]),
            definitions_version=int(raw["definitions_version"]),
            computed_at=raw["computed_at"],
            written_at_ms=int(raw["written_at_ms"]),
            values={f.name: decode(raw.get(f.name, ""), f.dtype) for f in self.features},
        )
