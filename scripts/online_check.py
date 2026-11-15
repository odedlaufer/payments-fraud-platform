"""Online store check (FR-F3): for every card, Redis holds exactly the newest
features.card record by (event_ms, transaction_id).

    uv run python scripts/online_check.py
"""

import sys
from typing import Any

import redis

from cdc_reconcile import load_env
from decision_service.online_store import OnlineStore
from features.model import load
from features_check import read_features


def order(record: dict[str, Any]) -> tuple[int, int]:
    return int(record["event_ms"]), int(record["transaction_id"])


def main() -> int:
    env = load_env()
    defs = load()
    newest: dict[int, dict[str, Any]] = {}
    for record in read_features(env).values():
        card = int(record["card_id"])
        if card not in newest or order(record) > order(newest[card]):
            newest[card] = record
    client = redis.Redis(
        host="127.0.0.1", port=int(env.get("REDIS_PORT", "6379")), decode_responses=True
    )
    store = OnlineStore(client, defs)
    names = [f.name for f in defs.for_entity("card")]
    missing: list[int] = []
    stale: list[str] = []
    wrong: list[int] = []
    for card_id, record in newest.items():
        stored = store.get_card(card_id)
        if stored is None:
            missing.append(card_id)
        elif (stored.event_ms, stored.transaction_id) != order(record):
            stale.append(
                f"card {card_id}: redis has txn {stored.transaction_id}, "
                f"newest is {record['transaction_id']}"
            )
        elif any(stored.values[n] != record[n] for n in names):
            wrong.append(card_id)
    print(
        f"{len(newest)} cards: missing={len(missing)} stale={len(stale)} "
        f"wrong_values={len(wrong)}"
    )
    for line in stale[:5]:
        print(f"  {line}")
    ok = not (missing or stale or wrong)
    print("online store matches features.card" if ok else "ONLINE STORE MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
