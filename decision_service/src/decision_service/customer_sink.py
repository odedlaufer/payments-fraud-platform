"""CDC events -> customer context in the online store (ADR-007).

Pure translation, called by feature-sink for pg.public.cards, pg.public.sessions and
pg.public.customers. Every update is idempotent (set, atomic minimum, sorted-set add), so
replays and duplicate deliveries leave the same state.
"""

from typing import Any, Protocol

from decision_service.customer_features import keep_ms
from features.model import Definitions
from features.timeparse import iso_to_ms

CDC_TOPICS = ("pg.public.cards", "pg.public.sessions", "pg.public.customers")


class CustomerSink(Protocol):
    def set_owner(self, card_id: int, customer_id: int) -> None: ...
    def put_session(
        self, session_id: int, customer_id: int, device_id: str, event_ms: int
    ) -> None: ...
    def see_device(self, feature: str, customer_id: int, device_id: str, event_ms: int) -> None: ...
    def add_change(self, feature: str, customer_id: int, event_ms: int, keep_ms: int) -> None: ...


def apply_cdc(defs: Definitions, store: CustomerSink, topic: str, event: dict[str, Any]) -> None:
    op, after, before = event.get("op"), event.get("after"), event.get("before")
    if after is None or op not in ("c", "r", "u"):
        return  # deletes and tombstones carry nothing the features use
    features = defs.for_entity("customer")
    if topic == "pg.public.cards":
        store.set_owner(int(after["card_id"]), int(after["customer_id"]))
    elif topic == "pg.public.sessions" and op in ("c", "r"):
        customer, ms = int(after["customer_id"]), iso_to_ms(str(after["event_ts"]))
        store.put_session(int(after["session_id"]), customer, str(after["device_id"]), ms)
        for f in features:
            if f.kind == "first_seen_within" and f.source == "sessions":
                store.see_device(f.name, customer, str(after[str(f.field)]), ms)
    elif topic == "pg.public.customers" and op == "u" and before is not None:
        for f in features:
            if f.kind == "changed_within" and any(before[c] != after[c] for c in f.columns):
                store.add_change(f.name, int(after["customer_id"]),
                                 iso_to_ms(str(after["event_ts"])), keep_ms(f))  # fmt: skip
