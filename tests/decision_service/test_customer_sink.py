"""CDC events -> customer context calls (no Redis)."""

from typing import Any

from decision_service.customer_sink import apply_cdc
from features.model import load


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def set_owner(self, *a: Any) -> None:
        self.calls.append(("owner", *a))

    def put_session(self, *a: Any) -> None:
        self.calls.append(("session", *a))

    def see_device(self, *a: Any) -> None:
        self.calls.append(("device", *a))

    def add_change(self, *a: Any) -> None:
        self.calls.append(("change", *a[:3]))


def run(topic: str, event: dict[str, Any]) -> list[tuple[Any, ...]]:
    rec = Recorder()
    apply_cdc(load(), rec, topic, event)
    return rec.calls


def customer(**cols: str) -> dict[str, Any]:
    row = {"customer_id": 7, "email": "a@x", "phone": "1", "address_line": "x", "risk_tier": "low",
           "event_ts": "2026-01-01T00:00:01.500Z"}  # fmt: skip
    row.update(cols)
    return row


def test_card_owner() -> None:
    assert run("pg.public.cards", {"op": "c", "after": {"card_id": 3, "customer_id": 7}}) == [
        ("owner", 3, 7)
    ]


def test_session_records_the_session_and_the_device_sighting() -> None:
    after = {
        "session_id": 9,
        "customer_id": 7,
        "device_id": "d1",
        "event_ts": "2026-01-01T00:00:02Z",
    }
    assert run("pg.public.sessions", {"op": "c", "after": after}) == [
        ("session", 9, 7, "d1", 1767225602000),
        ("device", "new_device_24h", 7, "d1", 1767225602000),
    ]


def test_only_watched_profile_changes_count() -> None:
    email = {"op": "u", "before": customer(), "after": customer(email="b@x")}
    tier = {"op": "u", "before": customer(), "after": customer(risk_tier="high")}
    assert run("pg.public.customers", email) == [("change", "profile_change_24h", 7, 1767225601500)]
    assert run("pg.public.customers", tier) == []


def test_deletes_and_tombstones_are_ignored() -> None:
    assert run("pg.public.cards", {"op": "d", "before": {"card_id": 3}, "after": None}) == []
    assert run("pg.public.customers", {"op": "c", "after": customer()}) == []
