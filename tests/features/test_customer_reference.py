"""Customer feature reference against values computed by hand (ADR-007)."""

from features.model import load
from features.reference import (
    CustomerTxn,
    ProfileChange,
    Session,
    customer_features,
)

H = 3_600_000
DAY = 24 * H
OWNERS = {10: 1, 20: 2}  # card -> customer


def run(txns: list[CustomerTxn], sessions: list[Session], changes: list[ProfileChange]):  # type: ignore[no-untyped-def]
    return customer_features(load(), txns, sessions, changes, OWNERS)


def test_new_device_window_is_inclusive_and_uses_first_sighting() -> None:
    sessions = [
        Session(1, 1, "phone", 0),  # first seen at 0
        Session(2, 1, "phone", 30 * H),  # same device, later session
        Session(3, 1, "laptop", 29 * H),  # a device first seen at 29 h
    ]
    out = run(
        [
            CustomerTxn(100, 10, 1, DAY),  # 24 h after first sighting: still new (inclusive)
            CustomerTxn(101, 10, 2, 30 * H),  # phone first seen 30 h ago: not new
            CustomerTxn(102, 10, 3, 29 * H + 5),  # laptop just appeared: new
            CustomerTxn(103, 10, None, 30 * H),  # card-present: no session
        ],
        sessions,
        [],
    )
    assert [out[i]["new_device_24h"] for i in (100, 101, 102, 103)] == [True, False, True, None]


def test_profile_change_counts_watched_columns_in_the_window() -> None:
    changes = [
        ProfileChange(1, frozenset({"email"}), 5 * H),
        ProfileChange(1, frozenset({"risk_tier"}), 40 * H),  # not watched
        ProfileChange(2, frozenset({"phone"}), 0),
    ]
    out = run(
        [
            CustomerTxn(200, 10, None, 5 * H),  # same instant as the change: included
            CustomerTxn(201, 10, None, 29 * H),  # exactly 24 h later: included
            CustomerTxn(202, 10, None, 29 * H + 1),  # 1 ms past the window
            CustomerTxn(203, 10, None, 41 * H),  # only a risk-tier change nearby
            CustomerTxn(204, 20, None, H),  # the other customer's phone change
            CustomerTxn(205, 10, None, 4 * H),  # change is in the future
            CustomerTxn(206, 99, None, H),  # unknown card owner
        ],
        [],
        changes,
    )
    assert [out[i]["profile_change_24h"] for i in range(200, 207)] == [
        True, True, False, False, True, False, None,
    ]  # fmt: skip
