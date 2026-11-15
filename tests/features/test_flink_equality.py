"""Feature-definition equality, online side (PRD 8.4): the generated Flink job, run in
real Flink, must produce exactly what the reference implementation computes.

Run with `make test-flink` (isolated environment with PyFlink; needs Java 17+).
"""

import json
import math
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from features.flink import CardJob
from features.model import load
from features.reference import Txn, card_features

pytestmark = pytest.mark.flink

T0 = datetime(2026, 1, 1, tzinfo=UTC)
TEL_AVIV, NEW_YORK, HAIFA = (32.0853, 34.7818), (40.7128, -74.0060), (32.7940, 34.9896)


def at(**delta: float) -> datetime:
    return T0 + timedelta(**delta)


# (transaction_id, card_id, merchant_id, amount, channel, location, event time, iso text)
# The iso text varies in fraction digits like Debezium's output does.
SCENARIO: list[tuple[int, int, int, str, str, tuple[float, float] | None, datetime, str]] = [
    (1, 1, 10, "10.00", "card_present", TEL_AVIV, at(), "2026-01-01T00:00:00Z"),
    (2, 2, 10, "99.99", "online", None, at(seconds=1), "2026-01-01T00:00:01.000999Z"),
    (3, 1, 11, "20.00", "online", None, at(seconds=30.5), "2026-01-01T00:00:30.5Z"),
    # exactly 60 s after #1: the 1-minute window [t-1m, t] includes it
    (4, 1, 10, "30.00", "card_present", NEW_YORK, at(minutes=1), "2026-01-01T00:01:00Z"),
    # 1 ms later: #1 falls out of the 1-minute window
    (5, 1, 12, "5.00", "online", None, at(minutes=1, milliseconds=1), "2026-01-01T00:01:00.001Z"),
    (6, 2, 13, "1.00", "card_present", HAIFA, at(minutes=5), "2026-01-01T00:05:00.123456Z"),
    # exactly 1 h after #1, and a tie at the same millisecond on the same card
    (7, 1, 14, "50.00", "card_present", TEL_AVIV, at(hours=1), "2026-01-01T01:00:00Z"),
    (8, 1, 15, "7.00", "online", None, at(hours=1), "2026-01-01T01:00:00.000400Z"),
    (9, 2, 13, "2.50", "card_present", HAIFA, at(hours=2), "2026-01-01T02:00:00Z"),
    # 24 h + 1 ms after #1
    (
        10,
        1,
        10,
        "12.00",
        "card_present",
        HAIFA,
        at(days=1, milliseconds=1),
        "2026-01-02T00:00:00.001Z",
    ),
    # 31 days later: alone in its 30-day window, so no prior mean
    (11, 1, 16, "80.00", "online", None, at(days=31), "2026-02-01T00:00:00Z"),
]


def event_ms(iso: str) -> int:
    ts = datetime.fromisoformat(iso)
    return int(ts.timestamp()) * 1000 + ts.microsecond // 1000


def debezium_line(row: tuple[Any, ...]) -> str:
    txn_id, card, merchant, amount, channel, loc, _, iso = row
    after = {
        "transaction_id": txn_id,
        "card_id": card,
        "merchant_id": merchant,
        "amount": float(amount),
        "channel": channel,
        "lat": loc[0] if loc else None,
        "lon": loc[1] if loc else None,
        "event_ts": iso,
    }
    commit_ms = event_ms(iso) + 250  # Debezium source.ts_ms: commit time
    return json.dumps({"op": "c", "before": None, "after": after, "source": {"ts_ms": commit_ms}})


@pytest.fixture(scope="module")
def flink_rows(tmp_path_factory: pytest.TempPathFactory) -> dict[int, dict[str, Any]]:
    pyflink_table = pytest.importorskip("pyflink.table")
    if shutil.which("java") is None:
        pytest.skip("Java 17+ is required to run Flink")
    data: Path = tmp_path_factory.mktemp("flink") / "transactions.json"
    # Include an update event: features must ignore everything but creations.
    lines = [debezium_line(r) for r in SCENARIO]
    lines.insert(3, json.dumps({"op": "u", "before": None, "after": json.loads(lines[0])["after"]}))
    data.write_text("\n".join(lines) + "\n")

    env = pyflink_table.TableEnvironment.create(
        pyflink_table.EnvironmentSettings.in_streaming_mode()
    )
    env.get_config().set("table.local-time-zone", "UTC")
    env.get_config().set("parallelism.default", "1")
    job = CardJob(load())
    env.execute_sql(
        job.source_ddl({"connector": "filesystem", "path": str(data), "format": "json"})
    )
    for view in job.views():
        env.execute_sql(view)
    result = env.execute_sql(job.select())
    names = result.get_resolved_schema().get_column_names()
    with result.collect() as results:
        rows = [dict(zip(names, list(r), strict=True)) for r in results]
    return {row["transaction_id"]: row for row in rows}


@pytest.fixture(scope="module")
def expected() -> dict[int, dict[str, object]]:
    txns = []
    for txn_id, card, merchant, amount, channel, loc, _, iso in SCENARIO:
        lat, lon = loc if loc else (None, None)
        txns.append(Txn(txn_id, card, merchant, Decimal(amount), channel, lat, lon, event_ms(iso)))
    return card_features(load(), txns)


def test_event_time_is_truncated_to_milliseconds(flink_rows: dict[int, dict[str, Any]]) -> None:
    for row in SCENARIO:
        assert flink_rows[row[0]]["event_ms"] == event_ms(row[7]), row[7]


def test_one_row_per_created_transaction(flink_rows: dict[int, dict[str, Any]]) -> None:
    assert sorted(flink_rows) == [r[0] for r in SCENARIO]


def test_commit_time_is_carried_through(flink_rows: dict[int, dict[str, Any]]) -> None:
    for row in SCENARIO:
        assert flink_rows[row[0]]["commit_ms"] == event_ms(row[7]) + 250


@pytest.mark.parametrize("feature", [f.name for f in load().for_entity("card")])
def test_flink_equals_reference(
    feature: str,
    flink_rows: dict[int, dict[str, Any]],
    expected: dict[int, dict[str, object]],
) -> None:
    for txn_id, want in expected.items():
        got = flink_rows[txn_id][feature]
        exp = want[feature]
        if isinstance(exp, float):
            assert got is not None and math.isclose(
                got, exp, rel_tol=1e-9, abs_tol=1e-9
            ), f"{feature} txn {txn_id}: flink={got} reference={exp}"
        else:
            assert got == exp, f"{feature} txn {txn_id}: flink={got!r} reference={exp!r}"
