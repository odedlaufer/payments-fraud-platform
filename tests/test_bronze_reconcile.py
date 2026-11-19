"""Comparison logic behind the bronze completeness check."""

import importlib.util
import sys
from collections import Counter
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "bronze_reconcile", ROOT / "scripts/bronze_reconcile.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["bronze_reconcile"] = module
    spec.loader.exec_module(module)
    return module


br = _load()


def topic(*events: tuple[int, int, str | None]) -> object:
    return br.topic_side((p, o, 1, None if op is None else {"op": op}) for p, o, op in events)


def bronze(*rows: tuple[int, int, str]) -> object:
    side = br.Side()
    for p, o, op in rows:
        side.rows += 1
        side.offsets.add((p, o))
        side.ops[op] += 1
    return side


def test_exact_match_ignores_tombstones() -> None:
    t = topic((0, 0, "c"), (0, 1, "d"), (0, 2, None))
    report = br.BronzeReport("merchants", t, bronze((0, 0, "c"), (0, 1, "d")))
    assert report.ok
    assert report.topic.ops == Counter({"c": 1, "d": 1})


def test_missing_event_fails() -> None:
    report = br.BronzeReport("x", topic((0, 0, "c"), (0, 1, "u")), bronze((0, 0, "c")))
    assert (report.missing, report.extra, report.duplicates) == (1, 0, 0)
    assert not report.ok


def test_duplicate_row_fails() -> None:
    report = br.BronzeReport("x", topic((0, 0, "c")), bronze((0, 0, "c"), (0, 0, "c")))
    assert report.duplicates == 1
    assert not report.ok


def test_extra_offset_fails() -> None:
    report = br.BronzeReport("x", topic((0, 0, "c")), bronze((0, 0, "c"), (0, 5, "c")))
    assert report.extra == 1
    assert not report.ok


def test_topic_side_records_end_offsets_including_tombstones() -> None:
    side = topic((0, 0, "c"), (0, 1, "d"), (0, 2, None))
    assert side.end == {0: 3}  # type: ignore[attr-defined]


class FakeCursor:
    def __init__(self, aged_out: int = 0) -> None:
        self.statements: list[str] = []
        self.aged_out = aged_out

    @property
    def sql(self) -> str:
        return self.statements[-1]

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def fetchone(self) -> tuple[int]:
        return (self.aged_out,)

    def fetchall(self) -> list[tuple[int, int, str]]:
        return [(0, 0, "c")]


def test_bronze_is_read_only_up_to_the_topic_snapshot() -> None:
    cur = FakeCursor()
    br.bronze_side(cur, "transactions", br.Side(end={0: 42}))
    assert "kafka_partition = 0 and kafka_offset >= 0 and kafka_offset < 42" in cur.sql
    assert br.bronze_side(FakeCursor(), "transactions", br.Side()).rows == 0


def test_rows_older_than_the_topic_are_counted_apart_not_compared() -> None:
    """After retention ages events out of Kafka, bronze still holds them: they are history
    kept only in bronze, not `extra` rows (docs/incidents/2026-11-18-kafka-retention.md)."""
    cur = FakeCursor(aged_out=72161)
    side = br.bronze_side(cur, "transactions", br.Side(end={0: 300221}, start={0: 72161}))
    assert side.aged_out == 72161
    assert "kafka_partition = 0 and kafka_offset < 72161" in cur.statements[0]
    assert "kafka_offset >= 72161 and kafka_offset < 300221" in cur.statements[-1]


def test_first_retained_offset_comes_from_the_topic() -> None:
    side = br.topic_side(
        [(0, 72161, None, {"op": "u"}), (0, 72162, None, None), (0, 72163, None, {"op": "c"})]
    )
    assert side.start == {0: 72161} and side.end == {0: 72164}
    assert side.rows == 2  # the tombstone is not a bronze row
