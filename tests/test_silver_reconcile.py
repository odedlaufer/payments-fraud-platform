"""Comparison rules behind DQ-08 (Postgres vs silver)."""

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "silver_reconcile", ROOT / "scripts/silver_reconcile.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["silver_reconcile"] = module
    spec.loader.exec_module(module)
    return module


sr = _load()
W = datetime(2026, 1, 1, tzinfo=UTC)


def run(
    pg: dict[int, dict[str, Any]],
    silver: dict[int, dict[str, Any]],
    in_flight: set[int] | None = None,
    deleted: set[int] | None = None,
) -> Any:
    return sr.compare("t", "id", ("status",), pg, in_flight or set(), silver, deleted or set(), W)


def test_identical_is_ok() -> None:
    report = run({1: {"status": "a"}}, {1: {"status": "a"}})
    assert report.ok and report.compared == 1


def test_value_mismatch_fails() -> None:
    report = run({1: {"status": "a"}}, {1: {"status": "b"}})
    assert not report.ok and len(report.mismatches) == 1


def test_missing_in_silver_fails() -> None:
    assert run({1: {"status": "a"}}, {}).missing == [1]


def test_recent_missing_or_changed_rows_are_tolerated_as_in_flight() -> None:
    pg = {1: {"status": "a"}, 2: {"status": "new"}, 3: {"status": "b"}}
    report = run(pg, {1: {"status": "a"}, 3: {"status": "old"}}, in_flight={2, 3})
    assert report.ok and report.in_flight == 2 and report.compared == 1


def test_recent_rows_that_already_match_are_still_compared() -> None:
    report = run({1: {"status": "a"}}, {1: {"status": "a"}}, in_flight={1})
    assert report.ok and report.compared == 1 and report.in_flight == 0


def test_extra_in_silver_fails_unless_deleted_after_watermark() -> None:
    assert run({}, {7: {"status": "a"}}).extra == [7]
    excused = run({}, {7: {"status": "a"}}, deleted={7})
    assert excused.ok and excused.excused_deletes == 1
