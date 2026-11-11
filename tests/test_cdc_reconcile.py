"""Changelog replay logic behind the CDC completeness gate."""

import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "cdc_reconcile", ROOT / "scripts/cdc_reconcile.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["cdc_reconcile"] = module
    spec.loader.exec_module(module)
    return module


rec = _load()


def ev(op: str, after: dict[str, object] | None = None) -> dict[str, object]:
    return {"op": op, "after": after}


def test_insert_update_delete_tombstone() -> None:
    r = rec.replay(
        [
            (1, ev("c", {"status": "pending"})),
            (1, ev("u", {"status": "approved"})),
            (2, ev("c", {"status": "pending"})),
            (2, ev("d")),
            (2, None),
        ]
    )
    assert r.rows == {1: {"status": "approved"}}
    assert (r.events, r.deletes, r.tombstones) == (5, 1, 1)
    assert r.deletes_without_tombstone == 0 and r.orphan_tombstones == 0


def test_snapshot_reads_count_as_rows() -> None:
    r = rec.replay([(7, ev("r", {"status": "active"}))])
    assert r.rows == {7: {"status": "active"}}


def test_delete_without_tombstone_is_detected() -> None:
    r = rec.replay([(1, ev("c", {})), (1, ev("d"))])
    assert r.deletes_without_tombstone == 1


def test_orphan_tombstone_is_detected() -> None:
    r = rec.replay([(1, None)])
    assert r.orphan_tombstones == 1


def test_compare_flags_missing_extra_and_values() -> None:
    cdc = rec.replay([(1, ev("c", {"status": "approved"})), (3, ev("c", {"status": "x"}))])
    pg = {1: {"status": "declined"}, 2: {"status": "approved"}}
    report = rec.compare("transactions", "transaction_id", ("status",), pg, cdc)
    assert (report.missing_in_cdc, report.extra_in_cdc, report.value_mismatches) == (1, 1, 1)
    assert not report.ok


def test_compare_matches_decimals_and_char_padding() -> None:
    cdc = rec.replay([(1, ev("c", {"amount": Decimal("10.50"), "currency": "USD"}))])
    pg = {1: {"amount": Decimal("10.5"), "currency": "USD "}}
    report = rec.compare("transactions", "transaction_id", ("amount", "currency"), pg, cdc)
    assert report.ok


def test_every_table_compares_its_primary_key() -> None:
    assert set(rec.TABLES) == {
        "customers",
        "cards",
        "merchants",
        "sessions",
        "transactions",
        "chargebacks",
    }
