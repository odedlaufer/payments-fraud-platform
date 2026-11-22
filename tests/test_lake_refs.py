"""Tags protecting published training sets from snapshot expiry (FR-PIT4, FR-L5)."""

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("lake_refs", ROOT / "scripts/lake_refs.py")
assert spec and spec.loader
refs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refs)

MAR30 = datetime(2026, 3, 30, tzinfo=UTC)
MAR15 = datetime(2026, 3, 15, tzinfo=UTC)


def test_one_tag_name_per_published_set() -> None:
    assert refs.tag_name(MAR30, 5386084145022979600) == "training_set_20260330_5386084145022979600"
    assert refs.tag_name("2026-03-30 00:00:00.000000 UTC", 1) == "training_set_20260330_1"
    # a republished cutoff gets its own tag: both snapshots stay reproducible
    assert refs.tag_name(MAR30, 8254010497186583662) != refs.tag_name(MAR30, 5386084145022979600)


def test_unprotected_lists_registered_snapshots_no_ref_points_at() -> None:
    registered = [(MAR30, 8254), (MAR15, 9119), (MAR30, 5386)]
    table_refs = {"main": 5386, "training_set_20260330_8254": 8254}
    assert refs.unprotected(registered, table_refs) == [9119]


def test_any_reference_protects_branches_included() -> None:
    """Expiry keeps every referenced snapshot: a branch at it counts like a tag."""
    assert refs.unprotected([(MAR15, 9119)], {"main": 1, "some_branch": 9119}) == []
