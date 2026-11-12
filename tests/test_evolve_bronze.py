"""Additive-only evolution of bronze row types (DC-4)."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / f"scripts/{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


evo = _load("evolve_bronze")
gen = sys.modules["gen_bronze"]


def test_row_fields_handles_decimal_commas() -> None:
    fields = evo.row_fields("row(id bigint, amount decimal(12, 2), note varchar)")
    assert fields == {"id": "bigint", "amount": "decimal(12,2)", "note": "varchar"}


def test_new_column_is_added_to_before_and_after() -> None:
    current = {"id": "bigint"}
    desired = [gen.Column("id", "bigint", None, None), gen.Column("score", "numeric", 5, 4)]
    assert evo.plan("transactions", current, desired) == [
        "ALTER TABLE lake.bronze.cdc_transactions ADD COLUMN before.score decimal(5,4)",
        "ALTER TABLE lake.bronze.cdc_transactions ADD COLUMN after.score decimal(5,4)",
    ]


def test_nothing_to_do_when_in_sync() -> None:
    assert evo.plan("t", {"id": "bigint"}, [gen.Column("id", "bigint", None, None)]) == []


@pytest.mark.parametrize(
    ("current", "desired"),
    [
        ({"id": "bigint", "gone": "varchar"}, [("id", "bigint")]),
        ({"id": "integer"}, [("id", "bigint")]),
    ],
)
def test_breaking_changes_are_refused(
    current: dict[str, str], desired: list[tuple[str, str]]
) -> None:
    columns = [gen.Column(name, t, None, None) for name, t in desired]
    with pytest.raises(SystemExit, match="breaking change refused"):
        evo.plan("t", current, columns)
