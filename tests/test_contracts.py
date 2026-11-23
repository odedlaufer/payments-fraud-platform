"""Data contracts (DC-2, DC-3): valid ODCS, faithful to the real schemas, versioned honestly."""

import importlib.util
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("gen_contracts", ROOT / "scripts/gen_contracts.py")
assert spec and spec.loader
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)

SILVER = [
    "customers_current",
    "cards_current",
    "merchants_current",
    "sessions_current",
    "transactions_current",
    "chargebacks_current",
]


def fake_columns(schema: str, table: str) -> list[tuple[str, str]]:
    if table == "training_set":
        return [
            ("cutoff", "timestamp(6) with time zone"),
            ("transaction_id", "bigint"),
            ("event_ts", "timestamp(6) with time zone"),
            ("label", "integer"),
            ("label_known_at", "timestamp(6) with time zone"),
        ]
    key = {
        "customers_current": "customer_id",
        "cards_current": "card_id",
        "merchants_current": "merchant_id",
        "sessions_current": "session_id",
        "transactions_current": "transaction_id",
        "chargebacks_current": "chargeback_id",
    }[table]
    return [
        (key, "bigint"),
        ("amount", "decimal(12,2)"),
        ("updated_at", "timestamp(6) with time zone"),
    ]


def committed() -> list[Path]:
    return sorted(CONTRACTS.glob("*.odcs.yaml"))


def datacontract() -> list[str] | None:
    return gen.DATACONTRACT if shutil.which("uvx") else None


def test_generator_builds_the_three_contracts() -> None:
    built = gen.build(fake_columns)
    assert set(built) == {"decisions.odcs.yaml", "silver.odcs.yaml", "training_set.odcs.yaml"}
    silver = built["silver.odcs.yaml"]
    assert [t["name"] for t in silver["schema"]] == SILVER
    tx = next(t for t in silver["schema"] if t["name"] == "transactions_current")
    key = next(p for p in tx["properties"] if p["name"] == "transaction_id")
    assert key["primaryKey"] and key["logicalType"] == "integer"
    amount = next(p for p in tx["properties"] if p["name"] == "amount")
    assert (amount["physicalType"], amount["logicalType"]) == ("decimal(12,2)", "number")
    assert "count(DISTINCT transaction_id)" in tx["quality"][0]["query"]  # {key} filled per table
    ts = built["training_set.odcs.yaml"]["schema"][0]
    assert [p["name"] for p in ts["properties"] if p.get("primaryKey")] == [
        "cutoff",
        "transaction_id",
    ]
    assert len(ts["quality"]) == 3


def test_regeneration_keeps_each_contracts_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shutil.copy(CONTRACTS / "agreements.yaml", tmp_path / "agreements.yaml")
    (tmp_path / "silver.odcs.yaml").write_text(yaml.safe_dump({"version": "2.1.0"}))
    monkeypatch.setattr(gen, "CONTRACTS", tmp_path)
    built = gen.build(fake_columns)
    assert built["silver.odcs.yaml"]["version"] == "2.1.0"  # bumping is a human decision
    assert built["decisions.odcs.yaml"]["version"] == "1.0.0"


def test_the_decisions_contract_is_the_services_avro_schema() -> None:
    path = CONTRACTS / "decisions.odcs.yaml"
    if not path.exists():
        pytest.skip("contracts not generated yet (make contracts)")
    doc = yaml.safe_load(path.read_text())
    expected = gen.build_one(
        "decisions",
        yaml.safe_load((CONTRACTS / "agreements.yaml").read_text())["contracts"]["decisions"],
        {},
        doc["version"],
        fake_columns,
    )
    assert (
        doc["schema"] == expected["schema"]
    ), "decision schema changed: make contracts, bump if breaking"


def test_quality_rules_query_columns_the_contract_has() -> None:
    for path in committed():
        for table in yaml.safe_load(path.read_text())["schema"]:
            columns = {p["name"] for p in table["properties"]}
            for rule in table.get("quality", []):
                used = set(re.findall(r"\b([a-z_]+)\b", rule.get("query", ""))) - {
                    "select",
                    "count",
                    "from",
                    "where",
                    "distinct",
                    "and",
                    "not",
                    "in",
                    "interval",
                    "day",
                    "object",
                }
                assert used <= columns, f"{path.name} {table['name']}: {sorted(used - columns)}"


def test_contracts_are_valid_odcs() -> None:
    command = datacontract()
    if command is None or not committed():
        pytest.skip("uvx or generated contracts not available")
    for path in committed():
        run = subprocess.run([*command, "lint", str(path)], capture_output=True, text=True)
        assert run.returncode == 0, f"{path.name}:\n{run.stdout[-1500:]}"


def base_ref() -> str | None:
    for ref in ("origin/main", "main"):
        if (
            subprocess.run(
                ["git", "rev-parse", "--verify", "-q", ref], cwd=ROOT, capture_output=True
            ).returncode
            == 0
        ):
            return ref
    return None


def major(version: str) -> int:
    return int(str(version).split(".")[0])


def test_breaking_changes_bump_the_major_version(tmp_path: Path) -> None:
    """DC-3: a contract change datacontract flags as breaking needs a new major version."""
    command, ref = datacontract(), base_ref()
    if command is None or ref is None:
        pytest.skip("uvx or a main branch not available")
    for path in committed():
        old = subprocess.run(
            ["git", "show", f"{ref}:contracts/{path.name}"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if old.returncode != 0 or old.stdout == path.read_text():
            continue  # new on this branch, or unchanged
        before = tmp_path / f"base-{path.name}"
        before.write_text(old.stdout)
        run = subprocess.run(
            [*command, "breaking", str(before), str(path)], capture_output=True, text=True
        )
        if run.returncode != 0:
            was: Any = yaml.safe_load(old.stdout)["version"]
            now: Any = yaml.safe_load(path.read_text())["version"]
            assert major(now) > major(
                was
            ), f"{path.name}: breaking change needs a major bump ({was} -> {now})"
