"""features/definitions.yml: the PRD catalogue, and strict validation (ADR-003)."""

import copy
from datetime import timedelta
from typing import Any

import pytest
import yaml

from features.catalogue import render
from features.cli import CATALOGUE, main
from features.model import DEFAULT_PATH, DefinitionError, load, parse, parse_window

RAW: dict[str, Any] = yaml.safe_load(DEFAULT_PATH.read_text())

# PRD section 4, feature catalogue v1.
PRD_FEATURES = {
    "txn_count_1m": "card",
    "txn_count_1h": "card",
    "txn_count_24h": "card",
    "txn_amount_sum_1h": "card",
    "txn_amount_sum_24h": "card",
    "distinct_merchants_1h": "card",
    "amount_vs_avg_30d": "card",
    "km_from_last_txn": "card",
    "minutes_since_last_txn": "card",
    "new_device_24h": "customer",
    "profile_change_24h": "customer",
    "merchant_refund_rate_7d": "merchant",
}


def test_catalogue_matches_the_prd() -> None:
    defs = load()
    assert {f.name: f.entity for f in defs.features} == PRD_FEATURES


def test_every_feature_starts_at_version_1() -> None:
    assert {f.version for f in load().features} == {1}


def test_windows_parse() -> None:
    assert parse_window("1m") == timedelta(minutes=1)
    assert parse_window("24h") == timedelta(hours=24)
    assert parse_window("30d") == timedelta(days=30)
    assert load().by_name("txn_count_1h").window_delta == timedelta(hours=1)


@pytest.mark.parametrize("bad", ["1w", "0h", "h", "1.5h", ""])
def test_bad_windows_are_rejected(bad: str) -> None:
    with pytest.raises(DefinitionError):
        parse_window(bad)


def mutate(feature: str, **changes: Any) -> dict[str, Any]:
    doc = copy.deepcopy(RAW)
    for raw in doc["features"]:
        if raw["name"] == feature:
            for key, value in changes.items():
                if value is None:
                    raw.pop(key, None)
                else:
                    raw[key] = value
    return doc


@pytest.mark.parametrize(
    ("feature", "changes", "message"),
    [
        ("txn_count_1h", {"window": "24h"}, "name says 1h"),
        ("txn_count_1h", {"kind": "window_avg"}, "unknown kind"),
        ("txn_amount_sum_1h", {"field": None}, "needs"),
        ("txn_count_1h", {"field": "amount"}, "does not take"),
        ("txn_count_1h", {"entity": "device"}, "unknown entity"),
        ("txn_count_1h", {"dtype": "float"}, "dtype"),
        ("txn_count_1h", {"version": 0}, "version"),
        ("txn_count_1h", {"descripton": "typo"}, "unknown keys"),
        ("txn_count_1h", {"description": " "}, "description is empty"),
        ("txn_count_1h", {"name": "TxnCount"}, "snake_case"),
        ("profile_change_24h", {"columns": []}, "non-empty list"),
    ],
)
def test_invalid_definitions_are_rejected(
    feature: str, changes: dict[str, Any], message: str
) -> None:
    with pytest.raises(DefinitionError, match=message):
        parse(mutate(feature, **changes))


def test_duplicate_names_are_rejected() -> None:
    doc = copy.deepcopy(RAW)
    doc["features"].append(copy.deepcopy(doc["features"][0]))
    with pytest.raises(DefinitionError, match="duplicate"):
        parse(doc)


def test_semantics_cannot_change_silently() -> None:
    doc = copy.deepcopy(RAW)
    doc["semantics"]["includes_current"] = False
    with pytest.raises(DefinitionError, match="semantics changed"):
        parse(doc)


def test_committed_catalogue_is_current() -> None:
    assert CATALOGUE.read_text() == render(load()), "run `uv run features catalogue`"


def test_cli_validate_and_check(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate"]) == 0
    assert main(["catalogue", "--check"]) == 0
    assert "12 features valid" in capsys.readouterr().out
