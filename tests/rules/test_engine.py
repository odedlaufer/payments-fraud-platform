"""The rule engine: semantics, null handling, and what it refuses to compile."""

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from rules.engine import RuleError, RuleSet, compile_condition

NAMES = frozenset({"a", "b", "amount", "channel", "txn_count_1h", "distinct_merchants_1h"})
ROOT = Path(__file__).resolve().parents[2]


def holds(condition: str, **ctx: object) -> bool:
    return compile_condition(condition, NAMES)(ctx) is True


@pytest.mark.parametrize(
    ("condition", "ctx", "expected"),
    [
        ("a >= 5", {"a": 5}, True),
        ("a > 5", {"a": 5}, False),
        ("a >= 5 and b < 2", {"a": 6, "b": 1}, True),
        ("a >= 5 and b < 2", {"a": 6, "b": 3}, False),
        ("a >= 5 or b < 2", {"a": 1, "b": 1}, True),
        ("not a >= 5", {"a": 1}, True),
        ("1 < a < 3", {"a": 2}, True),
        ("1 < a < 3", {"a": 3}, False),
        ("a * 2 + 1 == 7", {"a": 3}, True),
        ("a / b > 2", {"a": 9, "b": 4}, True),
        ("channel == 'online'", {"channel": "online"}, True),
        ("amount >= 10.5", {"amount": Decimal("10.50")}, True),
        ("-a < 0", {"a": 1}, True),
    ],
)
def test_semantics(condition: str, ctx: dict[str, Any], expected: bool) -> None:
    assert holds(condition, **ctx) is expected


@pytest.mark.parametrize(
    "condition",
    ["a >= 5", "a < 5", "a == a", "a != 1", "a * 2 > 1", "a / b > 1", "not not a > 1"],
)
def test_a_null_never_makes_a_condition_true(condition: str) -> None:
    assert holds(condition, a=None, b=None) is False


def test_division_by_zero_is_null_not_an_error() -> None:
    assert holds("a / b > 1", a=1, b=0) is False


@pytest.mark.parametrize(
    "condition",
    [
        "__import__('os').system('true')",
        "a.real > 1",
        "a[0] > 1",
        "(lambda: 1)() > 0",
        "[x for x in [1]] == [1]",
        "a ** 2 > 1",
        "a in [1, 2]",
        "a is None",
        "unknown_feature > 1",
        "a >",
        "{'k': 1} == a",
    ],
)
def test_unsafe_or_invalid_conditions_are_rejected(condition: str) -> None:
    with pytest.raises(RuleError):
        compile_condition(condition, NAMES)


def ruleset(*rules: dict[str, Any]) -> RuleSet:
    return RuleSet.parse({"version": 1, "description": "t", "rules": list(rules)}, NAMES)


def rule(rid: str, condition: str, action: str = "review", severity: int = 50) -> dict[str, Any]:
    return {"id": rid, "description": "d", "condition": condition, "action": action,
            "severity": severity}  # fmt: skip


def test_most_severe_action_wins_and_score_is_highest_severity() -> None:
    rs = ruleset(rule("r1", "a > 0", "review", 40), rule("r2", "a > 1", "decline", 90),
                 rule("r3", "a > 2", "review", 95))  # fmt: skip
    evaluation = rs.evaluate({"a": 5})
    assert evaluation.outcome == "decline"
    assert evaluation.score == 95
    assert evaluation.fired == ("r1", "r2", "r3")


def test_nothing_fired_is_approve() -> None:
    assert ruleset(rule("r1", "a > 10")).evaluate({"a": 1}).outcome == "approve"


@pytest.mark.parametrize(
    ("bad", "message"),
    [
        (rule("R1", "a > 1"), "snake_case"),
        (rule("r1", "a > 1", action="approve"), "review or decline"),
        (rule("r1", "a > 1", severity=0), "severity"),
        ({"id": "r1", "condition": "a > 1"}, "needs"),
    ],
)
def test_invalid_rules_are_rejected(bad: dict[str, Any], message: str) -> None:
    with pytest.raises(RuleError, match=message):
        ruleset(bad)


def test_duplicate_ids_are_rejected() -> None:
    with pytest.raises(RuleError, match="duplicate"):
        ruleset(rule("r1", "a > 1"), rule("r1", "a > 2"))


def test_file_name_must_match_version(tmp_path: Path) -> None:
    path = tmp_path / "rules_v2.yml"
    path.write_text("version: 1\ndescription: x\nrules:\n" + "  - " + str(rule("r1", "a > 1")))
    with pytest.raises(RuleError):
        RuleSet.load(path, NAMES)


def test_rules_v1_loads_with_card_features() -> None:
    from decision_service.context import RULE_NAMES

    rs = RuleSet.load(ROOT / "rules/rules_v1.yml", RULE_NAMES)
    assert rs.version == 1
    assert [r.id for r in rs.rules] == [
        "card_testing",
        "velocity_burst",
        "spend_spike",
        "daily_spend",
    ]
    burst = {"txn_count_1h": 6, "distinct_merchants_1h": 5, "txn_count_1m": 3, "amount": 2.0}
    assert rs.evaluate(burst).outcome == "decline"
