"""Backtest metrics (FR-BT2), computed with the same rule engine as the decision service.

For every transaction the rules are evaluated on the same context a live decision sees: the
features and a few transaction fields. Labels are the training set's, matured and as known
at its cutoff. An alert is any outcome other than approve (review or decline):

- precision: positives among alerts; recall: alerted positives among all positives;
- alert rate: alerts per transaction;
- $ fraud declined: amount of positives declined; $ legit declined: amount of negatives
  declined, the friction a rule puts on good customers.

Per rule the same, for the transactions that rule fired on.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from rules.engine import RuleSet

ALL_RULES = "(all rules)"


@dataclass
class Stats:
    rule_id: str
    action: str
    alerts: int = 0
    true_positives: int = 0
    declined: int = 0
    fraud_declined_usd: Decimal = Decimal("0")
    legit_declined_usd: Decimal = Decimal("0")

    def add(self, label: int, amount: Decimal, declined: bool) -> None:
        self.alerts += 1
        self.true_positives += label
        if declined:
            self.declined += 1
            if label:
                self.fraud_declined_usd += amount
            else:
                self.legit_declined_usd += amount


@dataclass
class Result:
    rules_version: int
    rows: int = 0
    positives: int = 0
    overall: Stats = field(default_factory=lambda: Stats(ALL_RULES, "-"))
    per_rule: dict[str, Stats] = field(default_factory=dict)

    def precision(self, s: Stats) -> float | None:
        return s.true_positives / s.alerts if s.alerts else None

    def recall(self, s: Stats) -> float | None:
        return s.true_positives / self.positives if self.positives else None

    def alert_rate(self, s: Stats) -> float | None:
        return s.alerts / self.rows if self.rows else None

    def table(self) -> list[Stats]:
        return [self.overall, *self.per_rule.values()]


def backtest(
    rows: Iterable[Mapping[str, Any]], ruleset: RuleSet, context_names: Iterable[str]
) -> Result:
    """rows: one mapping per transaction with the context fields, `amount` and `label`."""
    names = list(context_names)
    result = Result(rules_version=ruleset.version)
    actions = {r.id: r.action for r in ruleset.rules}
    for rule in ruleset.rules:
        result.per_rule[rule.id] = Stats(rule.id, rule.action)
    for row in rows:
        label = int(row["label"])
        amount = Decimal(str(row["amount"]))
        result.rows += 1
        result.positives += label
        evaluation = ruleset.evaluate({n: row.get(n) for n in names})
        if evaluation.outcome != "approve":
            result.overall.add(label, amount, evaluation.outcome == "decline")
        for rule_id in evaluation.fired:
            result.per_rule[rule_id].add(label, amount, actions[rule_id] == "decline")
    return result
