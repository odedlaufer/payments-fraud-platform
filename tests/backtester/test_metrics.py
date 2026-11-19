"""Backtest metrics, computed by hand on a small set; same engine and context as decisions."""

from decimal import Decimal
from typing import Any

import duckdb

from backtester.data import training_rows
from backtester.metrics import ALL_RULES, backtest
from decision_service.context import RULE_NAMES, SNAPSHOT_FEATURES, TRANSACTION_FIELDS
from rules.engine import RuleSet

CONTEXT = [*SNAPSHOT_FEATURES, *TRANSACTION_FIELDS]
RULES = RuleSet.parse(
    {
        "version": 7,
        "description": "test rules",
        "rules": [
            {"id": "burst", "description": "", "condition": "txn_count_1h >= 5",
             "action": "decline", "severity": 90},
            {"id": "big", "description": "", "condition": "amount >= 1000",
             "action": "review", "severity": 50},
        ],
    },
    RULE_NAMES,
)  # fmt: skip


def row(txn: int, count_1h: int, amount: str, label: int) -> dict[str, Any]:
    r: dict[str, Any] = dict.fromkeys(CONTEXT)
    r.update(
        transaction_id=txn,
        txn_count_1h=count_1h,
        amount=Decimal(amount),
        label=label,
        channel="online",
    )
    return r


ROWS = [
    row(1, 6, "10.00", 1),  # burst: declined fraud
    row(2, 6, "20.00", 0),  # burst: declined legit (friction)
    row(3, 1, "1500.00", 1),  # big: reviewed fraud
    row(4, 7, "2000.00", 1),  # burst and big: declined fraud, counted once overall
    row(5, 1, "50.00", 1),  # fraud nobody catches
    row(6, 1, "30.00", 0),  # quiet legit
]


def test_overall_metrics() -> None:
    r = backtest(ROWS, RULES, CONTEXT)
    assert (r.rows, r.positives) == (6, 4)
    o = r.overall
    assert o.rule_id == ALL_RULES
    assert (o.alerts, o.true_positives, o.declined) == (4, 3, 3)  # 1, 2, 3, 4 alerted
    assert r.precision(o) == 3 / 4 and r.recall(o) == 3 / 4 and r.alert_rate(o) == 4 / 6
    assert o.fraud_declined_usd == Decimal("2010.00")  # 1 and 4 declined
    assert o.legit_declined_usd == Decimal("20.00")  # 2 declined


def test_per_rule_metrics_count_every_rule_that_fired() -> None:
    r = backtest(ROWS, RULES, CONTEXT)
    burst, big = r.per_rule["burst"], r.per_rule["big"]
    assert (burst.alerts, burst.true_positives) == (3, 2)  # 1, 2, 4
    assert r.precision(burst) == 2 / 3 and r.recall(burst) == 2 / 4
    assert (burst.fraud_declined_usd, burst.legit_declined_usd) == (
        Decimal("2010.00"),
        Decimal("20.00"),
    )
    assert (big.alerts, big.true_positives, big.declined) == (2, 2, 0)  # reviews decline nothing
    assert big.fraud_declined_usd == Decimal("0")


def test_same_rules_and_rows_give_identical_results() -> None:
    a, b = backtest(ROWS, RULES, CONTEXT), backtest(list(ROWS), RULES, CONTEXT)
    assert [vars(s) for s in a.table()] == [vars(s) for s in b.table()]


def test_the_context_is_exactly_what_decisions_evaluate() -> None:
    """The backtester feeds the rules the decision service's own context, nothing more."""
    assert set(CONTEXT) == set(RULE_NAMES) and len(CONTEXT) == len(RULE_NAMES)


def test_rules_files_in_the_repo_backtest() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for path in sorted((root / "rules").glob("rules_v*.yml")):
        r = backtest(ROWS, RuleSet.load(path, RULE_NAMES), CONTEXT)
        assert r.rows == 6 and set(r.per_rule) == {
            rule.id for rule in RuleSet.load(path, RULE_NAMES).rules
        }


def test_training_rows_select_one_cutoff_and_window() -> None:
    con = duckdb.connect()
    con.execute("set timezone = 'UTC'")
    con.execute(
        "create table ts (transaction_id bigint, event_ts timestamptz, label integer, "
        "amount decimal(12,2), cutoff timestamptz, txn_count_1h bigint)"
    )
    con.execute("""insert into ts values
        (1, '2026-01-10 08:00:00+00', 0, 5, '2026-03-30 00:00:00+00', 1),
        (2, '2026-01-12 23:59:59+00', 1, 9, '2026-03-30 00:00:00+00', 2),
        (3, '2026-01-13 00:00:00+00', 0, 7, '2026-03-30 00:00:00+00', 3),
        (4, '2026-01-12 10:00:00+00', 0, 4, '2026-03-15 00:00:00+00', 4)""")
    cutoff = con.execute("select cast('2026-03-30 00:00:00+00' as timestamptz)").fetchone()[0]  # type: ignore[index]
    all_rows = training_rows(con, "ts", cutoff, ["txn_count_1h"])
    assert [r["transaction_id"] for r in all_rows] == [1, 2, 3]  # the other cutoff excluded
    window = training_rows(con, "ts", cutoff, ["txn_count_1h"], ("2026-01-11", "2026-01-12"))
    assert [r["transaction_id"] for r in window] == [2]  # inclusive by day: through 2026-01-12
    assert window[0]["amount"] == Decimal("9.00") and window[0]["label"] == 1
