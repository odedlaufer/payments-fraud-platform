"""Live detection report: what the decision service did with injected fraud.

Joins the decisions topic with the hidden ground truth (sim.sim_truth, never captured
by CDC). Per pattern: transactions flagged (review or decline), attacks caught (at least
one transaction flagged), rules that fired; plus false positives on legitimate traffic,
split into clean ones and collateral (the card saw fraud in the previous 24 hours).

    uv run python scripts/detection_report.py [--since-minutes 30]
"""

import argparse
import sys
from collections import Counter, defaultdict
from itertools import pairwise
from typing import Any

import psycopg

from cdc_reconcile import load_env
from decisions_check import read_decisions

ATTACK_GAP_MS = 3 * 3_600_000  # transactions of one attack: same card, under 3 h apart
DAY_MS = 86_400_000


def attacks(
    ids: list[int], decisions: dict[int, dict[str, Any]], key: str = "card_id"
) -> list[list[int]]:
    """Transactions of one attack: same card (or merchant, for a bust-out), under 3 h apart."""
    by_card: dict[int, list[int]] = defaultdict(list)
    for i in ids:
        by_card[decisions[i][key]].append(i)
    groups: list[list[int]] = []
    for seq in by_card.values():
        seq.sort(key=lambda i: decisions[i]["event_ms"])
        groups.append([seq[0]])
        for prev, cur in pairwise(seq):
            if decisions[cur]["event_ms"] - decisions[prev]["event_ms"] > ATTACK_GAP_MS:
                groups.append([])
            groups[-1].append(cur)
    return groups


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-minutes", type=float, default=None)
    parser.add_argument(
        "--require",
        action="append",
        default=[],
        metavar="PATTERN=RULE",
        help="fail unless at least one PATTERN attack was caught by RULE (repeatable)",
    )
    args = parser.parse_args()
    env = load_env()
    decided = {d["transaction_id"]: d for d in read_decisions(env)}
    if args.since_minutes is not None:
        latest = max(d["decided_ms"] for d in decided.values())
        cutoff = latest - int(args.since_minutes * 60_000)
        decided = {i: d for i, d in decided.items() if d["commit_ms"] >= cutoff}
    dsn = (
        f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
        f"user={env['POSTGRES_USER']} password={env.get('POSTGRES_PASSWORD', '')}"
    )
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "select transaction_id, pattern from sim.sim_truth "
            "where is_fraud and transaction_id = any(%s)",
            (list(decided),),
        ).fetchall()
    truth: dict[int, str] = {int(r[0]): str(r[1]) for r in rows}

    def flagged(i: int) -> bool:
        return bool(decided[i]["decision"] != "approve")

    legit = [i for i in decided if i not in truth]
    fraud_times: dict[int, list[int]] = defaultdict(list)
    for i in truth:
        fraud_times[decided[i]["card_id"]].append(decided[i]["event_ms"])
    collateral = {
        i
        for i in legit
        if any(
            0 <= decided[i]["event_ms"] - t <= DAY_MS for t in fraud_times[decided[i]["card_id"]]
        )
    }
    clean_fp = [i for i in legit if flagged(i) and i not in collateral]
    stale = sum(decided[i]["reason"] == "stale_features" for i in legit)

    print(f"{len(decided)} decisions, {len(truth)} on injected fraud (rules v"
          f"{next(iter(decided.values()))['rules_version']})")  # fmt: skip
    fp_rules = Counter(r for i in clean_fp for r in decided[i]["fired_rules"])
    share = 100 * len(clean_fp) / max(len(legit), 1)
    print(f"legitimate flagged: {len(clean_fp)} of {len(legit)} ({share:.3f}%) "
          f"by {dict(fp_rules.most_common())}")  # fmt: skip
    print(f"  plus {sum(flagged(i) for i in collateral)} collateral after fraud on the card, "
          f"and {stale} stale-feature reviews")  # fmt: skip
    by_pattern: dict[str, list[int]] = defaultdict(list)
    for i, pattern in truth.items():
        by_pattern[pattern].append(i)
    print(f"{'pattern':<18}{'txns':>6}{'flagged':>9}{'declined':>10}{'txn recall':>12}"
          f"{'attacks':>9}{'caught':>8}  rules")  # fmt: skip
    caught_by: dict[str, Counter[str]] = defaultdict(Counter)
    for pattern, ids in sorted(by_pattern.items()):
        groups = attacks(
            ids, decided, "merchant_id" if pattern == "merchant_bust_out" else "card_id"
        )
        caught = sum(any(flagged(i) for i in g) for g in groups)
        n_flagged = sum(flagged(i) for i in ids)
        declined = sum(decided[i]["decision"] == "decline" for i in ids)
        rules = Counter(r for i in ids for r in decided[i]["fired_rules"])
        for g in groups:
            for rule in {r for i in g for r in decided[i]["fired_rules"]}:
                caught_by[pattern][rule] += 1
        print(f"{pattern:<18}{len(ids):>6}{n_flagged:>9}{declined:>10}"
              f"{100 * n_flagged / len(ids):>11.1f}%{len(groups):>9}{caught:>8}  "
              f"{dict(rules.most_common())}")  # fmt: skip
    failed = []
    for requirement in args.require:
        pattern, rule = requirement.split("=", 1)
        n = caught_by[pattern][rule]
        print(f"require {pattern} caught by {rule}: {n} attack(s) {'OK' if n else 'FAIL'}")
        if not n:
            failed.append(requirement)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
