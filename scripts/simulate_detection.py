"""Offline detection estimate for a rules file: simulate days in memory (with injected
fraud), compute card features with the reference implementation, apply the rules.

No stack needed: use it to calibrate a rules version before it decides live traffic.

    uv run python scripts/simulate_detection.py [--rules rules/rules_v1.yml] [--days 4]
"""

import argparse
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

from decision_service.context import RULE_NAMES
from features.model import load
from features.reference import (
    CustomerTxn,
    MerchantTxn,
    ProfileChange,
    Session,
    StatusChange,
    Txn,
    card_features,
    customer_features,
    merchant_features,
)
from features.timeparse import dt_to_ms
from rules.engine import RuleSet
from simulator.engine import Engine
from simulator.entities import generate_population
from simulator.rates import Rates
from simulator.store import MemoryStore
from simulator.world import CardState, CustomerState, MerchantState, World

START = datetime(2026, 1, 1, tzinfo=UTC)


def world(seed: int, customers: int, merchants: int) -> World:
    pop = generate_population(seed, customers, merchants, START)
    w = World(customers={}, cards={}, merchants={}, last_event_ts=START)
    for i, c in enumerate(pop.customers, start=1):
        w.customers[i] = CustomerState(i, c.country_code, c.home_lat, c.home_lon)
    for i, k in enumerate(pop.cards, start=1):
        w.cards[i] = CardState(i, k.customer_index + 1, k.card_type, k.status, k.expires_on)
    for i, m in enumerate(pop.merchants, start=1):
        w.merchants[i] = MerchantState(i, m.mcc, m.country_code, m.lat, m.lon, m.status)
    return w


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rules", type=Path, default=Path("rules/rules_v1.yml"))
    parser.add_argument("--days", type=float, default=4.0)
    parser.add_argument("--fraud-rate", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    store = MemoryStore(world(args.seed, 2000, 300))
    engine = Engine(store.world, store, seed=5, tx_per_sim_second=20 / 144,
                    rates=Rates().with_fraud(args.fraud_rate), start=START)  # fmt: skip
    while engine.now < START + timedelta(days=args.days):
        engine.advance(engine.now + timedelta(seconds=60))

    rows = sorted(store.transactions.items(), key=lambda kv: (kv[1]["event_ts"], kv[0]))
    txns = [
        Txn(i, int(str(r["card_id"])), int(str(r["merchant_id"])), Decimal(str(r["amount"])),
            str(r["channel"]), r["lat"], r["lon"], int(r["event_ts"].timestamp() * 1000))  # type: ignore[attr-defined,arg-type]
        for i, r in rows
    ]  # fmt: skip
    features = card_features(load(), txns)
    sessions = [
        Session(sid, int(str(s["customer_id"])), str(s["device_id"]), dt_to_ms(s["event_ts"]))  # type: ignore[arg-type]
        for sid, s in store.sessions.items()
    ]
    changes = [
        ProfileChange(c, frozenset({col}), dt_to_ms(at)) for c, col, at in store.customer_updates
    ]
    owners = {card.card_id: card.customer_id for card in store.world.cards.values()}

    def session_of(r: dict[str, object]) -> int | None:
        return None if r["session_id"] is None else int(str(r["session_id"]))

    asks = [
        CustomerTxn(i, int(str(r["card_id"])), session_of(r), dt_to_ms(r["event_ts"]))  # type: ignore[arg-type]
        for i, r in rows
    ]
    for i, values in customer_features(load(), asks, sessions, changes, owners).items():
        features[i].update(values)
    m_txns = [MerchantTxn(i, int(str(r["merchant_id"])), dt_to_ms(r["event_ts"])) for i, r in rows]  # type: ignore[arg-type]
    statuses = [
        StatusChange(i, int(str(r["merchant_id"])), status, dt_to_ms(at))
        for i, r in rows
        for status, at in (h for h in r["history"] if isinstance(h, tuple))  # type: ignore[attr-defined]
    ]
    for i, values in merchant_features(load(), m_txns, statuses).items():
        features[i].update(values)
    ruleset = RuleSet.load(args.rules, RULE_NAMES)
    outcome: dict[int, str] = {}
    fired: dict[int, tuple[str, ...]] = {}
    for i, r in rows:
        ctx = {**features[i], "amount": r["amount"], "channel": r["channel"],
               "device_risk_score": r["device_risk_score"]}  # fmt: skip
        ev = ruleset.evaluate(ctx)
        outcome[i], fired[i] = ev.outcome, ev.fired

    # Friendly fraud is a legitimate transaction disputed anyway: label noise, not an attack.
    fraud = {i: p for i, p in store.truth.items() if p != "friendly_fraud"}
    legit = [i for i, _ in rows if i not in fraud]
    flagged_legit = Counter(r for i in legit for r in fired[i])
    fp = sum(outcome[i] != "approve" for i in legit)
    after = after_fraud(legit, rows, store)
    collateral = sum(outcome[i] != "approve" for i in legit if i in after)
    print(f"rules v{ruleset.version}: {len(rows)} transactions over {args.days:g} simulated days, "
          f"{len(fraud)} fraudulent")  # fmt: skip
    print(f"legitimate flagged: {fp} of {len(legit)} ({100 * fp / len(legit):.3f}%) "
          f"by {dict(flagged_legit.most_common())}")  # fmt: skip
    print(f"  of which {collateral} follow a fraudulent purchase on the same card within 24 h")

    by_pattern: dict[str, list[int]] = defaultdict(list)
    for i, pattern in fraud.items():
        by_pattern[pattern].append(i)
    print(f"{'pattern':<18}{'txns':>6}{'flagged':>9}{'declined':>10}{'txn recall':>12}"
          f"{'attacks':>9}{'caught':>8}  rules")  # fmt: skip
    for pattern, ids in sorted(by_pattern.items()):
        attacks = attack_groups(ids, store)
        caught = sum(any(outcome[i] != "approve" for i in a) for a in attacks)
        flagged = sum(outcome[i] != "approve" for i in ids)
        declined = sum(outcome[i] == "decline" for i in ids)
        rules_hit = Counter(r for i in ids for r in fired[i])
        recall = 100 * flagged / len(ids)
        print(f"{pattern:<18}{len(ids):>6}{flagged:>9}{declined:>10}{recall:>11.1f}%"
              f"{len(attacks):>9}{caught:>8}  {dict(rules_hit.most_common())}")  # fmt: skip
    return 0


def after_fraud(
    legit: list[int], rows: list[tuple[int, dict[str, object]]], store: MemoryStore
) -> set[int]:
    """Legitimate transactions on a card that saw fraud in the previous 24 hours."""
    last_fraud: dict[int, datetime] = {}
    out = set()
    for i, r in rows:
        card, at = int(str(r["card_id"])), r["event_ts"]
        if store.truth.get(i, "friendly_fraud") != "friendly_fraud":
            last_fraud[card] = at  # type: ignore[assignment]
        elif card in last_fraud and at - last_fraud[card] <= timedelta(hours=24):  # type: ignore[operator]
            out.add(i)
    return out


def attack_groups(ids: list[int], store: MemoryStore) -> list[list[int]]:
    """Transactions of one attack, less than 3 hours apart: same card, or for a merchant
    bust-out (many cards, one merchant) the same merchant."""
    by_card: dict[int, list[int]] = defaultdict(list)
    for i in ids:
        row = store.transactions[i]
        key = "merchant_id" if store.truth.get(i) == "merchant_bust_out" else "card_id"
        by_card[int(str(row[key]))].append(i)
    groups: list[list[int]] = []
    for seq in by_card.values():
        seq.sort(key=lambda i: store.transactions[i]["event_ts"])  # type: ignore[arg-type,return-value]
        groups.append([seq[0]])
        for prev, cur in pairwise(seq):
            gap = store.transactions[cur]["event_ts"] - store.transactions[prev]["event_ts"]  # type: ignore[operator]
            if gap > timedelta(hours=3):
                groups.append([])
            groups[-1].append(cur)
    return groups


if __name__ == "__main__":
    sys.exit(main())
