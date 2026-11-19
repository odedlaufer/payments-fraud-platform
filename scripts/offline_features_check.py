"""Live check: lake features.card_offline equals the reference recomputed from Postgres.

Every feature of every transaction in the offline model is compared with
features.reference over the same history read from Postgres (and, for profile changes,
the customers CDC log). Run with no traffic, after a dbt build, so the lake has caught up.
"""

import sys
from decimal import Decimal
from typing import Any

import psycopg
import trino

from cdc_reconcile import load_env
from decisions_check import profile_changes
from features.model import load
from features.reference import (
    CustomerTxn,
    Session,
    card_features,
    customer_features,
    merchant_features,
)
from features.timeparse import dt_to_ms
from features_check import card_history, merchant_history


def close(got: Any, want: Any) -> bool:
    if got is None or want is None:
        return got is None and want is None
    if isinstance(got, float) or isinstance(want, float):
        return abs(float(got) - float(want)) <= 1e-9 * max(1.0, abs(float(want)))
    if isinstance(got, Decimal) or isinstance(want, Decimal):
        return Decimal(str(got)) == Decimal(str(want))
    return bool(got == want)


def main() -> int:
    env = load_env()
    defs = load()
    names = [f.name for f in defs.features]
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="offline-check"
    ).cursor()
    cur.execute(
        f"select transaction_id, card_id, merchant_id, {', '.join(names)} "
        "from lake.features.card_offline"
    )
    offline = {int(r[0]): r for r in cur.fetchall()}
    print(f"{len(offline)} transactions in lake.features.card_offline")

    want = card_features(defs, card_history(env, {int(r[1]) for r in offline.values()}))
    for txn_id, values in merchant_features(
        defs, *merchant_history(env, {int(r[2]) for r in offline.values()})
    ).items():
        want.setdefault(txn_id, {}).update(values)
    dsn = (f"host=localhost port={env.get('POSTGRES_PORT', '5432')} dbname={env['POSTGRES_DB']} "
           f"user={env['POSTGRES_USER']} password={env.get('POSTGRES_PASSWORD', '')}")  # fmt: skip
    with psycopg.connect(dsn) as conn:
        txns = [
            CustomerTxn(int(r[0]), int(r[1]), None if r[2] is None else int(r[2]), dt_to_ms(r[3]))
            for r in conn.execute(
                "select transaction_id, card_id, session_id, event_ts from transactions"
            )
        ]
        sessions = [
            Session(int(r[0]), int(r[1]), str(r[2]), dt_to_ms(r[3]))
            for r in conn.execute(
                "select session_id, customer_id, device_id, event_ts from sessions"
            )
        ]
        owners = {
            int(r[0]): int(r[1]) for r in conn.execute("select card_id, customer_id from cards")
        }
    for txn_id, values in customer_features(
        defs, txns, sessions, profile_changes(env), owners
    ).items():
        want.setdefault(txn_id, {}).update(values)

    ok = True
    for k, name in enumerate(names, start=3):
        bad = [
            (i, r[k], want[i].get(name))
            for i, r in offline.items()
            if not close(r[k], want[i].get(name))
        ]
        print(f"  {name:<24} {'OK' if not bad else f'{len(bad)} MISMATCHES, e.g. {bad[:2]}'}")
        ok = ok and not bad
    print(
        "offline features equal the reference"
        if ok
        else "OFFLINE FEATURES DIFFER FROM THE REFERENCE"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
