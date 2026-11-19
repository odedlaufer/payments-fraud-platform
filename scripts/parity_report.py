"""Online/offline feature parity from the lake (FR-PAR2, FR-PAR3): the PRD's headline.

Reads marts.parity_daily and ops.parity_mismatches (run make dbt-build first).
"""

import sys

import trino

from cdc_reconcile import load_env

SLO = 0.999


def main() -> int:
    env = load_env()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="parity-report"
    ).cursor()
    cur.execute(
        "select feature, sum(compared), sum(matching), sum(not_compared), max(max_abs_diff), "
        "min(event_day), max(event_day) from lake.marts.parity_daily group by feature "
        "order by feature = '(all features)' desc, feature"
    )
    rows = cur.fetchall()
    if not rows:
        print("no parity data: run make dbt-build")
        return 1
    overall = rows[0]
    compared, matched, skipped = int(overall[1]), int(overall[2]), int(overall[3])
    rate = matched / compared if compared else 0.0
    print(
        f"parity: {rate:.4%} of {compared} decisions "
        f"({matched} with every logged feature equal); "
        f"simulated days {overall[5]} .. {overall[6]}"
    )  # fmt: skip
    print(f"  not compared: {skipped} decisions (stale reviews: no features logged)")
    print(f"\n  {'feature':<24}{'compared':>10}{'match rate':>12}{'max |diff|':>13}{'skipped':>9}")
    for feature, comp, mat, skip, diff, _, _ in rows[1:]:
        r = f"{int(mat) / int(comp):.4%}" if comp else "-"
        d = f"{diff:.2e}" if diff is not None else "-"
        print(f"  {feature:<24}{int(comp):>10}{r:>12}{d:>13}{int(skip):>9}")
    cur.execute(
        "select cause, count(*), count(distinct transaction_id), "
        "array_join(slice(array_agg(cast(transaction_id as varchar) || ':' || feature "
        "|| ' logged=' || coalesce(logged_value, 'null') || ' offline=' "
        "|| coalesce(offline_value, 'null')), 1, 2), '; ') "
        "from lake.ops.parity_mismatches group by cause order by 2 desc"
    )
    causes = cur.fetchall()
    if causes:
        print("\n  mismatches by cause (ops.parity_mismatches):")
        for cause, n, txns, examples in causes:
            print(f"    {cause:<17}{n:>6} values in {txns} decisions   e.g. {examples}")
    ok = rate >= SLO
    print(f"\n{'parity meets' if ok else 'PARITY BELOW'} the {SLO:.1%} SLO (DQ-14)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
