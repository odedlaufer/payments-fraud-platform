"""make backtest-fixture: record a small training set for CI's rule backtests (rules.yml).

From the latest published training set, at its Iceberg snapshot: every labelled fraud plus a
deterministic tenth of the rest (by a hash of the transaction id), only the columns the
backtester reads. The data is synthetic. Written to tests/fixtures/training_set.parquet.
"""

import sys

from backtester.cli import ROOT, load_env, snapshot_source
from backtester.data import connect, published
from decision_service.context import SNAPSHOT_FEATURES, TRANSACTION_FIELDS

OUT = ROOT / "tests/fixtures/training_set.parquet"
COLUMNS = [
    "transaction_id",
    "event_ts",
    "label",
    "amount",
    "cutoff",
    *SNAPSHOT_FEATURES,
    *TRANSACTION_FIELDS,
]


def main() -> int:
    con = connect(load_env())
    sets = published(con)
    if not sets:
        print("no published training set: make publish-training-set first")
        return 1
    latest = sets[0]
    cutoff = latest["cutoff"].isoformat(sep=" ")
    columns = ", ".join(dict.fromkeys(COLUMNS))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"copy (select {columns} from {snapshot_source(latest['snapshot_id'])} "
        f"where cutoff = timestamptz '{cutoff}' and (label = 1 or hash(transaction_id) % 10 = 0) "
        f"order by transaction_id) to '{OUT}' (format parquet, compression zstd)"
    )
    rows, positives = con.execute(
        f"select count(*), sum(label) from read_parquet('{OUT}')"
    ).fetchone()  # type: ignore[misc]
    size = OUT.stat().st_size // 1024
    print(
        f"{OUT.relative_to(ROOT)}: {rows} rows, {positives} labelled fraud, {size} KB "
        f"(cutoff {cutoff}, snapshot {latest['snapshot_id']})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
