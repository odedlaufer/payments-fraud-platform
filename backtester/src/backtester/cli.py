"""backtest: replay a rules version over a published training set (FR-BT1, FR-BT2).

    uv run backtest --rules rules/rules_v1.yml                      # latest published set
    uv run backtest --rules rules/rules_v2.yml --snapshot-id 8254010497186583662
    uv run backtest --rules rules/rules_v2.yml --from 2026-01-10 --to 2026-01-20

Reads the training set through DuckDB from the lake at its Iceberg snapshot, evaluates the
rules with the decision service's own engine and context, prints and writes a Markdown
report (backtester/reports/), and appends the metrics to lake.marts.rule_backtests.
"""

import argparse
import hashlib
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backtester.data import connect, published, training_rows
from backtester.metrics import Result, backtest
from backtester.report import markdown
from decision_service.context import RULE_NAMES, SNAPSHOT_FEATURES, TRANSACTION_FIELDS
from rules.engine import RuleSet

ROOT = Path(__file__).resolve().parents[3]
REPORTS = ROOT / "backtester/reports"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (ROOT / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def store(env: dict[str, str], result: Result, run: dict[str, Any]) -> None:
    """Appends one row per rule (and the overall row) to lake.marts.rule_backtests."""
    import trino

    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="backtest"
    ).cursor()
    cur.execute(
        "create table if not exists lake.marts.rule_backtests (run_at timestamp(6) with time zone, "
        "rules_version integer, rules_sha256 varchar, snapshot_id bigint, "
        "cutoff timestamp(6) with time zone, window_from varchar, window_to varchar, "
        "rule_id varchar, action varchar, transactions bigint, positives bigint, alerts bigint, "
        "true_positives bigint, precision double, recall double, alert_rate double, "
        "fraud_declined_usd decimal(18,2), legit_declined_usd decimal(18,2))"
    )
    cur.fetchall()
    values = []
    for s in result.table():

        def num(x: float | None) -> str:
            return "null" if x is None else repr(x)

        values.append(
            f"(timestamp '{run['run_at']}', {result.rules_version}, '{run['sha']}', "
            f"{run['snapshot_id']}, timestamp '{run['cutoff']}', "
            f"{_sql(run['from'])}, {_sql(run['to'])}, '{s.rule_id}', '{s.action}', "
            f"{result.rows}, {result.positives}, {s.alerts}, {s.true_positives}, "
            f"{num(result.precision(s))}, {num(result.recall(s))}, {num(result.alert_rate(s))}, "
            f"{s.fraud_declined_usd}, {s.legit_declined_usd})"
        )
    cur.execute("insert into lake.marts.rule_backtests values " + ", ".join(values))
    cur.fetchall()


def snapshot_source(snapshot_id: int) -> str:
    """The published training sets as they were at one Iceberg snapshot (time travel)."""
    return f"lake.ml.training_set AT (VERSION => {snapshot_id})"


def _sql(value: str | None) -> str:
    return "null" if value is None else f"'{value}'"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--rules", required=True, type=Path)
    parser.add_argument(
        "--snapshot-id", type=int, help="default: the latest published training set"
    )
    parser.add_argument("--from", dest="window_from", help="first event day, YYYY-MM-DD")
    parser.add_argument("--to", dest="window_to", help="last event day, YYYY-MM-DD")
    parser.add_argument("--no-store", action="store_true", help="do not append to rule_backtests")
    args = parser.parse_args(argv)

    ruleset = RuleSet.load(args.rules, RULE_NAMES)
    sha = hashlib.sha256(args.rules.read_bytes()).hexdigest()
    env = load_env()
    con = connect(env)
    sets = published(con)
    if not sets:
        print("no published training sets: run make publish-training-set first")
        return 1
    chosen = (
        next((s for s in sets if s["snapshot_id"] == args.snapshot_id), None)
        if args.snapshot_id
        else sets[0]
    )
    if chosen is None:
        print(f"snapshot {args.snapshot_id} is not a published training set")
        return 1
    source = snapshot_source(chosen["snapshot_id"])
    columns = [*SNAPSHOT_FEATURES, *TRANSACTION_FIELDS]
    rows = training_rows(con, source, chosen["cutoff"], columns, (args.window_from, args.window_to))
    result = backtest(rows, ruleset, columns)

    cutoff = chosen["cutoff"].isoformat(sep=" ")
    header = {
        "rules": f"{args.rules} (sha256 {sha[:12]})",
        "training set": f"cutoff {cutoff}, snapshot {chosen['snapshot_id']}",
        "event window": f"{args.window_from or 'start'} .. {args.window_to or 'end'}",
    }
    report = markdown(result, header)
    print(report)
    REPORTS.mkdir(parents=True, exist_ok=True)
    path = REPORTS / f"{args.rules.stem}_{chosen['snapshot_id']}.md"
    path.write_text(report)
    print(f"report: {path.relative_to(ROOT)}")
    if not args.no_store:
        run_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
        run = {
            "run_at": run_at,
            "sha": sha,
            "snapshot_id": chosen["snapshot_id"],
            "cutoff": cutoff,
            "from": args.window_from,
            "to": args.window_to,
        }
        store(env, result, run)
        print("metrics appended to lake.marts.rule_backtests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
