"""Rule promotion gate (PRD 6.3 gate 3): a rules version goes live only on evidence.

    uv run python scripts/promote_rules.py --candidate rules/rules_v2.yml
    uv run python scripts/promote_rules.py --candidate rules/rules_v2.yml --dry-run

Against the latest published training set, the candidate goes live only if
  1. its backtest exists on that set, and the live version's too (make backtest);
  2. online/offline parity meets the SLO (DQ-14, >= 99.9%);
  3. it keeps at least 90% of the live version's precision (--max-precision-drop, relative:
     with precision as low as a few percent, an absolute threshold would pass a rule that
     declines almost everyone);
  4. its file is in rules/ (the decision service image only ships those).
Then rules/live.env points at it and the decision service is rebuilt and restarted. Every
decision, promoted or refused, is recorded in lake.ops.rule_promotions with its evidence.
"""

import argparse
import hashlib
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import trino

from cdc_reconcile import load_env
from gen_bronze import ROOT

LIVE_ENV = ROOT / "rules/live.env"
PARITY_SLO = 0.999
ALL_RULES = "(all rules)"


@dataclass(frozen=True)
class Backtest:
    precision: float | None
    recall: float | None
    alerts: int


@dataclass(frozen=True)
class Evidence:
    candidate_version: int
    live_version: int
    snapshot_id: int
    candidate: Backtest | None
    live: Backtest | None
    parity: float | None
    deployable: bool


def verdict(e: Evidence, max_drop: float, slo: float = PARITY_SLO) -> list[str]:
    """Reasons to refuse; empty means promote."""
    reasons = []
    if e.candidate_version == e.live_version:
        reasons.append(f"v{e.candidate_version} is already live")
    if e.candidate is None:
        reasons.append(f"no backtest of the candidate on training set snapshot {e.snapshot_id}")
    if e.live is None:
        reasons.append(
            f"no backtest of live v{e.live_version} on training set snapshot {e.snapshot_id}"
        )
    if e.parity is None:
        reasons.append("no parity data (make dbt-build)")
    elif e.parity < slo:
        reasons.append(f"parity {e.parity:.4%} is below the {slo:.1%} SLO")
    if e.candidate and e.live:
        cp, lp = e.candidate.precision, e.live.precision
        if cp is None:
            reasons.append("the candidate raises no alerts on the training set")
        elif lp is not None and cp < lp * (1 - max_drop):
            reasons.append(
                f"precision {cp:.2%} keeps {cp / lp:.0%} of live {lp:.2%} "
                f"(must keep at least {1 - max_drop:.0%})"
            )
    if not e.deployable:
        reasons.append(
            "the candidate file is not in rules/ (the decision service image ships only those)"
        )
    return reasons


def live_rules() -> Path:
    for line in LIVE_ENV.read_text().splitlines():
        if line.startswith("RULES_FILE="):
            return ROOT / "rules" / Path(line.split("=", 1)[1].strip()).name
    sys.exit(f"STOP: no RULES_FILE in {LIVE_ENV}")


def version_of(path: Path) -> int:
    return int(path.stem.removeprefix("rules_v"))


def fetch(cur: Any, sql: str) -> list[Any]:
    cur.execute(sql)
    return list(cur.fetchall())


def backtest_of(cur: Any, path: Path, snapshot: int) -> Backtest | None:
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    rows = fetch(
        cur,
        "select precision, recall, alerts from lake.marts.rule_backtests "
        f"where rules_sha256 = '{sha}' and snapshot_id = {snapshot} and rule_id = '{ALL_RULES}' "
        "and window_from is null and window_to is null order by run_at desc limit 1",
    )
    return Backtest(rows[0][0], rows[0][1], int(rows[0][2])) if rows else None


def record(cur: Any, e: Evidence, candidate: Path, reasons: list[str]) -> None:
    fetch(
        cur,
        "create table if not exists lake.ops.rule_promotions ("
        "decided_at timestamp(6) with time zone, candidate_version integer, "
        "candidate_file varchar, live_version integer, snapshot_id bigint, "
        "candidate_precision double, live_precision double, candidate_recall double, "
        "live_recall double, parity double, outcome varchar, reasons varchar)",
    )

    def num(x: float | None) -> str:
        return "null" if x is None else repr(x)

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    c, lv = e.candidate, e.live
    why = "; ".join(reasons).replace("'", "''")
    fetch(
        cur,
        "insert into lake.ops.rule_promotions values ("
        f"timestamp '{now}', {e.candidate_version}, '{candidate.name}', {e.live_version}, "
        f"{e.snapshot_id}, {num(c.precision if c else None)}, {num(lv.precision if lv else None)}, "
        f"{num(c.recall if c else None)}, {num(lv.recall if lv else None)}, {num(e.parity)}, "
        f"'{'refused' if reasons else 'promoted'}', '{why}')",
    )


def deploy(candidate: Path) -> bool:
    LIVE_ENV.write_text(
        "\n".join(
            f"RULES_FILE=/app/rules/{candidate.name}" if line.startswith("RULES_FILE=") else line
            for line in LIVE_ENV.read_text().splitlines()
        )
        + "\n"
    )
    for cmd in (
        ["docker", "compose", "build", "decision-service"],
        ["docker", "compose", "up", "-d", "--no-deps", "decision-service"],
    ):
        if subprocess.run(cmd, cwd=ROOT, capture_output=True).returncode != 0:
            print(f"FAILED: {' '.join(cmd)}")
            return False
    want = f"rules v{version_of(candidate)},"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        logs = subprocess.run(
            ["docker", "compose", "logs", "--no-log-prefix", "--since", "2m", "decision-service"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        ).stdout
        if want in logs:
            return True
        time.sleep(3)
    print(f"FAILED: the decision service did not report '{want}' within 90 s")
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument(
        "--max-precision-drop", type=float, default=0.10, help="relative, e.g. 0.10 = 10%%"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="evaluate and record; change nothing"
    )
    args = parser.parse_args()
    candidate = args.candidate.resolve()
    live = live_rules()
    env = load_env()
    cur = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host="127.0.0.1", port=int(env.get("TRINO_PORT", "8088")), user="promote-rules"
    ).cursor()
    latest = fetch(
        cur,
        "select snapshot_id, cutoff from lake.ops.training_sets order by published_at desc limit 1",
    )
    if not latest:
        print("REFUSED: no published training set (make publish-training-set)")
        return 1
    snapshot, cutoff = int(latest[0][0]), latest[0][1]
    parity = fetch(
        cur,
        "select cast(sum(matching) as double) / nullif(sum(compared), 0) "
        "from lake.marts.parity_daily "
        "where feature = '(all features)'",
    )[0][0]
    e = Evidence(
        candidate_version=version_of(candidate),
        live_version=version_of(live),
        snapshot_id=snapshot,
        candidate=backtest_of(cur, candidate, snapshot),
        live=backtest_of(cur, live, snapshot),
        parity=parity,
        deployable=candidate.parent == (ROOT / "rules").resolve(),
    )
    reasons = verdict(e, args.max_precision_drop)
    print(
        f"candidate v{e.candidate_version} vs live v{e.live_version}, training set cutoff {cutoff} "
        f"(snapshot {snapshot})"
    )
    for label, b in (("candidate", e.candidate), ("live", e.live)):
        if b:
            print(
                f"  {label:<9} precision {b.precision or 0:.2%}  "
                f"recall {b.recall or 0:.2%}  alerts {b.alerts:,}"
            )
    print(f"  parity    {e.parity:.4%}" if e.parity is not None else "  parity    -")
    record(cur, e, candidate, reasons)
    if reasons:
        print("REFUSED:\n" + "\n".join(f"  - {r}" for r in reasons))
        return 1
    if args.dry_run:
        print("would promote (dry run): nothing changed")
        return 0
    if not deploy(candidate):
        return 1
    print(f"PROMOTED: v{e.candidate_version} is live (rules/live.env; decision service restarted)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
