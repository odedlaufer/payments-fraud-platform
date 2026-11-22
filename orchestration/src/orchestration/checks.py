"""The silver gate (PRD 6.3, gate 1; FR-O4): a blocking check on every silver asset.

It runs the two reconciliations the project already trusts: every CDC event is in bronze
exactly once (bronze_reconcile) and silver equals Postgres row for row (silver_reconcile,
DQ-08). If either fails, Dagster does not build anything downstream of silver in the run.
"""

import subprocess
import sys
from pathlib import Path

import dagster as dg

from orchestration.dbt_assets import ROOT, silver_dbt

RECONCILIATIONS = ("scripts/bronze_reconcile.py", "scripts/silver_reconcile.py")
CHECK = "lake_matches_postgres"


def run_script(path: str, root: Path = ROOT) -> tuple[int, str]:
    """(exit code, output) of one project script, run with this interpreter."""
    done = subprocess.run(
        [sys.executable, str(root / path)], cwd=root, capture_output=True, text=True, timeout=1800
    )
    return done.returncode, (done.stdout + done.stderr).strip()


@dg.multi_asset_check(
    specs=[
        dg.AssetCheckSpec(
            CHECK, asset=key, blocking=True, description="bronze and silver equal Postgres"
        )
        for key in sorted(silver_dbt.keys, key=lambda k: k.to_user_string())
    ],
)
def lake_matches_postgres(context: dg.AssetCheckExecutionContext):  # type: ignore[no-untyped-def]
    summary = {}
    for path in RECONCILIATIONS:
        code, output = run_script(path)
        for line in output.splitlines():
            context.log.info(line)
        summary[Path(path).stem] = (code, output.splitlines()[-1] if output else "")
    passed = all(code == 0 for code, _ in summary.values())
    metadata = {name: f"exit {code}: {last}" for name, (code, last) in summary.items()}
    for key in silver_dbt.keys:
        yield dg.AssetCheckResult(asset_key=key, check_name=CHECK, passed=passed, metadata=metadata)
