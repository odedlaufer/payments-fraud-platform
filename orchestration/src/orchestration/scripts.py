"""Running the project's own scripts from Dagster: one implementation, used by Make and
Dagster alike. Output goes to the step's log; a non-zero exit fails the step."""

import subprocess
import sys

import dagster as dg

from orchestration.dbt_assets import ROOT


def run_script(args: list[str]) -> tuple[int, str]:
    """(exit code, output) of `python <args>` from the repository root, this interpreter."""
    done = subprocess.run(
        [sys.executable, *args], cwd=ROOT, capture_output=True, text=True, timeout=3600
    )
    return done.returncode, (done.stdout + done.stderr).strip()


def run_or_fail(log: dg.DagsterLogManager, args: list[str]) -> str:
    code, output = run_script(args)
    for line in output.splitlines():
        log.info(line)
    if code != 0:
        tail = "\n".join(output.splitlines()[-15:])
        raise dg.Failure(f"{' '.join(args)} exited {code}", metadata={"output": tail})
    return output
