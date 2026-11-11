"""The committed bronze definitions match the live Postgres schema (DC-4 groundwork)."""

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]


def test_generated_bronze_matches_postgres() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/gen_bronze.py"), "--check"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
