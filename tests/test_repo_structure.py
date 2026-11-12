"""Guards the repository layout defined in PRD section 8.1."""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_DIRS = [
    "simulator",
    "postgres",
    "connect",
    "schemas",
    "flink/sql",
    "flink/udfs",
    "features",
    "decision_service",
    "rules",
    "backtester",
    "dbt",
    "orchestration",
    "contracts",
    "observability",
    "chaos",
    "docs/adr",
]

REQUIRED_FILES = [
    "Makefile",
    "README.md",
    "pyproject.toml",
    "uv.lock",
    ".env.example",
    ".gitignore",
    ".pre-commit-config.yaml",
    "docker-compose.yml",
    "scripts/health.sh",
    "scripts/lakekeeper-bootstrap.sh",
    "scripts/phase0-gate.sh",
    "trino/etc/jvm.config",
    "trino/etc/catalog/lake.properties",
    "postgres/.sqlfluff",
    "connect/Dockerfile",
    "connect/connectors/payments-postgres.json",
    "postgres/roles.psql",
    "scripts/register-connectors.sh",
    "scripts/cdc_reconcile.py",
    "scripts/lock-jars.sh",
    "scripts/flink-iceberg-check.sh",
    "flink/Dockerfile",
    "flink/jars.txt",
    "flink/jars.lock",
    "scripts/gen_bronze.py",
    "scripts/submit-flink-jobs.sh",
    "trino/ddl/bronze.sql",
    "flink/sql/bronze/bronze_cdc.sql",
    "scripts/bronze_reconcile.py",
    "scripts/silver_reconcile.py",
    "scripts/phase2-gate.sh",
    "scripts/evolve_bronze.py",
    "scripts/flink-savepoint-stop.sh",
    "scripts/schema-evolve.sh",
    "chaos/ch1-bronze.sh",
    "dbt/dbt_project.yml",
    "dbt/profiles.yml",
    "dbt/macros/current_state.sql",
    "dbt/macros/scd2.sql",
    "dbt/models/staging/_bronze__sources.yml",
]


@pytest.mark.parametrize("rel", REQUIRED_DIRS)
def test_required_dir_exists(rel: str) -> None:
    assert (ROOT / rel).is_dir(), f"missing directory: {rel}"


@pytest.mark.parametrize("rel", REQUIRED_FILES)
def test_required_file_exists(rel: str) -> None:
    assert (ROOT / rel).is_file(), f"missing file: {rel}"


def test_env_is_gitignored() -> None:
    lines = (ROOT / ".gitignore").read_text().splitlines()
    assert ".env" in lines
