"""Static guards on OLTP migrations (DC-3 groundwork)."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = sorted((ROOT / "postgres/migrations").glob("*.sql"))
NAME = re.compile(r"^\d{14}_[a-z0-9_]+\.sql$")


def test_migrations_exist() -> None:
    assert MIGRATIONS


@pytest.mark.parametrize("path", MIGRATIONS, ids=lambda p: p.name)
def test_file_name_is_timestamped_snake_case(path: Path) -> None:
    assert NAME.match(path.name), path.name


@pytest.mark.parametrize("path", MIGRATIONS, ids=lambda p: p.name)
def test_has_up_and_non_empty_down(path: Path) -> None:
    text = path.read_text()
    assert text.count("-- migrate:up") == 1
    assert text.count("-- migrate:down") == 1
    down = text.split("-- migrate:down", 1)[1]
    statements = [line for line in down.splitlines() if line.strip() and not line.startswith("--")]
    assert statements, f"{path.name}: down migration is empty"


def test_versions_are_unique() -> None:
    versions = [p.name.split("_", 1)[0] for p in MIGRATIONS]
    assert len(versions) == len(set(versions))


def test_sim_schema_never_published() -> None:
    """sim.sim_truth must never reach CDC (PRD section 3)."""
    for path in MIGRATIONS:
        code = "\n".join(
            line
            for line in path.read_text().lower().splitlines()
            if not line.strip().startswith("--")
        )
        for statement in code.split(";"):
            if "publication" in statement:
                assert "sim." not in statement, f"{path.name} publishes the sim schema"
