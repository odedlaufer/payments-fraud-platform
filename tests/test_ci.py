"""CI workflows stay in step with the project (PRD 8.3)."""

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"


def workflow(name: str) -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load((WORKFLOWS / name).read_text())
    return data


def runs(name: str) -> str:
    return "\n".join(
        s.get("run", "") for job in workflow(name)["jobs"].values() for s in job["steps"]
    )


def test_ci_runs_what_make_check_runs() -> None:
    script = runs("ci.yml")
    assert (
        "pre-commit run --all-files" in script
        and "make typecheck" in script
        and "make test" in script
    )


def test_ci_promtool_is_the_prometheus_version_compose_runs() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    image = compose["services"]["prometheus"]["image"]  # prom/prometheus:vX.Y.Z
    version = image.split(":v")[1]
    assert f"releases/download/v{version}/prometheus-{version}.linux-amd64.tar.gz" in runs("ci.yml")


def test_every_integration_test_named_in_ci_exists() -> None:
    named = re.findall(r"tests/integration/\S+\.py", runs("integration.yml"))
    assert named and all((ROOT / path).exists() for path in named)


def test_integration_runs_the_core_profile_only() -> None:
    assert "COMPOSE_PROFILES=core/" in runs("integration.yml")
