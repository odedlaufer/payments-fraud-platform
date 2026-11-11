"""Static guards on docker-compose.yml (PRD ENG-2, ENG-3, Phase 0 gate)."""

from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE: dict[str, Any] = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
SERVICES: dict[str, dict[str, Any]] = COMPOSE["services"]
ONE_SHOT = {name for name, svc in SERVICES.items() if svc.get("restart") == "no"}
LONG_RUNNING = sorted(set(SERVICES) - ONE_SHOT)
# Services checked over HTTP by scripts/health.sh instead of a container healthcheck.
HEALTHCHECKED_EXTERNALLY = {"console"}

EXPECTED_SERVICES = {
    "postgres",
    "redpanda",
    "console",
    "seaweedfs",
    "catalog-db",
    "lakekeeper-migrate",
    "lakekeeper",
    "lakekeeper-init",
}


def test_expected_services_present() -> None:
    assert set(SERVICES) >= EXPECTED_SERVICES


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_image_pinned(name: str) -> None:
    image: str = SERVICES[name]["image"]
    tag = image.rsplit(":", 1)[-1] if ":" in image.rsplit("/", 1)[-1] else ""
    assert tag, f"{name}: image has no tag"
    assert tag != "latest", f"{name}: image uses :latest"


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_memory_limit_set(name: str) -> None:
    limits = SERVICES[name].get("deploy", {}).get("resources", {}).get("limits", {})
    assert "memory" in limits, f"{name}: no memory limit"


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_profile_assigned(name: str) -> None:
    assert SERVICES[name].get("profiles"), f"{name}: no compose profile"


@pytest.mark.parametrize("name", LONG_RUNNING)
def test_long_running_has_healthcheck(name: str) -> None:
    if name in HEALTHCHECKED_EXTERNALLY:
        pytest.skip("checked by scripts/health.sh")
    assert "healthcheck" in SERVICES[name], f"{name}: no healthcheck"


def test_total_memory_budget_under_16gb() -> None:
    def to_mb(value: str) -> int:
        value = value.lower()
        if value.endswith("g"):
            return int(float(value[:-1]) * 1024)
        if value.endswith("m"):
            return int(value[:-1])
        raise ValueError(value)

    total = sum(to_mb(s["deploy"]["resources"]["limits"]["memory"]) for s in SERVICES.values())
    assert total <= 12 * 1024, f"compose memory limits total {total} MB; keep headroom on 16 GB"


def test_catalog_db_is_not_the_oltp_source() -> None:
    """ADR-002: the catalog must not share the CDC source database."""
    env = SERVICES["lakekeeper"]["environment"]
    assert "@catalog-db:" in env["LAKEKEEPER__PG_DATABASE_URL_WRITE"]
    assert "@postgres:" not in env["LAKEKEEPER__PG_DATABASE_URL_WRITE"]


def test_catalog_db_not_exposed_to_host() -> None:
    assert "ports" not in SERVICES["catalog-db"]


@pytest.mark.parametrize("name", sorted(n for n, s in SERVICES.items() if "ports" in s))
def test_ports_bound_to_localhost(name: str) -> None:
    """Auth is off on local services, so nothing may listen on the LAN."""
    for port in SERVICES[name]["ports"]:
        assert str(port).startswith("127.0.0.1:"), f"{name}: {port} is exposed beyond localhost"
