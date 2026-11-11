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
HEALTHCHECKED_EXTERNALLY = {"console", "flink-taskmanager"}

EXPECTED_SERVICES = {
    "postgres",
    "postgres-migrate",
    "postgres-roles",
    "connect",
    "connect-init",
    "redpanda",
    "console",
    "seaweedfs",
    "catalog-db",
    "lakekeeper-migrate",
    "lakekeeper",
    "lakekeeper-init",
    "trino",
    "flink-jobmanager",
    "flink-taskmanager",
    "bronze-ddl",
    "flink-jobs",
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


def test_long_running_services_fit_an_8gb_docker_vm() -> None:
    """A 16 GB laptop gives Docker about 8 GB; steady-state limits must fit with headroom.

    One-shot containers (migrations, init, registration) run briefly at startup only.
    """

    def to_mb(value: str) -> int:
        value = value.lower()
        if value.endswith("g"):
            return int(float(value[:-1]) * 1024)
        if value.endswith("m"):
            return int(value[:-1])
        raise ValueError(value)

    total = sum(
        to_mb(SERVICES[name]["deploy"]["resources"]["limits"]["memory"]) for name in LONG_RUNNING
    )
    assert total <= 7680, f"long-running memory limits total {total} MB; budget is 7680 MB"


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


def test_postgres_runs_logical_replication() -> None:
    """FR-CDC1: Debezium needs wal_level=logical."""
    assert "wal_level=logical" in SERVICES["postgres"]["command"]


def test_connect_image_pins_avro_converter_checksum() -> None:
    """The converter archive is downloaded at build time; its SHA-256 must be pinned."""
    dockerfile = (ROOT / "connect/Dockerfile").read_text()
    assert "FROM quay.io/debezium/connect:3.7.0.Final" in dockerfile
    sha = [line for line in dockerfile.splitlines() if "ARG AVRO_CONVERTER_SHA256=" in line]
    assert len(sha) == 1
    value = sha[0].split("=", 1)[1]
    assert len(value) == 64 and all(c in "0123456789abcdef" for c in value), value


def test_flink_sql_uses_configured_warehouse_and_no_credentials() -> None:
    sql = (ROOT / "flink/sql/checks/iceberg_write_check.sql").read_text()
    env = (ROOT / ".env.example").read_text().splitlines()
    warehouse = next(line.split("=", 1)[1] for line in env if line.startswith("WAREHOUSE_NAME="))
    assert f"'warehouse' = '{warehouse}'" in sql
    # Credentials come from the container environment, never from catalog options.
    assert "'s3.access-key-id'" not in sql and "'s3.secret-access-key'" not in sql


def test_flink_jars_are_locked() -> None:
    """Every jar in flink/jars.txt is pinned by SHA-256 in flink/jars.lock."""
    wanted = [
        line.strip()
        for line in (ROOT / "flink/jars.txt").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    locked = [
        line.split()
        for line in (ROOT / "flink/jars.lock").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert [url for _, _, url in locked] == wanted, "jars.lock is stale: run make lock-jars"
    for sha, name, url in locked:
        assert len(sha) == 64 and all(c in "0123456789abcdef" for c in sha), name
        assert url.rsplit("/", 1)[1] == name
