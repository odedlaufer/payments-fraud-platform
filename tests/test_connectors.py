"""Static guards on connector configs (FR-CDC2, DC-1)."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = json.loads((ROOT / "connect/connectors/payments-postgres.json").read_text())
MIGRATIONS = "\n".join(p.read_text() for p in (ROOT / "postgres/migrations").glob("*.sql"))


def test_no_literal_secrets() -> None:
    for key, value in CONFIG.items():
        if "password" in key:
            assert value.startswith("${env:"), f"{key} must come from the env config provider"


def test_tables_match_publication() -> None:
    """Debezium captures exactly the tables the publication exposes."""
    match = re.search(r"create publication payments_cdc for table(.*?);", MIGRATIONS, re.S)
    assert match
    published = sorted(t.strip() for t in match.group(1).split(",") if t.strip())
    assert sorted(CONFIG["table.include.list"].split(",")) == published


def test_never_captures_sim_schema() -> None:
    assert "sim." not in CONFIG["table.include.list"]


def test_publication_not_autocreated() -> None:
    """The publication is owned by migrations; Debezium must not alter it."""
    assert CONFIG["publication.autocreate.mode"] == "disabled"
    assert CONFIG["publication.name"] == "payments_cdc"


def test_avro_with_schema_registry() -> None:
    for side in ("key", "value"):
        assert CONFIG[f"{side}.converter"] == "io.confluent.connect.avro.AvroConverter"
        assert CONFIG[f"{side}.converter.schema.registry.url"] == "http://redpanda:8081"


def test_deletes_emit_tombstones() -> None:
    """FR-CDC4: deletes are explicit downstream, never silent."""
    assert CONFIG["tombstones.on.delete"] == "true"


def test_heartbeat_targets_heartbeat_table() -> None:
    assert "public.cdc_heartbeat" in CONFIG["heartbeat.action.query"]
