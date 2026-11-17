"""Bronze for the decisions topic is generated from the decision record's own schema."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location(
    "gen_decisions_bronze", ROOT / "scripts/gen_decisions_bronze.py"
)
assert spec and spec.loader
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)

from decision_service.decision import avro_schema  # noqa: E402

SCHEMA = avro_schema()


def test_generated_files_are_up_to_date() -> None:
    for path, text in gen.outputs().items():
        assert path.read_text() == text, f"{path.name}: run scripts/gen_decisions_bronze.py"


def test_every_decision_field_reaches_both_sides() -> None:
    flink, trino = gen.flink_job(SCHEMA), gen.trino_ddl(SCHEMA)
    for f in SCHEMA["fields"]:
        assert f"`{f['name']}` " in flink
        assert f"    {f['name']} " in trino


def test_types_follow_the_avro_schema() -> None:
    assert gen.flink_type({"type": "array", "items": "string"}) == "ARRAY<STRING>"
    assert gen.trino_type(["null", "long"]) == "bigint"
    features = next(f for f in SCHEMA["fields"] if f["name"] == "features")
    row = gen.trino_type(features["type"])
    assert row.startswith("row(txn_count_1m bigint") and "new_device_24h boolean" in row
    assert "txn_amount_sum_1h varchar" in row  # decimals travel as exact strings


def test_bronze_metadata_matches_cdc_bronze() -> None:
    ddl = gen.trino_ddl(SCHEMA)
    for col in (
        "kafka_partition integer",
        "kafka_offset bigint",
        "kafka_timestamp timestamp(6) with time zone",
        "ingested_at timestamp(6) with time zone",
    ):
        assert col in ddl
    assert "partitioning = ARRAY['day(decided_at)']" in ddl
