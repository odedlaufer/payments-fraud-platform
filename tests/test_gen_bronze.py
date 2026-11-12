"""Type mapping behind the generated bronze layer (FR-F5)."""

import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gen_bronze", ROOT / "scripts/gen_bronze.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_bronze"] = module
    spec.loader.exec_module(module)
    return module


gen = _load()


@pytest.mark.parametrize(
    ("pg_type", "precision", "scale", "flink", "trino"),
    [
        ("bigint", None, None, "BIGINT", "bigint"),
        ("integer", None, None, "INT", "integer"),
        ("date", None, None, "INT", "integer"),  # Debezium: days since epoch
        ("numeric", 12, 2, "DECIMAL(12, 2)", "decimal(12,2)"),
        ("double precision", None, None, "DOUBLE", "double"),
        ("boolean", None, None, "BOOLEAN", "boolean"),
        ("text", None, None, "STRING", "varchar"),
        ("character", None, None, "STRING", "varchar"),
        ("inet", None, None, "STRING", "varchar"),
        ("timestamp with time zone", None, None, "STRING", "varchar"),  # ISO-8601 text
    ],
)
def test_type_mapping(
    pg_type: str, precision: int | None, scale: int | None, flink: str, trino: str
) -> None:
    column = gen.Column("c", pg_type, precision, scale)
    assert column.flink_type() == flink
    assert column.trino_type() == trino


def test_unknown_type_is_rejected() -> None:
    with pytest.raises(ValueError, match="no bronze mapping"):
        gen.Column("c", "jsonb", None, None).flink_type()


def test_unbounded_numeric_is_rejected() -> None:
    with pytest.raises(ValueError, match="unbounded numeric"):
        gen.Column("c", "numeric", None, None).flink_type()


def test_committed_job_covers_every_table_and_never_sim() -> None:
    job = (ROOT / "flink/sql/bronze/bronze_cdc.sql").read_text()
    ddl = (ROOT / "trino/ddl/bronze.sql").read_text()
    for table in gen.TABLES:
        assert f"INSERT INTO lake.bronze.cdc_{table}" in job
        assert f"CREATE TABLE IF NOT EXISTS lake.bronze.cdc_{table}" in ddl
    assert "sim" not in gen.TABLES and "sim_truth" not in job


def test_job_checkpoints_exactly_once_at_the_cluster_interval() -> None:
    """FR-F5: 60 s by default; the interval is cluster config so dev can shorten it."""
    job = (ROOT / "flink/sql/bronze/bronze_cdc.sql").read_text()
    assert "SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';" in job
    assert "SET 'execution.checkpointing.interval'" not in job
    compose = (ROOT / "docker-compose.yml").read_text()
    assert "execution.checkpointing.interval: ${FLINK_CHECKPOINT_INTERVAL:-60s}" in compose


def test_topic_patterns_match_exactly_their_topic() -> None:
    """A regex that matches no topic leaves the job RUNNING but reading nothing."""
    job = (ROOT / "flink/sql/bronze/bronze_cdc.sql").read_text()
    patterns = re.findall(r"'topic-pattern' = '([^']+)'", job)
    assert len(patterns) == len(gen.TABLES)
    for table, pattern in zip(gen.TABLES, patterns, strict=True):
        assert re.fullmatch(pattern, f"pg.public.{table}"), pattern
        assert not re.fullmatch(pattern, f"pgXpublicX{table}"), f"{pattern}: dot must be literal"
        assert not re.fullmatch(pattern, f"pg.public.{table}_v2"), pattern


@pytest.mark.parametrize(
    ("pg_type", "expected"),
    [
        (
            "timestamp with time zone",
            "cast(from_iso8601_timestamp_nanos(r.c) as timestamp(6) with time zone)",
        ),
        ("date", "date_add('day', r.c, date '1970-01-01')"),
        ("bigint", "r.c"),
        ("numeric", "r.c"),
    ],
)
def test_staging_parses_debezium_types(pg_type: str, expected: str) -> None:
    assert gen.Column("c", pg_type, 12, 2).staged("r") == expected


def test_staging_models_generated_for_every_table() -> None:
    for table in gen.TABLES:
        model = (ROOT / f"dbt/models/staging/stg_cdc__{table}.sql").read_text()
        assert f"source('bronze', 'cdc_{table}')" in model
        assert "case when op = 'd' then before else after end" in model
        assert "op = 'd' as is_deleted" in model
