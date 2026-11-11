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


def test_job_checkpoints_every_60s_exactly_once() -> None:
    job = (ROOT / "flink/sql/bronze/bronze_cdc.sql").read_text()
    assert "SET 'execution.checkpointing.interval' = '60s';" in job
    assert "SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';" in job


def test_topic_patterns_match_exactly_their_topic() -> None:
    """A regex that matches no topic leaves the job RUNNING but reading nothing."""
    job = (ROOT / "flink/sql/bronze/bronze_cdc.sql").read_text()
    patterns = re.findall(r"'topic-pattern' = '([^']+)'", job)
    assert len(patterns) == len(gen.TABLES)
    for table, pattern in zip(gen.TABLES, patterns, strict=True):
        assert re.fullmatch(pattern, f"pg.public.{table}"), pattern
        assert not re.fullmatch(pattern, f"pgXpublicX{table}"), f"{pattern}: dot must be literal"
        assert not re.fullmatch(pattern, f"pg.public.{table}_v2"), pattern
