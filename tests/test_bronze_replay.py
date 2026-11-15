"""The bronze replay job reuses the live job's definitions, bounded to a damaged range."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / f"scripts/{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


replay = _load("bronze_replay")
gen = sys.modules["gen_bronze"]
COLS = [gen.Column("transaction_id", "bigint", None, None),
        gen.Column("status_event_ts", "timestamp with time zone", None, None)]  # fmt: skip


def test_replay_reads_exactly_the_range_in_batch() -> None:
    sql = replay.replay_sql("transactions", COLS, 100, 200)
    assert "SET 'execution.runtime-mode' = 'batch';" in sql
    assert "SET 'table.dml-sync' = 'true';" in sql
    assert "'scan.startup.specific-offsets' = 'partition:0,offset:100'" in sql
    assert "'scan.bounded.specific-offsets' = 'partition:0,offset:200'" in sql
    assert "WHERE `kafka_offset` >= 100 AND `kafka_offset` < 200;" in sql
    assert "'topic' = 'pg.public.transactions'" in sql
    assert "\\\\n" not in sql  # real newlines, not escaped ones


def test_replay_writes_rows_exactly_like_the_live_job() -> None:
    live = gen.bronze_insert("transactions")
    sql = replay.replay_sql("transactions", COLS, 100, 200)
    assert live.removesuffix(";\n") in sql
    assert gen.kafka_source("transactions", COLS, "X").split("WITH (")[0] in sql


def test_live_job_topic_patterns_have_one_backslash() -> None:
    """The live source options must keep the exact-match regex pg\\.public\\.<table>."""
    rendered = gen.LIVE_SOURCE.format(table="cards")
    assert "'topic-pattern' = 'pg\\.public\\.cards'" in rendered
