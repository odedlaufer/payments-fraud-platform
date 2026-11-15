"""Structure of the generated card-feature job (runs without Flink)."""

from features.cli import CARD_JOB, KAFKA, REGISTRY, main
from features.flink import CardJob, interval
from features.model import load

JOB = CardJob(load())


def test_one_view_per_window_size_plus_previous() -> None:
    names = [v.split()[3] for v in JOB.views()]
    assert names == [
        "card_txn",
        "card_late",
        "card_w_1m",
        "card_w_1h",
        "card_w_24h",
        "card_w_30d",
        "card_prev",
    ]


def test_window_views_have_distinct_plans() -> None:
    """Guard for the Flink 2.1.3 planner merging OVER windows that differ only in bounds."""
    filters = [v.rsplit("WHERE", 1)[1] for v in JOB.views() if " card_w_" in v]
    assert len(set(filters)) == len(filters)


def test_intervals() -> None:
    assert interval("1m") == "INTERVAL '1' MINUTE"
    assert interval("30d") == "INTERVAL '30' DAY"


def test_every_card_feature_reaches_the_sink() -> None:
    job = JOB.job(KAFKA, REGISTRY)
    for f in JOB.features:
        assert f"{f.name} " in JOB.sink_columns()
        assert f" AS {f.name}" in JOB.select()
    assert "'topic' = 'features.card'" in job and "'topic' = 'dlq.late_events'" in job
    assert "'sink.delivery-guarantee' = 'at-least-once'" in job
    assert "commit_ms BIGINT" in JOB.sink_columns() and "`source`.ts_ms AS commit_ms" in job


def test_committed_job_is_current() -> None:
    assert CARD_JOB.read_text() == JOB.job(KAFKA, REGISTRY), "run `uv run features flink`"
    assert main(["flink", "--check"]) == 0


def test_event_time_parsing_sql_is_exact() -> None:
    """One backslash: the regex Flink receives is \\.([0-9]+), a literal dot then digits."""
    ddl = JOB.source_ddl({})
    assert r"REGEXP_EXTRACT(COALESCE(`after`.event_ts, `before`.event_ts), '\.([0-9]+)', 1)" in ddl
    assert r"'\\.([0-9]+)'" not in ddl
    assert "'yyyy-MM-dd''T''HH:mm:ss'" in ddl
    assert "WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND" in ddl


def test_stateful_feature_job_rebuilds_state_on_fresh_start() -> None:
    """Windows are history: a fresh start must replay, not resume from committed offsets."""
    job = JOB.job(KAFKA, REGISTRY)
    assert "'scan.startup.mode' = 'earliest-offset'" in job
    assert "group-offsets" not in job
    bronze = (CARD_JOB.parents[1] / "bronze/bronze_cdc.sql").read_text()
    assert "'scan.startup.mode' = 'group-offsets'" in bronze  # stateless: resume is correct
