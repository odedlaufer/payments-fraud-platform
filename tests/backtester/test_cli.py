"""The backtest command end to end, on a DuckDB database laid out like the lake."""

from pathlib import Path
from typing import Any

import duckdb
import pytest

import backtester.cli as cli
from decision_service.context import SNAPSHOT_FEATURES, TRANSACTION_FIELDS

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def lake(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("set timezone = 'UTC'")
    con.execute("attach ':memory:' as lake")
    con.execute("create schema lake.ml; create schema lake.ops")
    con.execute(
        "create table lake.ops.training_sets (snapshot_id bigint, cutoff timestamptz, rows bigint, "
        "positives bigint, published_at timestamptz)"
    )
    con.execute("""insert into lake.ops.training_sets values
        (111, '2026-03-30 00:00:00+00', 2, 1, '2026-11-19 11:49:53+00'),
        (222, '2026-03-15 00:00:00+00', 1, 0, '2026-11-19 11:50:00+00')""")
    context = [*SNAPSHOT_FEATURES, *TRANSACTION_FIELDS]
    types = {c: "varchar" if c == "channel" else "double" for c in context}
    types["amount"] = "decimal(12,2)"
    cols = ", ".join(f"{c} {t}" for c, t in types.items())
    con.execute(
        "create table lake.ml.training_set (transaction_id bigint, event_ts timestamptz, "
        f"label integer, cutoff timestamptz, {cols})"
    )
    con.execute("""insert into lake.ml.training_set
        (transaction_id, event_ts, label, cutoff, amount, txn_count_1h, channel)
        values (1, '2026-01-10 00:00:00+00', 1, '2026-03-30 00:00:00+00', 4000, 9, 'online'),
               (2, '2026-01-11 00:00:00+00', 0, '2026-03-30 00:00:00+00', 20, 1, 'online'),
               (3, '2026-01-05 00:00:00+00', 0, '2026-03-15 00:00:00+00', 30, 1, 'online')""")
    monkeypatch.setattr(cli, "connect", lambda env: con)
    monkeypatch.setattr(cli, "snapshot_source", lambda snapshot_id: "lake.ml.training_set")
    monkeypatch.setattr(cli, "load_env", lambda: {})
    monkeypatch.setattr(cli, "REPORTS", tmp_path)
    monkeypatch.setattr(cli, "ROOT", tmp_path.parent)
    return con


def test_backtests_the_latest_published_set(
    lake: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = cli.main(["--rules", str(ROOT / "rules/rules_v1.yml"), "--no-store"])
    out = capsys.readouterr().out
    assert code == 0
    assert "snapshot 222" in out  # the latest publication (published_at), cutoff 2026-03-15
    assert "**1**" not in out and "transactions:** 1," in out  # only that cutoff's row
    assert (tmp_path / "rules_v1_222.md").exists()


def test_an_explicit_snapshot_is_used(
    lake: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        cli.main(
            ["--rules", str(ROOT / "rules/rules_v1.yml"), "--snapshot-id", "111", "--no-store"]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "transactions:** 2, of which **1** labelled fraud" in out
    assert "daily_spend" in out  # every rule of the file reported


def test_an_unknown_snapshot_is_refused(lake: Any, capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        cli.main(
            ["--rules", str(ROOT / "rules/rules_v1.yml"), "--snapshot-id", "999", "--no-store"]
        )
        == 1
    )
    assert "not a published training set" in capsys.readouterr().out


def test_compares_versions_side_by_side(
    lake: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rules = [
        "--rules",
        str(ROOT / "rules/rules_v1.yml"),
        "--rules",
        str(ROOT / "rules/rules_v2.yml"),
    ]
    assert cli.main([*rules, "--snapshot-id", "111", "--no-store"]) == 0
    out = capsys.readouterr().out
    assert "# Backtest comparison: v1 vs v2" in out
    assert "| overall | v1 | v2 | change |" in out
    takeover = next(line for line in out.splitlines() if line.startswith("| account_takeover |"))
    assert takeover.split("|")[2].strip() == "-"  # not a v1 rule
    assert (tmp_path / "compare_rules_v1_vs_rules_v2_111.md").exists()
    assert (tmp_path / "rules_v1_111.md").exists() and (tmp_path / "rules_v2_111.md").exists()


def test_a_recorded_fixture_needs_no_lake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    """CI's rules.yml: backtest from a Parquet training set, nothing stored."""

    def column(c: str) -> str:
        kind = "varchar" if c == "channel" else "double"
        value = "9" if c == "txn_count_1h" else "null"
        return f"cast({value} as {kind}) as {c}"

    features = ", ".join(
        column(c) for c in [*SNAPSHOT_FEATURES, *TRANSACTION_FIELDS] if c != "amount"
    )
    row = (
        "1::bigint as transaction_id, timestamptz '2026-01-10 00:00:00+00' as event_ts, "
        "1 as label, timestamptz '2026-03-30 00:00:00+00' as cutoff, "
        "4000.00::decimal(12,2) as amount"
    )
    fixture = tmp_path / "training_set.parquet"
    duckdb.connect().execute(f"copy (select {row}, {features}) to '{fixture}' (format parquet)")
    monkeypatch.setattr(cli, "REPORTS", tmp_path)
    monkeypatch.setattr(cli, "ROOT", tmp_path.parent)
    monkeypatch.setattr(cli, "store", lambda *a: pytest.fail("a fixture run must not store"))
    rules = [
        "--rules",
        str(ROOT / "rules/rules_v1.yml"),
        "--rules",
        str(ROOT / "rules/rules_v2.yml"),
    ]
    assert cli.main(["--fixture", str(fixture), *rules]) == 0
    out = capsys.readouterr().out
    assert (
        "snapshot fixture training_set.parquet" in out
        and "transactions:** 1, of which **1**" in out
    )
    assert (tmp_path / "compare_rules_v1_vs_rules_v2_fixture training_set.parquet.md").exists()
