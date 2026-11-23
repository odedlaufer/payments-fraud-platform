"""Lineage (OBS-2): one connected graph from Postgres to the training set, sent best-effort."""

import json
import re
import subprocess
from pathlib import Path
from typing import Any, cast

import pytest

from orchestration import lineage

ROOT = Path(__file__).resolve().parents[2]


def test_trino_namespace_is_what_dbt_ol_derives_from_the_profile() -> None:
    """dbt-ol names datasets trino://{host}:{port} from the dbt profile; ours must match."""
    profile = (ROOT / "dbt/profiles.yml").read_text()
    host = re.search(r"host: \"\{\{ env_var\('TRINO_HOST', '([^']+)'\) \}\}\"", profile)
    port = re.search(r"port: \"\{\{ env_var\('TRINO_PORT', '([^']+)'\)", profile)
    assert host and port
    assert f"trino://{host.group(1)}:{port.group(1)}" == lineage.TRINO


def test_every_dbt_source_is_produced_by_the_streaming_path() -> None:
    """So dbt's lineage and the streaming jobs' lineage form one graph in Marquez."""
    manifest = json.loads((ROOT / "dbt/target/manifest.json").read_text())
    sources = {
        f"{s['database']}.{s['schema']}.{s['identifier']}" for s in manifest["sources"].values()
    }
    produced = {
        name
        for _, _, outputs in lineage.streaming_jobs("payments")
        for ns, name in outputs
        if ns == lineage.TRINO
    }
    assert sources and sources <= produced, sorted(sources - produced)


def test_every_streaming_input_traces_back_to_postgres() -> None:
    jobs = lineage.streaming_jobs("payments")
    produced = {d for _, _, outputs in jobs for d in outputs}
    for job, inputs, _ in jobs:
        for dataset in inputs:
            assert (
                dataset in produced or dataset[0] == lineage.POSTGRES
            ), f"{job}: {dataset} comes from nowhere"


class FakeClient:
    def __init__(self, fail: bool = False) -> None:
        self.events: list[Any] = []
        self.fail = fail

    def emit(self, event: Any) -> None:
        if self.fail:
            raise ConnectionError("Marquez is down")
        self.events.append(event)


def test_a_run_is_a_start_and_a_complete_with_its_datasets() -> None:
    fake = FakeClient()
    ok = lineage.emit_run(
        "dagster.rule_backtests",
        [lineage.lake("ml.training_set")],
        [lineage.lake("marts.rule_backtests")],
        cast(Any, fake),
    )
    assert ok
    start, complete = fake.events
    assert (start.eventType.value, complete.eventType.value) == ("START", "COMPLETE")
    assert start.run.runId == complete.run.runId
    assert start.job.namespace == lineage.NAMESPACE
    assert [(d.namespace, d.name) for d in start.inputs] == [
        (lineage.TRINO, "lake.ml.training_set")
    ]


def test_marquez_down_never_fails_the_caller() -> None:
    assert lineage.emit_run("x", [], [], cast(Any, FakeClient(fail=True))) is False


def test_dbt_lineage_reads_that_builds_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_run(args: list[str], **kwargs: Any) -> Any:
        seen.update(args=args, env=kwargs["env"])
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert lineage.send_dbt("/tmp/run-123")
    assert seen["args"][1:2] == ["send-events"] and seen["args"][-2:] == [
        "--target-path",
        "/tmp/run-123",
    ]
    assert seen["env"]["OPENLINEAGE_NAMESPACE"] == lineage.NAMESPACE
    assert seen["env"]["DBT_PROFILES_DIR"] == str(ROOT / "dbt")

    def missing(args: list[str], **kwargs: Any) -> Any:
        raise FileNotFoundError("dbt-ol")

    monkeypatch.setattr(subprocess, "run", missing)
    assert lineage.send_dbt("/tmp/run-123") is False
