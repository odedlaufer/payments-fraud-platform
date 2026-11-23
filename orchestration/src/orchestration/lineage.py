"""OpenLineage events to Marquez (OBS-2): lineage from Postgres to the training set.

- dbt models: after each dbt build, `dbt-ol send-events` reads that build's artifacts and
  emits every model's inputs and outputs (dagster-dbt keeps running plain dbt).
- Dagster's Python assets emit their own run (publish: marts.training_set -> ml.training_set;
  backtests: ml.training_set -> marts.rule_backtests).
- The streaming path (Debezium, Flink, the feature sink, the decision service) is described
  here from the platform's configuration and sent by `make lineage`.

Dataset namespaces follow OpenLineage's naming: Trino tables use the dbt profile's address
(trino://127.0.0.1:8088, as dbt-ol computes it), so dbt's sources and the streaming jobs'
outputs are the same datasets and the graph connects. Lineage is best-effort: when Marquez
is down a warning is logged and no data job fails because of it.
"""

import logging
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.transport.http import HttpConfig, HttpTransport

ROOT = Path(__file__).resolve().parents[3]
URL = os.environ.get("OPENLINEAGE_URL", "http://localhost:5050")
NAMESPACE = "payments-fraud-platform"  # namespace of the jobs
PRODUCER = "https://github.com/odedlaufer/payments-fraud-platform"
TRINO = (
    f"trino://{os.environ.get('TRINO_HOST', '127.0.0.1')}:{os.environ.get('TRINO_PORT', '8088')}"
)
POSTGRES, KAFKA, REDIS = "postgres://postgres:5432", "kafka://redpanda:9092", "redis://redis:6379"
TABLES = ("customers", "cards", "merchants", "sessions", "transactions", "chargebacks")
Dataset = tuple[str, str]  # (namespace, name)

log = logging.getLogger(__name__)


def lake(name: str) -> Dataset:
    return (TRINO, f"lake.{name}")


def streaming_jobs(database: str) -> list[tuple[str, list[Dataset], list[Dataset]]]:
    """(job, inputs, outputs) of the streaming path, from the platform's configuration."""
    topics = [(KAFKA, f"pg.public.{t}") for t in TABLES]
    return [
        (
            "debezium.payments-postgres",
            [(POSTGRES, f"{database}.public.{t}") for t in TABLES],
            topics,
        ),
        ("flink.bronze-cdc", topics, [lake(f"bronze.cdc_{t}") for t in TABLES]),
        ("flink.features-card", [(KAFKA, "pg.public.transactions")], [(KAFKA, "features.card")]),
        ("feature-sink", [(KAFKA, "features.card")], [(REDIS, "card-features")]),
        (
            "decision-service",
            [(KAFKA, "pg.public.transactions"), (REDIS, "card-features")],
            [(KAFKA, "decisions"), (KAFKA, "dlq.decision-service")],
        ),
        ("flink.bronze-decisions", [(KAFKA, "decisions")], [lake("bronze.decisions")]),
    ]


def client() -> OpenLineageClient:
    config = HttpConfig(url=URL, timeout=3.0, retry={"total": 0})  # never slow a data job down
    return OpenLineageClient(transport=HttpTransport(config))


def emit_run(
    job: str, inputs: list[Dataset], outputs: list[Dataset], ol: OpenLineageClient | None = None
) -> bool:
    """A completed run of `job` reading `inputs` and writing `outputs`. False if not sent."""
    ol = ol or client()
    run, now = Run(runId=str(uuid.uuid4())), datetime.now(UTC).isoformat()
    events = [
        RunEvent(
            eventType=state,
            eventTime=now,
            run=run,
            job=Job(namespace=NAMESPACE, name=job),
            producer=PRODUCER,
            inputs=[InputDataset(namespace=ns, name=name) for ns, name in inputs],
            outputs=[OutputDataset(namespace=ns, name=name) for ns, name in outputs],
        )
        for state in (RunState.START, RunState.COMPLETE)
    ]
    try:
        for event in events:
            ol.emit(event)
        return True
    except Exception as exc:
        log.warning("lineage for %s not sent to %s: %s", job, URL, exc)
        return False


def send_dbt(target_path: Path | str) -> bool:
    """Every model of one dbt build, from its artifacts (dbt-ol send-events)."""
    dbt_ol = Path(sys.executable).parent / "dbt-ol"
    env = {
        **os.environ,
        "OPENLINEAGE_URL": URL,
        "OPENLINEAGE_NAMESPACE": NAMESPACE,
        "DBT_PROFILES_DIR": str(ROOT / "dbt"),
    }
    try:
        run = subprocess.run(
            [
                str(dbt_ol),
                "send-events",
                "--project-dir",
                str(ROOT / "dbt"),
                "--target-path",
                str(target_path),
            ],
            cwd=ROOT / "dbt",
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        log.warning("dbt lineage not sent: %s", exc)
        return False
    if run.returncode != 0:
        log.warning(
            "dbt lineage not sent (dbt-ol exited %s): %s", run.returncode, run.stderr[-500:]
        )
    return run.returncode == 0
