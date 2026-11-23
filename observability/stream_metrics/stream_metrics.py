"""Streaming-path metrics for Prometheus (OBS-1, PRD section 7 alerts), served on port 9110.

- pg_replication_slot_active{slot}, pg_replication_slot_lag_bytes{slot}: the CDC slot is
  held by Debezium and keeps up (a heartbeat every 10 s advances it even when idle).
- connect_task_running{connector, task}: 1 when RUNNING; 0 otherwise, also when Connect is
  unreachable (a stopped worker is a stalled CDC pipeline).
- flink_job_running{flink_job}, flink_job_restarts{flink_job}, flink_checkpoints_completed,
  flink_checkpoints_failed{flink_job}: from Flink's REST API (counters since the job started).
  The label is flink_job, not job: Prometheus keeps job for the scrape target.
- stream_metrics_errors{source}: failed reads.
"""

import json
import os
import time
import urllib.request
from typing import Any

import psycopg
from prometheus_client import Counter, Gauge, start_http_server

SLOT_ACTIVE = Gauge("pg_replication_slot_active", "Replication slot held by a consumer", ["slot"])
SLOT_LAG = Gauge(
    "pg_replication_slot_lag_bytes", "WAL not yet confirmed by the slot's consumer", ["slot"]
)
CONNECT_TASK = Gauge(
    "connect_task_running", "Kafka Connect task RUNNING (1) or not (0)", ["connector", "task"]
)
FLINK_RUNNING = Gauge("flink_job_running", "Flink job RUNNING (1) or not (0)", ["flink_job"])
FLINK_RESTARTS = Gauge("flink_job_restarts", "Flink job restarts since submission", ["flink_job"])
CKPT_DONE = Gauge(
    "flink_checkpoints_completed", "Completed checkpoints since the job started", ["flink_job"]
)
CKPT_FAILED = Gauge(
    "flink_checkpoints_failed", "Failed checkpoints since the job started", ["flink_job"]
)
ERRORS = Counter("stream_metrics_errors", "Failed reads", ["source"])

CONNECTOR = "payments-postgres"
FLINK_JOBS = ("bronze-cdc", "features-card", "bronze-decisions")


def get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def read_slots(dsn: str) -> None:
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            rows = conn.execute(
                "select slot_name, active, "
                "coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0) "
                "from pg_replication_slots"
            ).fetchall()
        for slot, active, lag in rows:
            SLOT_ACTIVE.labels(slot=slot).set(1 if active else 0)
            SLOT_LAG.labels(slot=slot).set(float(lag))
    except Exception:
        ERRORS.labels(source="postgres").inc()


def read_connect(base: str) -> None:
    try:
        status = get_json(f"{base}/connectors/{CONNECTOR}/status")
        tasks = status.get("tasks") or [{"id": 0, "state": "UNASSIGNED"}]
        for task in tasks:
            CONNECT_TASK.labels(connector=CONNECTOR, task=str(task["id"])).set(
                1 if task["state"] == "RUNNING" else 0
            )
    except Exception:
        CONNECT_TASK.labels(connector=CONNECTOR, task="0").set(0)
        ERRORS.labels(source="connect").inc()


def read_flink(base: str) -> None:
    try:
        overview = get_json(f"{base}/jobs/overview")["jobs"]
    except Exception:
        for name in FLINK_JOBS:
            FLINK_RUNNING.labels(flink_job=name).set(0)
        ERRORS.labels(source="flink").inc()
        return
    for name in FLINK_JOBS:
        # cancelled and finished runs stay listed under the same name: the running one counts
        same = [j for j in overview if j["name"] == name]
        job = next((j for j in same if j["state"] == "RUNNING"), same[0] if same else None)
        if job is None or job["state"] != "RUNNING":
            FLINK_RUNNING.labels(flink_job=name).set(0)
            continue
        FLINK_RUNNING.labels(flink_job=name).set(1)
        try:
            counts = get_json(f"{base}/jobs/{job['jid']}/checkpoints")["counts"]
            CKPT_DONE.labels(flink_job=name).set(counts["completed"])
            CKPT_FAILED.labels(flink_job=name).set(counts["failed"])
            metric = get_json(f"{base}/jobs/{job['jid']}/metrics?get=numRestarts")
            if metric:
                FLINK_RESTARTS.labels(flink_job=name).set(float(metric[0]["value"]))
        except Exception:
            ERRORS.labels(source=f"flink:{name}").inc()


def main() -> None:
    start_http_server(int(os.environ.get("METRICS_PORT", "9110")))
    dsn = (
        f"host={os.environ.get('POSTGRES_HOST', 'postgres')} port=5432 "
        f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )
    connect = os.environ.get("CONNECT_URL", "http://connect:8083")
    flink = os.environ.get("FLINK_URL", "http://flink-jobmanager:8081")
    while True:
        read_slots(dsn)
        read_connect(connect)
        read_flink(flink)
        time.sleep(int(os.environ.get("REFRESH_SECONDS", "15")))


if __name__ == "__main__":
    main()
