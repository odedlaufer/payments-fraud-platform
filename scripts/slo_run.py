"""make slo-run: the PRD's service-level run, 7 accelerated weeks (section 2, Phase 7 gate).

49 simulated days at 20 payments a second; at 10 real minutes per simulated day that is about
8.2 hours, so one night. Dagster should run in another terminal (make orchestrate) so its real
schedules build silver, marts, parity, training sets and backtests throughout.

    make slo-run                 # 49 simulated days
    make slo-run ARGS="--sim-days 1"   # a short rehearsal

The run is recorded in observability/slo-runs/<start>.json (with the simulator's log next to it);
Ctrl-C ends it early and is recorded too. Then: make slo-report.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

from cdc_reconcile import load_env
from gen_bronze import ROOT

RUNS = ROOT / "observability/slo-runs"
PROGRESS_EVERY_S = 15 * 60
MINUTES_PER_SIM_DAY = 10  # the simulator's default pace, passed explicitly below


def sim_time(env: dict[str, str]) -> str | None:
    """The newest simulated event time in Postgres: where the run starts or ended."""
    import psycopg

    parts = {
        "host": "localhost",
        "port": env.get("POSTGRES_PORT", "5432"),
        "dbname": env.get("POSTGRES_DB", ""),
        "user": env.get("POSTGRES_USER", ""),
        "password": env.get("POSTGRES_PASSWORD", ""),
    }
    dsn = " ".join(f"{k}={v}" for k, v in parts.items())
    with psycopg.connect(dsn) as conn:
        row = conn.execute("select max(event_ts) from transactions").fetchone()
    return row[0].isoformat() if row and row[0] else None


def reachable(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=3):
            return True
    except Exception:
        return False


def other_projects() -> list[str]:
    run = subprocess.run(
        ["docker", "ps", "--format", '{{.Label "com.docker.compose.project"}}'],
        capture_output=True,
        text=True,
    )
    return sorted({p for p in run.stdout.split() if p and p != "payments-fraud-platform"})


def progress(env: dict[str, str]) -> str:
    def q(expr: str) -> float | None:
        url = (
            f"http://localhost:{env.get('PROMETHEUS_PORT', '9095')}/api/v1/query?"
            + urllib.parse.urlencode({"query": expr})
        )
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                res = json.load(r)["data"]["result"]
            return float(res[0]["value"][1]) if res else None
        except Exception:
            return None

    p95 = q("histogram_quantile(0.95, sum by (le) (rate(decision_latency_seconds_bucket[5m])))")
    alerts = q('count(ALERTS{alertstate="firing"}) or vector(0)')
    return (
        f"decision p95 {p95:.2f} s, alerts firing {alerts:.0f}"
        if p95 is not None and alerts is not None
        else "no metrics"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sim-days", type=float, default=49)
    parser.add_argument("--tps", type=int, default=20)
    args = parser.parse_args()
    env = load_env()

    health = subprocess.run(
        ["make", "--no-print-directory", "health"], cwd=ROOT, capture_output=True, text=True
    )
    if health.returncode != 0:
        print("STOP: the stack is not healthy (make health): fix that first")
        return 1
    if not reachable("http://localhost:3070/server_info"):
        print(
            "WARNING: Dagster is not running: start make orchestrate in another terminal, "
            "so the run includes its schedules (silver, marts, parity, training sets, backtests)"
        )
    if others := other_projects():
        print(
            f"WARNING: other Docker projects are running ({', '.join(others)}): "
            "they share the VM's memory"
        )

    started = datetime.now(UTC)
    RUNS.mkdir(parents=True, exist_ok=True)
    name = started.strftime("%Y%m%dT%H%MZ")
    record: dict[str, Any] = {
        "started_at": started.isoformat(),
        "sim_days": args.sim_days,
        "tps": args.tps,
        "sim_start": sim_time(env),
    }
    (RUNS / f"{name}.json").write_text(json.dumps(record, indent=2) + "\n")
    awake = None
    if shutil.which("caffeinate"):  # macOS: no sleep while this process lives
        awake = subprocess.Popen(["caffeinate", "-dimsu", "-w", str(os.getpid())])
    hours = args.sim_days * MINUTES_PER_SIM_DAY / 60
    print(
        f"SLO run {name}: {args.sim_days:g} simulated days at {args.tps} payments/s, "
        f"about {hours:.1f} hours. Ctrl-C ends it early."
    )

    log = (RUNS / f"{name}.log").open("w")
    sim = subprocess.Popen(
        [
            "uv",
            "run",
            "simulator",
            "run",
            "--sim-days",
            str(args.sim_days),
            "--tps",
            str(args.tps),
            "--minutes-per-day",
            str(MINUTES_PER_SIM_DAY),
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        while sim.poll() is None:
            try:
                sim.wait(timeout=PROGRESS_EVERY_S)
            except subprocess.TimeoutExpired:
                elapsed = (datetime.now(UTC) - started).total_seconds() / 3600
                print(
                    f"  {datetime.now(UTC):%H:%M} UTC, {elapsed:.1f} h: {progress(env)}", flush=True
                )
        outcome = "completed" if sim.returncode == 0 else f"simulator exited {sim.returncode}"
    except KeyboardInterrupt:
        sim.terminate()
        sim.wait()
        outcome = "ended early (Ctrl-C)"
    finally:
        log.close()
        if awake:
            awake.terminate()
    ended = datetime.now(UTC)
    record.update(
        ended_at=ended.isoformat(),
        outcome=outcome,
        sim_end=sim_time(env),
        hours=round((ended - started).total_seconds() / 3600, 2),
    )
    (RUNS / f"{name}.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"SLO run {name}: {outcome} after {record['hours']} h. Next: make slo-report")
    return 0 if outcome == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
