"""make slo-report: the evidence for the latest SLO run (make slo-run) in docs/slo-report.md.

For every minute of the run, from Prometheus: the share of minutes each SLO held, its median and
worst value. Then every alert that fired and for how long; decisions, stale share and dead
letters; parity for each simulated day from the lakehouse; Dagster's runs by job and status;
and a final reconciliation of CDC, bronze and decisions bronze against Postgres.
"""

import json
import statistics
import subprocess
import sys
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cdc_reconcile import load_env
from gen_bronze import ROOT

RUNS = ROOT / "observability/slo-runs"
REPORT = ROOT / "docs/slo-report.md"
P95 = "histogram_quantile(0.95, sum by (le) (rate({m}_bucket[5m])))"


@dataclass(frozen=True)
class Slo:
    name: str
    expr: str
    target: str
    holds: Callable[[float], bool]
    show: Callable[[float], str]
    higher_is_better: bool = False


SLOS = [
    Slo(
        "Decision latency p95",
        P95.format(m="decision_latency_seconds"),
        "under 5 s",
        lambda v: v < 5,
        lambda v: f"{v:.2f} s",
    ),
    Slo(
        "Change capture lag p95",
        P95.format(m="cdc_lag_seconds"),
        "under 2 s",
        lambda v: v < 2,
        lambda v: f"{v * 1000:.0f} ms",
    ),
    Slo(
        "Online feature freshness p95",
        P95.format(m="feature_age_seconds"),
        "under 5 s",
        lambda v: v < 5,
        lambda v: f"{v * 1000:.0f} ms",
    ),
    Slo(
        "Lakehouse freshness",
        'time() - lake_table_last_commit_timestamp_seconds{table="cdc_transactions"}',
        "under 5 min",
        lambda v: v < 300,
        lambda v: f"{v:.0f} s",
    ),
    Slo(
        "Online/offline parity",
        "lake_parity_ratio",
        "at least 99.9%",
        lambda v: v >= 0.999,
        lambda v: f"{v:.4%}",
        higher_is_better=True,
    ),
]


def summarize(values: list[float], slo: Slo) -> dict[str, Any]:
    """Share of minutes the SLO held, the median and the worst minute; NaN (no traffic) skipped."""
    seen = [v for v in values if v == v]
    if not seen:
        return {"minutes": 0}
    worst = min(seen) if slo.higher_is_better else max(seen)
    return {
        "minutes": len(seen),
        "held": sum(slo.holds(v) for v in seen) / len(seen),
        "median": statistics.median(seen),
        "worst": worst,
    }


def alert_minutes(series: list[dict[str, Any]]) -> dict[str, int]:
    """Minutes each alert was firing, from a range query of ALERTS at a 60 s step."""
    minutes: Counter[str] = Counter()
    for s in series:
        minutes[s["metric"].get("alertname", "?")] += len(s["values"])
    return dict(minutes)


def prometheus(path: str, **params: Any) -> Any:
    env = load_env()
    url = (
        f"http://localhost:{env.get('PROMETHEUS_PORT', '9095')}/api/v1/{path}?"
        + urllib.parse.urlencode(params)
    )
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.load(response)["data"]["result"]


def latest_run() -> tuple[str, dict[str, Any]]:
    runs = sorted(RUNS.glob("*.json"))
    if not runs:
        sys.exit("no SLO run recorded: make slo-run first")
    return runs[-1].stem, json.loads(runs[-1].read_text())


def check(script: str) -> tuple[bool, str]:
    run = subprocess.run(
        ["uv", "run", "python", f"scripts/{script}"], cwd=ROOT, capture_output=True, text=True
    )
    # librdkafka's own log lines start with "%": the script's verdict is the last other line
    lines = [
        ln.strip()
        for ln in (run.stdout + run.stderr).splitlines()
        if ln.strip() and not ln.lstrip().startswith("%")
    ]
    return run.returncode == 0, lines[-1] if lines else ""


def trino_rows(sql: str) -> list[tuple[Any, ...]]:
    import trino

    port = int(load_env().get("TRINO_PORT", "8088"))
    cur = trino.dbapi.connect(host="127.0.0.1", port=port, user="slo-report").cursor()  # type: ignore[no-untyped-call]
    cur.execute(sql)
    return [tuple(r) for r in cur.fetchall()]


def dagster_runs(since: float) -> dict[str, Counter[str]]:
    runs = "runsOrError(filter: {createdAfter: " + str(since) + "}, limit: 2000)"
    query = "query { " + runs + " { ... on Runs { results { jobName status } } } }"
    body = json.dumps({"query": query}).encode()
    request = urllib.request.Request(
        "http://localhost:3070/graphql", data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        results = json.load(response)["data"]["runsOrError"]["results"]
    out: dict[str, Counter[str]] = {}
    for r in results:
        out.setdefault(r["jobName"], Counter())[r["status"]] += 1
    return out


def render(
    name: str, run: dict[str, Any], slos: list[tuple[Slo, dict[str, Any]]], extra: dict[str, Any]
) -> str:
    lines = [
        "# SLO run report",
        "",
        f"Generated by `make slo-report` for run `{name}`: {run.get('outcome', 'running')}, "
        f"{run.get('hours', '?')} hours of real time covering {run['sim_days']:g} simulated days "
        f"({str(run.get('sim_start', '?'))[:10]} to {str(run.get('sim_end', '?'))[:10]}) "
        f"at {run['tps']} payments a second.",
        "",
        "| Service level | Target | Minutes measured | Minutes within target | Median "
        "| Worst minute |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for slo, s in slos:
        if not s.get("minutes"):
            lines.append(f"| {slo.name} | {slo.target} | 0 | no data | | |")
            continue
        lines.append(
            f"| {slo.name} | {slo.target} | {s['minutes']:,} | {s['held']:.2%} | "
            f"{slo.show(s['median'])} | {slo.show(s['worst'])} |"
        )
    lines += ["", "## During the run", ""]
    lines.append(
        f"- **Decisions:** {extra['decisions']:,.0f}; "
        f"held for stale features: {extra['stale_share']:.3%}; "
        f"dead letters: {extra['dead_letters']:,.0f}"
    )
    alerts = extra["alerts"]
    lines.append(
        "- **Alerts:** none fired"
        if not alerts
        else "- **Alerts fired:** " + ", ".join(f"{k} ({v} min)" for k, v in sorted(alerts.items()))
    )
    if extra.get("parity_days"):
        days, compared, worst = extra["parity_days"]
        lines.append(
            f"- **Parity by simulated day:** {days} days, {compared:,} decisions compared, "
            f"worst day {worst:.4%}"
        )
    if extra.get("dagster"):
        jobs = "; ".join(
            f"{job} {', '.join(f'{n} {st.lower()}' for st, n in c.most_common())}"
            for job, c in sorted(extra["dagster"].items())
        )
        lines.append(f"- **Dagster runs:** {jobs}")
    lines += ["", "## At the end", ""]
    for label, (ok, detail) in extra["checks"].items():
        lines.append(f"- **{label}:** {'passed' if ok else 'FAILED'} ({detail})")
    return "\n".join(lines) + "\n"


def main() -> int:
    name, run = latest_run()
    start = datetime.fromisoformat(run["started_at"]).timestamp()
    end = (
        datetime.fromisoformat(run["ended_at"]).timestamp()
        if run.get("ended_at")
        else datetime.now().timestamp()
    )
    results = []
    for slo in SLOS:
        series = prometheus("query_range", query=slo.expr, start=start, end=end, step=60)
        values = [float(v) for _, v in series[0]["values"]] if series else []
        results.append((slo, summarize(values, slo)))
    window = f"{int(end - start)}s"

    def total(expr: str) -> float:
        res = prometheus("query", query=expr, time=end)
        return float(res[0]["value"][1]) if res else 0.0

    decisions = total(f"sum(increase(decisions_total[{window}]))")
    extra: dict[str, Any] = {
        "decisions": decisions,
        "stale_share": total(f'sum(increase(decisions_total{{reason="stale_features"}}[{window}]))')
        / decisions
        if decisions
        else 0.0,
        "dead_letters": total(f"sum(increase(dead_lettered_total[{window}]))"),
        "alerts": alert_minutes(
            prometheus(
                "query_range", query='ALERTS{alertstate="firing"}', start=start, end=end, step=60
            )
        ),
    }
    if run.get("sim_start") and run.get("sim_end"):
        rows = trino_rows(
            "select count(*), sum(compared), min(cast(matching as double) / compared) "
            "from lake.marts.parity_daily where feature = '(all features)' and compared > 0 "
            f"and event_day between date '{run['sim_start'][:10]}' and date '{run['sim_end'][:10]}'"
        )
        if rows and rows[0][0]:
            extra["parity_days"] = (int(rows[0][0]), int(rows[0][1]), float(rows[0][2]))
    try:
        extra["dagster"] = dagster_runs(start)
    except Exception:
        extra["dagster"] = {}
    extra["checks"] = {
        "CDC complete (Postgres vs the change log)": check("cdc_reconcile.py"),
        "Bronze exactly once (change log vs Iceberg)": check("bronze_reconcile.py"),
        "Every decision in the lakehouse once": check("bronze_decisions_reconcile.py"),
    }
    REPORT.write_text(render(name, run, results, extra))
    print(REPORT.read_text())
    return 0 if all(ok for ok, _ in extra["checks"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
