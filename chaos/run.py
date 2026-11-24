"""Chaos suite (PRD 8.5): inject each fault during traffic, measure, check the expected outcome.

    make chaos                    # CH-1 .. CH-6 (about 35 minutes)
    make chaos ARGS="CH-3 CH-6"   # some

Stop Dagster first (its schedules would build dbt during the faults). Results are kept in
chaos/results.json and rendered to docs/chaos.md.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "chaos/results.json"
DOC = ROOT / "docs/chaos.md"


@dataclass
class Result:
    id: str
    scenario: str
    expected: str
    passed: bool = False
    outcome: str = ""
    measured: dict[str, Any] = field(default_factory=dict)
    ran_at: str = ""


def load_env() -> dict[str, str]:
    env = {}
    for line in (ROOT / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


ENV = load_env() if (ROOT / ".env").exists() else {}


def sh(*cmd: str, timeout: int = 1800) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(cmd), cwd=ROOT, capture_output=True, text=True, timeout=timeout)


def compose(*args: str) -> None:
    sh("docker", "compose", *args)


def check(*script: str) -> tuple[bool, str]:
    """A project check script: (passed, its last line)."""
    run = sh("uv", "run", "python", *script)
    lines = [ln for ln in (run.stdout + run.stderr).splitlines() if ln.strip()]
    return run.returncode == 0, lines[-1].strip() if lines else ""


def prom(query: str) -> float:
    port = ENV.get("PROMETHEUS_PORT", "9095")
    url = f"http://localhost:{port}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=10) as response:
        result = json.load(response)["data"]["result"]
    return float(result[0]["value"][1]) if result else 0.0


def get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def wait_until(condition: Callable[[], bool], timeout: float, step: float = 3) -> float | None:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            if condition():
                return time.monotonic() - start
        except Exception:
            pass
        time.sleep(step)
    return None


def traffic(seconds: int, tps: int = 20) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        ["uv", "run", "simulator", "run", "--duration", str(seconds), "--tps", str(tps)],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def psql(sql: str) -> str:
    user, db = ENV.get("POSTGRES_USER", "payments"), ENV.get("POSTGRES_DB", "payments")
    return sh(
        "docker", "compose", "exec", "-T", "postgres", "psql", "-U", user, "-d", db, "-tAc", sql
    ).stdout.strip()


def flink_jobs_running() -> bool:
    jobs = get_json(f"http://localhost:{ENV.get('FLINK_UI_PORT', '8081')}/jobs/overview")["jobs"]
    running = {j["name"] for j in jobs if j["state"] == "RUNNING"}
    return {"bronze-cdc", "features-card", "bronze-decisions"} <= running


def end_offset(topic: str) -> int:
    """Messages ever written to a topic; 0 for a topic not created yet (Redpanda creates
    a topic on its first write: dlq.late_events exists only once an event was late)."""
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": f"localhost:{ENV.get('REDPANDA_KAFKA_PORT', '19092')}",
            "group.id": "chaos-probe",
        }
    )
    try:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
        return int(high)
    except KafkaException as exc:
        if exc.args[0].code() in (KafkaError._UNKNOWN_PARTITION, KafkaError.UNKNOWN_TOPIC_OR_PART):
            return 0
        raise
    finally:
        consumer.close()


def decisions_between(lo_ms: int, hi_ms: int) -> tuple[int, int, int]:
    """(approved, stale reviews, all) decided in [lo_ms, hi_ms], from lake.bronze.decisions."""
    import trino

    port = int(ENV.get("TRINO_PORT", "8088"))
    cur = trino.dbapi.connect(host="127.0.0.1", port=port, user="chaos").cursor()  # type: ignore[no-untyped-call]
    cur.execute(
        "select count_if(decision = 'approve'), count_if(reason = 'stale_features'), count(*) "
        f"from lake.bronze.decisions where decided_ms between {lo_ms} and {hi_ms}"
    )
    approved, stale, total = cur.fetchone()
    return int(approved), int(stale), int(total)


def now_ms() -> int:
    return int(time.time() * 1000)


# PRD 8.5: scenario and expected outcome
PRD: dict[str, tuple[str, str]] = {
    "CH-1": (
        "Kill the Flink TaskManager mid-run",
        "Restores from checkpoint; no duplicate features; decisions resume < 60 s",
    ),
    "CH-2": (
        "Stop Kafka Connect for 5 minutes",
        "Debezium resumes from its offset; zero missing rows (DQ-08 passes)",
    ),
    "CH-3": (
        "Stop Redis for 2 minutes",
        "Decisions fall back to review (stale_features); no silent approvals",
    ),
    "CH-4": ("Publish malformed events", "Events land in the DLQ; the pipeline keeps running"),
    "CH-5": (
        "Breaking schema change (drop column)",
        "Rejected by the schema registry / CI contract check",
    ),
    "CH-6": (
        "Late events beyond the watermark",
        "Counted in dlq.late_events; the decision explains itself",
    ),
}

FRESH = 'sum(decisions_total{reason!="stale_features"})'


def ch1() -> Result:
    r = Result("CH-1", *PRD["CH-1"])
    sim = traffic(180)
    time.sleep(40)
    compose("kill", "-s", "SIGKILL", "flink-taskmanager")
    killed = time.monotonic()
    time.sleep(5)
    compose("up", "-d", "--no-deps", "flink-taskmanager")
    running = wait_until(flink_jobs_running, 240)
    base = prom(FRESH)
    resumed = wait_until(lambda: prom(FRESH) > base, 240, 2)
    recovery = None if running is None or resumed is None else round(time.monotonic() - killed, 1)
    sim.wait()
    time.sleep(70)  # bronze commits every 60 s
    bronze_ok, bronze = check("scripts/bronze_reconcile.py")
    online_ok, online = check("scripts/online_check.py")
    decided_ok, decided = check("scripts/decisions_check.py", "--since-minutes", "6")
    lake_ok, lake = check("scripts/bronze_decisions_reconcile.py")
    r.measured = {
        "recovery_s": recovery,
        "bronze": bronze,
        "online_store": online,
        "decisions": decided,
    }
    r.passed = (
        recovery is not None
        and recovery < 60
        and bronze_ok
        and online_ok
        and decided_ok
        and lake_ok
    )
    r.outcome = (
        f"fresh decisions resumed {recovery} s after the kill; "
        if recovery
        else "did not recover; "
    ) + (
        "bronze exactly once, online features equal the newest records, every decision once"
        if bronze_ok and online_ok and decided_ok and lake_ok
        else "a check failed (see measured)"
    )
    return r


def ch2() -> Result:
    r = Result("CH-2", *PRD["CH-2"])
    sim = traffic(330)
    time.sleep(10)
    compose("stop", "connect")
    stalled_alert = wait_until(
        lambda: prom('ALERTS{alertname="CdcStalled",alertstate="firing"}') > 0, 290, 10
    )
    time.sleep(max(0, 300 - 10 - (stalled_alert or 290)))
    restarted = now_ms()
    compose("start", "connect")
    resumed = wait_until(
        lambda: psql(
            "select active from pg_replication_slots where slot_name = 'payments_cdc_slot'"
        )
        == "t",
        240,
    )
    sim.wait()
    time.sleep(30)
    cdc_ok, cdc = check("scripts/cdc_reconcile.py")
    silver_build = [
        "uv",
        "run",
        "dbt",
        "build",
        "--profiles-dir",
        ".",
        "--select",
        "staging",
        "silver",
    ]
    build = subprocess.run(
        [*silver_build, "--indirect-selection", "cautious"],
        cwd=ROOT / "dbt",
        capture_output=True,
        text=True,
        timeout=1800,
    )
    silver_ok, silver = check("scripts/silver_reconcile.py")
    _, catchup_stale, catchup = decisions_between(restarted, restarted + 120_000)
    r.measured = {
        "cdc_stalled_alert_after_s": stalled_alert,
        "slot_active_again_after_s": resumed,
        "cdc": cdc,
        "silver_build_rc": build.returncode,
        "silver": silver,
    }
    r.passed = resumed is not None and cdc_ok and build.returncode == 0 and silver_ok
    r.outcome = (f"CdcStalled fired after {stalled_alert:.0f} s; " if stalled_alert else "") + (
        f"Debezium resumed {resumed:.0f} s after the restart; "
        "CDC complete and silver equals Postgres"
        if r.passed
        else "a check failed (see measured)"
    )
    return r


def ch3() -> Result:
    r = Result("CH-3", *PRD["CH-3"])
    sim = traffic(240)
    time.sleep(20)
    stopped = now_ms()
    compose("stop", "redis")
    time.sleep(120)
    restarted = now_ms()
    compose("start", "redis")
    healthy = wait_until(
        lambda: "healthy"
        in sh("docker", "compose", "ps", "feature-sink", "--format", "{{.Status}}").stdout,
        900,
        10,
    )
    sim.wait()
    time.sleep(75)  # the outage's decisions reach bronze (60 s commits)
    # from 3 s after the stop: a decision fetched before it may complete within its 3 s wait
    approved, stale, total = decisions_between(stopped + 3000, restarted)
    r.measured = {
        "decisions_during_outage": total,
        "approved_during_outage": approved,
        "stale_reviews_during_outage": stale,
        "feature_sink_healthy_after_s": healthy,
        "counted_from": "lake.bronze.decisions decided_ms (exact), not Prometheus (15 s scrapes)",
    }
    r.passed = total > 0 and approved == 0 and stale == total and healthy is not None
    r.outcome = (
        f"all {total} decisions during the outage were review/stale_features, 0 approved; "
        f"the feature sink rebuilt Redis in {healthy:.0f} s"
        if r.passed
        else f"{approved} of {total} decisions approved during the outage, or no recovery"
    )
    return r


def ch4() -> Result:
    r = Result("CH-4", *PRD["CH-4"])
    from confluent_kafka import Producer

    before = prom("sum(dead_lettered_total)")
    restarts0 = sh(
        "docker", "inspect", "-f", "{{.RestartCount}}", "payments-fraud-platform-decision-service-1"
    ).stdout.strip()
    producer = Producer(
        {"bootstrap.servers": f"localhost:{ENV.get('REDPANDA_KAFKA_PORT', '19092')}"}
    )
    for i in range(25):
        producer.produce(
            "retry.decision-service",
            key=f"chaos-{i}".encode(),
            value=b"\x00not avro " + str(i).encode(),
        )
    producer.flush()
    landed = wait_until(lambda: prom("sum(dead_lettered_total)") - before >= 25, 120, 5)
    restarts = sh(
        "docker", "inspect", "-f", "{{.RestartCount}}", "payments-fraud-platform-decision-service-1"
    ).stdout.strip()
    flink_ok = wait_until(flink_jobs_running, 30) is not None
    r.measured = {
        "dead_lettered": prom("sum(dead_lettered_total)") - before,
        "service_restarts": int(restarts) - int(restarts0),
        "flink_jobs_running": flink_ok,
        "scope": "the decision service's input (retry topic); see the note on CDC topics",
    }
    r.passed = landed is not None and restarts == restarts0 and flink_ok
    r.outcome = (
        f"25 malformed events dead-lettered in {landed:.0f} s, 0 restarts, Flink unaffected"
        if r.passed
        else "a check failed (see measured)"
    )
    return r


def value_record(schema: dict[str, Any]) -> dict[str, Any]:
    """Debezium's row record: defined once (in before) and referenced by name in after."""
    for f in schema["fields"]:
        for t in f["type"] if isinstance(f["type"], list) else [f["type"]]:
            if isinstance(t, dict) and t.get("type") == "record":
                return t
    raise ValueError("no row record in the envelope schema")


def ch5() -> Result:
    r = Result("CH-5", *PRD["CH-5"])
    registry = f"http://localhost:{ENV.get('REDPANDA_SCHEMA_REGISTRY_PORT', '18081')}"
    subject = "pg.public.transactions-value"
    latest = get_json(f"{registry}/subjects/{subject}/versions/latest")
    schema = json.loads(latest["schema"])
    record = value_record(schema)

    def compatible(candidate: dict[str, Any]) -> bool:
        body = json.dumps({"schema": json.dumps(candidate)}).encode()
        request = urllib.request.Request(
            f"{registry}/compatibility/subjects/{subject}/versions/latest",
            data=body,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return bool(json.load(response)["is_compatible"])

    required = json.loads(json.dumps(schema))
    value_record(required)["fields"].append(
        {"name": "chaos_required", "type": "string"}
    )  # no default
    dropped = json.loads(json.dumps(schema))
    rec = value_record(dropped)
    rec["fields"] = [f for f in rec["fields"] if f["name"] != "amount"]
    add_required_ok, drop_ok = compatible(required), compatible(dropped)
    contract = ROOT / "contracts/decisions.odcs.yaml"
    import yaml

    doc = yaml.safe_load(contract.read_text())
    doc["schema"][0]["properties"] = [
        p for p in doc["schema"][0]["properties"] if p["name"] != "card_id"
    ]
    broken = Path("/tmp/chaos-decisions.odcs.yaml")
    broken.write_text(yaml.safe_dump(doc, sort_keys=False))
    breaking = sh(
        "uvx",
        "--from",
        "datacontract-cli[avro]==1.2.9",
        "datacontract",
        "breaking",
        str(contract),
        str(broken),
    ).returncode
    r.measured = {
        "registry_accepts_new_required_field": add_required_ok,
        "registry_accepts_dropped_column": drop_ok,
        "contract_check_flags_dropped_field": breaking != 0,
        "fields_in_record": len(record["fields"]),
    }
    r.passed = not add_required_ok and breaking != 0
    r.outcome = (
        "the registry (BACKWARD) rejects a new required field; a dropped column is "
        "backward compatible for readers, so the contract check (DC-3) refuses it: "
        "datacontract flags it as breaking"
    )
    return r


def ch6() -> Result:
    r = Result("CH-6", *PRD["CH-6"])
    sim = traffic(120)
    time.sleep(40)
    late0 = end_offset("dlq.late_events")
    cols = (
        "card_id, merchant_id, session_id, amount, currency, channel, status, lat, lon, "
        "device_risk_score"
    )
    txn = psql(
        f"insert into transactions ({cols}, event_ts) select {cols}, event_ts - interval '3 hours' "
        "from transactions where transaction_id = (select max(transaction_id) from transactions) "
        "returning transaction_id"
    ).splitlines()[0]
    counted = wait_until(lambda: end_offset("dlq.late_events") > late0, 120, 5)
    sim.wait()
    time.sleep(75)  # decision reaches bronze.decisions (60 s commits)
    port = ENV.get("TRINO_PORT", "8088")
    decision = sh(
        "docker",
        "compose",
        "exec",
        "-T",
        "trino",
        "trino",
        "--execute",
        f"select decision, reason from lake.bronze.decisions where transaction_id = {txn}",
    ).stdout.strip()
    r.measured = {
        "transaction_id": int(txn),
        "late_events_counted": end_offset("dlq.late_events") - late0,
        "decision": decision,
        "trino_port": port,
    }
    r.passed = counted is not None and "approve" not in decision and decision != ""
    r.outcome = (
        f"the 3-hour-late transaction {txn} went to dlq.late_events in {counted:.0f} s; "
        f"its decision: {decision or 'none'}"
        if counted
        else "the late event was not counted"
    )
    return r


SCENARIOS: dict[str, Callable[[], Result]] = {
    "CH-1": ch1,
    "CH-2": ch2,
    "CH-3": ch3,
    "CH-4": ch4,
    "CH-5": ch5,
    "CH-6": ch6,
}

NOTES = """\
## Notes

- **CH-4 scope.** Malformed events are published to the decision service's input (its retry
  topic), which dead-letters them. The CDC topics are written only by Debezium against the
  schema registry (DC-1); the Flink jobs that read them have no dead-letter path, so a
  malformed record there would stop those jobs until skipped. Closing that needs a raw-bytes
  source with a decoding step that routes failures to a DLQ.
- **CH-5.** Under BACKWARD compatibility a dropped column is readable by new consumers, so the
  registry accepts it; the contract check (DC-3) is the gate that refuses it.
- **CH-6.** A late transaction's features are never computed online (it goes to
  dlq.late_events), so its decision is review with stale_features and it is excluded from
  parity rather than counted as a mismatch.
"""


def render(results: dict[str, dict[str, Any]]) -> str:
    lines = [
        "# Chaos results (PRD 8.5)",
        "",
        "Generated by `make chaos` (chaos/run.py) from chaos/results.json.",
        "",
        "| ID | Scenario | Expected | Result | Outcome |",
        "| --- | --- | --- | --- | --- |",
    ]
    for cid in sorted(results):
        x = results[cid]
        lines.append(
            f"| {cid} | {x['scenario']} | {x['expected']} | "
            f"{'PASS' if x['passed'] else 'FAIL'} | {x['outcome']} |"
        )
    lines += ["", "## Measured", ""]
    for cid in sorted(results):
        x = results[cid]
        lines.append(
            f"- **{cid}** ({x['ran_at']}): "
            + ", ".join(f"{k} = {v}" for k, v in x["measured"].items())
        )
    return "\n".join(lines) + "\n\n" + NOTES


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ids", nargs="*", default=list(SCENARIOS))
    args = parser.parse_args()
    results = json.loads(RESULTS.read_text()) if RESULTS.exists() else {}
    failed = 0
    for cid in args.ids:
        print(f"== {cid}: {SCENARIOS[cid].__name__} ...", flush=True)
        result = SCENARIOS[cid]()
        result.ran_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        results[cid] = asdict(result)
        RESULTS.write_text(json.dumps(results, indent=2) + "\n")
        DOC.write_text(render(results))
        print(f"   {'PASS' if result.passed else 'FAIL'}: {result.outcome}", flush=True)
        failed += not result.passed
    print(f"{len(args.ids) - failed} of {len(args.ids)} passed; docs/chaos.md updated")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
