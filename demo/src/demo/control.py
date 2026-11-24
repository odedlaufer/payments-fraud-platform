"""Traffic, attacks and failures, driven from the console.

Traffic runs one simulator at a time, in back-to-back chunks (two at once would interleave
event times). An attack cuts the running chunk short (SIGINT: the simulator commits and stops
cleanly) so the next chunk starts with simulator run --attack <pattern>.

Failures, each with a recovery timer:
- kill_flink: SIGKILL the TaskManager; recovered when the jobs run and fresh decisions flow.
- pause_features: freeze the feature sink for 40 s; decisions must fall back to review, and
  approvals during the pause are counted (they must be 0); recovered when features are fresh.
- stop_cdc: stop Kafka Connect for 45 s; recovered when Debezium holds its slot again.
"""

import json
import signal
import subprocess
import threading
import time
import urllib.request
from collections import deque
from datetime import UTC, datetime
from typing import Any, ClassVar

from demo.config import ROOT, port
from demo.feed import DecisionFeed
from demo.status import HEALTH, P95, prometheus

CHUNK_SECONDS = 60
FRESH = 'sum(decisions_total{reason!="stale_features"})'


def now_ms() -> int:
    return int(time.time() * 1000)


class Traffic:
    def __init__(self, tps: int = 20) -> None:
        self.tps, self.on = tps, False
        self.pending: str | None = None
        self.proc: subprocess.Popen[str] | None = None
        self.log: deque[dict[str, str]] = deque(maxlen=12)
        self.lock = threading.Lock()

    def start(self) -> None:
        with self.lock:
            if self.on:
                return
            self.on = True
        threading.Thread(target=self._loop, name="traffic", daemon=True).start()

    def stop(self) -> None:
        self.on = False
        self._interrupt()

    def attack(self, pattern: str) -> None:
        # the entry first: the traffic thread may launch the attack the moment it is pending
        self.log.appendleft(
            {"at": datetime.now(UTC).strftime("%H:%M:%S"), "attack": pattern, "status": "queued"}
        )
        self.pending = pattern
        if self.on:
            self._interrupt()  # start the attack now, not at the end of the chunk
        else:
            self.start()

    def _interrupt(self) -> None:
        proc = self.proc
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGINT)

    def command(self, attack: str | None) -> list[str]:
        cmd = [
            "uv",
            "run",
            "simulator",
            "run",
            "--duration",
            str(CHUNK_SECONDS),
            "--tps",
            str(self.tps),
        ]
        return [*cmd, "--attack", attack] if attack else cmd

    def _loop(self) -> None:
        while self.on:
            attack, self.pending = self.pending, None
            if attack:
                self._mark(attack, "launched")  # this chunk carries it
            self.proc = subprocess.Popen(
                self.command(attack),
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            output, _ = self.proc.communicate()
            if attack and f"attack: no target for {attack}" in output:
                self._mark(attack, "no target right now: try again", current="launched")
        self.proc = None

    def _mark(self, attack: str, status: str, current: str = "queued") -> None:
        for entry in self.log:  # newest first
            if entry["attack"] == attack and entry["status"] == current:
                entry["status"] = status
                return

    def view(self) -> dict[str, Any]:
        return {"on": self.on, "tps": self.tps, "attacks": list(self.log)}


def flink_jobs_running() -> bool:
    url = f"http://localhost:{port('FLINK_UI_PORT', '8081')}/jobs/overview"
    with urllib.request.urlopen(url, timeout=3) as response:
        jobs = json.load(response)["jobs"]
    running = {j["name"] for j in jobs if j["state"] == "RUNNING"}
    return {"bronze-cdc", "features-card", "bronze-decisions"} <= running


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True, timeout=120)


class Chaos:
    KINDS: ClassVar[dict[str, str]] = {
        "kill_flink": "Kill the Flink TaskManager",
        "pause_features": "Cut the online feature store for 40 s",
        "stop_cdc": "Stop CDC (Kafka Connect) for 45 s",
    }

    def __init__(self, feed: DecisionFeed) -> None:
        self.feed = feed
        self.state: dict[str, Any] = {"kind": None, "phase": "idle"}

    def busy(self) -> bool:
        return self.state["phase"] in ("injecting", "recovering")

    def run(self, kind: str) -> bool:
        if kind not in self.KINDS or self.busy():
            return False
        self.state = {
            "kind": kind,
            "title": self.KINDS[kind],
            "phase": "injecting",
            "started": time.time(),
            "recovered_after_s": None,
            "note": "",
        }
        threading.Thread(
            target=getattr(self, f"_{kind}"), name=f"chaos-{kind}", daemon=True
        ).start()
        return True

    def _until(self, condition: Any, timeout: float = 240) -> float | None:
        start = time.time()
        while time.time() - start < timeout:
            try:
                if condition():
                    return time.time() - float(self.state["started"])
            except Exception:
                pass
            time.sleep(1)
        return None

    def _finish(self, recovered: float | None, note: str) -> None:
        self.state.update(
            phase="recovered" if recovered is not None else "not recovered",
            recovered_after_s=None if recovered is None else round(recovered, 1),
            note=note,
        )

    def _kill_flink(self) -> None:
        compose("kill", "-s", "SIGKILL", "flink-taskmanager")
        time.sleep(3)
        compose("up", "-d", "--no-deps", "flink-taskmanager")
        self.state["phase"] = "recovering"
        self._until(flink_jobs_running)
        base = prometheus(FRESH) or 0.0
        recovered = self._until(lambda: (prometheus(FRESH) or 0.0) > base)
        self._finish(
            recovered,
            "jobs restored from their last checkpoint: nothing lost, nothing counted twice",
        )

    def _pause_features(self) -> None:
        compose("pause", "feature-sink")
        paused = now_ms()
        self.state["phase"] = "recovering"
        time.sleep(40)
        compose("unpause", "feature-sink")
        resumed = now_ms()
        approved = self.feed.approvals_between(
            paused + 3000, resumed
        )  # 3 s: decisions already in flight
        fresh = P95.format(m="feature_age_seconds")
        recovered = self._until(lambda: (prometheus(fresh) or 99) < 5)
        self._finish(
            recovered,
            f"{approved} payments approved while features were stale: "
            "every one was held for review instead",
        )

    def _stop_cdc(self) -> None:
        compose("stop", "connect")
        self.state["phase"] = "recovering"
        time.sleep(45)
        compose("start", "connect")
        recovered = self._until(lambda: (prometheus(HEALTH["debezium"]) or 0) >= 1)
        self._finish(
            recovered,
            "Debezium resumed from its replication slot: the paused changes "
            "streamed through, none lost",
        )

    def view(self) -> dict[str, Any]:
        state = dict(self.state)
        if state.get("started") and self.busy():
            state["elapsed_s"] = round(time.time() - state["started"], 1)
        return state
