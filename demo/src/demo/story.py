"""Auto-play: the platform's story in about eight minutes, with narration.

Each step acts (traffic, an attack, a failure), waits until its outcome shows in the live
data (an attack caught, a recovery), and reports what was measured.
"""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from demo.control import Chaos, Traffic
from demo.feed import DecisionFeed
from demo.status import HEADLINE, prometheus

# What the presenter's banner says at each step
NARRATION = {
    "live": (
        "A payments app writes every card payment to Postgres. Debezium streams each change "
        "into Redpanda, Flink turns it into card features in Redis, and every payment gets an "
        "approve, review or decline decision, all within about a second."
    ),
    "card_testing": (
        "Fraudsters test stolen card numbers with tiny payments at many merchants in minutes. "
        "The card's features count the burst; the card_testing rule declines it."
    ),
    "impossible_travel": (
        "A cloned card is used in person thousands of kilometres from where its owner just "
        "paid. The features know the distance and the minutes between the two."
    ),
    "account_takeover": (
        "A new device, a changed email, then a large purchase. Customer features in Redis "
        "connect the three; the account_takeover rule declines the payment."
    ),
    "kill_flink": (
        "Now something breaks in the middle of traffic: the Flink TaskManager that computes "
        "every feature is killed. Watch the timer."
    ),
    "pause_features": (
        "If features cannot be trusted, the platform must never approve blind. The feature "
        "store is cut for 40 seconds."
    ),
    "lake": (
        "Every change and every decision also lands in an Iceberg lakehouse. dbt builds "
        "history and point-in-time training sets, and recomputes every feature offline to "
        "prove the online ones right. Lineage, SLOs and alerts are one click away below."
    ),
}


@dataclass
class Step:
    title: str
    narration: str
    action: Callable[[], Any] | None = None
    done: Callable[[], bool] | None = None
    timeout: float = 120
    min_seconds: float = 8
    result: Callable[[], str] | None = None


def number(name: str) -> float | None:
    try:
        return prometheus(HEADLINE[name])
    except Exception:
        return None


class Story:
    def __init__(self, traffic: Traffic, chaos: Chaos, feed: DecisionFeed) -> None:
        self.traffic, self.chaos, self.feed = traffic, chaos, feed
        self.state: dict[str, Any] = {"running": False, "index": -1}
        self._stop = threading.Event()

    def caught(self, pattern: str) -> int:
        return int(self.feed.view()["attacks"].get(pattern, {}).get("caught", 0))

    def attack_step(
        self, pattern: str, title: str, narration: str, needed: int, timeout: float
    ) -> Step:
        base: dict[str, int] = {}

        def act() -> None:
            base["caught"] = self.caught(pattern)
            self.traffic.attack(pattern)

        def tally() -> str:
            v = self.feed.view()["attacks"].get(pattern, {})
            caught, held, missed = (v.get(k, 0) for k in ("caught", "held", "missed"))
            return f"{caught} caught by rules, {held} held for review, {missed} missed"

        return Step(
            title,
            narration,
            act,
            lambda: self.caught(pattern) >= base.get("caught", 0) + needed,
            timeout,
            10,
            tally,
        )

    def chaos_step(self, kind: str, title: str, narration: str, timeout: float) -> Step:
        def finished() -> bool:
            return self.chaos.state.get("kind") == kind and self.chaos.state["phase"] in (
                "recovered",
                "not recovered",
            )

        def result() -> str:
            s = self.chaos.state
            took = (
                f"recovered in {s['recovered_after_s']} s; "
                if s.get("recovered_after_s") is not None
                else "not recovered; "
            )
            return took + str(s.get("note", ""))

        return Step(title, narration, lambda: self.chaos.run(kind), finished, timeout, 10, result)

    def steps(self) -> list[Step]:
        def live() -> str:
            p95, dpm = number("decision_p95_s"), number("decisions_per_min")
            return (
                f"p95 commit-to-decision {p95:.2f} s, {dpm:.0f} decisions a minute"
                if p95 is not None and dpm is not None
                else "traffic flowing"
            )

        def lake() -> str:
            parity = number("parity")
            return (
                f"online/offline feature parity {parity:.4%}"
                if parity is not None
                else "parity: no data yet"
            )

        def summary() -> str:
            attacks = self.feed.view()["attacks"]
            caught = sum(v.get("caught", 0) for v in attacks.values())
            missed = sum(v.get("missed", 0) for v in attacks.values())
            return f"attack payments: {caught} caught, {missed} missed; traffic keeps running"

        return [
            Step(
                "Live payments",
                NARRATION["live"],
                self.traffic.start,
                None,
                60,
                35,
                live,
            ),
            self.attack_step(
                "card_testing",
                "Attack: card testing",
                NARRATION["card_testing"],
                3,
                150,
            ),
            self.attack_step(
                "impossible_travel",
                "Attack: impossible travel",
                NARRATION["impossible_travel"],
                1,
                180,
            ),
            self.attack_step(
                "account_takeover",
                "Attack: account takeover",
                NARRATION["account_takeover"],
                1,
                210,
            ),
            self.chaos_step(
                "kill_flink",
                "Failure: kill Flink mid-traffic",
                NARRATION["kill_flink"],
                180,
            ),
            self.chaos_step(
                "pause_features",
                "Failure: the feature store goes away",
                NARRATION["pause_features"],
                150,
            ),
            Step(
                "The lakehouse behind it",
                NARRATION["lake"],
                None,
                None,
                30,
                25,
                lake,
            ),
            Step("Summary", "That was the platform end to end.", None, None, 10, 5, summary),
        ]

    def start(self) -> bool:
        if self.state.get("running"):
            return False
        self._stop.clear()
        threading.Thread(target=self._play, name="story", daemon=True).start()
        return True

    def stop(self) -> None:
        self._stop.set()

    def _play(self) -> None:
        steps = self.steps()
        self.state = {"running": True, "index": 0, "total": len(steps), "results": []}
        for i, step in enumerate(steps):
            if self._stop.is_set():
                break
            started = time.time()
            self.state.update(index=i, title=step.title, narration=step.narration, started=started)
            if step.action:
                step.action()
            while not self._stop.is_set():
                elapsed = time.time() - started
                if elapsed >= step.timeout:
                    break
                if elapsed >= step.min_seconds and (step.done is None or step.done()):
                    break
                time.sleep(1)
            outcome = step.result() if step.result else ""
            self.state["results"] = [
                *self.state["results"],
                {"title": step.title, "result": outcome},
            ]
        self.state.update(running=False, index=len(steps), title="Finished", narration="")

    def view(self) -> dict[str, Any]:
        state = dict(self.state)
        if state.get("running") and state.get("started"):
            state["elapsed_s"] = round(time.time() - state["started"], 1)
        return state
