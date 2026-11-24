"""Demo console: make demo, then http://localhost:8090.

One page (static/index.html) polls /api/state each second; buttons call the actions below.
"""

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from demo import status
from demo.config import LINKS
from demo.control import Chaos, Traffic
from demo.feed import DecisionFeed
from demo.story import Story

STATIC = Path(__file__).parent / "static"
# The attacks the rules catch; merchant bust-out has no rule yet (a known gap), so no button.
ATTACKS = {
    "card_testing": "Card testing",
    "impossible_travel": "Impossible travel",
    "account_takeover": "Account takeover",
}

feed = DecisionFeed()
traffic = Traffic()
chaos = Chaos(feed)
story = Story(traffic, chaos, feed)
_cache: dict[str, Any] = {"at": 0.0, "status": None}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    feed.start()
    yield
    story.stop()
    traffic.stop()
    feed.stop()


app = FastAPI(title="Payments fraud platform: demo console", lifespan=lifespan)


@app.get("/")
def page() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/state")
def state() -> dict[str, Any]:
    if (
        time.time() - _cache["at"] > 2 or _cache["status"] is None
    ):  # Prometheus scrapes every 15 s anyway
        _cache.update(at=time.time(), status=status.snapshot())
    return {
        **_cache["status"],
        "feed": feed.view(),
        "traffic": traffic.view(),
        "chaos": chaos.view(),
        "story": story.view(),
        "attacks": ATTACKS,
        "failures": Chaos.KINDS,
        "links": LINKS,
    }


@app.post("/api/traffic/{action}")
def traffic_action(action: str) -> dict[str, bool]:
    if action == "start":
        traffic.start()
    elif action == "stop":
        traffic.stop()
    else:
        raise HTTPException(404, f"unknown action {action}")
    return {"on": traffic.on}


@app.post("/api/attack/{pattern}")
def attack(pattern: str) -> dict[str, str]:
    if pattern not in ATTACKS:
        raise HTTPException(404, f"unknown attack {pattern}")
    traffic.attack(pattern)
    return {"queued": pattern}


@app.post("/api/chaos/{kind}")
def failure(kind: str) -> dict[str, str]:
    if kind not in Chaos.KINDS:
        raise HTTPException(404, f"unknown failure {kind}")
    if not chaos.run(kind):
        raise HTTPException(409, "a failure is already in progress")
    return {"started": kind}


@app.post("/api/story/{action}")
def story_action(action: str) -> dict[str, bool]:
    if action == "start":
        story.start()
    elif action == "stop":
        story.stop()
    else:
        raise HTTPException(404, f"unknown action {action}")
    return {"running": bool(story.state.get("running"))}
