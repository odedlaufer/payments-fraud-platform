"""Drives the engine: paced by the accelerated clock, or stepped as fast as possible."""

import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import FrameType

from simulator.clock import SimClock
from simulator.engine import Engine
from simulator.store import Store


@dataclass(frozen=True)
class StopAt:
    max_transactions: int | None = None
    real_seconds: float | None = None
    sim_until: datetime | None = None


class _Interrupt:
    def __init__(self) -> None:
        self.requested = False

    def __call__(self, signum: int, frame: FrameType | None) -> None:
        self.requested = True


def _done(
    engine: Engine, store: Store, stop: StopAt, started: float, interrupt: _Interrupt
) -> bool:
    if interrupt.requested:
        return True
    inserted = store.ops["transactions", "insert"]
    if stop.max_transactions is not None and inserted >= stop.max_transactions:
        return True
    if stop.real_seconds is not None and time.monotonic() - started >= stop.real_seconds:
        return True
    return stop.sim_until is not None and engine.now >= stop.sim_until


def run(
    engine: Engine,
    store: Store,
    commit: Callable[[], None],
    stop: StopAt,
    *,
    clock: SimClock | None,
    tick_real_s: float = 0.05,
    fast_step: timedelta = timedelta(seconds=1),
    drain: timedelta = timedelta(seconds=15),
) -> None:
    """clock=None runs in fast mode: deterministic fixed steps, no sleeping.

    After the stop condition, simulated time advances by `drain` without new
    transactions, so every pending transaction gets its approve/decline update.
    """
    interrupt = _Interrupt()
    previous = signal.signal(signal.SIGINT, interrupt)
    started = time.monotonic()
    try:
        while not _done(engine, store, stop, started, interrupt):
            if clock is None:
                engine.advance(engine.now + fast_step)
            else:
                time.sleep(tick_real_s)
                engine.advance(clock.now())
            commit()
        rate = engine.tx_per_sim_second
        engine.tx_per_sim_second = 0.0
        engine.advance(engine.now + drain)
        engine.tx_per_sim_second = rate
        commit()
    finally:
        signal.signal(signal.SIGINT, previous)
