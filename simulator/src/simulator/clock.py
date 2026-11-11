"""Accelerated simulated clock (FR-SIM6).

Simulated time drives every event_ts; real time drives throughput and latency SLOs.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

SECONDS_PER_DAY = 86_400


def speed_for(real_minutes_per_sim_day: float) -> float:
    """Simulated seconds per real second, e.g. 10 real minutes per day -> 144x."""
    if real_minutes_per_sim_day <= 0:
        raise ValueError("real_minutes_per_sim_day must be positive")
    return SECONDS_PER_DAY / (real_minutes_per_sim_day * 60)


@dataclass
class SimClock:
    """Maps elapsed real time to simulated time: sim = start + elapsed_real * speed."""

    start: datetime
    speed: float
    real_time: Callable[[], float] = time.monotonic
    _real_start: float = field(init=False)

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        if self.speed <= 0:
            raise ValueError("speed must be positive")
        self._real_start = self.real_time()

    def now(self) -> datetime:
        elapsed = self.real_time() - self._real_start
        return self.start + timedelta(seconds=elapsed * self.speed)

    def real_seconds_until(self, sim_time: datetime) -> float:
        """How long to sleep in real time until sim_time is reached (0 if already past)."""
        return max(0.0, (sim_time - self.now()).total_seconds() / self.speed)
