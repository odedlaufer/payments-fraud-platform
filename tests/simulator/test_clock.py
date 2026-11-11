from datetime import UTC, datetime, timedelta

import pytest

from simulator.clock import SimClock, speed_for

START = datetime(2026, 1, 1, tzinfo=UTC)


class FakeTime:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def test_default_speed_is_144x() -> None:
    """PRD: 1 simulated day = 10 real minutes."""
    assert speed_for(10) == 144


def test_now_advances_by_speed() -> None:
    fake = FakeTime()
    clock = SimClock(START, speed=144, real_time=fake)
    assert clock.now() == START
    fake.t += 600  # 10 real minutes
    assert clock.now() == START + timedelta(days=1)


def test_real_seconds_until() -> None:
    fake = FakeTime()
    clock = SimClock(START, speed=144, real_time=fake)
    assert clock.real_seconds_until(START + timedelta(seconds=144)) == pytest.approx(1.0)
    assert clock.real_seconds_until(START - timedelta(seconds=1)) == 0.0


@pytest.mark.parametrize("bad", [0, -1])
def test_rejects_non_positive_speed(bad: float) -> None:
    with pytest.raises(ValueError):
        SimClock(START, speed=bad)


def test_rejects_naive_start() -> None:
    with pytest.raises(ValueError):
        SimClock(datetime(2026, 1, 1), speed=1)
