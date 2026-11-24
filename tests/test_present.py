"""make present: only this project is started; other projects are stopped only on request."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("present", ROOT / "scripts/present.py")
assert spec and spec.loader
present = importlib.util.module_from_spec(spec)
spec.loader.exec_module(present)

PS = (
    "payments-fraud-platform\tpayments-fraud-platform-redis-1\n"
    "lichess\tlichess-spark-master-1\n"
    "lichess\tlichess-weed-1\n"
    "\tbuildx_buildkit_default\n"
)


def test_this_projects_containers_are_never_others() -> None:
    found = present.others(PS)
    assert found == {
        "lichess": ["lichess-spark-master-1", "lichess-weed-1"],
        "": ["buildx_buildkit_default"],
    }


def test_other_projects_stop_and_start_by_name() -> None:
    stop, start = present.stop_commands(present.others(PS))
    assert ["docker", "compose", "-p", "lichess", "stop"] in stop
    assert ["docker", "compose", "-p", "lichess", "start"] in start
    assert ["docker", "stop", "buildx_buildkit_default"] in stop  # not from compose: by container
    assert not any("payments-fraud-platform" in " ".join(c) for c in stop)


@pytest.mark.parametrize(
    ("usage", "expected"),
    [
        ("512.5MiB / 11.7GiB", 512.5),
        ("1.5GiB / 11.7GiB", 1536.0),
        ("2048KiB / 1GiB", 2.0),
        ("0B / 0B", 0.0),
    ],
)
def test_memory_readings(usage: str, expected: float) -> None:
    assert present.mib(usage) == pytest.approx(expected)


def test_nothing_is_stopped_without_an_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert present.ask("Stop them?") is False
