from datetime import UTC, datetime

import pytest

from simulator.cli import build_parser


def test_seed_defaults() -> None:
    args = build_parser().parse_args(["seed"])
    assert (args.customers, args.merchants, args.seed) == (2000, 300, 42)
    assert args.sim_start == datetime(2026, 1, 1, tzinfo=UTC)
    assert args.append is False


def test_naive_sim_start_is_utc() -> None:
    args = build_parser().parse_args(["seed", "--sim-start", "2026-03-01T00:00:00"])
    assert args.sim_start == datetime(2026, 3, 1, tzinfo=UTC)


def test_command_required() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([])
