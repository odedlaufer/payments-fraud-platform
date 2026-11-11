"""`simulator` command line (FR-SIM6)."""

import argparse
import sys
from datetime import UTC, datetime, timedelta

import psycopg

from simulator.clock import SimClock, speed_for
from simulator.db import count_customers, dsn_from_env, insert_population
from simulator.engine import Engine
from simulator.entities import generate_population
from simulator.rates import Rates
from simulator.runner import StopAt, run
from simulator.store import PostgresStore

DEFAULT_SIM_START = datetime(2026, 1, 1, tzinfo=UTC)


def _parse_start(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def cmd_seed(args: argparse.Namespace) -> int:
    population = generate_population(args.seed, args.customers, args.merchants, args.sim_start)
    with psycopg.connect(dsn_from_env()) as conn:
        existing = count_customers(conn)
        if existing and not args.append:
            print(
                f"refusing to seed: {existing} customers already exist "
                "(use --append to add more, or `make nuke` for a clean slate)",
                file=sys.stderr,
            )
            return 1
        counts = insert_population(conn, population)
    print(" ".join(f"{k}={v}" for k, v in counts.items()), f"seed={args.seed}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    speed = speed_for(args.minutes_per_day)
    with psycopg.connect(dsn_from_env()) as conn:
        store = PostgresStore(conn)
        world = store.load_world()
        if not world.customers or not world.merchants:
            print("nothing to simulate: run `make seed` first", file=sys.stderr)
            return 1
        # Resume after the latest simulated event so the timeline only moves forward.
        start = (world.last_event_ts or args.sim_start) + timedelta(seconds=1)
        engine = Engine(
            world,
            store,
            seed=args.seed,
            tx_per_sim_second=args.tps / speed,
            rates=Rates().scaled(args.change_rate),
            start=start,
        )
        stop = StopAt(
            max_transactions=args.max_transactions,
            real_seconds=args.duration,
            sim_until=start + timedelta(days=args.sim_days) if args.sim_days else None,
        )
        clock = None if args.fast else SimClock(start, speed)
        mode = (
            "fast"
            if args.fast
            else f"real-time {args.tps} tx/s, 1 sim day = {args.minutes_per_day} min"
        )
        print(f"simulating from {start.isoformat()} ({mode}); Ctrl+C to stop", file=sys.stderr)
        run(engine, store, conn.commit, stop, clock=clock)
        summary = " ".join(f"{t}.{op}={n}" for (t, op), n in sorted(store.ops.items()))
        print(f"sim_end={engine.now.isoformat()} {summary}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="simulator", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed", help="insert reference data: customers, cards, merchants")
    seed.add_argument("--customers", type=int, default=2000)
    seed.add_argument("--merchants", type=int, default=300)
    seed.add_argument("--seed", type=int, default=42)
    seed.add_argument("--sim-start", type=_parse_start, default=DEFAULT_SIM_START)
    seed.add_argument("--append", action="store_true", help="allow seeding a non-empty database")
    seed.set_defaults(func=cmd_seed)

    runp = sub.add_parser("run", help="simulate payments traffic against the seeded database")
    runp.add_argument("--tps", type=float, default=20.0, help="transactions per real second")
    runp.add_argument(
        "--minutes-per-day", type=float, default=10.0, help="real minutes per simulated day"
    )
    runp.add_argument("--seed", type=int, default=42)
    runp.add_argument(
        "--change-rate", type=float, default=1.0, help="multiplier for FR-SIM5 changes"
    )
    runp.add_argument("--fast", action="store_true", help="no pacing; deterministic for a seed")
    runp.add_argument("--max-transactions", type=int, default=None)
    runp.add_argument("--duration", type=float, default=None, help="stop after N real seconds")
    runp.add_argument("--sim-days", type=float, default=None, help="stop after N simulated days")
    runp.add_argument("--sim-start", type=_parse_start, default=DEFAULT_SIM_START)
    runp.set_defaults(func=cmd_run)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result: int = args.func(args)
    except psycopg.OperationalError as exc:
        print(f"cannot reach Postgres (is the stack up? `make up`): {exc}", file=sys.stderr)
        return 2
    return result


if __name__ == "__main__":
    raise SystemExit(main())
