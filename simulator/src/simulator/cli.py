"""`simulator` command line (FR-SIM6)."""

import argparse
import sys
from datetime import UTC, datetime

import psycopg

from simulator.db import count_customers, dsn_from_env, insert_population
from simulator.entities import generate_population

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
