"""A new simulator run puts customers where they really are (no restart teleport).

Trips live in memory only. Measured live: 16 legitimate impossible_travel declines followed a
purchase made in an earlier run, by a customer the new run had put back home. Here: two runs
in a row with the trip state cleared in between, as a new process starts.
"""

from datetime import timedelta

from simulator.engine import Engine
from simulator.geo import haversine_km
from simulator.rates import Rates
from simulator.store import MemoryStore
from tests.simulator.test_engine import TX_PER_SIM_SECOND, make_world, simulate
from tests.simulator.test_fraud import card_present_legs, tx

QUIET = Rates().with_fraud(0)  # legitimate traffic only


def last_seen(store: MemoryStore, engine: Engine) -> dict[int, tuple[float, float, object]]:
    """What PostgresStore.load_world reads: each customer's last legitimate in-person purchase."""
    seen: dict[int, tuple[float, float, object]] = {}
    for _, r in sorted(tx(store).items(), key=lambda kv: (kv[1]["event_ts"], kv[0])):
        if r["channel"] == "card_present":
            customer = engine.world.cards[r["card_id"]].customer_id
            seen[customer] = (r["lat"], r["lon"], r["event_ts"])
    return seen


def two_runs(restore: bool) -> list[float]:
    """Speeds of legitimate legs over 500 km, across a restart after two days."""
    first, store = simulate(48, rates=QUIET, world=make_world(customers=600))
    world = first.world
    abroad_at_restart = sum(1 for c in world.customers.values() if c.trip_city is not None)
    assert abroad_at_restart > 0, "the scenario needs customers abroad when the run ends"
    for c in world.customers.values():  # a new process: no trips in memory
        c.trip_city = c.trip_arrive = c.trip_leave = c.trip_home = None
    world.last_seen = last_seen(store, first) if restore else {}  # type: ignore[assignment]
    second = Engine(
        world, store, seed=5, tx_per_sim_second=TX_PER_SIM_SECOND, rates=QUIET, start=first.now
    )
    second.advance(first.now + timedelta(days=3))
    rows = tx(store)
    return [
        speed
        for a, b, speed in card_present_legs(rows)
        if haversine_km(rows[a]["lat"], rows[a]["lon"], rows[b]["lat"], rows[b]["lon"]) > 500
    ]


def test_customers_resume_where_they_were() -> None:
    speeds = two_runs(restore=True)
    assert speeds, "long legitimate legs exist"
    assert max(speeds) < 900, f"legitimate leg at {max(speeds):.0f} km/h across the restart"


def test_without_it_a_restart_teleports_customers() -> None:
    """The control: the test sees the bug it guards against."""
    assert max(two_runs(restore=False)) > 900
