"""When the feature sink resumes or rebuilds, and when it counts as caught up."""

from decision_service.feature_sink import caught_up, read_positions, start_mode


def test_resumes_when_redis_kept_the_state_it_built() -> None:
    assert start_mode("run-a", "run-a") == "resume"


def test_rebuilds_after_a_redis_restart_or_on_first_start() -> None:
    assert start_mode("run-a", "run-b") == "rebuild"  # Redis restarted: new run_id, empty
    assert start_mode(None, "run-a") == "rebuild"  # never built (or marker lost with data)


def test_caught_up_only_when_every_partition_reached_its_end() -> None:
    marks = {("features.card", 0): (0, 100), ("pg.public.cards", 0): (2836, 5856)}
    assert not caught_up({("features.card", 0): 100, ("pg.public.cards", 0): 5000}, marks)
    assert caught_up({("features.card", 0): 100, ("pg.public.cards", 0): 5856}, marks)


def test_not_caught_up_before_the_first_fetch() -> None:
    assert not caught_up({("features.card", 0): -1001}, {("features.card", 0): (0, 100)})


def test_empty_partitions_count_as_caught_up() -> None:
    assert caught_up({}, {("pg.public.chargebacks", 0): (0, 0), ("x", 0): (7, 7)})


def test_a_resume_at_the_end_is_caught_up_without_new_messages() -> None:
    """Resumed with no traffic: no position until a message arrives, so the committed offset
    (where it resumed) decides. This exact case once kept the sink from ever being ready."""
    marks = {("features.card", 0): (0, 100), ("pg.public.customers", 0): (2310, 5105)}
    positions = {("features.card", 0): -1001, ("pg.public.customers", 0): -1001}
    committed = {("features.card", 0): 100, ("pg.public.customers", 0): 5105}
    assert caught_up(read_positions("resume", positions, committed), marks)


def test_a_rebuild_ignores_old_committed_offsets() -> None:
    """During a rebuild an old committed offset at the end must not look caught up."""
    marks = {("features.card", 0): (0, 100)}
    positions = {("features.card", 0): -1001}
    assert not caught_up(read_positions("rebuild", positions, {("features.card", 0): 100}), marks)
