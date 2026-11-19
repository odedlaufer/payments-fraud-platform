"""Rule promotion gate (PRD 6.3 gate 3): each condition refuses on its own."""

import importlib.util
import sys
from dataclasses import replace
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("promote_rules", ROOT / "scripts/promote_rules.py")
assert spec and spec.loader
pr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pr)

GOOD = pr.Evidence(
    candidate_version=2,
    live_version=1,
    snapshot_id=42,
    candidate=pr.Backtest(precision=0.44, recall=0.58, alerts=1000),
    live=pr.Backtest(precision=0.38, recall=0.43, alerts=900),
    parity=1.0,
    deployable=True,
)


def test_promotes_on_good_evidence() -> None:
    assert pr.verdict(GOOD, max_drop=0.02) == []


def test_needs_both_backtests_on_the_training_set() -> None:
    assert any(
        "no backtest of the candidate" in r for r in pr.verdict(replace(GOOD, candidate=None), 0.02)
    )
    assert any("no backtest of live v1" in r for r in pr.verdict(replace(GOOD, live=None), 0.02))


def test_needs_parity_within_the_slo() -> None:
    assert any("below the 99.9% SLO" in r for r in pr.verdict(replace(GOOD, parity=0.998), 0.02))
    assert any("no parity data" in r for r in pr.verdict(replace(GOOD, parity=None), 0.02))


def test_refuses_a_precision_drop_beyond_the_threshold() -> None:
    worse = replace(GOOD, candidate=pr.Backtest(precision=0.33, recall=0.9, alerts=5000))
    (reason,) = pr.verdict(worse, max_drop=0.10)
    assert "keeps 87% of live 38.00% (must keep at least 90%)" in reason
    assert pr.verdict(worse, max_drop=0.15) == []  # within a looser threshold


def test_low_precision_regimes_still_refuse_a_rule_that_declines_everyone() -> None:
    """Measured: live v1 1.76% precision, a rule declining 99.97% of transactions 0.38%. An
    absolute 2 pp threshold let it through; the relative one does not."""
    live = replace(GOOD, live=pr.Backtest(precision=0.0176, recall=0.45, alerts=7947))
    everyone = replace(live, candidate=pr.Backtest(precision=0.0038, recall=0.99, alerts=81443))
    (reason,) = pr.verdict(everyone, max_drop=0.10)
    assert "keeps 22% of live 1.76%" in reason


def test_refuses_what_is_already_live_or_cannot_deploy() -> None:
    assert any("already live" in r for r in pr.verdict(replace(GOOD, candidate_version=1), 0.02))
    assert any("not in rules/" in r for r in pr.verdict(replace(GOOD, deployable=False), 0.02))


def test_refuses_a_candidate_that_never_alerts() -> None:
    silent = replace(GOOD, candidate=pr.Backtest(precision=None, recall=0.0, alerts=0))
    assert any("raises no alerts" in r for r in pr.verdict(silent, 0.02))


def test_the_live_version_comes_from_rules_live_env() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"][
        "decision-service"
    ]
    assert compose["env_file"] == ["rules/live.env"]
    assert "RULES_FILE" not in compose.get("environment", {})  # one source of truth
    live = pr.live_rules()
    assert live.exists() and pr.version_of(live) >= 1
