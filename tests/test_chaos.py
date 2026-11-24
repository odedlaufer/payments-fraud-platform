"""Chaos suite (PRD 8.5): the six scenarios, their report, and how results accumulate."""

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("chaos_run", ROOT / "chaos/run.py")
assert spec and spec.loader
chaos = importlib.util.module_from_spec(spec)
spec.loader.exec_module(chaos)


def test_the_six_prd_scenarios_with_their_expected_outcomes() -> None:
    assert list(chaos.SCENARIOS) == ["CH-1", "CH-2", "CH-3", "CH-4", "CH-5", "CH-6"]
    assert set(chaos.PRD) == set(chaos.SCENARIOS)
    assert "decisions resume < 60 s" in chaos.PRD["CH-1"][1]
    assert "zero missing rows" in chaos.PRD["CH-2"][1]
    assert "no silent approvals" in chaos.PRD["CH-3"][1]
    assert "DLQ" in chaos.PRD["CH-4"][1]
    assert "schema registry" in chaos.PRD["CH-5"][1].lower()
    assert "dlq.late_events" in chaos.PRD["CH-6"][1]


def result(cid: str, passed: bool) -> dict[str, Any]:
    scenario, expected = chaos.PRD[cid]
    return {
        "id": cid,
        "scenario": scenario,
        "expected": expected,
        "passed": passed,
        "outcome": "measured outcome",
        "measured": {"recovery_s": 12.5},
        "ran_at": "2026-11-23 12:00 UTC",
    }


def test_the_report_shows_pass_and_fail_with_measurements() -> None:
    text = chaos.render({"CH-1": result("CH-1", True), "CH-3": result("CH-3", False)})
    assert "| CH-1 | Kill the Flink TaskManager mid-run |" in text and "| PASS |" in text
    assert "| FAIL |" in text
    assert "recovery_s = 12.5" in text
    assert "## Notes" in text and "CH-4 scope" in text


def test_a_run_replaces_only_the_scenarios_it_ran(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, doc = tmp_path / "results.json", tmp_path / "chaos.md"
    results.write_text(json.dumps({"CH-2": result("CH-2", True)}))
    monkeypatch.setattr(chaos, "RESULTS", results)
    monkeypatch.setattr(chaos, "DOC", doc)
    monkeypatch.setattr(
        chaos,
        "SCENARIOS",
        {"CH-5": lambda: chaos.Result("CH-5", *chaos.PRD["CH-5"], passed=False, outcome="x")},
    )
    monkeypatch.setattr("sys.argv", ["run.py", "CH-5"])
    assert chaos.main() == 1  # a failing scenario fails the run
    kept = json.loads(results.read_text())
    assert set(kept) == {"CH-2", "CH-5"} and kept["CH-2"]["passed"] and not kept["CH-5"]["passed"]
    assert "| CH-5 |" in doc.read_text()


def test_the_row_record_is_found_where_debezium_defines_it() -> None:
    """Debezium defines the row record once, in before; after only names it (CH-5 broke on it)."""
    row = {"type": "record", "name": "Value", "fields": [{"name": "amount", "type": "string"}]}
    envelope = {
        "type": "record",
        "name": "Envelope",
        "fields": [
            {"name": "before", "type": ["null", row]},
            {"name": "after", "type": ["null", "Value"]},
            {"name": "op", "type": "string"},
        ],
    }
    assert chaos.value_record(envelope) is row
    with pytest.raises(ValueError):
        chaos.value_record({"fields": [{"name": "op", "type": "string"}]})


def test_a_topic_not_created_yet_has_zero_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    """dlq.late_events exists only after the first late event (CH-6 stopped on it once)."""
    import confluent_kafka
    from confluent_kafka import KafkaError, KafkaException

    class NoTopic:
        def __init__(self, conf: Any) -> None:
            pass

        def get_watermark_offsets(self, tp: Any, timeout: float) -> Any:
            raise KafkaException(KafkaError(KafkaError._UNKNOWN_PARTITION))

        def close(self) -> None:
            pass

    monkeypatch.setattr(confluent_kafka, "Consumer", NoTopic)
    assert chaos.end_offset("dlq.late_events") == 0
