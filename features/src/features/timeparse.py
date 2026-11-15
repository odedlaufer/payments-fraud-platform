"""Event time as the definitions require it: epoch milliseconds, truncated (ADR-003)."""

from datetime import UTC, datetime, timedelta

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MS = timedelta(milliseconds=1)


def iso_to_ms(text: str) -> int:
    """'2026-01-01T00:00:30.123999Z' -> 1767225630123 (exact integer arithmetic)."""
    return (datetime.fromisoformat(text) - _EPOCH) // _MS


def dt_to_ms(value: datetime) -> int:
    """An aware datetime -> epoch milliseconds, truncated (exact integer arithmetic)."""
    return (value - _EPOCH) // _MS
