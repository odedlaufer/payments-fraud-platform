"""Hash value encoding for the online store (no Redis needed)."""

from decimal import Decimal

import pytest

from decision_service.online_store import decode, encode


@pytest.mark.parametrize(
    ("value", "dtype"),
    [
        (7, "bigint"),
        (Decimal("315.48"), "decimal(18,2)"),
        (0.2864245040680582, "double"),
        (1 / 3, "double"),
        (True, "boolean"),
        (False, "boolean"),
        (None, "double"),
        (None, "bigint"),
    ],
)
def test_round_trip_is_exact(value: object, dtype: str) -> None:
    assert decode(encode(value), dtype) == value


def test_null_is_empty_string() -> None:
    assert encode(None) == ""


def test_unknown_dtype_is_rejected() -> None:
    with pytest.raises(ValueError):
        decode("1", "float")
