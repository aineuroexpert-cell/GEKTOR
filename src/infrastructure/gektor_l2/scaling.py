"""Strict decimal / scaled-integer parsing (no float on monetary paths)."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from src.infrastructure.gektor_l2.constants import SCALE


def decimal_from_str(value: Any) -> Decimal:
    """Parse exchange string/number into Decimal; rejects silent garbage."""
    if value is None:
        raise ValueError("null monetary value")
    s = str(value).strip()
    if not s:
        raise ValueError("empty monetary value")
    try:
        return Decimal(s)
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal: {value!r}") from exc


def to_scaled_int(d: Decimal) -> int:
    """Quantize to SCALE fixed-point int (1e8)."""
    q = int((d * SCALE).to_integral_value(rounding="ROUND_DOWN"))
    return q


def parse_scaled_int(value: Any) -> int:
    """Parse API string/int/float-as-string into scaled int without float."""
    return to_scaled_int(decimal_from_str(value))
