"""Shared numeric constants for L2 engine (scaled fixed-point)."""

from __future__ import annotations

# Bybit-style 1e8 scaling for prices and base-asset quantities in hot paths.
SCALE: int = 100_000_000

# Taker fee 0.045% → multiply notional by this rational: fee = notional * 45 // FEE_DIVISOR
TAKER_FEE_NUMERATOR: int = 45
TAKER_FEE_DENOMINATOR: int = 100_000

# Universe filter: 24h turnover in quote currency (USDT) — not scaled.
MIN_TURNOVER_USDT: int = 50_000_000
