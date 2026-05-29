"""[GEKTOR v3.6.2] Regression for AdaptiveDollarThresholdProvider.

Pins down:
  * threshold clamping (min/max bounds applied correctly)
  * fallback to default when turnover is unknown (cache miss)
  * Bybit tickers contract parsing (string→float, USDT filter)
  * refresh failures leave the prior cache intact (fault tolerance)
"""
from __future__ import annotations

import pytest

from src.infrastructure.adaptive_threshold import (
    AdaptiveDollarThresholdProvider,
)


class _FakeBybit:
    """Minimal stub matching the get_tickers protocol."""

    def __init__(self, tickers):
        self._tickers = tickers
        self.call_count = 0

    async def get_tickers(self, symbol=None):
        self.call_count += 1
        if isinstance(self._tickers, Exception):
            raise self._tickers
        return self._tickers


def test_init_rejects_bad_params() -> None:
    rest = _FakeBybit([])
    with pytest.raises(ValueError):
        AdaptiveDollarThresholdProvider(
            rest_client=rest, target_bars_per_day=0
        )
    with pytest.raises(ValueError):
        AdaptiveDollarThresholdProvider(rest_client=rest, min_usd=0)
    with pytest.raises(ValueError):
        AdaptiveDollarThresholdProvider(
            rest_client=rest, min_usd=100, max_usd=50
        )
    with pytest.raises(ValueError):
        # default outside [min, max]
        AdaptiveDollarThresholdProvider(
            rest_client=rest, min_usd=100, max_usd=1000, default_usd=10
        )


@pytest.mark.asyncio
async def test_threshold_clamping() -> None:
    rest = _FakeBybit([
        # BTC: turnover huge → would request $250M bar, must clamp to max.
        {"symbol": "BTCUSDT", "turnover24h": "50000000000"},
        # ETH: turnover medium → exact match.
        {"symbol": "ETHUSDT", "turnover24h": "20000000"},  # $20M/d / 200 = $100k
        # NIL: turnover tiny → must clamp to min.
        {"symbol": "NILUSDT", "turnover24h": "1000000"},  # $1M/d / 200 = $5k → clamp to $20k
        # Non-USDT: filtered out.
        {"symbol": "BTCUSDC", "turnover24h": "999"},
        # Empty turnover string: still added to cache, but turnover=0 ⇒
        # threshold falls back to default_usd via _compute_threshold(0).
        {"symbol": "GARBAGEUSDT", "turnover24h": ""},
    ])
    p = AdaptiveDollarThresholdProvider(
        rest_client=rest,
        target_bars_per_day=200,
        min_usd=20_000.0,
        max_usd=5_000_000.0,
        default_usd=1_000_000.0,
    )
    n = await p.refresh()
    assert n == 4  # BTC, ETH, NIL, GARBAGE — USDC excluded
    assert p.threshold_for("BTCUSDT") == 5000000.0
    assert p.threshold_for("ETHUSDT") == 100000.0
    assert p.threshold_for("NILUSDT") == 20000.0
    # GARBAGEUSDT: turnover=0 in cache → _compute_threshold(0) returns default.
    assert p.threshold_for("GARBAGEUSDT") == 1000000.0
    # Unknown symbol: falls back to default.
    assert p.threshold_for("UNKNOWN") == 1000000.0


@pytest.mark.asyncio
async def test_refresh_failure_preserves_prior_cache() -> None:
    rest_ok = _FakeBybit([
        {"symbol": "BTCUSDT", "turnover24h": "50000000000"},
    ])
    p = AdaptiveDollarThresholdProvider(
        rest_client=rest_ok,
        target_bars_per_day=200,
        min_usd=20_000.0,
        max_usd=5_000_000.0,
        default_usd=1_000_000.0,
    )
    n = await p.refresh()
    assert n == 1
    btc_before = p.threshold_for("BTCUSDT")

    # Swap in a broken client.
    rest_broken = _FakeBybit(RuntimeError("network down"))
    p._rest_client = rest_broken  # type: ignore[attr-defined]
    n2 = await p.refresh()
    assert n2 == 0
    # Cache must be intact.
    assert p.threshold_for("BTCUSDT") == btc_before


@pytest.mark.asyncio
async def test_turnover_for_returns_zero_for_unknown() -> None:
    rest = _FakeBybit([{"symbol": "BTCUSDT", "turnover24h": "1000"}])
    p = AdaptiveDollarThresholdProvider(rest_client=rest)
    await p.refresh()
    assert p.turnover_for("BTCUSDT") == 1000.0
    assert p.turnover_for("UNKNOWN") == 0.0


@pytest.mark.asyncio
async def test_refresh_handles_non_list_response() -> None:
    rest = _FakeBybit({"some": "dict"})  # bad shape
    p = AdaptiveDollarThresholdProvider(rest_client=rest)
    n = await p.refresh()
    assert n == 0
    assert p.cache_size == 0


def test_threshold_for_before_first_refresh_returns_default() -> None:
    rest = _FakeBybit([])
    p = AdaptiveDollarThresholdProvider(
        rest_client=rest,
        default_usd=500_000.0,
    )
    assert p.threshold_for("BTCUSDT") == 500000.0
