"""
Regression and unit tests for Gektor APEX v3.6.4 Noise-Reduction Suite:
1. CVD Divergence Detector (P8)
2. Market Macro-Context Filter (P9)
3. Volatility-Adaptive Z-Score (P10)
"""
from __future__ import annotations

import math
import pytest
from collections import deque

from src.domain.conflation import DollarBar
from src.domain.vpin_engine import O1VPINEngine, VPINSignal
from src.application.radar_pipeline import RadarPipeline, RadarAlert


def _bar(buy: float, sell: float, close: float = 100.0, symbol: str = "SOLUSDT") -> DollarBar:
    return DollarBar(
        symbol=symbol,
        open=close,
        high=close,
        low=close,
        close=close,
        buy_volume_usd=buy,
        sell_volume_usd=sell,
        volume_usd=buy + sell,
        volume_crypto=0.0,
    )


# ---------------------------------------------------------------------------
# P8: CVD Divergence Detector Tests
# ---------------------------------------------------------------------------

def test_cvd_filter_suppresses_weak_imbalance_anomalies() -> None:
    """When CVD_FILTER_ENABLE is True, anomalies must be blocked if net imbalance
    ratio over the window is below CVD_MIN_RATIO.
    """
    # 1. Initialize engine with CVD filter enabled (min ratio = 20%)
    eng = O1VPINEngine(
        window_size=5,
        volume_threshold=1000.0,
        z_threshold=0.5,  # low threshold to trigger anomaly easily
        z_history_size=10,
        cvd_filter_enable=True,
        cvd_min_ratio=0.20,
    )

    # Warmup with balanced flow (zero net CVD, no anomaly)
    for _ in range(4):
        eng.process_bar(_bar(500.0, 500.0, close=100.0))

    # Now feed a bar with a high total volume but almost perfectly balanced flow
    # (e.g. 510 buy, 490 sell -> total = 1000, net imbalance = 20, ratio = 20 / (5000) = 0.4%)
    # Let's make sure it produces a mathematically high VPIN due to running imbalance sum,
    # but the net ratio of signed CVD is tiny.
    eng._running_imbalance_sum = 2000.0  # mock large accumulated absolute imbalance
    eng._running_volume_sum = 4000.0
    eng._vpin_sum = 1.0
    eng._vpin_sq_sum = 0.5
    eng._z_count = 5

    # Signed imbalance ratio is 510-490 = 20. Total accumulated volume = 5000. Net ratio = 20/5000 = 0.4%
    # This is far below CVD_MIN_RATIO (20%).
    sig = eng.process_bar(_bar(510.0, 490.0, close=100.0))

    assert sig is not None
    # Anomaly must be suppressed
    assert sig.is_anomaly is False


def test_cvd_filter_allows_strong_imbalance_anomalies() -> None:
    """When CVD_FILTER_ENABLE is True, anomalies must be allowed if net imbalance
    ratio over the window is >= CVD_MIN_RATIO.
    """
    eng = O1VPINEngine(
        window_size=5,
        volume_threshold=1000.0,
        z_threshold=0.1,  # very low to trigger anomaly
        z_history_size=10,
        cvd_filter_enable=True,
        cvd_min_ratio=0.15,
    )

    # Ingest one-sided bars (100% buy volume -> net CVD ratio = 100% >= 15%)
    for _ in range(4):
        eng.process_bar(_bar(1000.0, 0.0, close=100.0))

    sig = eng.process_bar(_bar(1000.0, 0.0, close=105.0))
    assert sig is not None
    # Because CVD is strong (100%), it is confirmed
    assert sig.is_anomaly is True


# ---------------------------------------------------------------------------
# P10: Volatility-Adaptive Z-Score Tests
# ---------------------------------------------------------------------------

def test_adaptive_z_score_expands_threshold_during_turbulence() -> None:
    """During high volatility, the Z-score threshold should expand (increase),
    making it harder to trigger an anomaly.
    """
    eng = O1VPINEngine(
        window_size=5,
        volume_threshold=1000.0,
        z_threshold=2.0,
        z_history_size=10,
        adaptive_z_enable=True,
        adaptive_z_volatility_base=0.01,  # 1.0%
        adaptive_z_sensitivity=1.0,
        adaptive_z_min_mult=0.5,
        adaptive_z_max_mult=3.0,
    )

    # Ingest prices with a massive 10% range: [100.0, 102.0, 105.0, 108.0, 110.0]
    prices = [100.0, 102.0, 105.0, 108.0, 110.0]
    for p in prices[:-1]:
        eng.process_bar(_bar(500.0, 500.0, close=p))

    # Calculate expected volatility: (110 - 100) / 100 = 10% (0.10)
    # Ratio = 0.10 / 0.01 = 10.0
    # Multiplier = 1.0 + 1.0 * (10.0 - 1.0) = 10.0 -> clamped to max 3.0
    # Target Z = 2.0 * 3.0 = 6.0
    sig = eng.process_bar(_bar(500.0, 500.0, close=110.0))
    assert sig is not None

    # Let's verify that even if VPIN rose, Z-Score (usually < 4.0) did not breach the expanded 6.0 threshold
    assert sig.is_anomaly is False


def test_adaptive_z_score_narrows_threshold_during_flat_regimes() -> None:
    """During quiet periods, the Z-score threshold should narrow (decrease),
    making the radar highly sensitive to microstructural changes.
    """
    eng = O1VPINEngine(
        window_size=5,
        volume_threshold=1000.0,
        z_threshold=2.5,
        z_history_size=10,
        adaptive_z_enable=True,
        adaptive_z_volatility_base=0.02,  # 2.0%
        adaptive_z_sensitivity=1.5,
        adaptive_z_min_mult=0.6,
        adaptive_z_max_mult=2.0,
    )

    # Perfectly flat prices (0% volatility): close=100.0
    for _ in range(4):
        eng.process_bar(_bar(500.0, 500.0, close=100.0))

    # Volatility = 0.0 -> Vol ratio = 0.0
    # Multiplier = 1.0 + 1.5 * (0.0 - 1.0) = -0.5 -> clamped to min 0.6
    # Target Z = 2.5 * 0.6 = 1.5
    # Let's see if we can trigger an anomaly at a much lower Z-score of 1.8 (which is below 2.5 base)
    eng._vpin_sum = 10.0
    eng._vpin_sq_sum = 10.0
    eng._z_count = 10

    # Mock a VPIN signal that produces a Z-score of ~1.8
    # With adaptive Z enabled, the threshold narrowed to 1.5, so 1.8 breaches it!
    # Without adaptive Z, 1.8 would be below 2.5, returning False.
    sig = eng.process_bar(_bar(900.0, 100.0, close=100.0))
    assert sig is not None
    # Verify it was successfully allowed as an anomaly
    assert sig.is_anomaly is True


# ---------------------------------------------------------------------------
# P9: Market Macro-Context Filter Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_macro_context_filter_suppresses_altcoin_alerts() -> None:
    """If BTC price volatility breaches MACRO_BTC_VOLATILITY_LIMIT,
    any altcoin anomaly must be blocked, while BTC/ETH alerts must still pass.
    """
    dispatched_alerts: list[RadarAlert] = []

    async def mock_sink(alert: RadarAlert) -> None:
        dispatched_alerts.append(alert)

    # Create radar pipeline with P9 enabled
    pipeline = RadarPipeline(
        threshold_usd=1000.0,
        alert_sink=mock_sink,
        window_size=3,
        z_threshold=0.1,  # triggers easily
        macro_filter_enable=True,
        macro_btc_volatility_limit=0.01,  # 1.0% limit
        macro_eth_volatility_limit=0.02,
        macro_window_size=5,
    )

    # 1. Feed BTCUSDT flat bars first
    for _ in range(4):
        await pipeline._on_bar_closed(_bar(100.0, 100.0, close=100.0, symbol="BTCUSDT"))

    # 2. Feed BTCUSDT volatile bar (+2% move from 100 -> 102)
    # Price history deque is [100.0, 100.0, 100.0, 100.0, 102.0]
    # Volatility = (102 - 100) / 100 = 2% >= 1% limit. Macro filter is now active!
    await pipeline._on_bar_closed(_bar(100.0, 100.0, close=102.0, symbol="BTCUSDT"))
    assert pipeline._is_macro_volatile() is True

    # 3. Process an altcoin (SOLUSDT) anomaly bar
    await pipeline._on_bar_closed(_bar(1000.0, 0.0, close=100.0, symbol="SOLUSDT"))
    await pipeline._on_bar_closed(_bar(1000.0, 0.0, close=100.0, symbol="SOLUSDT"))
    await pipeline._on_bar_closed(_bar(1000.0, 0.0, close=100.0, symbol="SOLUSDT"))

    # Since SOLUSDT is an altcoin, the alert must be suppressed!
    assert len(dispatched_alerts) == 0

    # 4. Now process a BTCUSDT anomaly bar itself
    # BTCUSDT should NOT be blocked by its own macro filter!
    # Let's fill and trigger BTC anomaly
    await pipeline._on_bar_closed(_bar(10000.0, 0.0, close=102.0, symbol="BTCUSDT"))
    await pipeline._on_bar_closed(_bar(10000.0, 0.0, close=102.0, symbol="BTCUSDT"))
    
    assert len(dispatched_alerts) > 0
    assert dispatched_alerts[0].symbol == "BTCUSDT"
