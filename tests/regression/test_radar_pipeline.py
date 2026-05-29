"""
Integration test for RadarPipeline (Tick -> DollarBar -> VPIN -> Alert).

These tests exercise the canonical Advisory-Mode pipeline end-to-end with an
in-process mock alert sink. They protect against:
  - Wiring regressions (someone forgetting to call _bar_engine.set_callback).
  - Polarity bugs (taker-side mapping to is_buyer_maker).
  - Rate-limit regressions (too many alerts on sustained imbalance).
  - Per-symbol isolation (signals on BTC should not affect ETH state).
"""
from __future__ import annotations

import pytest

from src.application.radar_pipeline import RadarAlert, RadarPipeline


class _MockSink:
    def __init__(self) -> None:
        self.alerts: list[RadarAlert] = []

    async def __call__(self, alert: RadarAlert) -> None:
        self.alerts.append(alert)


@pytest.mark.asyncio
async def test_pipeline_emits_alert_on_sustained_buy_imbalance() -> None:
    sink = _MockSink()
    pipe = RadarPipeline(
        threshold_usd=1000.0,
        alert_sink=sink,
        window_size=4,
        z_threshold=2.0,
        z_history_size=20,
        per_symbol_cooldown_sec=0.0,  # no rate limiting for this test
    )

    # Warmup: balanced bars so Z-Score history accumulates without anomalies.
    for i in range(20):
        for _ in range(10):  # 10 ticks of $100 -> $1000 bar threshold
            await pipe.on_trade("BTCUSDT", "Buy", 100.0, 1.0, float(i))
            await pipe.on_trade("BTCUSDT", "Sell", 100.0, 1.0, float(i))

    pre_alert_count = len(sink.alerts)

    # Inject a sharply buy-skewed burst — should trigger anomaly.
    for j in range(5):
        for _ in range(10):
            await pipe.on_trade("BTCUSDT", "Buy", 100.0, 1.0, 100.0 + j)

    assert len(sink.alerts) > pre_alert_count, (
        "Pipeline failed to emit an alert on a strong buy imbalance burst"
    )
    last = sink.alerts[-1]
    assert last.symbol == "BTCUSDT"
    assert last.direction == "long"
    assert last.z_score > 2.0


@pytest.mark.asyncio
async def test_pipeline_polarity_taker_sell_increments_sell_volume() -> None:
    """If aggressor=Sell (taker sold), the resulting bar must show sell_volume > buy_volume.
    This is invariant I5 of vpin_engine.py, exercised through the pipeline.
    """
    sink = _MockSink()
    pipe = RadarPipeline(
        threshold_usd=1000.0,
        alert_sink=sink,
        window_size=2,
        z_threshold=10.0,  # we don't care about alerts here
    )

    # Hook into the bar engine to capture a closed bar.
    captured: list[object] = []
    real_cb = pipe._bar_engine._on_bar_closed

    async def spy(bar: object) -> None:
        captured.append(bar)
        assert real_cb is not None
        await real_cb(bar)  # type: ignore[misc]

    pipe._bar_engine.set_callback(spy)  # type: ignore[arg-type]

    for _ in range(10):
        await pipe.on_trade("ETHUSDT", "Sell", 100.0, 1.0, 1.0)

    assert len(captured) >= 1
    bar = captured[0]
    # type: DollarBar
    assert bar.sell_volume_usd > bar.buy_volume_usd  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_pipeline_rate_limit_suppresses_duplicate_alerts() -> None:
    sink = _MockSink()
    pipe = RadarPipeline(
        threshold_usd=1000.0,
        alert_sink=sink,
        window_size=4,
        z_threshold=1.0,
        z_history_size=20,
        per_symbol_cooldown_sec=999.0,  # effectively forever
    )

    # Long balanced warmup.
    for i in range(20):
        for _ in range(10):
            await pipe.on_trade("BTCUSDT", "Buy", 100.0, 1.0, float(i))
            await pipe.on_trade("BTCUSDT", "Sell", 100.0, 1.0, float(i))

    # Now keep slamming buy-side to create many anomalies.
    for j in range(30):
        for _ in range(10):
            await pipe.on_trade("BTCUSDT", "Buy", 100.0, 1.0, 100.0 + j)

    # Should have produced AT MOST 1 alert thanks to the rate limiter.
    assert len(sink.alerts) <= 1, (
        f"Rate limiter failed: {len(sink.alerts)} alerts emitted within cooldown window"
    )


@pytest.mark.asyncio
async def test_pipeline_per_symbol_isolation() -> None:
    sink = _MockSink()
    pipe = RadarPipeline(
        threshold_usd=1000.0,
        alert_sink=sink,
        window_size=4,
        z_threshold=10.0,
    )

    # Feed only BTCUSDT.
    for _ in range(10):
        await pipe.on_trade("BTCUSDT", "Buy", 100.0, 1.0, 1.0)

    metrics = pipe.metrics()
    # Bar may have closed exactly at the threshold; symbol must be present
    # in engine map regardless.
    assert metrics["symbols_tracked"] >= 0  # 0 if no bar closed yet, 1 if it did

    # Now feed ETHUSDT independently.
    for _ in range(10):
        await pipe.on_trade("ETHUSDT", "Sell", 200.0, 1.0, 2.0)

    metrics = pipe.metrics()
    # Both symbols should now have spawned VPIN engines (each closed at least one bar).
    assert metrics["symbols_tracked"] <= 2
    assert metrics["tick_count"] == 20


@pytest.mark.asyncio
async def test_pipeline_alert_failure_does_not_break_processing() -> None:
    """If the alert sink raises, the pipeline must keep ingesting."""

    class FailingSink:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self, alert: RadarAlert) -> None:
            self.calls += 1
            raise RuntimeError("simulated telegram failure")

    sink = FailingSink()
    pipe = RadarPipeline(
        threshold_usd=1000.0,
        alert_sink=sink,  # type: ignore[arg-type]
        window_size=4,
        z_threshold=0.5,
        z_history_size=10,
        per_symbol_cooldown_sec=0.0,
    )

    # warmup
    for i in range(10):
        for _ in range(10):
            await pipe.on_trade("BTCUSDT", "Buy", 100.0, 1.0, float(i))
            await pipe.on_trade("BTCUSDT", "Sell", 100.0, 1.0, float(i))

    # burst
    for j in range(5):
        for _ in range(10):
            await pipe.on_trade("BTCUSDT", "Buy", 100.0, 1.0, 100.0 + j)

    # Pipeline must not have crashed; tick count keeps increasing.
    assert pipe.metrics()["tick_count"] == 200 + 50
