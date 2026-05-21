"""
Integration test: full ingestor -> radar pipeline against a synthetic
Bybit-format trade tape.

We do NOT spin up a real websocket. Instead we feed BybitWSIngestion._process_message
with the exact byte payload Bybit emits, exercising the entire orjson +
polarity-mapping + RadarPipeline path.

This catches regressions in:
  - JSON parsing (orjson)
  - is_buyer_maker mapping (trade['S'] == 'Sell')
  - Decimal conversion of price/qty
  - End-to-end alert dispatch under a realistic feed
"""
from __future__ import annotations

from decimal import Decimal

import orjson
import pytest

from src.application.radar_pipeline import RadarAlert, RadarPipeline
from src.infrastructure.bybit_ws_ingestion import BybitWSIngestion


def _bybit_msg(symbol: str, ticks: list[tuple[str, str, str]]) -> bytes:
    """Build a Bybit publicTrade WS message.

    Each tick: (side, price, qty). side is "Buy" or "Sell".
    """
    data = []
    ts = 1_700_000_000_000
    for side, price, qty in ticks:
        data.append(
            {
                "T": ts,
                "s": symbol,
                "S": side,
                "v": qty,
                "p": price,
            }
        )
        ts += 100
    return orjson.dumps({"topic": f"publicTrade.{symbol}", "data": data})


class _RecordingSink:
    def __init__(self) -> None:
        self.alerts: list[RadarAlert] = []

    async def __call__(self, alert: RadarAlert) -> None:
        self.alerts.append(alert)


@pytest.mark.asyncio
async def test_full_ingestor_to_alert_pipeline() -> None:
    """Feed a synthetic Bybit tape through the WS ingestor and assert that
    a sustained one-sided burst produces a long-direction alert.

    Each tick is qty=1 * price=100 = $100. The dollar threshold is $1000,
    so 10 ticks coalesce into a single bar. Warmup uses balanced pairs
    (Buy/Sell) per bar to fill the Z-Score history with low VPIN values,
    then a pure-buy burst flips imbalance hard.
    """
    sink = _RecordingSink()
    pipe = RadarPipeline(
        threshold_usd=Decimal("1000"),
        alert_sink=sink,
        window_size=4,
        z_threshold=2.0,
        z_history_size=20,
        per_symbol_cooldown_sec=0.0,
    )
    ingestor = BybitWSIngestion(ws_url="ws://mock", aggregator=pipe)

    # Warmup: 30 bars, each made of 5 buy + 5 sell ticks of $100 -> bar=$1000.
    for _ in range(30):
        msg = _bybit_msg(
            "BTCUSDT",
            [("Buy", "100.0", "1.0"), ("Sell", "100.0", "1.0")] * 5,
        )
        await ingestor._process_message(msg)

    pre_alert = len(sink.alerts)

    # Burst: 10 bars of pure-buy ticks ($1000 buy_volume each).
    for _ in range(10):
        msg = _bybit_msg(
            "BTCUSDT",
            [("Buy", "100.0", "1.0")] * 10,
        )
        await ingestor._process_message(msg)

    assert len(sink.alerts) > pre_alert, "End-to-end pipeline failed to emit alert"
    assert sink.alerts[-1].symbol == "BTCUSDT"
    assert sink.alerts[-1].direction == "long"


@pytest.mark.asyncio
async def test_ingestor_polarity_aggressor_buy_maps_to_buy_volume() -> None:
    """Bybit S='Buy' means TAKER bought -> is_buyer_maker=False -> buy_volume_usd.
    Verifies the wire-format polarity through the entire stack.
    """
    captured: list[object] = []

    class CaptureSink:
        async def __call__(self, alert: RadarAlert) -> None:  # pragma: no cover
            pass

    pipe = RadarPipeline(
        threshold_usd=Decimal("1000"),
        alert_sink=CaptureSink(),
        window_size=2,
        z_threshold=10.0,
    )
    real_cb = pipe._bar_engine._on_bar_closed

    async def spy(bar: object) -> None:
        captured.append(bar)
        assert real_cb is not None
        await real_cb(bar)  # type: ignore[misc]

    pipe._bar_engine.set_callback(spy)  # type: ignore[arg-type]
    ingestor = BybitWSIngestion(ws_url="ws://mock", aggregator=pipe)

    # 10 buy-aggressor ticks of $100 each fill the $1000 bar exactly.
    msg = _bybit_msg(
        "ETHUSDT",
        [("Buy", "100.0", "1.0")] * 10,
    )
    await ingestor._process_message(msg)

    assert len(captured) >= 1
    bar = captured[0]
    assert bar.buy_volume_usd > bar.sell_volume_usd  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_ingestor_ignores_non_trade_topics() -> None:
    sink = _RecordingSink()
    pipe = RadarPipeline(
        threshold_usd=Decimal("1000"),
        alert_sink=sink,
        window_size=2,
        z_threshold=10.0,
    )
    ingestor = BybitWSIngestion(ws_url="ws://mock", aggregator=pipe)

    # ticker, orderbook, etc. — must be ignored without error.
    msg = orjson.dumps({"topic": "tickers.BTCUSDT", "data": [{"foo": "bar"}]})
    await ingestor._process_message(msg)
    assert pipe.metrics()["tick_count"] == 0


@pytest.mark.asyncio
async def test_ingestor_survives_malformed_payload() -> None:
    sink = _RecordingSink()
    pipe = RadarPipeline(
        threshold_usd=Decimal("1000"),
        alert_sink=sink,
        window_size=2,
        z_threshold=10.0,
    )
    ingestor = BybitWSIngestion(ws_url="ws://mock", aggregator=pipe)

    await ingestor._process_message(b"this is not json")
    await ingestor._process_message(b"{}")
    await ingestor._process_message(b'{"topic": "publicTrade.X", "data": [{"missing": "fields"}]}')

    # Pipeline still in good shape — and zero side-effects.
    assert pipe.metrics()["tick_count"] == 0
