"""
[GEKTOR APEX v3.6.0 "APEX-RADAR"] RadarPipeline — Advisory Mode core.

Wires the Quantitative Core (DollarBarEngine + O1VPINEngine) to the
Presentation layer (Outbox -> Telegram). Per-symbol state is created lazily
on the first tick.

Data flow:
    BybitWSIngestion._process_message
        -> RadarPipeline.on_trade(symbol, side, price, size, ts)
            -> DollarBarEngine.process_tick(...)
                # bar closes when threshold reached
                -> _on_bar_closed(bar)
                    -> O1VPINEngine.process_bar(bar)
                        # signal emitted when warmup done
                        -> _dispatch_alert(symbol, bar, signal)

THIS IS THE CANONICAL PIPELINE. It replaces the previous TODO-stub
`_radar_engine` in main.py.

Polarity contract (invariant I5 of vpin_engine.py):
    BybitWSIngestion sets is_buyer_maker = (trade["S"] == "Sell").
    RadarPipeline forwards is_buyer_maker unchanged to DollarBarEngine.
    DollarBarEngine (conflation.py:96-101) then increments sell_volume_usd
    when is_buyer_maker=True. Do not invert.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loguru import logger

from src.domain.conflation import DollarBar, DollarBarEngine
from src.domain.liquidity_detectors import (
    LiquidityAlert,
    LiquidityDetectorBank,
)
from src.domain.vpin_engine import O1VPINEngine, VPINSignal


@dataclass(slots=True)
class RadarAlert:
    symbol: str
    timestamp: float
    bar_open: float
    bar_close: float
    vpin: float
    z_score: float
    direction: str  # "long" | "short"
    absorption: bool


# Type for the side-effect callback that ships an alert to the outbox /
# notifier. Kept simple so it can be a closure over a Telegram client.
AlertSink = Callable[[RadarAlert], Awaitable[None]]
LiquidityAlertSink = Callable[[LiquidityAlert], Awaitable[None]]
ThresholdProvider = Callable[[str], float]


class _SymbolPerSymbolRateLimiter:
    """Suppresses duplicate alerts for the same symbol fired within `cooldown` seconds.

    The pipeline can otherwise emit a flurry of alerts on a sustained
    imbalance (Z-Score stays above threshold for many bars). One alert per
    symbol per `cooldown_sec` is enough for a manual operator.
    """

    __slots__ = ("_cooldown_sec", "_last_alert")

    def __init__(self, cooldown_sec: float) -> None:
        self._cooldown_sec = cooldown_sec
        self._last_alert: dict[str, float] = {}

    def allow(self, symbol: str, now: float | None = None) -> bool:
        if now is None:
            now = time.monotonic()
        last = self._last_alert.get(symbol)
        if last is not None and (now - last) < self._cooldown_sec:
            return False
        self._last_alert[symbol] = now
        return True


class RadarPipeline:
    """Advisory-mode market microstructure radar.

    Manages per-symbol DollarBar + VPIN engines and forwards anomaly signals
    to the alert sink. **Does not perform any I/O of its own** — pure
    application-layer orchestration.
    """

    def __init__(
        self,
        threshold_usd: float,
        alert_sink: AlertSink,
        window_size: int = 50,
        z_threshold: float = 2.5,
        z_history_size: int = 500,
        per_symbol_cooldown_sec: float = 300.0,
        threshold_provider: ThresholdProvider | None = None,
        liquidity_detectors: LiquidityDetectorBank | None = None,
        liquidity_alert_sink: LiquidityAlertSink | None = None,
    ) -> None:
        self.threshold_usd = threshold_usd
        self._alert_sink = alert_sink
        self._window_size = window_size
        self._z_threshold = z_threshold
        self._z_history_size = z_history_size
        self._threshold_provider = threshold_provider

        # Shared DollarBarEngine — its internal `_current_bars` dict already
        # partitions by symbol, so a single instance handles the whole universe.
        # v3.6.2: per-symbol adaptive threshold provider can override the base.
        self._bar_engine: DollarBarEngine = DollarBarEngine(
            threshold_usd=threshold_usd,
            threshold_provider=threshold_provider,
        )
        self._bar_engine.set_callback(self._on_bar_closed)

        # One VPIN engine per symbol. Created lazily.
        self._vpin_engines: dict[str, O1VPINEngine] = {}

        # Per-symbol rate limiter to keep Telegram traffic civilised.
        self._rate_limiter = _SymbolPerSymbolRateLimiter(per_symbol_cooldown_sec)

        # v3.6.2: optional instant-fire liquidity detectors (no warmup).
        self._liquidity_detectors = liquidity_detectors
        self._liquidity_alert_sink = liquidity_alert_sink or self._default_liquidity_sink(alert_sink)

        # Lightweight metrics (read by /health endpoint and tests).
        self._tick_count: int = 0
        self._bar_count: int = 0
        self._signal_count: int = 0
        self._alert_count: int = 0
        self._liquidity_alert_count: int = 0
        self._last_tick_ts: float | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def on_trade(
        self,
        symbol: str,
        side: str,
        price: float,
        size: float,
        exchange_ts: float,
    ) -> None:
        """Ingest a single trade tick from the WS feed.

        `side` is the AGGRESSOR side as reported by Bybit ("Buy" or "Sell").
        We map it to is_buyer_maker: when the aggressor is a seller, the
        maker (resting order) was a buyer.
        """
        is_buyer_maker = side == "Sell"  # invariant I5
        await self.process_tick(
            symbol=symbol,
            price=price,
            size=size,
            is_buyer_maker=is_buyer_maker,
            exchange_ts=exchange_ts,
        )

    async def process_tick(
        self,
        symbol: str,
        price: float,
        size: float,
        is_buyer_maker: bool,
        exchange_ts: float,
    ) -> None:
        """IBarAggregator protocol entry point used by BybitWSIngestion.

        This is the canonical hot-path called by the ingestor. `on_trade`
        is a convenience wrapper that accepts the raw Bybit `side` string.
        """
        self._tick_count += 1
        self._last_tick_ts = exchange_ts

        # v3.6.2: run instant-fire liquidity detectors BEFORE bar aggregation,
        # so they can alert on raw ticks regardless of bar warmup state.
        if self._liquidity_detectors is not None and self._liquidity_detectors.enabled:
            liquidity_alerts = self._liquidity_detectors.process_tick(
                symbol=symbol,
                is_buyer_maker=is_buyer_maker,
                price=price,
                size=size,
                ts=exchange_ts,
            )
            for alert in liquidity_alerts:
                # Rate-limit liquidity alerts under the same per-symbol cooldown
                # as VPIN alerts — prevents flooding when both fire on the same
                # event. Use a derived key so Sweep and OFI Pulse don't shadow VPIN.
                if not self._rate_limiter.allow(f"{alert.kind}:{alert.symbol}"):
                    continue
                await self._dispatch_liquidity_alert(alert)

        await self._bar_engine.process_tick(
            symbol=symbol,
            price=price,
            size=size,
            is_buyer_maker=is_buyer_maker,
            exchange_ts=exchange_ts,
        )

    async def handle_resync(self) -> None:
        """Drop poisoned in-flight bars on WS reconnect.

        VPIN ring buffers are deliberately NOT reset here. We trust the
        statistics already accumulated; a single missing bar is preferable
        to losing the warmup history.
        """
        await self._bar_engine.handle_resync()

    def metrics(self) -> dict[str, float | int | None]:
        return {
            "tick_count": self._tick_count,
            "bar_count": self._bar_count,
            "signal_count": self._signal_count,
            "alert_count": self._alert_count,
            "liquidity_alert_count": self._liquidity_alert_count,
            "last_tick_ts": self._last_tick_ts,
            "symbols_tracked": len(self._vpin_engines),
        }

    # ------------------------------------------------------------------
    # Internal: bar closure callback
    # ------------------------------------------------------------------

    async def _on_bar_closed(self, bar: DollarBar) -> None:
        self._bar_count += 1
        engine = self._vpin_engines.get(bar.symbol)
        if engine is None:
            engine = O1VPINEngine(
                window_size=self._window_size,
                volume_threshold=float(self.threshold_usd),
                z_threshold=self._z_threshold,
                z_history_size=self._z_history_size,
            )
            self._vpin_engines[bar.symbol] = engine
            logger.info(
                f"[RadarPipeline] Spawned VPIN engine for {bar.symbol} "
                f"(window={self._window_size}, z_thresh={self._z_threshold})"
            )

        signal: VPINSignal | None = engine.process_bar(bar)
        if signal is None:
            return

        self._signal_count += 1
        if not signal.is_anomaly:
            return

        if not self._rate_limiter.allow(bar.symbol):
            logger.debug(
                f"[RadarPipeline] Suppressed alert for {bar.symbol} "
                f"(rate-limited; z={signal.z_score:.2f})"
            )
            return

        await self._dispatch_alert(bar, signal)

    async def _dispatch_alert(self, bar: DollarBar, signal: VPINSignal) -> None:
        alert = RadarAlert(
            symbol=bar.symbol,
            timestamp=bar.end_ts,
            bar_open=bar.open,
            bar_close=bar.close,
            vpin=signal.vpin_value,
            z_score=signal.z_score,
            direction=signal.direction,
            absorption=signal.absorption_detected,
        )
        try:
            await self._alert_sink(alert)
            self._alert_count += 1
            logger.success(
                f"[RadarPipeline] ALERT {alert.symbol} {alert.direction.upper()} "
                f"z={alert.z_score:.2f} vpin={alert.vpin:.4f} "
                f"absorption={alert.absorption}"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Never let a failed alert poison the pipeline. Log and move on.
            logger.error(
                f"[RadarPipeline] Failed to dispatch alert for {alert.symbol}: {exc!r}"
            )

    async def _dispatch_liquidity_alert(self, alert: LiquidityAlert) -> None:
        try:
            await self._liquidity_alert_sink(alert)
            self._liquidity_alert_count += 1
            logger.success(
                f"[RadarPipeline] LIQUIDITY {alert.kind} {alert.symbol} "
                f"{alert.direction.upper()} notional=${alert.notional_usd:,.0f}"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                f"[RadarPipeline] Failed to dispatch liquidity alert "
                f"{alert.kind} for {alert.symbol}: {exc!r}"
            )

    @staticmethod
    def _default_liquidity_sink(
        vpin_sink: AlertSink,
    ) -> LiquidityAlertSink:
        """Default fan-in: shape a LiquidityAlert into a RadarAlert and
        push it through the same outbox channel. Lets callers wire up
        liquidity detectors without supplying a separate sink.
        """

        async def sink(la: LiquidityAlert) -> None:
            ra = RadarAlert(
                symbol=la.symbol,
                timestamp=la.timestamp,
                bar_open=la.price,
                bar_close=la.price,
                vpin=0.0,
                z_score=float(la.extra.get("ratio_to_median", 0.0))
                if la.kind == "OFI_PULSE"
                else 0.0,
                direction="long" if la.direction == "buy" else "short",
                absorption=False,
            )
            await vpin_sink(ra)

        return sink
