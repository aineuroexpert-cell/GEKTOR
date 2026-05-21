# src/application/radar_pipeline.py
"""
[GEKTOR APEX v3.0.0] Radar Pipeline — Advisory Mode Engine.

The brain of the Predator Radar. Orchestrates the full data flow:

  Bybit Public WS (ALL linear futures)
    → Trade Tape Ingestion (orjson zero-copy)
    → Dollar Bar Aggregation (per-symbol, Decimal-based)
    → O(1) VPIN Engine (per-symbol, numpy ring buffer)
    → Anomaly Detection (Z-Score threshold + Iceberg Verification)
    → Telegram Alert Dispatch (debounced, HTML-formatted)

Architecture:
  - Single asyncio Task per concern (no blocking, no threading).
  - Symbol discovery via Bybit REST API (auto-refresh every 6 hours).
  - Per-symbol state isolation: each symbol gets its own DollarBarEngine + VPIN.
  - Alert debouncing: max 1 alert per symbol per 5 minutes.
  - No execution logic. Read-only market data. Advisory Only.

Philosophy:
  "The goal is not to trade — it's to detect the moment
   when informed participants reveal their hand."
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Dict, Optional, Any
from dataclasses import dataclass

from loguru import logger

from src.domain.conflation import DollarBar, DollarBarEngine
from src.domain.vpin_engine import O1VPINEngine, VPINSignal
from src.infrastructure.bybit import BybitRestClient
from src.infrastructure.config import settings
from src.shared.alpha_config import alpha


# ═══════════════════════════════════════════════════════════════════
# VALUE OBJECTS
# ═══════════════════════════════════════════════════════════════════

@dataclass(slots=True, frozen=True)
class AnomalyAlert:
    """Immutable alert payload for Telegram dispatch."""
    symbol: str
    vpin: float
    z_score: float
    absorption: bool
    bar_close_price: float
    bar_volume_usd: float
    detected_at: float  # monotonic timestamp


# ═══════════════════════════════════════════════════════════════════
# ALERT DEBOUNCER
# ═══════════════════════════════════════════════════════════════════

class AlertDebouncer:
    """
    Per-symbol cooldown to prevent alert spam.
    Only 1 alert per symbol per `cooldown_sec` seconds.
    """
    __slots__ = ('_last_alert', '_cooldown')

    def __init__(self, cooldown_sec: float = 300.0):
        self._last_alert: Dict[str, float] = {}
        self._cooldown = cooldown_sec

    def allow(self, symbol: str, now: float) -> bool:
        last = self._last_alert.get(symbol, 0.0)
        if now - last >= self._cooldown:
            self._last_alert[symbol] = now
            return True
        return False

    def remaining_sec(self, symbol: str, now: float) -> float:
        last = self._last_alert.get(symbol, 0.0)
        return max(0.0, self._cooldown - (now - last))


# ═══════════════════════════════════════════════════════════════════
# CORE PIPELINE
# ═══════════════════════════════════════════════════════════════════

class RadarPipeline:
    """
    [GEKTOR v3.0.0] The Predator Radar Core.

    Responsibilities:
      1. Discover ALL USDT-linear futures on Bybit (auto-refresh).
      2. Subscribe to publicTrade streams for each symbol.
      3. Aggregate raw ticks into Dollar Bars (per-symbol, $1M default threshold).
      4. Feed closed bars into O(1) VPIN Engine.
      5. On anomaly detection → format and dispatch Telegram alert.

    NOT responsible for: execution, position management, order routing.
    """

    def __init__(
        self,
        tg_notify_callback,
        db_push_callback=None,
        dollar_threshold_usd: float = 1_000_000.0,
        vpin_window: int = 50,
        vpin_z_threshold: float = 2.5,
        alert_cooldown_sec: float = 300.0,
    ):
        self._tg_notify = tg_notify_callback
        self._db_push = db_push_callback

        # Configuration
        self._dollar_threshold = Decimal(str(dollar_threshold_usd))
        self._vpin_window = vpin_window
        self._vpin_z_threshold = vpin_z_threshold

        # Per-symbol engines (created lazily on first trade)
        self._bar_engines: Dict[str, DollarBarEngine] = {}
        self._vpin_engines: Dict[str, O1VPINEngine] = {}

        # Alert debouncing
        self._debouncer = AlertDebouncer(cooldown_sec=alert_cooldown_sec)

        # Metrics
        self._total_ticks: int = 0
        self._total_bars: int = 0
        self._total_anomalies: int = 0
        self._symbols_active: int = 0
        self._start_time: float = 0.0

    # ──────────────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────────────

    async def on_trade(self, symbol: str, trade: dict) -> None:
        """
        Hot path entry point. Called by BybitIngestor for every trade tick.

        Args:
            symbol: e.g. "BTCUSDT"
            trade: {"price": float, "volume": float, "side": str, "ts": int}
        """
        self._total_ticks += 1

        price = Decimal(str(trade["price"]))
        size = Decimal(str(trade["volume"]))
        side = trade.get("side", "Buy")
        ts = trade.get("ts", time.time())

        # side — это сторона TAKER'а на Bybit Linear Futures publicTrade
        # is_buyer_maker=True означает: taker продал, значит maker купил
        is_buyer_maker = (side == "Sell")  # True when taker=Sell → maker=Buy

        # Lazy initialization of per-symbol engines
        if symbol not in self._bar_engines:
            self._init_symbol(symbol)

        bar_engine = self._bar_engines[symbol]
        await bar_engine.process_tick(symbol, price, size, is_buyer_maker, float(ts))

    async def get_stats(self) -> dict:
        """Returns pipeline statistics for health monitoring."""
        uptime = time.monotonic() - self._start_time if self._start_time else 0
        return {
            "symbols_tracked": len(self._bar_engines),
            "total_ticks": self._total_ticks,
            "total_bars_closed": self._total_bars,
            "total_anomalies": self._total_anomalies,
            "uptime_hours": round(uptime / 3600, 2),
            "ticks_per_sec": round(self._total_ticks / max(1, uptime), 1),
        }

    def start(self) -> None:
        """Mark pipeline start time."""
        self._start_time = time.monotonic()
        logger.info(
            f"🎯 [RADAR] Pipeline armed. "
            f"Dollar threshold: ${self._dollar_threshold:,.0f} | "
            f"VPIN window: {self._vpin_window} | "
            f"Z-threshold: {self._vpin_z_threshold}"
        )

    # ──────────────────────────────────────────────
    # PRIVATE: Per-Symbol Initialization
    # ──────────────────────────────────────────────

    def _init_symbol(self, symbol: str) -> None:
        """Lazily create Dollar Bar + VPIN engines for a new symbol."""
        bar_engine = DollarBarEngine(threshold_usd=self._dollar_threshold)
        vpin_engine = O1VPINEngine(
            window_size=self._vpin_window,
            volume_threshold=float(self._dollar_threshold),
            z_threshold=self._vpin_z_threshold,
        )

        # Wire callback: when bar closes → feed into VPIN
        bar_engine.set_callback(self._make_bar_handler(symbol, vpin_engine))

        self._bar_engines[symbol] = bar_engine
        self._vpin_engines[symbol] = vpin_engine
        self._symbols_active += 1

        if self._symbols_active % 50 == 0:
            logger.info(f"📡 [RADAR] Tracking {self._symbols_active} symbols now.")

    def _make_bar_handler(self, symbol: str, vpin_engine: O1VPINEngine):
        """Creates a closure that processes a closed Dollar Bar through VPIN."""

        async def _on_bar_closed(bar: DollarBar) -> None:
            self._total_bars += 1

            # Feed into VPIN
            signal: Optional[VPINSignal] = vpin_engine.process_bar(bar)

            if signal is None:
                return  # VPIN still warming up

            if signal.is_anomaly:
                self._total_anomalies += 1
                now = time.monotonic()

                if self._debouncer.allow(symbol, now):
                    alert = AnomalyAlert(
                        symbol=symbol,
                        vpin=signal.vpin_value,
                        z_score=signal.z_score,
                        absorption=signal.absorption_detected,
                        bar_close_price=float(bar.close),
                        bar_volume_usd=float(bar.volume_usd),
                        detected_at=now,
                    )
                    await self._dispatch_alert(alert)
                else:
                    remaining = self._debouncer.remaining_sec(symbol, now)
                    logger.debug(
                        f"🔕 [RADAR] {symbol} anomaly suppressed (cooldown {remaining:.0f}s left)"
                    )

        return _on_bar_closed

    # ──────────────────────────────────────────────
    # PRIVATE: Alert Formatting & Dispatch
    # ──────────────────────────────────────────────

    async def _dispatch_alert(self, alert: AnomalyAlert) -> None:
        """Format and send Telegram alert for detected anomaly."""

        # Direction hint based on absorption
        if alert.absorption:
            direction = "🧊 ICEBERG (Скрытый игрок поглощает)"
            emoji = "🔴"
        else:
            direction = "⚡ Агрессивный дисбаланс"
            emoji = "🟡"

        z_bar = "█" * min(10, int(abs(alert.z_score)))

        message = (
            f"{emoji} <b>[АНОМАЛИЯ] {alert.symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>VPIN:</b> <code>{alert.vpin:.4f}</code>\n"
            f"📈 <b>Z-Score:</b> <code>{alert.z_score:+.2f}</code> {z_bar}\n"
            f"🎯 <b>Тип:</b> {direction}\n"
            f"💰 <b>Цена:</b> <code>${alert.bar_close_price:,.4f}</code>\n"
            f"📦 <b>Объём бара:</b> <code>${alert.bar_volume_usd:,.0f}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>Advisory Only — не является торговой рекомендацией</i>"
        )

        try:
            await self._tg_notify(message, "ANOMALY")
            logger.success(
                f"🎯 [RADAR] ALERT FIRED: {alert.symbol} | "
                f"VPIN={alert.vpin:.4f} | Z={alert.z_score:+.2f} | "
                f"Absorption={alert.absorption}"
            )
        except Exception as e:
            logger.error(f"❌ [RADAR] Failed to dispatch alert for {alert.symbol}: {e}")

        # Persist to DB if available
        if self._db_push:
            try:
                await self._db_push(
                    "INSERT INTO signals (signal_id, symbol, state, entry_bid, exit_vpin, created_at) "
                    "VALUES (:sig_id, :symbol, 'ANOMALY', :price, :vpin, CURRENT_TIMESTAMP)",
                    {
                        "sig_id": f"RADAR-{alert.symbol}-{int(alert.detected_at)}",
                        "symbol": alert.symbol,
                        "price": alert.bar_close_price,
                        "vpin": alert.vpin,
                    }
                )
            except Exception as e:
                logger.error(f"❌ [RADAR] DB persist failed: {e}")
