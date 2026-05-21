"""
[GEKTOR APEX v3.6.0] Partial-Blindness Watchdog.

Periodically inspects the live tick stream and raises an alert if the
radar has gone silent (no ticks for `silence_threshold_sec`).

This catches:
  * Bybit WS connected but no trades arrive (dead market, partial outage)
  * Aggregator stuck (deadlock, runaway exception loop)
  * Network partition where TCP keepalive doesn't tear down the socket

Surface this through the SAME OutboxAlertSink so the operator sees the
warning in Telegram, not just in logs.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Protocol

from loguru import logger


class _MetricsProvider(Protocol):
    def metrics(self) -> dict[str, float | int | None]: ...


WatchdogAlertSink = Callable[[str, dict[str, float | int | None]], Awaitable[None]]


class PartialBlindnessWatchdog:
    """Polls `pipeline.metrics()` once per `poll_interval_sec` and fires
    an alert if `now - last_tick_ts > silence_threshold_sec`.

    The watchdog deduplicates alerts: at most one alert per silence
    episode (re-arms once a fresh tick arrives).
    """

    def __init__(
        self,
        pipeline: _MetricsProvider,
        alert_sink: WatchdogAlertSink,
        silence_threshold_sec: float = 60.0,
        poll_interval_sec: float = 10.0,
    ) -> None:
        self._pipeline = pipeline
        self._alert_sink = alert_sink
        self._silence_threshold_sec = silence_threshold_sec
        self._poll_interval_sec = poll_interval_sec
        self._already_alerted = False

    async def run(self, shutdown_event: asyncio.Event) -> None:
        logger.info(
            f"[Watchdog] Started (silence_threshold={self._silence_threshold_sec}s, "
            f"poll={self._poll_interval_sec}s)"
        )
        # Give the system a warmup grace period equal to the silence
        # threshold so we don't fire immediately at boot.
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=self._silence_threshold_sec
            )
            return
        except asyncio.TimeoutError:
            pass

        while not shutdown_event.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — defensive, watchdog must never die
                logger.error(f"[Watchdog] Internal error (ignored): {exc}")
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=self._poll_interval_sec
                )
                return
            except asyncio.TimeoutError:
                continue

    async def _tick(self) -> None:
        m = self._pipeline.metrics()
        last_ts = m.get("last_tick_ts")
        now = time.time()
        if last_ts is None:
            silence = self._silence_threshold_sec + 1.0
        else:
            silence = now - float(last_ts)

        if silence > self._silence_threshold_sec:
            if not self._already_alerted:
                self._already_alerted = True
                logger.warning(
                    f"[Watchdog] PARTIAL BLINDNESS — no ticks for {silence:.0f}s "
                    f"(threshold={self._silence_threshold_sec}s)"
                )
                await self._alert_sink("PARTIAL_BLINDNESS", m)
        else:
            if self._already_alerted:
                logger.info("[Watchdog] Tick stream recovered.")
                await self._alert_sink("PARTIAL_BLINDNESS_CLEARED", m)
            self._already_alerted = False
