"""Regression tests for the daemon lifecycle (Task 6.2 from ТЗ v2.0).

The ТЗ's original `test_service_does_not_exit_immediately` references the
old `BybitIngestor` (the heavyweight L2/Trading-Mode class). In PR #2 the
radar uses the focused `BybitWSIngestion` (Advisory-Mode, public trade
tape only), so this sentinel test is adapted to the new class.

What we guard against:

    The class-of-bug described in ТЗ v2.0 §1.1 — the ingestor's `run()`
    method falls through after subscription, `asyncio.gather()` finds
    nothing left to wait on, and the daemon exits with status=0 within
    a few seconds of starting. On the Tokyo box this manifested as
    "Deactivated successfully" appearing in journalctl every ~8 seconds.

How we test:

    Start `BybitWSIngestion.run(symbols, shutdown_event)` against a tiny
    in-process WebSocket echo server (no Bybit network). Verify:

      1. The coroutine is still pending after a grace window — it must
         NOT return on its own. If it returns within `GRACE_SEC`, the
         while-loop has been broken or the run() method is no longer
         blocking.

      2. After `shutdown_event.set()`, the coroutine completes within
         `SHUTDOWN_SEC`. Clean shutdown must still work.

We deliberately do NOT depend on the real Bybit endpoint or on the
network — the test serves a single Bybit-shaped trade message from
localhost and immediately closes, which is enough to exercise the
post-subscribe wait without any external dependency.
"""
from __future__ import annotations

import asyncio

import orjson
import pytest
from aiohttp import web

from src.infrastructure.bybit_ws_ingestion import BybitWSIngestion

GRACE_SEC = 1.5      # how long run() must keep blocking even after the WS closes
SHUTDOWN_SEC = 2.0   # how long run() may take to finish after shutdown_event.set()


class _StubAggregator:
    """Minimal aggregator stub: counts ticks, no real bar logic."""

    def __init__(self) -> None:
        self.tick_count = 0
        self.resync_count = 0

    async def process_tick(self, symbol, price, size, is_buyer_maker, exchange_ts) -> None:
        self.tick_count += 1

    async def handle_resync(self) -> None:
        self.resync_count += 1


async def _ws_handler(request: web.Request) -> web.WebSocketResponse:
    """Serve one trade frame, then close the socket — simulates a brief
    connection that the ingestor must survive (backoff + reconnect)."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # Send a single Bybit-shaped publicTrade frame.
    frame = {
        "topic": "publicTrade.BTCUSDT",
        "data": [
            {
                "p": "50000.0",
                "v": "0.1",
                "S": "Buy",
                "T": 1700000000000,
            }
        ],
    }
    await ws.send_bytes(orjson.dumps(frame))
    await ws.close()
    return ws


@pytest.mark.asyncio
async def test_run_keeps_event_loop_alive_until_shutdown(unused_tcp_port):
    """run() MUST NOT return on its own — only when shutdown_event is set.

    This is the regression sentinel for the Tokyo "service exits every 8s"
    bug (ТЗ v2.0 §1.1 / Task 1).
    """
    port = unused_tcp_port

    app = web.Application()
    app.router.add_get("/ws", _ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()

    aggregator = _StubAggregator()
    ingestor = BybitWSIngestion(ws_url=f"ws://127.0.0.1:{port}/ws", aggregator=aggregator)
    shutdown_event = asyncio.Event()

    try:
        task = asyncio.create_task(ingestor.run(["BTCUSDT"], shutdown_event))

        await asyncio.sleep(GRACE_SEC)
        assert not task.done(), (
            "BybitWSIngestion.run() returned on its own — the reconnect "
            "loop is broken. This is the Tokyo 'service exits every 8s' "
            "regression (ТЗ v2.0 §1.1)."
        )

        shutdown_event.set()
        try:
            await asyncio.wait_for(task, timeout=SHUTDOWN_SEC)
        except asyncio.TimeoutError:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            pytest.fail("run() did not honour shutdown_event within SHUTDOWN_SEC")
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_run_processes_at_least_one_tick_before_close(unused_tcp_port):
    """Sanity: the test harness actually delivers a tick to the aggregator."""
    port = unused_tcp_port

    app = web.Application()
    app.router.add_get("/ws", _ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()

    aggregator = _StubAggregator()
    ingestor = BybitWSIngestion(ws_url=f"ws://127.0.0.1:{port}/ws", aggregator=aggregator)
    shutdown_event = asyncio.Event()

    try:
        task = asyncio.create_task(ingestor.run(["BTCUSDT"], shutdown_event))
        await asyncio.sleep(0.4)  # let one tick through

        assert aggregator.tick_count >= 1, "test harness did not deliver any ticks"
        assert aggregator.resync_count >= 1, "ingestor did not call handle_resync on connect"

        shutdown_event.set()
        try:
            await asyncio.wait_for(task, timeout=SHUTDOWN_SEC)
        except asyncio.TimeoutError:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await runner.cleanup()
