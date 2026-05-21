"""
Watchdog regression tests.

We replay a tape of metric snapshots via a fake clock to verify the
state machine fires once on silence, then re-arms after recovery.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from src.application.watchdog import PartialBlindnessWatchdog


class _FakePipeline:
    def __init__(self, last_tick_ts: float | None) -> None:
        self._last_tick_ts = last_tick_ts

    def metrics(self) -> dict[str, float | int | None]:
        return {
            "last_tick_ts": self._last_tick_ts,
            "tick_count": 0,
            "bar_count": 0,
            "signal_count": 0,
            "alert_count": 0,
            "symbols_tracked": 0,
        }

    def set_last(self, last_tick_ts: float | None) -> None:
        self._last_tick_ts = last_tick_ts


@pytest.mark.asyncio
async def test_watchdog_fires_once_on_silence() -> None:
    pipe = _FakePipeline(last_tick_ts=time.time() - 120.0)
    sink_calls: list[tuple[str, dict]] = []

    async def sink(kind: str, m: dict) -> None:
        sink_calls.append((kind, m))

    wd = PartialBlindnessWatchdog(
        pipeline=pipe, alert_sink=sink, silence_threshold_sec=60.0, poll_interval_sec=0.05
    )
    # Call the internal tick directly to avoid sleeping.
    await wd._tick()
    await wd._tick()
    await wd._tick()
    assert [c[0] for c in sink_calls] == ["PARTIAL_BLINDNESS"]


@pytest.mark.asyncio
async def test_watchdog_rearms_after_recovery() -> None:
    pipe = _FakePipeline(last_tick_ts=time.time() - 120.0)
    sink_calls: list[tuple[str, dict]] = []

    async def sink(kind: str, m: dict) -> None:
        sink_calls.append((kind, m))

    wd = PartialBlindnessWatchdog(
        pipeline=pipe, alert_sink=sink, silence_threshold_sec=60.0, poll_interval_sec=0.05
    )
    await wd._tick()  # fires PARTIAL_BLINDNESS

    pipe.set_last(time.time())  # ticks arrive again
    await wd._tick()  # fires PARTIAL_BLINDNESS_CLEARED + rearms

    pipe.set_last(time.time() - 120.0)  # silence again
    await wd._tick()  # fires PARTIAL_BLINDNESS AGAIN

    assert [c[0] for c in sink_calls] == [
        "PARTIAL_BLINDNESS",
        "PARTIAL_BLINDNESS_CLEARED",
        "PARTIAL_BLINDNESS",
    ]


@pytest.mark.asyncio
async def test_watchdog_silent_when_ticks_flowing() -> None:
    pipe = _FakePipeline(last_tick_ts=time.time())
    sink_calls: list[tuple[str, dict]] = []

    async def sink(kind: str, m: dict) -> None:
        sink_calls.append((kind, m))

    wd = PartialBlindnessWatchdog(
        pipeline=pipe, alert_sink=sink, silence_threshold_sec=60.0, poll_interval_sec=0.05
    )
    for _ in range(5):
        await wd._tick()
    assert sink_calls == []


@pytest.mark.asyncio
async def test_watchdog_treats_none_last_tick_as_silence() -> None:
    """Watchdog at startup before any tick has been received."""
    pipe = _FakePipeline(last_tick_ts=None)
    sink_calls: list[tuple[str, dict]] = []

    async def sink(kind: str, m: dict) -> None:
        sink_calls.append((kind, m))

    wd = PartialBlindnessWatchdog(
        pipeline=pipe, alert_sink=sink, silence_threshold_sec=60.0, poll_interval_sec=0.05
    )
    await wd._tick()
    assert [c[0] for c in sink_calls] == ["PARTIAL_BLINDNESS"]


@pytest.mark.asyncio
async def test_watchdog_survives_sink_exception() -> None:
    pipe = _FakePipeline(last_tick_ts=time.time() - 120.0)
    calls = 0

    async def bad_sink(kind: str, m: dict) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("simulated sink failure")

    wd = PartialBlindnessWatchdog(
        pipeline=pipe, alert_sink=bad_sink, silence_threshold_sec=60.0, poll_interval_sec=0.05
    )
    shutdown = asyncio.Event()

    # Run for ~0.2 seconds then stop.
    async def stopper() -> None:
        await asyncio.sleep(0.05 * 4)
        shutdown.set()

    # We invoke .run() directly to exercise the loop path including the
    # try/except guard.
    # NOTE: We override the initial grace sleep to keep the test fast.
    wd._silence_threshold_sec = 0.01
    await asyncio.gather(wd.run(shutdown), stopper())
    assert calls >= 1  # Sink was called at least once; loop did not die.
