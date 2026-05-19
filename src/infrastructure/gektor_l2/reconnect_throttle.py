"""Token-bucket style limiter for WS reconnect storms (async, non-blocking)."""

from __future__ import annotations

import asyncio
import time
from typing import Final


class AsyncReconnectTokenBucket:
    """
    Limits how often new connections may be attempted.
    Default: refill 2 tokens/sec, burst capacity 2 (≈2 new connects/sec sustained).
    """

    __slots__ = ("_capacity", "_rate", "_tokens", "_updated_monotonic", "_lock")

    def __init__(self, *, tokens_per_second: float = 2.0, burst: float = 2.0) -> None:
        if tokens_per_second <= 0 or burst <= 0:
            raise ValueError("tokens_per_second and burst must be positive")
        self._rate: Final[float] = float(tokens_per_second)
        self._capacity: Final[float] = float(burst)
        self._tokens: float = float(burst)
        self._updated_monotonic: float = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> None:
        if cost <= 0:
            raise ValueError("cost must be positive")
        while True:
            wait_s = 0.0
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated_monotonic
                self._updated_monotonic = now
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                if self._tokens + 1e-12 >= cost:
                    self._tokens -= cost
                    return
                deficit = cost - self._tokens
                wait_s = deficit / self._rate
            await asyncio.sleep(wait_s)
