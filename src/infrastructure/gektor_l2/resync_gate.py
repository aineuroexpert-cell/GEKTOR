"""Global throttling + circuit breaker for REST orderbook resync (many symbols, one API)."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import Final, TypeVar

from loguru import logger

from src.infrastructure.gektor_l2.errors import BybitRestRateLimited

T = TypeVar("T")


class RestResyncGate:
    """
    - Limits concurrent REST calls (default 2).
    - Minimum spacing between request starts (default 50ms).
    - On `BybitRestRateLimited`: exponential backoff + jitter; opens circuit after repeated failures.
    """

    __slots__ = (
        "_sem",
        "_min_interval",
        "_spacing_lock",
        "_next_allowed_mono",
        "_failures",
        "_circuit_open_until",
        "_circuit_fail_threshold",
        "_circuit_cooldown",
        "_backoff_base",
        "_backoff_max",
    )

    def __init__(
        self,
        *,
        max_concurrent: int = 2,
        min_interval_sec: float = 0.05,
        circuit_fail_threshold: int = 6,
        circuit_cooldown_sec: float = 30.0,
        backoff_base_sec: float = 0.25,
        backoff_max_sec: float = 12.0,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        self._sem: Final[asyncio.Semaphore] = asyncio.Semaphore(int(max_concurrent))
        self._min_interval: Final[float] = float(min_interval_sec)
        self._spacing_lock = asyncio.Lock()
        self._next_allowed_mono: float = 0.0
        self._failures: int = 0
        self._circuit_open_until: float = 0.0
        self._circuit_fail_threshold: Final[int] = int(circuit_fail_threshold)
        self._circuit_cooldown: Final[float] = float(circuit_cooldown_sec)
        self._backoff_base: Final[float] = float(backoff_base_sec)
        self._backoff_max: Final[float] = float(backoff_max_sec)

    async def run_throttled(self, factory: Callable[[], Awaitable[T]]) -> T:
        async with self._sem:
            now = time.monotonic()
            if now < self._circuit_open_until:
                wait_c = self._circuit_open_until - now
                logger.warning(
                    "RestResyncGate: circuit open {:.1f}s remaining — sleeping",
                    wait_c,
                )
                await asyncio.sleep(wait_c)

            async with self._spacing_lock:
                now2 = time.monotonic()
                gap = self._next_allowed_mono - now2
                if gap > 0:
                    await asyncio.sleep(gap)

            try:
                result = await factory()
            except BybitRestRateLimited:
                await self._on_rate_limit()
                raise
            except Exception:
                async with self._spacing_lock:
                    self._next_allowed_mono = time.monotonic() + self._min_interval
                raise

            async with self._spacing_lock:
                self._next_allowed_mono = time.monotonic() + self._min_interval
                self._failures = 0
            return result

    @property
    def is_circuit_open(self) -> bool:
        """Expose circuit breaker state for BookReadiness disambiguation."""
        return time.monotonic() < self._circuit_open_until

    async def _on_rate_limit(self) -> None:
        async with self._spacing_lock:
            self._failures += 1
            exp = min(self._backoff_max, self._backoff_base * (2 ** min(self._failures, 8)))
            jitter = random.uniform(0.0, exp * 0.15)
            sleep_s = exp + jitter
            if self._failures >= self._circuit_fail_threshold:
                self._circuit_open_until = time.monotonic() + self._circuit_cooldown
                logger.error(
                    "RestResyncGate: circuit OPEN for {:.0f}s after {} rate-limit hits",
                    self._circuit_cooldown,
                    self._failures,
                )
                self._failures = 0
            self._next_allowed_mono = time.monotonic() + self._min_interval + sleep_s
        logger.warning("RestResyncGate: backing off {:.2f}s after rate limit", sleep_s)
        await asyncio.sleep(sleep_s)
