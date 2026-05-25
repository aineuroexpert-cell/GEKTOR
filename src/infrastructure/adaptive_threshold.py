"""
[GEKTOR APEX v3.6.2] Adaptive per-symbol dollar-bar threshold provider.

Problem solved
--------------
A fixed `DOLLAR_THRESHOLD_BASE` (e.g. $1M) gives the trader two
unacceptable extremes simultaneously:

  * BTCUSDT ($50B/day turnover): a $1M bar closes in ~1.7 sec → the
    VPIN ring buffer is fed ~50k bars/day → noise overload.
  * NILUSDT ($10M/day turnover): a $1M bar would take ~2.4 hours →
    the 50-bar VPIN warmup takes ~5 days → effectively no alerts.

This module sizes the bar **per symbol** so that every symbol produces
roughly the same number of bars per day (default: 200). After clamping
to a sane min / max range, the result is a per-symbol threshold like:

    threshold_usd[symbol] = clamp(
        turnover_24h[symbol] / target_bars_per_day,
        min_usd,
        max_usd,
    )

For BTC: 50 000 000 000 / 200 = 250 000 000 → clamped to max_usd=5M.
For NIL: 10 000 000 / 200 = 50 000 → above min_usd=20k, so 50k.
This brings NIL warmup from ~5 days to ~2 hours.

Lifecycle
---------
  * Created once at startup.
  * `refresh()` is called once at boot to populate the cache, then
    once per hour by a background task in `main.py`.
  * `threshold_for(symbol)` is synchronous and O(1). It is called by
    `DollarBarEngine` on every closed bar (rarely), and by the
    pipeline at engine construction.
  * `turnover_for(symbol)` is also synchronous and O(1). It is used by
    `LargePrintDetector` to compute % of 24h turnover per tick.

Fault tolerance
---------------
If the REST call fails (network, Bybit 5xx, etc), the provider falls
back to the last-known cache. If there has never been a successful
fetch, it returns `default_threshold_usd` for every symbol.

Performance
-----------
The cache is a simple `dict[str, float]`. Hot path access is one
lookup. Refresh fetches ALL tickers in a single REST call (Bybit
returns ~700 contracts in one shot), so refresh is ~1 second of work
regardless of universe size.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Protocol

from loguru import logger

# ----------------------------------------------------------------------
# REST adapter protocol
# ----------------------------------------------------------------------


class _TickersFetcher(Protocol):
    """Anything with `get_tickers()` that returns a list of dicts, where
    each dict has at least keys `symbol` and `turnover24h` (Bybit V5
    `/v5/market/tickers?category=linear` shape).
    """

    async def get_tickers(
        self, symbol: str | None = None
    ) -> list[dict[str, Any]] | float: ...


def _safe_turnover(raw: Any) -> float:
    """Coerce Bybit's string-typed turnover to float, defensively."""
    if raw is None or raw == "":
        return 0.0
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return v if v > 0.0 else 0.0


# ----------------------------------------------------------------------
# Provider
# ----------------------------------------------------------------------


class AdaptiveDollarThresholdProvider:
    """Per-symbol dollar-bar threshold sized by 24h turnover.

    Construction
    ------------
        provider = AdaptiveDollarThresholdProvider(
            rest_client=bybit_rest,
            target_bars_per_day=200,
            min_usd=20_000,
            max_usd=5_000_000,
            default_usd=1_000_000,
        )
        await provider.refresh()        # populate cache

    Usage (sync, hot path)
    ----------------------
        threshold = provider.threshold_for("BTCUSDT")
        # → Decimal("5000000")

        turnover = provider.turnover_for("BTCUSDT")
        # → 5.0e10
    """

    __slots__ = (
        "_rest_client",
        "_target_bars_per_day",
        "_min_usd",
        "_max_usd",
        "_default_usd",
        "_turnover_cache",
        "_threshold_cache",
        "_lock",
        "_refresh_count",
    )

    def __init__(
        self,
        rest_client: _TickersFetcher,
        target_bars_per_day: int = 200,
        min_usd: float = 20_000.0,
        max_usd: float = 5_000_000.0,
        default_usd: float = 1_000_000.0,
    ) -> None:
        if target_bars_per_day < 1:
            raise ValueError("target_bars_per_day must be >= 1")
        if min_usd <= 0:
            raise ValueError("min_usd must be > 0")
        if max_usd < min_usd:
            raise ValueError("max_usd must be >= min_usd")
        if default_usd < min_usd or default_usd > max_usd:
            raise ValueError(
                "default_usd must be within [min_usd, max_usd]"
            )

        self._rest_client = rest_client
        self._target_bars_per_day = target_bars_per_day
        self._min_usd = min_usd
        self._max_usd = max_usd
        self._default_usd = default_usd
        self._turnover_cache: dict[str, float] = {}
        self._threshold_cache: dict[str, Decimal] = {}
        self._lock = asyncio.Lock()
        self._refresh_count = 0

    # ------------------------------------------------------------------
    # Sync hot-path accessors
    # ------------------------------------------------------------------

    def threshold_for(self, symbol: str) -> Decimal:
        cached = self._threshold_cache.get(symbol)
        if cached is not None:
            return cached
        # Fallback before first refresh or for unknown symbols.
        return Decimal(str(self._default_usd))

    def turnover_for(self, symbol: str) -> float:
        return self._turnover_cache.get(symbol, 0.0)

    @property
    def refresh_count(self) -> int:
        return self._refresh_count

    @property
    def cache_size(self) -> int:
        return len(self._threshold_cache)

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    async def refresh(self) -> int:
        """Pull all USDT-Linear tickers and rebuild the threshold cache.

        Returns the number of symbols cached. On REST failure, the
        previous cache is preserved untouched and 0 is returned.
        """
        async with self._lock:
            try:
                raw_tickers = await self._rest_client.get_tickers()
            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except Exception as exc:
                logger.warning(
                    f"[AdaptiveThreshold] refresh failed, keeping last cache "
                    f"(size={len(self._threshold_cache)}): {exc!r}"
                )
                return 0

            if not isinstance(raw_tickers, list):
                logger.warning(
                    "[AdaptiveThreshold] get_tickers() returned non-list; "
                    "skipping refresh"
                )
                return 0

            new_turnover: dict[str, float] = {}
            new_threshold: dict[str, Decimal] = {}
            for entry in raw_tickers:
                sym = entry.get("symbol")
                if not sym or not isinstance(sym, str):
                    continue
                if not sym.endswith("USDT"):
                    continue
                turnover = _safe_turnover(entry.get("turnover24h"))
                new_turnover[sym] = turnover
                new_threshold[sym] = self._compute_threshold(turnover)

            self._turnover_cache = new_turnover
            self._threshold_cache = new_threshold
            self._refresh_count += 1
            logger.info(
                f"[AdaptiveThreshold] refresh #{self._refresh_count}: "
                f"sized {len(new_threshold)} symbols "
                f"(min={self._min_usd:,.0f}, max={self._max_usd:,.0f}, "
                f"target_bars={self._target_bars_per_day}/day)"
            )
            return len(new_threshold)

    def _compute_threshold(self, turnover_24h: float) -> Decimal:
        if turnover_24h <= 0.0:
            return Decimal(str(self._default_usd))
        raw = turnover_24h / float(self._target_bars_per_day)
        clamped = max(self._min_usd, min(self._max_usd, raw))
        return Decimal(str(round(clamped, 2)))


# ----------------------------------------------------------------------
# Background refresher
# ----------------------------------------------------------------------


async def adaptive_threshold_refresher(
    provider: AdaptiveDollarThresholdProvider,
    interval_sec: float = 3600.0,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Background task: refresh the provider every `interval_sec`.

    Lives alongside the radar pipeline. Cancels cleanly on shutdown.
    Errors during refresh are already logged inside `refresh()`; this
    loop never crashes the radar.
    """
    if interval_sec <= 0:
        raise ValueError("interval_sec must be > 0")

    while True:
        try:
            if stop_event is not None and stop_event.is_set():
                return
            await asyncio.sleep(interval_sec)
            await provider.refresh()
        except asyncio.CancelledError:
            logger.info("[AdaptiveThreshold] refresher task cancelled, exiting.")
            return
