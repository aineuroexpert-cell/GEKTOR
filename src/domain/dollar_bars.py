# src/domain/dollar_bars.py
"""
[GEKTOR APEX v5.1] Zero-Allocation Dollar Bar Engine.

Converts raw L1 trade tape into Dollar Bars — bars closed not by time,
but by a fixed dollar volume threshold. This restores IID-like properties
to the return series, which is the mathematical prerequisite for any
ML model (VPIN, Meta-Labeling, HMM) to have predictive power.

Architecture:
  - Ring buffer: pre-allocated numpy int64 ndarray. Zero GC pressure.
  - Scaled integers: all prices/volumes are int64. No IEEE 754 drift.
  - O(1) bar close: incremental EMA + Welford variance.
  - Elephant Order splitting: a single trade exceeding N×threshold is
    split into N synthetic micro-bars + remainder.
  - ingest_trade returns int (bars produced), NOT a list.
    Subscribers read the ring buffer directly via get_bars_view().

Philosophy (López de Prado):
  "Time bars oversample during quiet periods and undersample during
   volatility storms. Dollar bars ensure each observation carries
   the same quantum of market information — the only foundation
   for statistically valid inference."
"""
from __future__ import annotations

from typing import Final, Callable, Any

import numpy as np
from loguru import logger


# Ring buffer column indices — public constants for zero-copy consumers
TS_OPEN: Final[int] = 0
TS_CLOSE: Final[int] = 1
OPEN: Final[int] = 2
HIGH: Final[int] = 3
LOW: Final[int] = 4
CLOSE: Final[int] = 5
VOLUME: Final[int] = 6
DOLLAR_VOL: Final[int] = 7
EMA: Final[int] = 8
BAR_IDX: Final[int] = 9
IS_SYNTHETIC: Final[int] = 10
NCOLS: Final[int] = 11


class ZeroAllocationDollarBarEngine:
    """
    Deterministic Dollar Bar generator with Elephant Order protection.

    Zero allocation in hot path. O(1) state update per trade.
    Ring buffer on pre-allocated numpy int64 ndarray.

    ingest_trade() returns int (number of bars produced).
    Subscribers receive bar_index range and read from the ring buffer
    directly via get_bar_row() — zero-copy, zero-alloc.

    Elephant Order Protocol:
      When a single trade's dollar value exceeds N × threshold:
        1. Fill the current (partial) bar to threshold → close it.
        2. Emit (N-1) synthetic micro-bars: OHLC = trade price.
        3. Put the remainder into a new open bar.
    """
    __slots__ = (
        '_threshold', '_capacity', '_head', '_bar_count',
        '_buf', '_dollar_acc', '_vol_acc', '_trade_count',
        '_bar_open_ts', '_bar_open_px', '_bar_high', '_bar_low',
        '_bar_close',
        '_ema_alpha_num', '_ema_alpha_den', '_ema',
        '_var_m2', '_var_mean', '_var_count',
        '_scale',
        '_listeners',
    )

    _ALPHA_SCALE: Final[int] = 1_000_000

    def __init__(
        self,
        dollar_threshold_scaled: int,
        *,
        capacity: int = 10_000,
        ema_period: int = 14,
        scale: int = 100_000_000,
    ) -> None:
        """
        Args:
            dollar_threshold_scaled: Dollar volume per bar (scaled integer).
            capacity: Ring buffer size (max bars in memory).
            ema_period: EMA lookback period (bars, not time).
            scale: Price/volume scaling factor (must match L2 SCALE).
        """
        if dollar_threshold_scaled <= 0:
            raise ValueError("dollar_threshold_scaled must be positive")

        self._threshold: int = dollar_threshold_scaled
        self._capacity: int = capacity
        self._head: int = 0
        self._bar_count: int = 0
        self._scale: int = scale

        # Pre-allocated ring buffer: zero GC pressure
        self._buf = np.zeros((capacity, NCOLS), dtype=np.int64)

        # Current bar accumulator state
        self._dollar_acc: int = 0
        self._vol_acc: int = 0
        self._trade_count: int = 0
        self._bar_open_ts: int = 0
        self._bar_open_px: int = 0
        self._bar_high: int = 0
        self._bar_low: int = 0
        self._bar_close: int = 0

        # EMA fixed-point
        self._ema_alpha_num: int = 2 * self._ALPHA_SCALE
        self._ema_alpha_den: int = (ema_period + 1) * self._ALPHA_SCALE
        self._ema: int = 0

        # Welford online variance
        self._var_count: int = 0
        self._var_mean: int = 0
        self._var_m2: int = 0

        # Listeners: callback(bars_produced: int, first_bar_index: int)
        self._listeners: list[Callable[[int, int], Any]] = []

    def add_listener(self, callback: Callable[[int, int], Any]) -> None:
        """
        Register a synchronous callback for bar close events.

        Signature: callback(bars_produced: int, first_bar_index: int) -> None.
        The listener reads bar data from the ring buffer directly via
        get_bar_row(bar_index) — zero-copy.
        """
        self._listeners.append(callback)

    def ingest_trade(
        self,
        price_scaled: int,
        qty_scaled: int,
        ts_exchange: int,
    ) -> int:
        """
        Ingest a single L1 trade tick. Returns number of bars closed (0, 1, or K).

        On Elephant Orders (dollar_value > N × threshold), returns K bars.
        Subscribers use get_bar_row() to read the closed bars from the ring buffer.

        O(1) amortized per normal trade. O(K) for K-bar elephant splits,
        but K is bounded by exchange max_trade_size / threshold.

        Args:
            price_scaled: Trade price (scaled integer).
            qty_scaled: Trade quantity (scaled integer).
            ts_exchange: Exchange-side timestamp (milliseconds).

        Returns:
            Number of bars closed by this trade (0 for normal accumulation).
        """
        if price_scaled <= 0 or qty_scaled <= 0:
            return 0

        dollar_value = price_scaled * qty_scaled // self._scale
        if dollar_value <= 0:
            return 0

        remaining_dollar = dollar_value
        remaining_qty = qty_scaled
        bars_produced = 0
        first_bar_index = self._bar_count

        while remaining_dollar > 0:
            # Initialize bar OHLC if accumulator is empty
            if self._trade_count == 0:
                self._bar_open_ts = ts_exchange
                self._bar_open_px = price_scaled
                self._bar_high = price_scaled
                self._bar_low = price_scaled
                # Mark synthetic if this is a continuation of elephant split
                self._buf[self._head, IS_SYNTHETIC] = 1 if bars_produced > 0 else 0

            # Update running OHLC
            if price_scaled > self._bar_high:
                self._bar_high = price_scaled
            if price_scaled < self._bar_low:
                self._bar_low = price_scaled
            self._bar_close = price_scaled
            self._trade_count += 1

            needed = self._threshold - self._dollar_acc

            if remaining_dollar >= needed:
                # Enough to close bar
                if remaining_dollar > 0:
                    chunk_qty = remaining_qty * needed // remaining_dollar
                else:
                    chunk_qty = remaining_qty

                self._dollar_acc += needed
                self._vol_acc += chunk_qty
                self._close_bar(ts_exchange)

                remaining_dollar -= needed
                remaining_qty -= chunk_qty
                bars_produced += 1
            else:
                # Trade absorbed into current bar
                self._dollar_acc += remaining_dollar
                self._vol_acc += remaining_qty
                remaining_dollar = 0
                remaining_qty = 0

        # Notify listeners with range
        if bars_produced > 0:
            for cb in self._listeners:
                try:
                    cb(bars_produced, first_bar_index)
                except Exception as e:
                    logger.error("💥 [DOLLAR_BAR] Listener error: {}", e)

        return bars_produced

    def _close_bar(self, ts_close: int) -> None:
        """
        Finalize current bar, write to ring buffer, update indicators. O(1).
        No return value — data lives in the ring buffer.
        """
        close_px = self._bar_close

        # ── O(1) Incremental EMA ──
        if self._ema == 0:
            self._ema = close_px
        else:
            self._ema = (
                close_px * self._ema_alpha_num
                + self._ema * (self._ema_alpha_den - self._ema_alpha_num)
            ) // self._ema_alpha_den

        # ── O(1) Welford Online Variance ──
        self._var_count += 1
        delta = close_px - self._var_mean
        self._var_mean += delta // self._var_count
        delta2 = close_px - self._var_mean
        self._var_m2 += delta * delta2

        # ── Write to ring buffer (in-place, zero alloc) ──
        row = self._buf[self._head]
        row[TS_OPEN] = self._bar_open_ts
        row[TS_CLOSE] = ts_close
        row[OPEN] = self._bar_open_px
        row[HIGH] = self._bar_high
        row[LOW] = self._bar_low
        row[CLOSE] = close_px
        row[VOLUME] = self._vol_acc
        row[DOLLAR_VOL] = self._dollar_acc
        row[EMA] = self._ema
        row[BAR_IDX] = self._bar_count
        # IS_SYNTHETIC already set during init phase

        # Advance
        self._bar_count += 1
        self._head = (self._head + 1) % self._capacity

        # Reset accumulator
        self._dollar_acc = 0
        self._vol_acc = 0
        self._trade_count = 0

        # Pre-zero next slot
        self._buf[self._head].fill(0)

    # ─────────────────────────── Public Accessors ───────────────────────────

    def get_bar_row(self, bar_index: int) -> np.ndarray:
        """
        Zero-copy access to a bar by its monotonic index.

        Returns a numpy int64 array view (not a copy) of the bar's row.
        Use column constants (OPEN, HIGH, LOW, CLOSE, etc.) to index.

        Raises IndexError if bar_index is outside the ring buffer window.
        """
        if bar_index < 0 or bar_index >= self._bar_count:
            raise IndexError(f"bar_index {bar_index} out of range [0, {self._bar_count})")

        oldest_available = max(0, self._bar_count - self._capacity)
        if bar_index < oldest_available:
            raise IndexError(
                f"bar_index {bar_index} evicted from ring buffer "
                f"(oldest available: {oldest_available})"
            )

        # Convert monotonic index to ring buffer position
        offset = bar_index - oldest_available
        ring_pos = (self._head - (self._bar_count - bar_index)) % self._capacity
        return self._buf[ring_pos]

    def get_bars_view(self, start_index: int, count: int) -> np.ndarray:
        """
        Return a view/copy of `count` bars starting from `start_index`.

        If the range wraps around the ring buffer, returns a copy (unavoidable).
        If contiguous, returns a zero-copy view.
        """
        oldest = max(0, self._bar_count - self._capacity)
        if start_index < oldest:
            start_index = oldest
        actual_count = min(count, self._bar_count - start_index)
        if actual_count <= 0:
            return np.empty((0, NCOLS), dtype=np.int64)

        ring_start = (self._head - (self._bar_count - start_index)) % self._capacity
        ring_end = ring_start + actual_count

        if ring_end <= self._capacity:
            # Contiguous — zero-copy view
            return self._buf[ring_start:ring_end]
        else:
            # Wraparound — must concatenate (single alloc)
            tail = self._buf[ring_start:self._capacity]
            head = self._buf[:ring_end - self._capacity]
            return np.concatenate((tail, head))

    @property
    def current_ema(self) -> int:
        """Current EMA value (scaled integer)."""
        return self._ema

    @property
    def current_variance(self) -> int:
        """Online variance of bar close prices. 0 if < 2 bars."""
        if self._var_count < 2:
            return 0
        return self._var_m2 // (self._var_count - 1)

    @property
    def bar_count(self) -> int:
        """Total bars closed since engine start (monotonic, never wraps)."""
        return self._bar_count

    @property
    def capacity(self) -> int:
        """Ring buffer capacity."""
        return self._capacity

    @property
    def current_accumulator_pct(self) -> int:
        """Current bar fill percentage (0-100)."""
        if self._threshold <= 0:
            return 0
        return self._dollar_acc * 100 // self._threshold

    @property
    def is_bar_open(self) -> bool:
        """True if there's a partially filled bar in progress."""
        return self._trade_count > 0

    @property
    def buf(self) -> np.ndarray:
        """Direct access to the ring buffer for advanced consumers."""
        return self._buf
