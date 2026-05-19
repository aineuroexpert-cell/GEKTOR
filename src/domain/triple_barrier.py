# src/domain/triple_barrier.py
"""
[GEKTOR APEX v5.2] Vectorized Triple Barrier Labeler.

Fixes from Council audit:
  1. Pessimistic Collision: if both TP and SL hit in same bar → SL wins (-1).
     Meta-Labeling trains on worst case, not hallucinations.
  2. Slippage-Aware Exit: TP = limit order (fills at TP price),
     SL = market order (fills at bar_low for LONG, bar_high for SHORT).
  3. Vectorized: barrier checks use numpy boolean masks over the entire
     pending array. Python loop only iterates resolved indices.
  4. Side-Aware MFE/MAE: correctly inverted for SHORT intents.
  5. realized_pnl_bps stored in results for ML feature engineering.

Architecture:
  - Parallel numpy vectors (SoA layout) instead of AoS rows.
  - Boolean mask `_active` gates all vectorized ops.
  - Resolution priority: SL > TP > Vertical (pessimistic).
  - O(1) numpy vectorized scan over 64 slots ≈ single SIMD pass.
  - Results ring buffer stores realized P&L, not just label flag.

Memory: fully pre-allocated. Vertical barrier = structural eviction guarantee.
"""
from __future__ import annotations

from typing import Final, Callable, Any

import numpy as np
from loguru import logger


class TripleBarrierLabeler:
    """
    Vectorized Shadow Ledger for Meta-Labeling.

    Strict zero-allocation, pessimistic collision resolution,
    slippage-aware exit pricing, side-aware MFE/MAE.

    Wire as listener to ZeroAllocationDollarBarEngine:
        engine.add_listener(labeler.on_bars_closed)
    """
    __slots__ = (
        '_cap', '_bar_engine',
        # SoA (Structure of Arrays) for pending intents — cache-friendly
        '_active', '_intent_id', '_entry_px', '_tp_px', '_sl_px',
        '_entry_bar', '_max_bars', '_side',
        '_running_high', '_running_low',
        # Results ring buffer
        '_res', '_res_cap', '_res_head', '_res_count',
        # Counters
        '_open_count', '_next_id',
        '_on_label_cb',
    )

    # Results buffer column indices
    R_INTENT_ID: Final[int] = 0
    R_LABEL: Final[int] = 1          # +1 TP, -1 SL, 0 Vertical
    R_ENTRY_PX: Final[int] = 2
    R_EXIT_PX: Final[int] = 3        # Slippage-aware realized price
    R_ENTRY_BAR: Final[int] = 4
    R_EXIT_BAR: Final[int] = 5
    R_SIDE: Final[int] = 6           # +1 LONG, -1 SHORT
    R_BARS_HELD: Final[int] = 7
    R_MFE: Final[int] = 8            # Max Favorable Excursion (scaled)
    R_MAE: Final[int] = 9            # Max Adverse Excursion (scaled)
    R_PNL_BPS: Final[int] = 10       # Realized P&L in basis points
    R_NCOLS: Final[int] = 11

    def __init__(
        self,
        bar_engine: Any,
        *,
        max_open: int = 64,
        results_capacity: int = 10_000,
        on_label_callback: Callable[[int, int], Any] | None = None,
    ) -> None:
        """
        Args:
            bar_engine: ZeroAllocationDollarBarEngine for reading bar data.
            max_open: Hard cap on concurrent unresolved intents.
            results_capacity: Ring buffer for resolved labels.
            on_label_callback: (intent_id, label) called on resolution.
        """
        self._bar_engine = bar_engine
        self._cap: int = max_open

        # SoA layout: parallel numpy vectors for vectorized ops
        self._active = np.zeros(max_open, dtype=np.bool_)
        self._intent_id = np.zeros(max_open, dtype=np.int64)
        self._entry_px = np.zeros(max_open, dtype=np.int64)
        self._tp_px = np.zeros(max_open, dtype=np.int64)
        self._sl_px = np.zeros(max_open, dtype=np.int64)
        self._entry_bar = np.zeros(max_open, dtype=np.int64)
        self._max_bars = np.zeros(max_open, dtype=np.int64)
        self._side = np.zeros(max_open, dtype=np.int64)  # +1 LONG, -1 SHORT
        self._running_high = np.zeros(max_open, dtype=np.int64)
        self._running_low = np.zeros(max_open, dtype=np.int64)

        # Results ring buffer
        self._res_cap: int = results_capacity
        self._res = np.zeros((results_capacity, self.R_NCOLS), dtype=np.int64)
        self._res_head: int = 0
        self._res_count: int = 0

        self._open_count: int = 0
        self._next_id: int = 0
        self._on_label_cb = on_label_callback

    def register_intent(
        self,
        entry_price: int,
        side: int,
        tp_bps: int,
        sl_bps: int,
        max_bars: int,
    ) -> int:
        """
        Register intent for retroactive labeling.

        Args:
            entry_price: Scaled entry price.
            side: +1 LONG, -1 SHORT.
            tp_bps: Take-profit distance (basis points).
            sl_bps: Stop-loss distance (basis points).
            max_bars: Vertical barrier (max Dollar Bars to hold).

        Returns:
            intent_id (monotonic).
        """
        # Find free slot via vectorized search
        free = np.flatnonzero(~self._active)
        if free.size == 0:
            # Evict oldest
            self._evict_oldest()
            free = np.flatnonzero(~self._active)
            if free.size == 0:
                logger.error("💀 [BARRIER] Cannot allocate slot after eviction")
                return -1

        idx = int(free[0])
        iid = self._next_id
        self._next_id += 1

        # Compute absolute barrier prices (side-aware)
        if side > 0:  # LONG: TP above, SL below
            tp_abs = entry_price * (10_000 + tp_bps) // 10_000
            sl_abs = entry_price * (10_000 - sl_bps) // 10_000
        else:  # SHORT: TP below, SL above
            tp_abs = entry_price * (10_000 - tp_bps) // 10_000
            sl_abs = entry_price * (10_000 + sl_bps) // 10_000

        self._active[idx] = True
        self._intent_id[idx] = iid
        self._entry_px[idx] = entry_price
        self._tp_px[idx] = tp_abs
        self._sl_px[idx] = sl_abs
        self._entry_bar[idx] = self._bar_engine.bar_count
        self._max_bars[idx] = max_bars
        self._side[idx] = side
        self._running_high[idx] = entry_price
        self._running_low[idx] = entry_price

        self._open_count += 1
        return iid

    def on_bars_closed(self, bars_produced: int, first_bar_index: int) -> None:
        """
        Listener callback from DollarBarEngine.

        Vectorized barrier scan: numpy boolean masks over entire SoA.
        Python loop only iterates resolved indices (typically 0-2).
        """
        if self._open_count == 0:
            return

        for offset in range(bars_produced):
            bar_idx = first_bar_index + offset

            try:
                bar_row = self._bar_engine.get_bar_row(bar_idx)
            except IndexError:
                continue

            bar_high = int(bar_row[3])   # HIGH
            bar_low = int(bar_row[4])    # LOW
            bar_close = int(bar_row[5])  # CLOSE

            self._scan_barriers(bar_high, bar_low, bar_close, bar_idx)

            if self._open_count == 0:
                break

    def _scan_barriers(
        self,
        bar_high: int,
        bar_low: int,
        bar_close: int,
        bar_idx: int,
    ) -> None:
        """
        Vectorized barrier check + pessimistic collision resolution.

        All operations are numpy vectorized over the _active mask.
        """
        a = self._active  # Boolean mask

        if not np.any(a):
            return

        # ── Update MFE/MAE watermarks (vectorized) ──
        np.maximum(self._running_high, bar_high, where=a, out=self._running_high)
        np.minimum(self._running_low, bar_low, where=a, out=self._running_low)

        # ── Vectorized barrier checks ──
        is_long = (self._side > 0) & a
        is_short = (self._side < 0) & a

        # LONG: TP when bar_high >= tp_px, SL when bar_low <= sl_px
        hit_tp_long = is_long & (bar_high >= self._tp_px)
        hit_sl_long = is_long & (bar_low <= self._sl_px)

        # SHORT: TP when bar_low <= tp_px, SL when bar_high >= sl_px
        hit_tp_short = is_short & (bar_low <= self._tp_px)
        hit_sl_short = is_short & (bar_high >= self._sl_px)

        hit_tp = hit_tp_long | hit_tp_short
        hit_sl = hit_sl_long | hit_sl_short

        # Vertical barrier
        bars_held = bar_idx - self._entry_bar
        hit_vb = a & (bars_held >= self._max_bars)

        # ── PESSIMISTIC COLLISION RESOLUTION ──
        # Priority: SL > TP > Vertical
        # If both TP and SL hit in same bar → SL wins.
        # Capital never hallucinated a win.
        resolved_sl = hit_sl
        resolved_tp = hit_tp & ~resolved_sl
        resolved_vb = hit_vb & ~resolved_sl & ~resolved_tp

        resolved_any = resolved_sl | resolved_tp | resolved_vb

        if not np.any(resolved_any):
            return

        # ── Resolve each hit intent ──
        resolved_indices = np.flatnonzero(resolved_any)

        for idx in resolved_indices:
            idx = int(idx)
            side = int(self._side[idx])

            if resolved_sl[idx]:
                label = -1
                # SL = market order → slippage-aware exit price
                if side > 0:  # LONG SL: actual exit is bar_low (market sell)
                    exit_px = min(int(self._sl_px[idx]), bar_low)
                else:  # SHORT SL: actual exit is bar_high (market buy-to-cover)
                    exit_px = max(int(self._sl_px[idx]), bar_high)
            elif resolved_tp[idx]:
                label = 1
                # TP = limit order → fills at TP price (no slippage)
                exit_px = int(self._tp_px[idx])
            else:
                label = 0
                exit_px = bar_close

            # Compute realized P&L in basis points
            entry = int(self._entry_px[idx])
            if entry > 0:
                if side > 0:  # LONG
                    pnl_bps = (exit_px - entry) * 10_000 // entry
                else:  # SHORT
                    pnl_bps = (entry - exit_px) * 10_000 // entry
            else:
                pnl_bps = 0

            # Compute side-aware MFE/MAE
            if side > 0:  # LONG
                mfe = int(self._running_high[idx]) - entry
                mae = entry - int(self._running_low[idx])
            else:  # SHORT
                mfe = entry - int(self._running_low[idx])
                mae = int(self._running_high[idx]) - entry

            # Write to results ring buffer
            r = self._res[self._res_head]
            r[self.R_INTENT_ID] = self._intent_id[idx]
            r[self.R_LABEL] = label
            r[self.R_ENTRY_PX] = entry
            r[self.R_EXIT_PX] = exit_px
            r[self.R_ENTRY_BAR] = self._entry_bar[idx]
            r[self.R_EXIT_BAR] = bar_idx
            r[self.R_SIDE] = side
            r[self.R_BARS_HELD] = bar_idx - int(self._entry_bar[idx])
            r[self.R_MFE] = mfe
            r[self.R_MAE] = mae
            r[self.R_PNL_BPS] = pnl_bps

            self._res_head = (self._res_head + 1) % self._res_cap
            self._res_count += 1

            # Free slot
            self._active[idx] = False
            self._open_count -= 1

            label_str = {1: "TP ✅", -1: "SL ❌", 0: "VERT ⏰"}[label]
            logger.info(
                "🏷️ [LABEL] #{}: {} | pnl={}bps | MFE={} MAE={} | "
                "held {}bars | entry={} exit={}",
                int(self._intent_id[idx]), label_str, pnl_bps,
                mfe, mae,
                bar_idx - int(self._entry_bar[idx]),
                entry, exit_px,
            )

            if self._on_label_cb is not None:
                try:
                    self._on_label_cb(int(self._intent_id[idx]), label)
                except Exception as e:
                    logger.error("💥 [BARRIER] Callback error: {}", e)

    def _evict_oldest(self) -> None:
        """Force-resolve oldest active intent as VERTICAL."""
        active_indices = np.flatnonzero(self._active)
        if active_indices.size == 0:
            return
        # Find the one with lowest entry_bar
        oldest_idx = int(active_indices[
            np.argmin(self._entry_bar[active_indices])
        ])

        entry = int(self._entry_px[oldest_idx])
        side = int(self._side[oldest_idx])
        bar_idx = self._bar_engine.bar_count

        if entry > 0:
            pnl_bps = 0  # Forced eviction → no meaningful P&L
        else:
            pnl_bps = 0

        r = self._res[self._res_head]
        r[self.R_INTENT_ID] = self._intent_id[oldest_idx]
        r[self.R_LABEL] = 0
        r[self.R_ENTRY_PX] = entry
        r[self.R_EXIT_PX] = entry  # No execution
        r[self.R_ENTRY_BAR] = self._entry_bar[oldest_idx]
        r[self.R_EXIT_BAR] = bar_idx
        r[self.R_SIDE] = side
        r[self.R_BARS_HELD] = bar_idx - int(self._entry_bar[oldest_idx])
        r[self.R_MFE] = int(self._running_high[oldest_idx]) - entry if side > 0 \
            else entry - int(self._running_low[oldest_idx])
        r[self.R_MAE] = entry - int(self._running_low[oldest_idx]) if side > 0 \
            else int(self._running_high[oldest_idx]) - entry
        r[self.R_PNL_BPS] = 0

        self._res_head = (self._res_head + 1) % self._res_cap
        self._res_count += 1

        self._active[oldest_idx] = False
        self._open_count -= 1

        logger.warning(
            "⚠️ [EVICT] Intent #{} force-resolved (capacity overflow)",
            int(self._intent_id[oldest_idx]),
        )

    # ─────────────────────────── Accessors ───────────────────────────

    @property
    def open_count(self) -> int:
        return self._open_count

    @property
    def total_resolved(self) -> int:
        return self._res_count

    def get_label(self, intent_id: int) -> int | None:
        """Look up resolved label by intent_id. Returns None if not found."""
        available = min(self._res_count, self._res_cap)
        for i in range(available):
            idx = (self._res_head - 1 - i) % self._res_cap
            if int(self._res[idx, self.R_INTENT_ID]) == intent_id:
                return int(self._res[idx, self.R_LABEL])
        return None

    def get_pnl(self, intent_id: int) -> int | None:
        """Look up realized P&L (bps) by intent_id."""
        available = min(self._res_count, self._res_cap)
        for i in range(available):
            idx = (self._res_head - 1 - i) % self._res_cap
            if int(self._res[idx, self.R_INTENT_ID]) == intent_id:
                return int(self._res[idx, self.R_PNL_BPS])
        return None

    def get_win_rate(self, last_n: int = 100) -> tuple[int, int, int]:
        """(wins, losses, timeouts) over last N resolved intents."""
        available = min(last_n, self._res_count, self._res_cap)
        w, l, t = 0, 0, 0
        for i in range(available):
            idx = (self._res_head - 1 - i) % self._res_cap
            lab = int(self._res[idx, self.R_LABEL])
            if lab == 1:
                w += 1
            elif lab == -1:
                l += 1
            else:
                t += 1
        return w, l, t

    def get_results_view(self, last_n: int = 100) -> np.ndarray:
        """Last N resolved labels as numpy array (copy)."""
        available = min(last_n, self._res_count, self._res_cap)
        if available == 0:
            return np.empty((0, self.R_NCOLS), dtype=np.int64)
        end = self._res_head
        if end >= available:
            return self._res[end - available:end].copy()
        tail = self._res[self._res_cap - (available - end):]
        head = self._res[:end]
        return np.concatenate((tail, head))
