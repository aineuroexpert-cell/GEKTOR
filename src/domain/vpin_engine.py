"""
[GEKTOR APEX v3.6.0 "APEX-RADAR"] O(1) VPIN Engine with Iceberg/Absorption detection.

This file is the **single source of truth** for the VPIN math layer.

CRITICAL INVARIANTS (do NOT break without updating tests/regression/test_vpin_invariants.py):
  I1.  oldest_idx (price_history) is read AFTER the ring index increments, so it
       points to the oldest stored bar. Reading it BEFORE the increment yields
       the bar we just wrote, which destroys absorption detection.
  I2.  z_history (Z-Score buffer) is INDEPENDENT from the VPIN window buffer.
       Z-Score history can be much larger (e.g. 500 bars) without affecting the
       VPIN window itself.
  I3.  During Z-Score warmup, the divisor is the actual number of populated
       slots (_z_count), NOT the buffer capacity. Using the capacity inflates
       sigma underflow and produces FALSE anomalies for the first ~500 bars.
  I4.  Time-decay must be applied CONSISTENTLY to scalar accumulators AND to
       the underlying numpy arrays. If one is decayed and the other is not,
       the invariant `sum == np.sum(array)` breaks irrecoverably and VPIN
       becomes hallucinatory after a single decay event.
  I5.  Polarity: `is_buyer_maker=True` means the TAKER sold to a resting buy
       order, contributing to sell_volume_usd. This is set upstream in
       conflation.py:96-101 and bybit_ws_ingestion.py:56. Do not invert.

ANTI-PATTERNS (forbidden in this file):
  - `import` statements inside `process_bar()` (hot path).
  - `np.sqrt` on scalars (use `math.sqrt`, pure C, no boxing overhead).
  - `_imbalances.fill(0.0)` in reset_o1 (numpy arrays are reused via ring index).
  - Re-allocating numpy arrays after __init__.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass

import numpy as np
from loguru import logger

from src.domain.conflation import DollarBar
from src.shared.alpha_config import alpha


@dataclass(slots=True)
class VPINSignal:
    vpin_value: float
    z_score: float
    is_anomaly: bool
    absorption_detected: bool  # Iceberg / hidden liquidity guard
    direction: str  # "long" | "short" | "neutral"


class O1VPINEngine:
    """O(1) NumPy Ring Buffer VPIN engine.

    Eliminates GC overhead and Event Loop jitter under load. All buffers are
    pre-allocated at __init__ and reused via a ring index.

    See module docstring for invariants I1-I5.
    """

    __slots__ = (
        "window_size",
        "z_history_size",
        "volume_threshold",
        "_anomaly_threshold_z",
        # VPIN window state
        "_imbalances",
        "_volumes",
        "_price_history",
        "_index",
        "_is_filled",
        "_running_imbalance_sum",
        "_running_volume_sum",
        # Z-Score independent buffer
        "_vpin_history",
        "_z_index",
        "_z_count",
        "_vpin_sum",
        "_vpin_sq_sum",
        # Time-decay state
        "_last_update_time",
        # Periodic rebuild counter (against IEEE-754 drift)
        "_bars_since_rebuild",
        "_rebuild_interval",
        # v3.6.4 noise filters
        "cvd_filter_enable",
        "cvd_min_ratio",
        "adaptive_z_enable",
        "adaptive_z_volatility_base",
        "adaptive_z_sensitivity",
        "adaptive_z_min_mult",
        "adaptive_z_max_mult",
        "_net_imbalances",
        "_running_net_imbalance_sum",
    )

    def __init__(
        self,
        window_size: int = 50,
        volume_threshold: float = 1_000_000.0,
        z_threshold: float = 2.5,
        z_history_size: int = 500,
        rebuild_interval: int = 10_000,
        # v3.6.4 parameters
        cvd_filter_enable: bool = False,
        cvd_min_ratio: float = 0.15,
        adaptive_z_enable: bool = False,
        adaptive_z_volatility_base: float = 0.01,
        adaptive_z_sensitivity: float = 0.5,
        adaptive_z_min_mult: float = 0.8,
        adaptive_z_max_mult: float = 2.0,
    ) -> None:
        if window_size < 2:
            raise ValueError("window_size must be >= 2")
        if z_history_size < window_size:
            raise ValueError("z_history_size must be >= window_size")

        self.window_size = window_size
        self.z_history_size = z_history_size
        self.volume_threshold = volume_threshold
        self._anomaly_threshold_z = z_threshold
        self._rebuild_interval = rebuild_interval

        # v3.6.4 noise filters
        self.cvd_filter_enable = cvd_filter_enable
        self.cvd_min_ratio = cvd_min_ratio
        self.adaptive_z_enable = adaptive_z_enable
        self.adaptive_z_volatility_base = adaptive_z_volatility_base
        self.adaptive_z_sensitivity = adaptive_z_sensitivity
        self.adaptive_z_min_mult = adaptive_z_min_mult
        self.adaptive_z_max_mult = adaptive_z_max_mult

        # Pre-allocated buffers — never re-allocated after this point.
        self._imbalances = np.zeros(window_size, dtype=np.float64)
        self._net_imbalances = np.zeros(window_size, dtype=np.float64)
        self._volumes = np.zeros(window_size, dtype=np.float64)
        self._price_history = np.zeros(window_size, dtype=np.float64)
        self._vpin_history = np.zeros(z_history_size, dtype=np.float64)

        self.reset_o1()

    def reset_o1(self) -> None:
        """O(1) Flush Protocol. No memory re-allocation; numpy arrays are reused."""
        self._index = 0
        self._is_filled = False
        self._running_imbalance_sum = 0.0
        self._running_net_imbalance_sum = 0.0
        self._running_volume_sum = 0.0
        self._z_index = 0
        self._z_count = 0
        self._vpin_sum = 0.0
        self._vpin_sq_sum = 0.0
        self._last_update_time = time.monotonic()
        self._bars_since_rebuild = 0
        # Numpy arrays are reused via the ring index — the old values get
        # overwritten when we lap the buffer. See invariant I-noFill.

    def _apply_time_decay(self, decay: float) -> None:
        """Apply exponential decay to BOTH scalars and arrays (invariant I4).

        Without this, delta-updates `sum += (new - old)` reference undecayed
        `old` values from the array, breaking sum/array consistency forever.
        """
        # Decay arrays first, then rebuild scalar sums from arrays. This is
        # O(N) but only fires on a time-gap event (rare, hours), not in the
        # hot path.
        self._imbalances *= decay
        self._net_imbalances *= decay
        self._volumes *= decay
        self._vpin_history *= decay
        self._running_imbalance_sum = float(self._imbalances.sum())
        self._running_net_imbalance_sum = float(self._net_imbalances.sum())
        self._running_volume_sum = float(self._volumes.sum())
        self._vpin_sum = float(self._vpin_history.sum())
        self._vpin_sq_sum = float((self._vpin_history**2).sum())

    def _rebuild_sums(self) -> None:
        """Periodic O(N) rebuild of running sums against IEEE-754 drift.

        Delta-updates `sum += (new - old)` accumulate rounding error after
        millions of bars. We rebuild from the canonical arrays every
        `_rebuild_interval` bars. This is invariant I-noDrift.
        """
        self._running_imbalance_sum = float(self._imbalances.sum())
        self._running_net_imbalance_sum = float(self._net_imbalances.sum())
        self._running_volume_sum = float(self._volumes.sum())
        self._vpin_sum = float(self._vpin_history.sum())
        self._vpin_sq_sum = float((self._vpin_history**2).sum())
        self._bars_since_rebuild = 0

    def process_bar(self, bar: DollarBar) -> VPINSignal | None:
        """Ingest a closed dollar bar and emit a VPIN signal (or None during warmup).

        Hot path: no imports here, no allocations, O(1) per bar.
        """
        buy_vol = bar.buy_volume_usd
        sell_vol = bar.sell_volume_usd
        price = bar.close

        imbalance = buy_vol - sell_vol
        abs_imbalance = abs(imbalance)

        # --- Time-decay guard (rare path) ---
        now = time.monotonic()
        time_delta = now - self._last_update_time
        self._last_update_time = now
        if time_delta > alpha.VPIN_TIME_GAP_SEC:
            decay = math.exp(-time_delta / (3600.0 * max(alpha.VPIN_DECAY_TAU_HOURS, 1)))
            self._apply_time_decay(decay)
            # Issue WARNING only for extreme gaps (e.g. > 4h), use DEBUG for expected slow trading intervals
            if time_delta > 14400.0:
                logger.warning(
                    f"[VPIN] Extreme time gap detected ({time_delta / 3600:.1f}h). "
                    f"Stats decayed by {100 * (1 - decay):.1f}%."
                )
            else:
                logger.debug(
                    f"[VPIN] Time gap detected ({time_delta / 60:.1f}m). "
                    f"Stats decayed by {100 * (1 - decay):.1f}%."
                )

        # --- VPIN window ring buffer (O(1)) ---
        current_idx = self._index
        old_abs_imbalance = self._imbalances[current_idx]
        old_net_imbalance = self._net_imbalances[current_idx]
        old_volume = self._volumes[current_idx]

        self._running_imbalance_sum += (abs_imbalance - old_abs_imbalance)
        self._running_net_imbalance_sum += (imbalance - old_net_imbalance)
        self._running_volume_sum += (bar.volume_usd - old_volume)

        self._imbalances[current_idx] = abs_imbalance
        self._net_imbalances[current_idx] = imbalance
        self._volumes[current_idx] = bar.volume_usd
        self._price_history[current_idx] = price

        self._index += 1
        if self._index >= self.window_size:
            self._index = 0
            self._is_filled = True

        # Invariant I1: oldest_idx read AFTER increment.
        oldest_idx = self._index
        price_start_window = self._price_history[oldest_idx]

        if not self._is_filled:
            return None

        # --- VPIN value ---
        total_volume = self._running_volume_sum
        if total_volume <= 0.0:
            total_volume = 1.0
        current_vpin = self._running_imbalance_sum / total_volume
        # Clamp to [0, 1] — VPIN is a probability by construction; tiny
        # negative values can arise from float drift.
        if current_vpin < 0.0:
            current_vpin = 0.0
        elif current_vpin > 1.0:
            current_vpin = 1.0

        # --- Z-Score independent ring buffer (O(1)) ---
        z_idx = self._z_index
        old_vpin = self._vpin_history[z_idx]
        self._vpin_sum += (current_vpin - old_vpin)
        self._vpin_sq_sum += (current_vpin**2 - old_vpin**2)
        self._vpin_history[z_idx] = current_vpin
        self._z_index += 1
        if self._z_index >= self.z_history_size:
            self._z_index = 0
        if self._z_count < self.z_history_size:
            self._z_count += 1

        # Invariant I3: divisor is _z_count, not z_history_size.
        n = self._z_count if self._z_count > 0 else 1
        mean_vpin = self._vpin_sum / n
        variance = (self._vpin_sq_sum / n) - (mean_vpin * mean_vpin)
        # Guard against IEEE-754-induced negative variance.
        variance = max(variance, 1e-12)
        std_dev = math.sqrt(variance)

        z_score = float((current_vpin - mean_vpin) / std_dev)

        # --- Volatility-Adaptive Z-Score (P10) ---
        target_z_threshold = self._anomaly_threshold_z
        if self.adaptive_z_enable:
            max_price = float(np.max(self._price_history))
            min_price = float(np.min(self._price_history))
            if min_price > 0.0:
                volatility = (max_price - min_price) / min_price
                volatility_ratio = volatility / self.adaptive_z_volatility_base
                multiplier = 1.0 + self.adaptive_z_sensitivity * (volatility_ratio - 1.0)
                multiplier = max(self.adaptive_z_min_mult, min(self.adaptive_z_max_mult, multiplier))
                target_z_threshold = self._anomaly_threshold_z * multiplier

        is_anomaly = bool(z_score > target_z_threshold)

        # --- CVD Divergence Filter (P8) ---
        if self.cvd_filter_enable:
            net_ratio = abs(self._running_net_imbalance_sum) / total_volume
            if net_ratio < self.cvd_min_ratio:
                is_anomaly = False

        # --- Periodic O(N) rebuild against drift (invariant I-noDrift) ---
        self._bars_since_rebuild += 1
        if self._bars_since_rebuild >= self._rebuild_interval:
            self._rebuild_sums()

        # --- Absorption / Iceberg filter (invariant I1 in action) ---
        price_return = float(price - price_start_window)
        absorption_detected = False
        direction = "neutral"
        if is_anomaly:
            if imbalance > 0:
                direction = "long"
                if price_return <= 0:
                    absorption_detected = True  # Hidden seller (bearish iceberg).
            elif imbalance < 0:
                direction = "short"
                if price_return >= 0:
                    absorption_detected = True  # Hidden buyer (bullish iceberg).

        return VPINSignal(
            vpin_value=float(current_vpin),
            z_score=z_score,
            is_anomaly=is_anomaly,
            absorption_detected=bool(absorption_detected),
            direction=direction,
        )
