# src/domain/gravitational_anchor.py
"""
[GEKTOR APEX v5.0] Gravitational Anchor — Macro Regime Classifier.

Monitors BTC/ETH (the "gravitational leaders") for phase transitions.
When the leader experiences a shock (rapid price move exceeding threshold),
ALL altcoin trading is blocked via Systemic Blackout.

Philosophy: On macro impulses, cross-asset correlation → 1.0.
What the radar classifies as "alpha" on OPUSDT is just lagged structural
noise from BTC. Market makers retract liquidity instantly on leader shocks.
An IOC order into a liquidity vacuum is capital destruction.

Physics: O(1) amortized tick ingestion. Zero allocations in hot path.
The `is_blackout` check is a single monotonic comparison — safe to call
from synchronous Pre-Flight Check without any async overhead.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Final

from loguru import logger


@dataclass(frozen=True, slots=True)
class MacroRegimeConfig:
    """Tunable knobs for the Gravitational Anchor."""

    shock_threshold_bps: int = 35
    """Minimum move in basis points to trigger blackout (0.35%)."""

    time_window_sec: float = 1.5
    """Rolling window for shock detection (seconds)."""

    blackout_duration_sec: float = 5.0
    """Duration of Systemic Blackout after a shock is detected."""

    spread_shock_threshold_bps: int = 100
    """BBO spread widening threshold that independently triggers blackout.
    When leaders' spreads blow out, it means MMs are pulling quotes."""


class GravitationalAnchor:
    """
    Macro-regime fuse. Analyzes leader ticks (BTC/ETH) for phase transitions.
    Blocks altcoin trading during structural liquidity collapse.

    Thread-safety: This class is designed for single-threaded asyncio.
    All mutations happen in the same event loop tick (ingest_leader_tick
    is called from the L1/L2 callback). The `is_blackout` property is
    a synchronous O(1) read — safe for Pre-Flight Check.
    """
    __slots__ = (
        '_config', '_leaders', '_blackout_until',
        '_last_shock_leader', '_last_shock_drift_bps',
        '_total_blackouts',
    )

    _TICK_BUFFER_SIZE: Final[int] = 500  # Max ticks per leader in rolling window

    def __init__(self, config: MacroRegimeConfig | None = None) -> None:
        self._config = config or MacroRegimeConfig()
        # leader_symbol → deque of (monotonic_time, price_scaled)
        self._leaders: dict[str, deque[tuple[float, int]]] = {}
        self._blackout_until: float = 0.0
        self._last_shock_leader: str = ""
        self._last_shock_drift_bps: int = 0
        self._total_blackouts: int = 0

    def register_leader(self, symbol: str) -> None:
        """Register a symbol as gravitational leader (call at init)."""
        if symbol not in self._leaders:
            self._leaders[symbol] = deque(maxlen=self._TICK_BUFFER_SIZE)

    def ingest_leader_tick(self, symbol: str, price_scaled: int) -> None:
        """
        Ingest L1 trade or mid-price tick from a leader symbol.

        Called from the WS callback — runs in the same event loop tick.
        O(1) amortized: append + popleft from deque.

        Args:
            symbol: Leader symbol (e.g. "BTCUSDT").
            price_scaled: Price multiplied by SCALE (integer arithmetic).
        """
        buf = self._leaders.get(symbol)
        if buf is None:
            return  # Not a registered leader — ignore silently

        now = time.monotonic()
        buf.append((now, price_scaled))

        # Evict stale ticks outside the rolling window
        cutoff = now - self._config.time_window_sec
        while buf and buf[0][0] < cutoff:
            buf.popleft()

        # Calculate price drift within window
        if len(buf) < 2:
            return

        oldest_price = buf[0][1]
        if oldest_price <= 0:
            return

        drift_bps = abs(price_scaled - oldest_price) * 10_000 // oldest_price

        if drift_bps >= self._config.shock_threshold_bps:
            self._activate_blackout(symbol, drift_bps, now)

    def ingest_leader_spread(
        self, symbol: str, bid_scaled: int, ask_scaled: int,
    ) -> None:
        """
        Independent spread-based shock detector.

        When market makers pull quotes, BBO spread explodes before
        the mid-price even moves. This catches the liquidity vacuum
        ~50ms earlier than pure price-based detection.
        """
        if bid_scaled <= 0:
            return
        spread_bps = (ask_scaled - bid_scaled) * 10_000 // bid_scaled
        if spread_bps >= self._config.spread_shock_threshold_bps:
            self._activate_blackout(
                symbol, spread_bps, time.monotonic(), reason="SPREAD_BLOWOUT",
            )

    def _activate_blackout(
        self,
        symbol: str,
        drift_bps: int,
        now: float,
        *,
        reason: str = "PRICE_SHOCK",
    ) -> None:
        """Activate Systemic Blackout. Idempotent — extends if already active."""
        new_deadline = now + self._config.blackout_duration_sec

        # Only log if this is a NEW blackout or a significant extension
        if new_deadline > self._blackout_until + 0.5:
            self._total_blackouts += 1
            logger.critical(
                "🌑 [GRAVITATIONAL ANCHOR] SYSTEMIC BLACKOUT #{} | "
                "Leader: {} | {}: {} bps | Duration: {:.1f}s",
                self._total_blackouts,
                symbol,
                reason,
                drift_bps,
                self._config.blackout_duration_sec,
            )

        self._blackout_until = max(self._blackout_until, new_deadline)
        self._last_shock_leader = symbol
        self._last_shock_drift_bps = drift_bps

    @property
    def is_blackout(self) -> bool:
        """
        Synchronous O(1) check. Safe for Pre-Flight Check (no awaits).

        Returns True if the market is in a phase transition state
        where altcoin alpha signals are structurally invalid.
        """
        return time.monotonic() < self._blackout_until

    @property
    def blackout_remaining_sec(self) -> float:
        """Seconds remaining in current blackout (0.0 if not active)."""
        remaining = self._blackout_until - time.monotonic()
        return max(0.0, remaining)

    @property
    def last_shock_info(self) -> tuple[str, int]:
        """(leader_symbol, drift_bps) of the last shock that triggered blackout."""
        return self._last_shock_leader, self._last_shock_drift_bps

    @property
    def total_blackouts(self) -> int:
        """Total number of blackout activations since process start."""
        return self._total_blackouts
