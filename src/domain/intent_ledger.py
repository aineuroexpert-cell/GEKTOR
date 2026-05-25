# src/domain/intent_ledger.py
"""
[GEKTOR APEX v5.0] Intent Versioning Ledger — Zero-Trust Signal Gate.

Solves the Ghost Intent problem: operator clicks a stale EXECUTE button
after the backend has already revoked the signal (e.g., due to macro shock).

Architecture:
  - Every signal emission gets a monotonically increasing `intent_version` (uint64).
  - The version is embedded in the Telegram/UI payload sent to the operator.
  - When the operator clicks EXECUTE, the request carries the `intent_version`.
  - Backend validates: if `current_version != request_version` → O(1) reject.
  - On revocation (blackout, TTL expiry, alpha decay), version is bumped
    and a REVOKE push is sent to the frontend.

This is a zero-trust contract: the frontend CANNOT execute without a valid
version, and the backend bumps the version atomically on any invalidation.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable, Optional

from loguru import logger


@dataclass(slots=True)
class ActiveIntent:
    """Immutable record of a live signal awaiting operator action."""
    version: int
    symbol: str
    side: str
    price: float
    qty: float
    msq: tuple[int, int] | None
    emitted_mono: float
    ttl_sec: float
    revoked: bool = False
    revoke_reason: str = ""


class IntentLedger:
    """
    Monotonic Intent Versioning with Push-Revoke.

    Single point of truth for whether an operator action is still valid.
    All checks are O(1) — a single integer comparison.

    Thread-safety: designed for single-threaded asyncio event loop.
    All mutations happen synchronously in the same tick.
    """
    __slots__ = (
        '_version', '_active', '_revoke_callback',
        '_total_revokes', '_total_stale_clicks',
    )

    def __init__(
        self,
        revoke_callback: Callable[[int, str, str], Any] | None = None,
    ) -> None:
        """
        Args:
            revoke_callback: Called synchronously on revocation.
                Signature: (version, symbol, reason) -> None.
                Used to push REVOKE event to frontend (Telegram/WS).
        """
        self._version: int = 0
        self._active: ActiveIntent | None = None
        self._revoke_callback = revoke_callback
        self._total_revokes: int = 0
        self._total_stale_clicks: int = 0

    def emit_intent(
        self,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        *,
        msq: tuple[int, int] | None = None,
        ttl_sec: float = 5.0,
    ) -> int:
        """
        Register a new signal intent. Atomically bumps version.

        Any previously active intent is implicitly revoked (version mismatch).

        Returns:
            The new intent_version (embed this in the UI payload).
        """
        self._version += 1
        self._active = ActiveIntent(
            version=self._version,
            symbol=symbol,
            side=side,
            price=price,
            qty=qty,
            msq=msq,
            emitted_mono=time.monotonic(),
            ttl_sec=ttl_sec,
        )
        logger.info(
            "📋 [INTENT] v{} emitted: {} {} {} @ {:.4f} (TTL: {:.1f}s)",
            self._version, side, qty, symbol, price, ttl_sec,
        )
        return self._version

    def validate_click(self, intent_version: int) -> ActiveIntent | None:
        """
        O(1) validation of an operator click.

        Returns the ActiveIntent if valid, None if stale/expired/revoked.
        This is the ONLY gate between operator action and execute_strike().
        """
        if self._active is None:
            self._total_stale_clicks += 1
            logger.warning(
                "👻 [GHOST CLICK] v{} — no active intent. Rejected.",
                intent_version,
            )
            return None

        # Version mismatch — stale click
        if intent_version != self._active.version:
            self._total_stale_clicks += 1
            logger.warning(
                "👻 [STALE CLICK] Operator clicked v{}, current is v{}. "
                "Δ={} versions behind. Rejected.",
                intent_version,
                self._active.version,
                self._active.version - intent_version,
            )
            return None

        # Already revoked
        if self._active.revoked:
            self._total_stale_clicks += 1
            logger.warning(
                "👻 [REVOKED CLICK] v{} was revoked: {}. Rejected.",
                intent_version,
                self._active.revoke_reason,
            )
            return None

        # TTL expired
        elapsed = time.monotonic() - self._active.emitted_mono
        if elapsed > self._active.ttl_sec:
            self._total_stale_clicks += 1
            self.revoke("TTL_EXPIRED")
            logger.warning(
                "⏰ [TTL CLICK] v{} expired ({:.1f}s > {:.1f}s TTL). Rejected.",
                intent_version, elapsed, self._active.ttl_sec,
            )
            return None

        return self._active

    def revoke(self, reason: str) -> None:
        """
        Atomically revoke the current intent and push notification.

        Called by:
          - GravitationalAnchor (macro shock)
          - Alpha decay detection
          - TTL expiry watchdog
          - Manual operator cancel

        The version is bumped so that any in-flight click is guaranteed
        to fail the O(1) comparison in validate_click().
        """
        if self._active is None or self._active.revoked:
            return

        old_version = self._active.version
        symbol = self._active.symbol

        self._active.revoked = True
        self._active.revoke_reason = reason
        self._total_revokes += 1

        # Bump version — any in-flight click referencing old_version
        # will fail the integer comparison in validate_click()
        self._version += 1

        logger.warning(
            "🚫 [REVOKE] v{} for {} revoked: {}. New version: v{}",
            old_version, symbol, reason, self._version,
        )

        # Push REVOKE to frontend (Telegram edit / WS push)
        if self._revoke_callback is not None:
            try:
                self._revoke_callback(old_version, symbol, reason)
            except Exception as e:
                logger.error(
                    "💥 [REVOKE PUSH] Failed to notify frontend: {}", e,
                )

    @property
    def current_version(self) -> int:
        """Current monotonic version counter."""
        return self._version

    @property
    def has_active_intent(self) -> bool:
        """True if there's a non-revoked, non-expired intent."""
        if self._active is None or self._active.revoked:
            return False
        elapsed = time.monotonic() - self._active.emitted_mono
        return elapsed <= self._active.ttl_sec

    @property
    def active_intent(self) -> ActiveIntent | None:
        """The current active intent, or None."""
        return self._active

    @property
    def stats(self) -> dict[str, int]:
        """Telemetry counters."""
        return {
            "total_revokes": self._total_revokes,
            "total_stale_clicks": self._total_stale_clicks,
            "current_version": self._version,
        }
