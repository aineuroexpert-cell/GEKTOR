# src/application/operator_gate.py
"""
[GEKTOR v15.1] Operator Execution Gate — Unified Tilt + Capsule Enforcement.

Integrates CognitiveSentinel (tilt detection) with CapsuleValidator
(cryptographic intent verification) into a single execution gate
that sits between the operator and L6 Gateway.

Architecture (Asymmetric Trust Model):

  ┌─────────────────────────────────────────────────────┐
  │                    EXECUTION PIPELINE               │
  │                                                     │
  │  PATH A (Machine / L6 Autonomous):                  │
  │    orchestrator → execute_strike()                  │
  │    ├── GravitationalAnchor.is_blackout  [O(1)]      │
  │    ├── IntentLedger.validate_click      [O(1)]      │
  │    └── MSQ drift check                 [O(1)]      │
  │    Latency: < 1μs total                             │
  │    Trust: IMPLICIT (in-process code path)            │
  │                                                     │
  │  PATH B (Human / Operator via WS):                  │
  │    WS frame → OperatorGate.execute()                │
  │    ├── CapsuleValidator.validate()     [~2μs]       │
  │    │   ├── HMAC-SHA256 verify           │           │
  │    │   ├── Nonce replay check           │           │
  │    │   ├── TTL freshness                │           │
  │    │   ├── Tilt generation match        │           │
  │    │   └── CognitiveSentinel gate       │           │
  │    ├── IntentLedger.validate_click      [O(1)]      │
  │    └── L6 Gateway.execute_strike()                  │
  │    Latency: < 5μs total overhead                    │
  │    Trust: ZERO (cryptographic verification)          │
  │                                                     │
  └─────────────────────────────────────────────────────┘

Thread-safety: Single-writer (asyncio event loop). No locks.
"""

from __future__ import annotations

import time
from typing import Optional, Any
from loguru import logger

from src.domain.tilt_breaker import CognitiveSentinel, TiltState
from src.domain.intent_capsule import (
    CapsuleForge, CapsuleValidator, IntentCapsule, CapsuleVerdict,
)
from src.domain.intent_ledger import IntentLedger


class OperatorGate:
    """
    Unified execution gate for operator-initiated trades.

    Lifecycle:
      1. Created when operator WS session starts
      2. Issues capsules when signals are displayed (issue_capsule)
      3. Validates capsules when operator clicks execute (validate_execution)
      4. Destroyed when WS session ends (ephemeral key dies)

    The gate holds references to domain services but performs
    no I/O itself — it's pure orchestration logic.
    """
    __slots__ = (
        '_forge', '_validator', '_sentinel', '_intent_ledger',
        '_operator_id', '_total_issued', '_total_executed',
    )

    def __init__(
        self,
        sentinel: CognitiveSentinel,
        intent_ledger: IntentLedger,
        operator_id: str = "primary",
        capsule_ttl_sec: float = 1.2,
    ) -> None:
        self._forge = CapsuleForge()
        self._validator = CapsuleValidator(
            forge=self._forge,
            tilt_sentinel=sentinel,
            ttl_sec=capsule_ttl_sec,
        )
        self._sentinel = sentinel
        self._intent_ledger = intent_ledger
        self._operator_id = operator_id
        self._total_issued: int = 0
        self._total_executed: int = 0

    # ──────────────────────────────────────────────────
    # ISSUANCE (called when signal is displayed to operator)
    # ──────────────────────────────────────────────────

    def issue_capsule(
        self,
        signal_id: str,
        symbol: str,
        side: str,
        mono_now: Optional[float] = None,
    ) -> IntentCapsule:
        """
        Mint a signed execution permit for the operator.

        This is called when an APPROVED_EXECUTION signal is pushed
        to the frontend via WebSocket. The capsule is embedded in
        the signal payload — the operator returns it when clicking EXECUTE.

        Args:
            signal_id: UUID of the signal.
            symbol: Trading pair.
            side: BUY or SELL.
            mono_now: Override for testing.

        Returns:
            Signed IntentCapsule (serialize and send via WS).
        """
        tilt_gen = self._sentinel.get_generation()

        capsule = self._forge.issue(
            signal_id=signal_id,
            symbol=symbol,
            side=side,
            tilt_generation=tilt_gen,
            mono_now=mono_now,
        )

        self._total_issued += 1

        logger.debug(
            "🔑 [CAPSULE] Issued #{} for {} {} {} | tilt_gen={} | session={}",
            self._total_issued, side, symbol, signal_id[:8],
            tilt_gen, self._forge.session_id,
        )

        return capsule

    # ──────────────────────────────────────────────────
    # VALIDATION (called when operator clicks EXECUTE)
    # ──────────────────────────────────────────────────

    def validate_execution(
        self,
        capsule: IntentCapsule,
        intent_version: int,
        mono_now: Optional[float] = None,
    ) -> CapsuleVerdict:
        """
        Full validation cascade for operator-initiated execution.

        Checks (in order):
          1. CapsuleValidator: HMAC, nonce, TTL, tilt_gen, sentinel gate
          2. IntentLedger: version match, revocation, TTL

        If both pass, the execution is authorized.
        If either fails, the execution is REJECTED and logged.

        Args:
            capsule: The IntentCapsule returned by the operator.
            intent_version: The intent_version from the operator's click.
            mono_now: Override for testing.

        Returns:
            CapsuleVerdict indicating whether execution is allowed.
        """
        if mono_now is None:
            mono_now = time.monotonic()

        current_tilt_gen = self._sentinel.get_generation()

        # ── STAGE 1: Cryptographic + Tilt validation ──
        verdict = self._validator.validate(
            capsule=capsule,
            current_tilt_generation=current_tilt_gen,
            mono_now=mono_now,
        )

        if not verdict.allowed:
            logger.warning(
                "🛑 [OPERATOR_GATE] REJECTED {} {} | Reason: {} | "
                "Operator: {} | Session: {}",
                capsule.side, capsule.symbol, verdict.rejection_reason,
                self._operator_id, self._forge.session_id,
            )
            return verdict

        # ── STAGE 2: Intent Ledger validation ──
        active_intent = self._intent_ledger.validate_click(intent_version)
        if active_intent is None:
            return CapsuleVerdict(
                allowed=False,
                rejection_reason="INTENT_INVALID",
                signal_id=capsule.signal_id,
            )

        # ── ALL GATES PASSED ──
        self._total_executed += 1
        logger.info(
            "✅ [OPERATOR_GATE] APPROVED #{} | {} {} {} | "
            "Capsule age: {:.0f}ms | Session: {}",
            self._total_executed,
            capsule.side, capsule.symbol, capsule.signal_id[:8],
            (mono_now - capsule.issued_at_mono) * 1000,
            self._forge.session_id,
        )

        return CapsuleVerdict(
            allowed=True,
            signal_id=capsule.signal_id,
        )

    # ──────────────────────────────────────────────────
    # SESSION MANAGEMENT
    # ──────────────────────────────────────────────────

    def rotate_session(self) -> None:
        """
        Called on WS reconnect. Invalidates ALL outstanding capsules.

        Every capsule signed with the old key will fail HMAC validation.
        This is the nuclear option — used when the operator reconnects
        after a disconnect (potentially from a different device/session).
        """
        old_session = self._forge.session_id
        self._forge.rotate_key()
        logger.warning(
            "🔄 [OPERATOR_GATE] Session rotated: {} → {} | "
            "All outstanding capsules invalidated.",
            old_session, self._forge.session_id,
        )

    # ──────────────────────────────────────────────────
    # SERIALIZATION (for WS transport)
    # ──────────────────────────────────────────────────

    @staticmethod
    def capsule_to_dict(capsule: IntentCapsule) -> dict:
        """Serialize capsule for WS JSON transport."""
        return {
            "signal_id": capsule.signal_id,
            "symbol": capsule.symbol,
            "side": capsule.side,
            "tilt_generation": capsule.tilt_generation,
            "nonce": capsule.nonce,
            "issued_at_mono": capsule.issued_at_mono,
            "signature": capsule.signature,
        }

    @staticmethod
    def dict_to_capsule(data: dict) -> IntentCapsule:
        """Deserialize capsule from WS JSON frame."""
        return IntentCapsule(
            signal_id=str(data["signal_id"]),
            symbol=str(data["symbol"]),
            side=str(data["side"]),
            tilt_generation=int(data["tilt_generation"]),
            nonce=str(data["nonce"]),
            issued_at_mono=float(data["issued_at_mono"]),
            signature=str(data["signature"]),
        )

    # ──────────────────────────────────────────────────
    # TELEMETRY
    # ──────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        return {
            "operator_id": self._operator_id,
            "session_id": self._forge.session_id,
            "total_capsules_issued": self._total_issued,
            "total_executions_approved": self._total_executed,
            "total_rejections": self._validator.total_rejections,
            "rejection_breakdown": self._validator.rejection_stats,
            "tilt_state": self._sentinel.get_current_state().name,
        }
