# src/domain/intent_capsule.py
"""
[GEKTOR v15.1] Cryptographic Intent Capsule — Anti-Bypass Execution Guard.

Solves the cURL/Script Attack Vector: an operator in tilt closes the
NeuroExpert UI and fires direct API requests to execute trades.

Architecture (Asymmetric Trust Model):

  PATH A — L6 Autonomous (Machine → Machine):
    In-process function call. No HTTP. No token.
    Trust is IMPLICIT (code path is hardcoded).
    Only check: CognitiveSentinel.is_execution_allowed() as safety net.
    Latency overhead: < 100ns (single integer comparison).

  PATH B — Operator-Initiated (Human → API):
    Requires a signed IntentCapsule containing:
      - signal_id: UUID of the signal that generated this intent
      - tilt_generation: CognitiveSentinel generation at issuance time
      - nonce: single-use random bytes (prevents replay)
      - issued_at: monotonic timestamp (enforces TTL)
      - hmac_signature: HMAC-SHA256 over all fields with ephemeral session key

    The session key is generated server-side on WebSocket connect.
    It is NEVER transmitted to the client. The client receives
    opaque signed capsules and returns them verbatim on execution.

    Even if the operator intercepts the capsule from WebSocket:
      1. Nonce is single-use → replay rejected
      2. TTL is 1200ms (human SLA) → stale capsule rejected
      3. tilt_generation check → if tilt activated since issuance, rejected
      4. Session key rotates on reconnect → old capsules dead

    Latency overhead: ~2μs (HMAC-SHA256 verify + set lookup).

ZERO I/O. ZERO infrastructure imports. Pure Domain Logic.
"""

from __future__ import annotations

import hmac
import hashlib
import os
import time
from dataclasses import dataclass
from typing import Optional


# ═══════════════════════════════════════════════════════════════════
# VALUE OBJECTS
# ═══════════════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class IntentCapsule:
    """
    Cryptographically signed execution permit — Value Object (DDD).

    Created server-side when a signal is displayed to the operator.
    Returned by the operator when they click EXECUTE.
    Validated server-side before any execution proceeds.
    """
    signal_id: str          # UUID of the originating signal
    symbol: str             # Trading pair
    side: str               # BUY or SELL
    tilt_generation: int    # CognitiveSentinel generation at issuance
    nonce: str              # Hex-encoded random bytes (single-use)
    issued_at_mono: float   # time.monotonic() at issuance
    signature: str          # HMAC-SHA256 hex digest


@dataclass(frozen=True, slots=True)
class CapsuleVerdict:
    """Result of capsule validation — Value Object."""
    allowed: bool
    rejection_reason: str = ""
    signal_id: str = ""


# ═══════════════════════════════════════════════════════════════════
# CAPSULE FORGE (Issuance)
# ═══════════════════════════════════════════════════════════════════

class CapsuleForge:
    """
    Server-side capsule factory. Issues cryptographically signed
    execution permits bound to a specific operator session.

    The session key is ephemeral — generated on WS connect,
    destroyed on WS disconnect. Never leaves the server process.

    Thread-safety: Single-writer (WS handler task). No locks.
    """
    __slots__ = ('_session_key', '_session_id')

    def __init__(self) -> None:
        """Generate a fresh ephemeral session key (32 bytes)."""
        self._session_key: bytes = os.urandom(32)
        self._session_id: str = os.urandom(8).hex()

    def issue(
        self,
        signal_id: str,
        symbol: str,
        side: str,
        tilt_generation: int,
        mono_now: Optional[float] = None,
    ) -> IntentCapsule:
        """
        Mint a new signed IntentCapsule for operator consumption.

        Args:
            signal_id: UUID of the signal being displayed.
            symbol: Trading pair.
            side: BUY or SELL.
            tilt_generation: Current CognitiveSentinel generation.
            mono_now: Override for testing.

        Returns:
            Signed IntentCapsule (send to frontend via WS).
        """
        if mono_now is None:
            mono_now = time.monotonic()

        nonce = os.urandom(16).hex()

        # Construct the message to sign
        message = self._build_message(
            signal_id, symbol, side, tilt_generation, nonce, mono_now,
        )

        signature = hmac.new(
            self._session_key, message, hashlib.sha256,
        ).hexdigest()

        return IntentCapsule(
            signal_id=signal_id,
            symbol=symbol,
            side=side,
            tilt_generation=tilt_generation,
            nonce=nonce,
            issued_at_mono=mono_now,
            signature=signature,
        )

    def verify_signature(self, capsule: IntentCapsule) -> bool:
        """
        O(1) HMAC verification. Constant-time comparison.

        Returns True if the capsule was signed by THIS forge instance.
        """
        message = self._build_message(
            capsule.signal_id,
            capsule.symbol,
            capsule.side,
            capsule.tilt_generation,
            capsule.nonce,
            capsule.issued_at_mono,
        )

        expected = hmac.new(
            self._session_key, message, hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected, capsule.signature)

    @property
    def session_id(self) -> str:
        """Public session identifier (for logging, not security)."""
        return self._session_id

    def rotate_key(self) -> None:
        """Rotate the ephemeral key. Called on WS reconnect."""
        self._session_key = os.urandom(32)
        self._session_id = os.urandom(8).hex()

    @staticmethod
    def _build_message(
        signal_id: str,
        symbol: str,
        side: str,
        tilt_gen: int,
        nonce: str,
        mono: float,
    ) -> bytes:
        """Deterministic message construction for HMAC."""
        # Fixed-format string — no JSON parsing overhead
        raw = f"{signal_id}|{symbol}|{side}|{tilt_gen}|{nonce}|{mono:.6f}"
        return raw.encode('utf-8')


# ═══════════════════════════════════════════════════════════════════
# CAPSULE VALIDATOR (Verification + Nonce + Tilt Gate)
# ═══════════════════════════════════════════════════════════════════

class CapsuleValidator:
    """
    Server-side execution gate. Validates operator-submitted capsules
    against the full defense cascade:

      Priority 0: HMAC signature (cryptographic integrity)
      Priority 1: Nonce replay protection (single-use enforcement)
      Priority 2: TTL freshness (capsule age < SLA window)
      Priority 3: Tilt generation match (cognitive state unchanged)
      Priority 4: CognitiveSentinel.is_execution_allowed() (hard gate)

    All checks are O(1). Total overhead: ~2-5μs.

    The nonce set is bounded by MAX_NONCE_CACHE to prevent memory leaks.
    Old nonces are evicted FIFO when the cache is full.

    Thread-safety: Single-writer (WS handler). No locks.
    """
    __slots__ = (
        '_forge', '_tilt_sentinel', '_used_nonces',
        '_nonce_order', '_max_nonce_cache', '_ttl_sec',
        '_total_rejections', '_rejection_reasons',
    )

    def __init__(
        self,
        forge: CapsuleForge,
        tilt_sentinel: object,  # CognitiveSentinel (duck-typed to avoid circular import)
        ttl_sec: float = 1.2,
        max_nonce_cache: int = 10_000,
    ) -> None:
        self._forge = forge
        self._tilt_sentinel = tilt_sentinel
        self._used_nonces: set[str] = set()
        self._nonce_order: list[str] = []
        self._max_nonce_cache = max_nonce_cache
        self._ttl_sec = ttl_sec
        self._total_rejections: int = 0
        self._rejection_reasons: dict[str, int] = {}

    def validate(
        self,
        capsule: IntentCapsule,
        current_tilt_generation: int,
        mono_now: Optional[float] = None,
    ) -> CapsuleVerdict:
        """
        Full validation cascade. O(1) amortized.

        Args:
            capsule: The IntentCapsule submitted by the operator.
            current_tilt_generation: Current CognitiveSentinel.get_generation().
            mono_now: Override for testing.

        Returns:
            CapsuleVerdict with allowed=True or rejection reason.
        """
        if mono_now is None:
            mono_now = time.monotonic()

        # ── PRIORITY 0: HMAC SIGNATURE ──
        # Constant-time comparison. Rejects forged/tampered capsules.
        if not self._forge.verify_signature(capsule):
            return self._reject("HMAC_INVALID", capsule.signal_id)

        # ── PRIORITY 1: NONCE REPLAY PROTECTION ──
        # O(1) set lookup. Rejects duplicate submissions.
        if capsule.nonce in self._used_nonces:
            return self._reject("NONCE_REPLAY", capsule.signal_id)

        # ── PRIORITY 2: TTL FRESHNESS ──
        # Monotonic comparison. Rejects stale capsules beyond SLA window.
        age_sec = mono_now - capsule.issued_at_mono
        if age_sec > self._ttl_sec:
            return self._reject(
                f"TTL_EXPIRED({age_sec:.3f}s>{self._ttl_sec}s)",
                capsule.signal_id,
            )

        # Negative age = clock manipulation attempt
        if age_sec < -0.1:
            return self._reject("CLOCK_MANIPULATION", capsule.signal_id)

        # ── PRIORITY 3: TILT GENERATION MATCH ──
        # If CognitiveSentinel state changed since capsule was issued,
        # the operator may be acting on pre-tilt psychology.
        if capsule.tilt_generation != current_tilt_generation:
            return self._reject(
                f"TILT_GEN_MISMATCH(capsule={capsule.tilt_generation},"
                f"current={current_tilt_generation})",
                capsule.signal_id,
            )

        # ── PRIORITY 4: COGNITIVE SENTINEL HARD GATE ──
        # O(1) integer comparison. Final defense.
        sentinel = self._tilt_sentinel
        if hasattr(sentinel, 'is_execution_allowed'):
            if not sentinel.is_execution_allowed():
                return self._reject("TILT_LOCKED", capsule.signal_id)

        # ── ALL CHECKS PASSED ──
        # Consume nonce (mark as used)
        self._consume_nonce(capsule.nonce)

        return CapsuleVerdict(
            allowed=True,
            signal_id=capsule.signal_id,
        )

    def _reject(self, reason: str, signal_id: str) -> CapsuleVerdict:
        """Record rejection for telemetry and return verdict."""
        self._total_rejections += 1
        # Bucket by base reason (strip parenthetical details)
        base_reason = reason.split("(")[0]
        self._rejection_reasons[base_reason] = (
            self._rejection_reasons.get(base_reason, 0) + 1
        )
        return CapsuleVerdict(
            allowed=False,
            rejection_reason=reason,
            signal_id=signal_id,
        )

    def _consume_nonce(self, nonce: str) -> None:
        """
        Add nonce to used set with FIFO eviction.
        Prevents unbounded memory growth.
        """
        self._used_nonces.add(nonce)
        self._nonce_order.append(nonce)

        # FIFO eviction when cache is full
        while len(self._used_nonces) > self._max_nonce_cache:
            oldest = self._nonce_order.pop(0)
            self._used_nonces.discard(oldest)

    @property
    def total_rejections(self) -> int:
        return self._total_rejections

    @property
    def rejection_stats(self) -> dict[str, int]:
        return dict(self._rejection_reasons)
