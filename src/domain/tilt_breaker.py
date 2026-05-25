# src/domain/tilt_breaker.py
"""
[GEKTOR v15.0] Tilt-Breaker: Cognitive Isolation Protocol (Domain Layer).

Detects emotional degradation of the Operator via three independent signals:
  1. Reaction Time Drift (EWMA baseline deviation)
  2. Sequential Error Accumulation (asymmetric decay)
  3. Click Entropy / Spam Detection (token bucket + burst analysis)

ZERO I/O. ZERO infrastructure imports. Pure Domain Logic.

References:
  - López de Prado: "Advances in Financial ML" — Drawdown-as-Emotion proxy
  - Kahneman: "Thinking, Fast and Slow" — System 1 override detection
  - Linux Kernel Seqlock — inspiration for generation-counter state sync
"""

from __future__ import annotations

import time
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Optional


class TiltState(IntEnum):
    """Finite State Machine for Operator cognitive health."""
    CLEAR = 0
    """Normal operations. All execution controls enabled."""

    ELEVATED = 1
    """Warning zone. UI shows amber indicators. Execution still permitted."""

    CRITICAL = 2
    """Danger zone. Auto-transitions to LOCKED. L6 Gateway suppressed."""

    LOCKED = 3
    """Hard lockout. ALL execution UI disabled. Mandatory cooldown required."""

    COOLDOWN = 4
    """Post-lock recovery period. Countdown visible, execution disabled."""

    BLIND = 5
    """WebSocket connection lost. Operator status unknown. L6 frozen."""


@dataclass(slots=True, frozen=True)
class TiltMetrics:
    """Immutable snapshot of tilt analysis — Value Object (DDD)."""
    reaction_drift: float       # [0.0, ∞) — deviation from baseline EWMA
    error_streak: int           # [0, ∞) — consecutive bad decisions
    spam_intensity: float       # [0.0, ∞) — click entropy score
    composite_score: float      # [0.0, 1.0] — weighted composite
    state: TiltState            # Current FSM state
    locked_until_mono: float    # monotonic timestamp when lock expires (0 if not locked)
    generation: int             # Seqlock-style counter for frontend sync


class ReactionTimeTracker:
    """
    [Pillar 1] EWMA-based reaction time baseline with ring buffer.

    Tracks operator response latency to incoming signals.
    Drift from personal baseline indicates cognitive fatigue or panic.

    Complexity: O(1) per update, O(1) per query.
    """
    __slots__ = (
        '_buffer', '_head', '_count', '_capacity',
        '_ewma', '_alpha', '_min_samples',
    )

    def __init__(self, capacity: int = 20, alpha: float = 0.15, min_samples: int = 5):
        self._buffer: list[float] = [0.0] * capacity
        self._head: int = 0
        self._count: int = 0
        self._capacity: int = capacity
        self._ewma: float = 0.0
        self._alpha: float = alpha
        self._min_samples: int = min_samples

    def record(self, reaction_ms: float) -> None:
        """Record a new reaction time sample. O(1)."""
        # Boundary validation: reject impossible values
        if not (0.0 < reaction_ms < 30_000.0):
            return

        self._buffer[self._head] = reaction_ms
        self._head = (self._head + 1) % self._capacity
        self._count = min(self._count + 1, self._capacity)

        # EWMA update
        if self._ewma == 0.0:
            self._ewma = reaction_ms
        else:
            self._ewma = self._alpha * reaction_ms + (1.0 - self._alpha) * self._ewma

    def get_drift(self) -> float:
        """
        Returns normalized drift from EWMA baseline.
        0.0 = perfect consistency. 1.0 = reaction time doubled. 2.0+ = severe degradation.
        """
        if self._count < self._min_samples:
            return 0.0  # Insufficient data — assume stable

        # Use most recent sample
        last_idx = (self._head - 1) % self._capacity
        current = self._buffer[last_idx]

        if self._ewma <= 0.0:
            return 0.0

        return abs(current - self._ewma) / self._ewma

    @property
    def baseline_ms(self) -> float:
        """Current EWMA baseline in milliseconds."""
        return self._ewma

    @property
    def sample_count(self) -> int:
        return self._count


class ClickEntropyDetector:
    """
    [Pillar 3] Token Bucket + Burst Pattern Analyzer.

    Detects two forms of cognitive breakdown:
      A) Sustained spam: Token bucket depletion (>5 clicks/sec averaged)
      B) Panic bursts: >3 clicks within 500ms window

    Both are independent triggers — either can escalate tilt score.
    """
    __slots__ = (
        '_tokens', '_max_tokens', '_refill_rate',
        '_last_refill_mono', '_burst_buffer', '_burst_head',
        '_burst_capacity', '_burst_count',
        '_rapid_fire_count', '_rapid_fire_window_start',
    )

    def __init__(
        self,
        max_tokens: float = 5.0,
        refill_rate: float = 1.0,  # tokens per second
        burst_capacity: int = 10,
    ):
        self._tokens: float = max_tokens
        self._max_tokens: float = max_tokens
        self._refill_rate: float = refill_rate
        self._last_refill_mono: float = 0.0  # Lazy init on first click

        # Ring buffer for burst detection (timestamps)
        self._burst_buffer: list[float] = [0.0] * burst_capacity
        self._burst_head: int = 0
        self._burst_capacity: int = burst_capacity
        self._burst_count: int = 0

        # Rapid-fire window (10s rolling)
        self._rapid_fire_count: int = 0
        self._rapid_fire_window_start: float = 0.0  # Lazy init on first click

    def record_click(self, mono_now: float) -> None:
        """Record a click event. O(1)."""
        # Lazy clock calibration: adopt whatever clock source the caller provides
        if self._last_refill_mono == 0.0:
            self._last_refill_mono = mono_now
        if self._rapid_fire_window_start == 0.0:
            self._rapid_fire_window_start = mono_now

        # Token bucket refill (clamp elapsed to 0 — guards against clock mismatch)
        elapsed = max(0.0, mono_now - self._last_refill_mono)
        self._tokens = min(
            self._max_tokens,
            self._tokens + elapsed * self._refill_rate,
        )
        self._last_refill_mono = mono_now

        # Consume token
        self._tokens = max(0.0, self._tokens - 1.0)

        # Record burst timestamp
        self._burst_buffer[self._burst_head] = mono_now
        self._burst_head = (self._burst_head + 1) % self._burst_capacity
        self._burst_count = min(self._burst_count + 1, self._burst_capacity)

        # Rapid-fire tracking (10s window)
        if mono_now - self._rapid_fire_window_start > 10.0:
            self._rapid_fire_count = 0
            self._rapid_fire_window_start = mono_now
        self._rapid_fire_count += 1

    def get_spam_intensity(self, mono_now: float) -> float:
        """
        Returns spam intensity score [0.0, ∞).
        Combines: token depletion + burst density + rapid fire rate.
        """
        # Factor A: Token depletion (0 = fully depleted, 1 = full bucket)
        depletion = 1.0 - (self._tokens / self._max_tokens)

        # Factor B: Burst density (check 500ms window)
        burst_count = 0
        for i in range(self._burst_count):
            idx = (self._burst_head - 1 - i) % self._burst_capacity
            if mono_now - self._burst_buffer[idx] <= 0.5:
                burst_count += 1
            else:
                break
        burst_factor = max(0.0, burst_count - 2) / 3.0  # >2 clicks in 500ms = problem

        # Factor C: Rapid fire rate (10s window)
        window_elapsed = max(0.1, mono_now - self._rapid_fire_window_start)
        rapid_rate = self._rapid_fire_count / window_elapsed
        rapid_factor = max(0.0, rapid_rate - 0.5) / 1.0  # >0.5 clicks/sec sustained

        return depletion * 2.0 + burst_factor * 3.0 + rapid_factor

    def reset(self) -> None:
        """Full reset after cooldown period."""
        self._tokens = self._max_tokens
        self._burst_count = 0
        self._rapid_fire_count = 0
        self._last_refill_mono = 0.0
        self._rapid_fire_window_start = 0.0


class ErrorStreakTracker:
    """
    [Pillar 2] Sequential Error Accumulation with Asymmetric Decay.

    Behavioral finance insight: Losses are psychologically 2.5x more impactful
    than gains (Prospect Theory). We use asymmetric decay:
      - Bad decision: +1
      - Good decision: -2 (faster recovery reward)

    This prevents a single lucky trade from erasing a dangerous loss streak.
    """
    __slots__ = ('_streak', '_max_streak_observed')

    def __init__(self):
        self._streak: int = 0
        self._max_streak_observed: int = 0

    def record_bad_decision(self) -> None:
        """Operator clicked, trade was unprofitable within 30s window."""
        self._streak += 1
        self._max_streak_observed = max(self._max_streak_observed, self._streak)

    def record_good_decision(self) -> None:
        """Operator clicked, trade was profitable. Asymmetric reward."""
        self._streak = max(0, self._streak - 2)

    @property
    def streak(self) -> int:
        return self._streak

    @property
    def max_streak(self) -> int:
        return self._max_streak_observed

    def reset(self) -> None:
        """Full reset after cooldown."""
        self._streak = 0


# ═══════════════════════════════════════════════════════════════════
# COMPOSITE SENTINEL — The Brain of Tilt Detection
# ═══════════════════════════════════════════════════════════════════

class CognitiveSentinel:
    """
    [GEKTOR v15.0] Composite Tilt Detection Engine.

    Orchestrates three independent detectors into a unified tilt score.
    Implements the FSM with hysteresis to prevent oscillation.

    Thread Safety: NOT thread-safe. Must be accessed from a single
    asyncio task (the WS heartbeat handler). This is by design —
    no locks, no contention, no GIL pressure.
    """
    __slots__ = (
        '_reaction_tracker', '_click_detector', '_error_tracker',
        '_state', '_generation', '_locked_until_mono',
        '_cooldown_sec', '_last_heartbeat_mono',
        '_heartbeat_timeout_sec', '_weights',
        '_threshold_elevated', '_threshold_critical',
        '_threshold_clear_hysteresis',
    )

    def __init__(
        self,
        cooldown_sec: float = 60.0,
        heartbeat_timeout_sec: float = 5.0,
        weight_reaction: float = 0.40,
        weight_errors: float = 0.35,
        weight_spam: float = 0.25,
    ):
        self._reaction_tracker = ReactionTimeTracker()
        self._click_detector = ClickEntropyDetector()
        self._error_tracker = ErrorStreakTracker()

        self._state: TiltState = TiltState.CLEAR
        self._generation: int = 0
        self._locked_until_mono: float = 0.0
        self._cooldown_sec: float = cooldown_sec
        self._last_heartbeat_mono: float = time.monotonic()
        self._heartbeat_timeout_sec: float = heartbeat_timeout_sec

        self._weights = (weight_reaction, weight_errors, weight_spam)

        # Thresholds with hysteresis band
        self._threshold_elevated: float = 0.4
        self._threshold_critical: float = 0.7
        self._threshold_clear_hysteresis: float = 0.35  # Must drop below this to clear

    # ──────────────────────────────────────────────────
    # PUBLIC API (called from WS handler)
    # ──────────────────────────────────────────────────

    def ingest_heartbeat(
        self,
        reaction_ms: float,
        click_count: int,
        error_delta: int,
        success_delta: int,
        mono_now: Optional[float] = None,
    ) -> TiltMetrics:
        """
        Process a heartbeat frame from the frontend.

        Args:
            reaction_ms: Last measured reaction time (ms). 0 if no signal was shown.
            click_count: Number of execution clicks since last heartbeat.
            error_delta: Number of bad decisions since last heartbeat.
            success_delta: Number of good decisions since last heartbeat.
            mono_now: Override for testing. Uses time.monotonic() if None.

        Returns:
            Immutable TiltMetrics snapshot for WS dispatch to frontend.
        """
        if mono_now is None:
            mono_now = time.monotonic()

        self._last_heartbeat_mono = mono_now

        # ── Feed raw data into detectors ──
        if reaction_ms > 0:
            self._reaction_tracker.record(reaction_ms)

        # Validate click_count bounds
        click_count = max(0, min(1000, click_count))
        for _ in range(click_count):
            self._click_detector.record_click(mono_now)

        for _ in range(error_delta):
            self._error_tracker.record_bad_decision()
        for _ in range(success_delta):
            self._error_tracker.record_good_decision()

        # ── Compute composite score ──
        return self._evaluate(mono_now)

    def check_blind(self, mono_now: Optional[float] = None) -> bool:
        """
        Called periodically (e.g., every 1s) to detect WS silence.
        Returns True if operator should be considered BLIND.
        """
        if mono_now is None:
            mono_now = time.monotonic()

        elapsed = mono_now - self._last_heartbeat_mono
        if elapsed > self._heartbeat_timeout_sec:
            if self._state != TiltState.BLIND:
                self._state = TiltState.BLIND
                self._generation += 1
            return True
        return False

    def is_execution_allowed(self) -> bool:
        """
        O(1) gate check. Called by L6 Gateway before every strike.
        Only CLEAR and ELEVATED allow execution.
        """
        return self._state in (TiltState.CLEAR, TiltState.ELEVATED)

    def get_current_state(self) -> TiltState:
        return self._state

    def get_generation(self) -> int:
        return self._generation

    def force_unlock(self) -> TiltMetrics:
        """
        Emergency administrative unlock (e.g., via Telegram 2FA command).
        Bypasses cooldown timer. Use with extreme caution.
        """
        self._state = TiltState.CLEAR
        self._locked_until_mono = 0.0
        self._generation += 1

        self._reaction_tracker = ReactionTimeTracker()
        self._click_detector.reset()
        self._error_tracker.reset()

        return TiltMetrics(
            reaction_drift=0.0,
            error_streak=0,
            spam_intensity=0.0,
            composite_score=0.0,
            state=TiltState.CLEAR,
            locked_until_mono=0.0,
            generation=self._generation,
        )

    # ──────────────────────────────────────────────────
    # PRIVATE: Evaluation & State Transitions
    # ──────────────────────────────────────────────────

    def _evaluate(self, mono_now: float) -> TiltMetrics:
        """Core evaluation. Computes score and transitions FSM."""
        # ── Phase 1: Resolve pending state transitions ──
        # LOCKED → COOLDOWN (timer was already set at lock entry)
        if self._state == TiltState.LOCKED:
            self._state = TiltState.COOLDOWN

        # COOLDOWN → CLEAR (check if timer expired)
        if self._state == TiltState.COOLDOWN:
            if mono_now >= self._locked_until_mono:
                self._state = TiltState.CLEAR
                self._locked_until_mono = 0.0
                self._reaction_tracker = ReactionTimeTracker()
                self._click_detector.reset()
                self._error_tracker.reset()

        # ── Phase 2: Compute raw signals ──
        reaction_drift = self._reaction_tracker.get_drift()
        error_streak = self._error_tracker.streak
        spam_intensity = self._click_detector.get_spam_intensity(mono_now)

        # Weighted composite, clamped to [0, 1]
        w_r, w_e, w_s = self._weights
        composite = (
            w_r * min(reaction_drift / 2.0, 1.0) +
            w_e * min(error_streak / 4.0, 1.0) +
            w_s * min(spam_intensity / 5.0, 1.0)
        )
        composite = min(1.0, max(0.0, composite))

        # ── Phase 3: FSM transitions (only from CLEAR or ELEVATED) ──
        if self._state in (TiltState.CLEAR, TiltState.ELEVATED):
            if composite >= self._threshold_critical:
                # CRITICAL → LOCKED: set cooldown timer at point of lock
                self._state = TiltState.LOCKED
                self._locked_until_mono = mono_now + self._cooldown_sec
            elif composite >= self._threshold_elevated:
                self._state = TiltState.ELEVATED
            elif composite < self._threshold_clear_hysteresis:
                self._state = TiltState.CLEAR

        self._generation += 1

        return TiltMetrics(
            reaction_drift=round(reaction_drift, 4),
            error_streak=error_streak,
            spam_intensity=round(spam_intensity, 4),
            composite_score=round(composite, 4),
            state=self._state,
            locked_until_mono=self._locked_until_mono,
            generation=self._generation,
        )
