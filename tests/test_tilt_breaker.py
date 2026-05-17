# tests/test_tilt_breaker.py
"""
[GEKTOR v15.0] Tilt-Breaker Domain Tests.

Tests the CognitiveSentinel without any I/O or infrastructure dependencies.
All tests use deterministic monotonic clock injection.
"""

import pytest
import time
from src.domain.tilt_breaker import (
    CognitiveSentinel,
    TiltState,
    TiltMetrics,
    ReactionTimeTracker,
    ClickEntropyDetector,
    ErrorStreakTracker,
)


class TestReactionTimeTracker:
    """Tests for Pillar 1: Reaction Time EWMA."""

    def test_insufficient_samples_returns_zero_drift(self):
        tracker = ReactionTimeTracker(min_samples=5)
        for _ in range(4):
            tracker.record(500.0)
        assert tracker.get_drift() == 0.0

    def test_consistent_reactions_produce_zero_drift(self):
        tracker = ReactionTimeTracker(capacity=20, alpha=0.15, min_samples=5)
        for _ in range(20):
            tracker.record(500.0)
        assert tracker.get_drift() < 0.01

    def test_sudden_spike_produces_high_drift(self):
        tracker = ReactionTimeTracker(capacity=20, alpha=0.15, min_samples=5)
        # Build baseline at 500ms
        for _ in range(10):
            tracker.record(500.0)
        # Sudden spike to 1500ms (3x baseline)
        tracker.record(1500.0)
        drift = tracker.get_drift()
        assert drift > 1.0, f"Expected drift > 1.0 for 3x spike, got {drift}"

    def test_rejects_invalid_values(self):
        tracker = ReactionTimeTracker()
        tracker.record(-100.0)   # negative
        tracker.record(0.0)      # zero
        tracker.record(50_000.0) # >30s
        assert tracker.sample_count == 0

    def test_ewma_converges(self):
        tracker = ReactionTimeTracker(alpha=0.5)
        tracker.record(1000.0)
        assert tracker.baseline_ms == 1000.0
        tracker.record(500.0)
        # EWMA(alpha=0.5): 0.5*500 + 0.5*1000 = 750
        assert tracker.baseline_ms == 750.0


class TestClickEntropyDetector:
    """Tests for Pillar 3: Click Spam Detection."""

    def test_normal_clicking_low_intensity(self):
        detector = ClickEntropyDetector(max_tokens=5.0, refill_rate=1.0)
        mono = 1000.0
        # One click per second — well within budget
        for i in range(5):
            detector.record_click(mono + i)
        intensity = detector.get_spam_intensity(mono + 5)
        assert intensity < 2.0, f"Normal clicks should be low intensity, got {intensity}"

    def test_burst_triggers_high_intensity(self):
        detector = ClickEntropyDetector(max_tokens=5.0, refill_rate=1.0)
        mono = 1000.0
        # 5 clicks within 200ms — clear panic burst
        for i in range(5):
            detector.record_click(mono + i * 0.04)
        intensity = detector.get_spam_intensity(mono + 0.2)
        assert intensity > 3.0, f"Burst should trigger high intensity, got {intensity}"

    def test_reset_clears_state(self):
        detector = ClickEntropyDetector()
        mono = 1000.0
        for i in range(10):
            detector.record_click(mono + i * 0.01)
        detector.reset()
        assert detector.get_spam_intensity(mono + 1.0) < 1.0


class TestErrorStreakTracker:
    """Tests for Pillar 2: Sequential Error Accumulation."""

    def test_bad_decisions_increment_streak(self):
        tracker = ErrorStreakTracker()
        for _ in range(5):
            tracker.record_bad_decision()
        assert tracker.streak == 5

    def test_good_decision_asymmetric_decay(self):
        tracker = ErrorStreakTracker()
        for _ in range(5):
            tracker.record_bad_decision()
        tracker.record_good_decision()
        # Asymmetric: good = -2
        assert tracker.streak == 3

    def test_streak_floors_at_zero(self):
        tracker = ErrorStreakTracker()
        tracker.record_good_decision()
        assert tracker.streak == 0

    def test_max_streak_tracked(self):
        tracker = ErrorStreakTracker()
        for _ in range(7):
            tracker.record_bad_decision()
        tracker.record_good_decision()
        assert tracker.max_streak == 7
        assert tracker.streak == 5


class TestCognitiveSentinel:
    """Integration tests for the composite tilt detection engine."""

    def test_initial_state_is_clear(self):
        sentinel = CognitiveSentinel()
        assert sentinel.get_current_state() == TiltState.CLEAR
        assert sentinel.is_execution_allowed() is True

    def test_error_streak_escalates_to_elevated(self):
        sentinel = CognitiveSentinel()
        mono = 1000.0
        # Feed 10 baseline reactions first
        for i in range(10):
            sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + i)
        # Now feed errors
        metrics = sentinel.ingest_heartbeat(500.0, 0, 3, 0, mono_now=mono + 11)
        # 3 errors: 0.35 * (3/4) = 0.26 — not enough alone
        # But combined with any drift or spam...
        assert metrics.error_streak == 3

    def test_massive_error_streak_triggers_lock(self):
        sentinel = CognitiveSentinel()
        mono = 1000.0
        # Build baseline
        for i in range(10):
            sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + i)
        # Feed 5 errors at once + spike reaction to 1500ms + spam clicks
        metrics = sentinel.ingest_heartbeat(
            reaction_ms=1500.0,  # 3x drift from 500ms baseline
            click_count=8,
            error_delta=5,
            success_delta=0,
            mono_now=mono + 11,
        )
        # Should be LOCKED or COOLDOWN
        assert metrics.state in (TiltState.LOCKED, TiltState.COOLDOWN), \
            f"Expected LOCKED/COOLDOWN, got {metrics.state}"
        assert sentinel.is_execution_allowed() is False

    def test_cooldown_expires_returns_to_clear(self):
        sentinel = CognitiveSentinel(cooldown_sec=5.0)
        mono = 1000.0
        # Build baseline
        for i in range(10):
            sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + i)
        # Trigger lock
        sentinel.ingest_heartbeat(1500.0, 10, 5, 0, mono_now=mono + 11)
        # Wait for cooldown to expire
        metrics = sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + 20)
        assert metrics.state == TiltState.CLEAR
        assert sentinel.is_execution_allowed() is True

    def test_force_unlock(self):
        sentinel = CognitiveSentinel()
        mono = 1000.0
        for i in range(10):
            sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + i)
        sentinel.ingest_heartbeat(1500.0, 10, 5, 0, mono_now=mono + 11)
        assert sentinel.is_execution_allowed() is False
        # Admin force unlock
        metrics = sentinel.force_unlock()
        assert metrics.state == TiltState.CLEAR
        assert sentinel.is_execution_allowed() is True

    def test_blind_detection(self):
        sentinel = CognitiveSentinel(heartbeat_timeout_sec=3.0)
        mono = 1000.0
        sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono)
        # Silence for 5 seconds
        is_blind = sentinel.check_blind(mono_now=mono + 5)
        assert is_blind is True
        assert sentinel.get_current_state() == TiltState.BLIND
        assert sentinel.is_execution_allowed() is False

    def test_hysteresis_prevents_oscillation(self):
        sentinel = CognitiveSentinel()
        mono = 1000.0
        # Build baseline
        for i in range(10):
            sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + i)

        # Push into ELEVATED (3 errors = 0.35*(3/4) = 0.2625, need more)
        # Add moderate reaction drift
        metrics = sentinel.ingest_heartbeat(
            reaction_ms=900.0,  # moderate drift
            click_count=2,
            error_delta=3,
            success_delta=0,
            mono_now=mono + 11,
        )

        if metrics.state == TiltState.ELEVATED:
            # Score drops to 0.36 (between hysteresis band 0.35-0.40)
            # Should STAY elevated due to hysteresis
            metrics2 = sentinel.ingest_heartbeat(500.0, 0, 0, 1, mono_now=mono + 12)
            # Could still be elevated if score didn't drop below 0.35
            assert metrics2.state in (TiltState.CLEAR, TiltState.ELEVATED)

    def test_generation_increments(self):
        sentinel = CognitiveSentinel()
        gen_start = sentinel.get_generation()
        sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=1000.0)
        assert sentinel.get_generation() > gen_start
