"""
Regression tests for src/domain/vpin_engine.py invariants I1-I5.

These tests are the SAFETY NET against AI-model hallucinations and refactor
regressions. If you break one of these, you have broken the math, not the
test. Update the engine, not the test, unless the invariant itself is being
formally revised in SINGLE_SOURCE_OF_TRUTH.md.

Each test references the invariant ID it protects.
"""
from __future__ import annotations

import ast
import math
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from src.domain.conflation import DollarBar
from src.domain.vpin_engine import O1VPINEngine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bar(buy: float, sell: float, close: float = 100.0, symbol: str = "BTCUSDT") -> DollarBar:
    """Build a DollarBar with Decimal volumes (matches conflation.py)."""
    return DollarBar(
        symbol=symbol,
        open=Decimal(str(close)),
        high=Decimal(str(close)),
        low=Decimal(str(close)),
        close=Decimal(str(close)),
        buy_volume_usd=Decimal(str(buy)),
        sell_volume_usd=Decimal(str(sell)),
        volume_usd=Decimal(str(buy + sell)),
        volume_crypto=Decimal("0"),
    )


def _balanced_bar(close: float) -> DollarBar:
    """Symmetric bar — used to fill warmup history without injecting bias."""
    return _bar(buy=500.0, sell=500.0, close=close)


# ---------------------------------------------------------------------------
# Invariant I1: oldest_idx read AFTER increment
# ---------------------------------------------------------------------------


def test_I1_oldest_idx_points_to_oldest_bar_after_increment() -> None:
    """The price baseline used for absorption MUST be the oldest stored price,
    not the bar we just wrote.

    Setup: fill a window of size 3 with prices [100, 101, 102]. The next bar
    closes at 103 with a bullish imbalance and a NON-positive return relative
    to the OLDEST price (100). Since 103 > 100, this is NOT absorption — but
    if oldest_idx were read BEFORE the increment, the baseline would be 103
    (the bar we just wrote), and price_return would be 0, producing a FALSE
    absorption signal.
    """
    eng = O1VPINEngine(
        window_size=3,
        volume_threshold=1000.0,
        z_threshold=0.0,  # any positive z is an anomaly
        z_history_size=3,
    )
    eng.process_bar(_bar(500.0, 500.0, close=100.0))
    eng.process_bar(_bar(500.0, 500.0, close=101.0))
    eng.process_bar(_bar(500.0, 500.0, close=102.0))
    sig = eng.process_bar(_bar(900.0, 100.0, close=103.0))
    assert sig is not None
    assert bool(sig.is_anomaly) is True
    assert sig.direction == "long"
    # 103 > 100 → return positive → NO absorption.
    assert bool(sig.absorption_detected) is False


def test_I1_absorption_long_when_baseline_price_was_higher() -> None:
    """If price drops below the oldest baseline despite bullish imbalance,
    this IS absorption (hidden iceberg seller)."""
    eng = O1VPINEngine(window_size=3, volume_threshold=1000.0, z_threshold=0.0, z_history_size=3)
    eng.process_bar(_bar(500.0, 500.0, close=110.0))  # oldest after window fills
    eng.process_bar(_bar(500.0, 500.0, close=109.0))
    eng.process_bar(_bar(500.0, 500.0, close=108.0))
    sig = eng.process_bar(_bar(900.0, 100.0, close=105.0))
    assert sig is not None
    assert sig.direction == "long"
    assert bool(sig.absorption_detected) is True


# ---------------------------------------------------------------------------
# Invariant I2: z_history is independent of window_size
# ---------------------------------------------------------------------------


def test_I2_z_history_independent_buffer() -> None:
    eng = O1VPINEngine(window_size=10, volume_threshold=1000.0, z_history_size=500)
    assert eng._vpin_history.shape == (500,)
    assert eng._imbalances.shape == (10,)


def test_I2_z_history_must_be_at_least_window_size() -> None:
    with pytest.raises(ValueError):
        O1VPINEngine(window_size=50, volume_threshold=1000.0, z_history_size=10)


# ---------------------------------------------------------------------------
# Invariant I3: Z-Score warmup uses _z_count, not capacity
# ---------------------------------------------------------------------------


def test_I3_no_false_anomalies_on_constant_stream() -> None:
    """With a perfectly constant balanced stream, no anomaly should ever fire.

    The pre-fix behaviour divided variance by `window_size` even during warmup,
    so the very first emitted VPIN looked enormous relative to a mean of zero,
    producing z ≈ +inf and a guaranteed false anomaly. With _z_count as the
    divisor, the first emitted VPIN reduces to (vpin - vpin)/eps = 0.
    """
    eng = O1VPINEngine(
        window_size=10,
        volume_threshold=1000.0,
        z_threshold=2.5,
        z_history_size=200,
    )
    anomalies = 0
    for _ in range(500):
        sig = eng.process_bar(_balanced_bar(close=100.0))
        if sig is not None and bool(sig.is_anomaly):
            anomalies += 1
    assert anomalies == 0, f"Constant stream produced {anomalies} false anomalies; I3 regression."


def test_I3_warmup_does_not_blow_up_zscore_on_first_signal() -> None:
    """On the very first emitted VPIN (when the window just filled), the Z-Score
    must be finite and ≈ 0, not +inf or a large positive number.

    Pre-fix: divisor was window_size, sigma underflowed, z ~ 1e9.
    Post-fix: divisor is _z_count=1, mean==vpin, variance==0, z==0.
    """
    eng = O1VPINEngine(
        window_size=4,
        volume_threshold=1000.0,
        z_threshold=2.5,
        z_history_size=100,
    )
    sig = None
    for _ in range(4):
        sig = eng.process_bar(_bar(buy=600.0, sell=400.0, close=100.0))
    assert sig is not None
    assert math.isfinite(sig.z_score)
    assert abs(sig.z_score) < 1.0, f"First-bar z_score must be tiny; got {sig.z_score}."


def test_I3_z_count_increments_independently() -> None:
    eng = O1VPINEngine(window_size=5, volume_threshold=1000.0, z_history_size=20)
    # Fill VPIN window
    for _ in range(5):
        eng.process_bar(_balanced_bar(100.0))
    # _z_count should now be 1 (we get a signal only after window is filled)
    assert eng._z_count == 1
    for _ in range(10):
        eng.process_bar(_balanced_bar(100.0))
    assert eng._z_count == 11


# ---------------------------------------------------------------------------
# Invariant I4: Time-decay consistency (scalars vs arrays)
# ---------------------------------------------------------------------------


def test_I4_decay_preserves_sum_array_invariant() -> None:
    """After a time-gap event applies decay, the running scalar sums MUST
    still equal np.sum of the underlying arrays.
    """
    eng = O1VPINEngine(window_size=5, volume_threshold=1000.0, z_history_size=10)
    for _ in range(20):
        eng.process_bar(_bar(buy=600.0, sell=400.0, close=100.0))

    # Force-apply decay (simulates a 4h gap).
    eng._apply_time_decay(decay=0.5)

    assert math.isclose(eng._running_imbalance_sum, float(eng._imbalances.sum()), rel_tol=1e-9)
    assert math.isclose(eng._vpin_sum, float(eng._vpin_history.sum()), rel_tol=1e-9)
    assert math.isclose(eng._vpin_sq_sum, float((eng._vpin_history**2).sum()), rel_tol=1e-9)

    # After decay, continuing the stream must STILL keep the invariant after
    # subsequent delta-updates.
    for _ in range(10):
        eng.process_bar(_bar(buy=600.0, sell=400.0, close=100.0))
    assert math.isclose(eng._running_imbalance_sum, float(eng._imbalances.sum()), rel_tol=1e-9)
    assert math.isclose(eng._vpin_sum, float(eng._vpin_history.sum()), rel_tol=1e-9)


# ---------------------------------------------------------------------------
# Invariant I-noDrift: Periodic rebuild against IEEE-754 drift
# ---------------------------------------------------------------------------


def test_periodic_rebuild_keeps_sums_canonical() -> None:
    eng = O1VPINEngine(
        window_size=10,
        volume_threshold=1000.0,
        z_history_size=50,
        rebuild_interval=100,
    )
    for _ in range(250):
        eng.process_bar(_bar(buy=550.0, sell=450.0, close=100.0))
    # Sums must remain very close to the canonical array sums.
    assert math.isclose(eng._running_imbalance_sum, float(eng._imbalances.sum()), rel_tol=1e-9)
    assert math.isclose(eng._vpin_sum, float(eng._vpin_history.sum()), rel_tol=1e-9)


# ---------------------------------------------------------------------------
# Invariant I-clamp: VPIN always in [0, 1]
# ---------------------------------------------------------------------------


def test_vpin_range_always_in_unit_interval() -> None:
    """VPIN is a probability by construction; tiny float drift can push it
    out of [0,1] but the engine must clamp."""
    eng = O1VPINEngine(window_size=4, volume_threshold=1000.0, z_history_size=4)
    # Force-construct extreme one-sided flow.
    for _ in range(20):
        sig = eng.process_bar(_bar(buy=1000.0, sell=0.0, close=100.0))
        if sig is not None:
            assert 0.0 <= sig.vpin_value <= 1.0
    # Try the opposite extreme.
    for _ in range(20):
        sig = eng.process_bar(_bar(buy=0.0, sell=1000.0, close=100.0))
        if sig is not None:
            assert 0.0 <= sig.vpin_value <= 1.0


# ---------------------------------------------------------------------------
# Anti-pattern: import inside process_bar (hot path)
# ---------------------------------------------------------------------------


def test_no_import_inside_process_bar_hot_path() -> None:
    """The previous implementation had `import time` inside process_bar(),
    which caused a sys.modules dict lookup on EVERY hot-path call. We forbid
    any ImportFrom or Import nodes inside process_bar."""
    src = Path("src/domain/vpin_engine.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "process_bar":
            for child in ast.walk(node):
                assert not isinstance(child, (ast.Import, ast.ImportFrom)), (
                    "process_bar() must not contain any import statements"
                )


# ---------------------------------------------------------------------------
# Anti-pattern: reset_o1 must not zero out numpy arrays
# ---------------------------------------------------------------------------


def test_reset_o1_does_not_zero_arrays() -> None:
    """Numpy arrays are reused via the ring index — re-zeroing them in
    reset_o1 would be wasted work AND would mask a bug (the ring index
    overwrites old values on its own)."""
    eng = O1VPINEngine(window_size=3, volume_threshold=1000.0, z_history_size=3)
    for _ in range(5):
        eng.process_bar(_bar(buy=500.0, sell=500.0, close=100.0))
    snapshot = eng._imbalances.copy()
    eng.reset_o1()
    assert np.array_equal(eng._imbalances, snapshot), (
        "reset_o1 must NOT zero the numpy arrays; they are reused via the ring index."
    )


# ---------------------------------------------------------------------------
# Invariant I5: polarity contract with conflation.py
# ---------------------------------------------------------------------------


def test_I5_polarity_taker_sell_increments_sell_volume() -> None:
    """conflation.py:96-101 says: is_buyer_maker=True → taker SOLD → sell_volume_usd.
    This is the contract the VPIN engine consumes. Verify the upstream side.
    """
    import asyncio

    from src.domain.conflation import DollarBarEngine

    eng = DollarBarEngine(threshold_usd=Decimal("1000"))

    closed_bars: list[DollarBar] = []

    async def on_close(bar: DollarBar) -> None:
        closed_bars.append(bar)

    eng.set_callback(on_close)

    async def feed() -> None:
        # Taker SELL (maker was buyer).
        await eng.process_tick(
            symbol="BTCUSDT",
            price=Decimal("100"),
            size=Decimal("12"),
            is_buyer_maker=True,
            exchange_ts=1.0,
        )

    asyncio.run(feed())
    assert len(closed_bars) == 1
    bar = closed_bars[0]
    assert bar.sell_volume_usd == Decimal("1200")
    assert bar.buy_volume_usd == Decimal("0")
