"""
Property-based tests for src/domain/vpin_engine.py (Hypothesis).

These tests assert PROPERTIES that must hold for any sequence of inputs.
They are not example-based; Hypothesis generates pathological inputs to
hunt for invariant violations. Treat any failure as a real bug.

Properties enforced:
  P1. VPIN is always in [0, 1] for any bar sequence.
  P2. Z-Score is always finite (never NaN or +-inf).
  P3. After any sequence of bars, the running scalar sums must equal the
      np.sum() of their canonical arrays to within float tolerance
      (delta-update vs ground truth).
  P4. A perfectly symmetric stream (buy_vol == sell_vol on every bar)
      never emits an anomaly, regardless of price or window size.
  P5. Polarity invariant: increasing the buy/sell ratio monotonically
      drives VPIN towards 1.0 (i.e. one-sided flow -> max imbalance).
"""
from __future__ import annotations

import math
from decimal import Decimal

import pytest

hypothesis = pytest.importorskip(
    "hypothesis",
    reason="hypothesis not installed — install it with: pip install hypothesis",
)
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from src.domain.conflation import DollarBar
from src.domain.vpin_engine import O1VPINEngine

def _bar(buy: float, sell: float, close: float, symbol: str = "BTCUSDT") -> DollarBar:
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


# --------------------------------------------------------------------------
# Strategies
# --------------------------------------------------------------------------

_FLOWS = st.lists(
    st.tuples(
        st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
        st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
        st.floats(min_value=0.01, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
    ),
    min_size=1,
    max_size=200,
)


# --------------------------------------------------------------------------
# Properties
# --------------------------------------------------------------------------


@settings(max_examples=80, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_FLOWS)
def test_P1_vpin_always_in_unit_interval(flows: list[tuple[float, float, float]]) -> None:
    eng = O1VPINEngine(window_size=5, volume_threshold=1000.0, z_history_size=20)
    for buy, sell, close in flows:
        sig = eng.process_bar(_bar(buy, sell, close))
        if sig is not None:
            assert 0.0 <= sig.vpin_value <= 1.0


@settings(max_examples=80, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_FLOWS)
def test_P2_z_score_always_finite(flows: list[tuple[float, float, float]]) -> None:
    eng = O1VPINEngine(window_size=5, volume_threshold=1000.0, z_history_size=20)
    for buy, sell, close in flows:
        sig = eng.process_bar(_bar(buy, sell, close))
        if sig is not None:
            assert math.isfinite(sig.z_score)


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_FLOWS)
def test_P3_running_sums_match_array_sums(flows: list[tuple[float, float, float]]) -> None:
    """The O(1) delta-update sums must stay consistent with np.sum() within
    machine precision, otherwise VPIN/Z-Score values diverge from the truth.
    """
    eng = O1VPINEngine(
        window_size=5, volume_threshold=1000.0, z_history_size=20, rebuild_interval=10_000
    )
    for buy, sell, close in flows:
        eng.process_bar(_bar(buy, sell, close))
    # Absolute tolerance generous to allow IEEE drift on long streams,
    # but a multiplicative orders-of-magnitude blow-up must NOT happen.
    canonical_imb = float(eng._imbalances.sum())
    canonical_vpin = float(eng._vpin_history.sum())
    assert math.isclose(eng._running_imbalance_sum, canonical_imb, rel_tol=1e-6, abs_tol=1e-3)
    assert math.isclose(eng._vpin_sum, canonical_vpin, rel_tol=1e-6, abs_tol=1e-6)


@settings(max_examples=40, deadline=None)
@given(
    st.lists(
        st.tuples(
            st.floats(min_value=1.0, max_value=10_000.0, allow_nan=False, allow_infinity=False),
            st.floats(min_value=0.01, max_value=1_000.0, allow_nan=False, allow_infinity=False),
        ),
        min_size=10,
        max_size=200,
    )
)
def test_P4_symmetric_stream_never_emits_anomaly(samples: list[tuple[float, float]]) -> None:
    """If buy_volume == sell_volume on EVERY bar, no anomaly can ever fire
    no matter what price does (VPIN is identically zero in this stream)."""
    eng = O1VPINEngine(
        window_size=5, volume_threshold=1000.0, z_threshold=2.5, z_history_size=20
    )
    for vol, close in samples:
        sig = eng.process_bar(_bar(buy=vol, sell=vol, close=close))
        if sig is not None:
            assert sig.is_anomaly is False, (
                "Symmetric flow produced an anomaly (VPIN should be 0 by construction)."
            )


def test_P5_extreme_one_sided_flow_drives_vpin_to_one() -> None:
    """With strict one-sided flow (sell=0), VPIN must converge to 1.0
    once the window is filled."""
    eng = O1VPINEngine(window_size=10, volume_threshold=1000.0, z_history_size=20)
    sig = None
    for _ in range(20):
        sig = eng.process_bar(_bar(buy=1000.0, sell=0.0, close=100.0))
    assert sig is not None
    assert sig.vpin_value == pytest.approx(1.0)
