"""[GEKTOR v3.6.2] Regression sentinels for instant-fire liquidity detectors.

These tests pin down:
  L1. Per-symbol isolation (a sweep on BTC must not affect SOL).
  L2. Polarity contract (is_buyer_maker=True ⇔ taker sold ⇔ "sell").
  L3. Threshold validation at __init__.
  L4. Cooldown suppression after firing.
  L5. Composite bank fan-in (returns all non-None alerts).

These complement the VPIN invariant tests; together they form the
canonical regression for the radar-contour.
"""
from __future__ import annotations

import pytest

from src.domain.liquidity_detectors import (
    LargePrintDetector,
    LiquidityAlert,
    LiquidityDetectorBank,
    OFIPulseDetector,
    SweepDetector,
)

# ----------------------------------------------------------------------
# SweepDetector
# ----------------------------------------------------------------------


def test_sweep_init_rejects_bad_params() -> None:
    with pytest.raises(ValueError):
        SweepDetector(min_trades=1)
    with pytest.raises(ValueError):
        SweepDetector(window_sec=0)
    with pytest.raises(ValueError):
        SweepDetector(min_notional_usd=-1)
    with pytest.raises(ValueError):
        SweepDetector(cooldown_sec=-1)


def test_sweep_fires_on_five_consecutive_buys() -> None:
    d = SweepDetector(min_trades=5, window_sec=30.0, min_notional_usd=100_000.0)
    out: list[LiquidityAlert] = []
    # 5 same-side aggressor buys of $25k each = $125k > threshold.
    for i in range(5):
        a = d.process_tick("BTCUSDT", is_buyer_maker=False, price=100.0, size=250.0, ts=float(i))
        if a is not None:
            out.append(a)
    assert len(out) == 1, "expected exactly one sweep alert"
    alert = out[0]
    assert alert.kind == "SWEEP"
    assert alert.direction == "buy"
    assert alert.notional_usd >= 100_000.0
    assert alert.extra["trade_count"] == 5
    assert alert.symbol == "BTCUSDT"


def test_sweep_polarity_sell() -> None:
    """is_buyer_maker=True means taker SOLD. Direction should be 'sell'."""
    d = SweepDetector(min_trades=3, window_sec=30.0, min_notional_usd=10_000.0)
    last_alert = None
    for i in range(3):
        last_alert = d.process_tick(
            "ETHUSDT", is_buyer_maker=True, price=100.0, size=50.0, ts=float(i)
        )
    assert last_alert is not None
    assert last_alert.direction == "sell"


def test_sweep_resets_on_direction_flip() -> None:
    d = SweepDetector(min_trades=3, window_sec=30.0, min_notional_usd=1_000.0)
    # 2 buys, then a sell — flips the accumulator.
    a1 = d.process_tick("BTCUSDT", False, 100.0, 5.0, 1.0)
    a2 = d.process_tick("BTCUSDT", False, 100.0, 5.0, 2.0)
    a_flip = d.process_tick("BTCUSDT", True, 100.0, 5.0, 3.0)
    assert a1 is None and a2 is None and a_flip is None
    # Now we need 2 more sells for the third trade after flip — total 3 sells.
    a3 = d.process_tick("BTCUSDT", True, 100.0, 5.0, 4.0)
    a4 = d.process_tick("BTCUSDT", True, 100.0, 5.0, 5.0)
    assert a3 is None
    assert a4 is not None
    assert a4.direction == "sell"


def test_sweep_window_expiry_resets() -> None:
    d = SweepDetector(min_trades=3, window_sec=10.0, min_notional_usd=1_000.0)
    d.process_tick("BTCUSDT", False, 100.0, 5.0, 0.0)
    d.process_tick("BTCUSDT", False, 100.0, 5.0, 1.0)
    # Big gap > window → accumulator resets.
    a = d.process_tick("BTCUSDT", False, 100.0, 5.0, 30.0)
    assert a is None  # only 1 trade in new window


def test_sweep_per_symbol_isolation() -> None:
    """Sweep on BTC must not contribute to SOL state (invariant L1)."""
    d = SweepDetector(min_trades=3, window_sec=30.0, min_notional_usd=1_000.0)
    d.process_tick("BTCUSDT", False, 100.0, 5.0, 0.0)
    d.process_tick("BTCUSDT", False, 100.0, 5.0, 1.0)
    # SOL has only ONE trade — no cross-symbol leak.
    a_sol = d.process_tick("SOLUSDT", False, 100.0, 5.0, 2.0)
    assert a_sol is None
    # Third BTC fires.
    a_btc = d.process_tick("BTCUSDT", False, 100.0, 5.0, 2.0)
    assert a_btc is not None
    assert a_btc.symbol == "BTCUSDT"


def test_sweep_cooldown_after_firing() -> None:
    d = SweepDetector(
        min_trades=3,
        window_sec=30.0,
        min_notional_usd=1_000.0,
        cooldown_sec=60.0,
    )
    for i in range(3):
        d.process_tick("BTCUSDT", False, 100.0, 5.0, float(i))
    # Within cooldown: another sweep should NOT fire.
    for i in range(3):
        a = d.process_tick("BTCUSDT", False, 100.0, 5.0, 10.0 + i)
    assert a is None


# ----------------------------------------------------------------------
# LargePrintDetector
# ----------------------------------------------------------------------


def test_large_print_init_rejects_bad_params() -> None:
    with pytest.raises(ValueError):
        LargePrintDetector(turnover_provider=lambda _: 0, pct_threshold=0)
    with pytest.raises(ValueError):
        LargePrintDetector(turnover_provider=lambda _: 0, min_notional_usd=0)


def test_large_print_fires_on_huge_trade() -> None:
    # Turnover provider returns $10M / day for BTCUSDT.
    d = LargePrintDetector(
        turnover_provider=lambda _: 10_000_000.0,
        pct_threshold=0.005,  # 0.5%
        min_notional_usd=25_000.0,
    )
    # A single $100k trade = 1% of turnover → fires.
    a = d.process_tick("BTCUSDT", is_buyer_maker=False, price=1.0, size=100_000.0, ts=0.0)
    assert a is not None
    assert a.kind == "LARGE_PRINT"
    assert a.direction == "buy"
    assert a.extra["pct_of_24h_turnover"] == pytest.approx(0.01)


def test_large_print_ignores_small_trade() -> None:
    d = LargePrintDetector(
        turnover_provider=lambda _: 10_000_000.0,
        pct_threshold=0.005,
        min_notional_usd=25_000.0,
    )
    # $10k = 0.1% < threshold → no alert.
    a = d.process_tick("BTCUSDT", False, 1.0, 10_000.0, 0.0)
    assert a is None


def test_large_print_uses_absolute_floor_when_turnover_unknown() -> None:
    d = LargePrintDetector(
        turnover_provider=lambda _: 0.0,
        pct_threshold=0.005,
        min_notional_usd=25_000.0,
    )
    # $250k = 10× floor → fires even without turnover data.
    a = d.process_tick("OBSCUREUSDT", False, 1.0, 250_000.0, 0.0)
    assert a is not None
    assert a.kind == "LARGE_PRINT"


def test_large_print_cooldown() -> None:
    d = LargePrintDetector(
        turnover_provider=lambda _: 10_000_000.0,
        pct_threshold=0.005,
        min_notional_usd=25_000.0,
        cooldown_sec=60.0,
    )
    a1 = d.process_tick("BTCUSDT", False, 1.0, 100_000.0, 0.0)
    a2 = d.process_tick("BTCUSDT", False, 1.0, 100_000.0, 30.0)
    assert a1 is not None
    assert a2 is None


# ----------------------------------------------------------------------
# OFIPulseDetector
# ----------------------------------------------------------------------


def test_ofi_pulse_init_rejects_bad_params() -> None:
    with pytest.raises(ValueError):
        OFIPulseDetector(k=1.0)  # must be > 1.0
    with pytest.raises(ValueError):
        OFIPulseDetector(history_buckets=1)
    with pytest.raises(ValueError):
        OFIPulseDetector(min_history=1)


def test_ofi_pulse_requires_warmup() -> None:
    d = OFIPulseDetector(bucket_sec=10.0, history_buckets=10, k=2.0, min_history=3)
    # Only the first bucket — no history, must NOT fire.
    for i in range(5):
        a = d.process_tick("BTCUSDT", False, 100.0, 100.0, float(i))
        assert a is None


def test_ofi_pulse_fires_on_breakout() -> None:
    d = OFIPulseDetector(
        bucket_sec=10.0, history_buckets=20, k=2.0, min_notional_usd=1.0, min_history=3
    )
    # Build 5 small buckets of ~$1k OFI each.
    for bucket_idx in range(5):
        ts = bucket_idx * 10.0 + 5.0
        d.process_tick("BTCUSDT", False, 1.0, 1_000.0, ts)
    # Sixth bucket: huge OFI imbalance, should fire.
    breakout_ts = 5 * 10.0 + 5.0
    a = d.process_tick("BTCUSDT", False, 1.0, 50_000.0, breakout_ts)
    assert a is not None
    assert a.kind == "OFI_PULSE"
    assert a.direction == "buy"
    assert a.extra["ratio_to_median"] >= 2.0


# ----------------------------------------------------------------------
# LiquidityDetectorBank
# ----------------------------------------------------------------------


def test_bank_disabled_when_all_none() -> None:
    bank = LiquidityDetectorBank(sweep=None, large_print=None, ofi_pulse=None)
    assert not bank.enabled
    out = bank.process_tick("BTCUSDT", False, 100.0, 1.0, 0.0)
    assert out == []


def test_bank_collects_alerts_from_all_detectors() -> None:
    sweep = SweepDetector(min_trades=2, window_sec=30.0, min_notional_usd=1_000.0)
    large_print = LargePrintDetector(
        turnover_provider=lambda _: 10_000_000.0,
        pct_threshold=0.001,
        min_notional_usd=1_000.0,
    )
    bank = LiquidityDetectorBank(sweep=sweep, large_print=large_print, ofi_pulse=None)
    assert bank.enabled
    # First tick: small, no sweep yet, no large print.
    out1 = bank.process_tick("BTCUSDT", False, 1.0, 100.0, 0.0)
    assert out1 == []
    # Second tick: huge size → triggers both sweep (2 trades, $100k total)
    # AND large print (0.1% of $10M turnover = $10k threshold; trade is $100k).
    out2 = bank.process_tick("BTCUSDT", False, 1.0, 100_000.0, 1.0)
    kinds = {a.kind for a in out2}
    assert "SWEEP" in kinds
    assert "LARGE_PRINT" in kinds
