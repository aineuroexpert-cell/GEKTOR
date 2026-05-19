import pytest
import math
import time
from decimal import Decimal
from src.shared.alpha_config import alpha
from src.domain.math_core import AdaptiveHybridClock, process_ticks_subroutine
from src.domain.vpin_engine import O1VPINEngine, VPINSignal
from src.domain.dollar_bar import RealtimeDollarBarGenerator
from src.domain.conflation import DollarBar
from src.domain.cortex import O1_WelfordCUSUM

def test_adaptive_hybrid_clock_volume_trigger():
    """
    Verifies that AdaptiveHybridClock closes a bucket when volume reaches the target threshold.
    """
    clock = AdaptiveHybridClock("BTCUSDT", target_usd=1000.0, max_ttl_sec=10.0)
    
    # Apply data under target volume
    res1 = clock.apply_data(trade_volume_usd=600.0, current_ofi=12.0)
    assert res1 is None
    
    # Apply remaining data to hit target volume
    res2 = clock.apply_data(trade_volume_usd=400.0, current_ofi=8.0)
    assert res2 is not None
    assert res2["symbol"] == "BTCUSDT"
    assert res2["volume"] == 1000.0
    assert res2["ticks"] == 2
    assert res2["trigger"] == "VOLUME_TARGET"
    assert res2["vnofi"] == pytest.approx((12.0 + 8.0) / 1000.0)
    assert res2["type"] == "HYBRID_BUCKET_COMPLETE"

def test_adaptive_hybrid_clock_ttl_expiry():
    """
    Verifies that AdaptiveHybridClock closes a bucket due to TTL expiry
    if volume is at least 80% of target.
    """
    clock = AdaptiveHybridClock("BTCUSDT", target_usd=1000.0, max_ttl_sec=0.01)
    
    # Ingest 90% of target
    clock.apply_data(trade_volume_usd=900.0, current_ofi=9.0)
    
    # Wait for TTL to expire
    time.sleep(0.015)
    
    # Trigger check
    res = clock.apply_data(trade_volume_usd=1.0, current_ofi=0.0)
    assert res is not None
    assert res["trigger"] == "TTL_EXPIRY"
    assert res["volume"] == 901.0
    assert res["ticks"] == 2

def test_adaptive_hybrid_clock_insufficient_volume_decay():
    """
    Verifies that AdaptiveHybridClock discards the bucket state (returns None)
    if TTL expires but volume is below 80% of target.
    """
    clock = AdaptiveHybridClock("BTCUSDT", target_usd=1000.0, max_ttl_sec=0.01)
    
    # Ingest 50% of target
    clock.apply_data(trade_volume_usd=500.0, current_ofi=5.0)
    
    # Wait for TTL to expire
    time.sleep(0.015)
    
    # Check trigger
    res = clock.apply_data(trade_volume_usd=1.0, current_ofi=0.0)
    assert res is None
    # Verify that the clock was reset
    assert clock.current_volume == 0.0

def test_realtime_dollar_bar_generator():
    """
    Tests tick slicing and OFI distribution in RealtimeDollarBarGenerator.
    """
    generator = RealtimeDollarBarGenerator("ETHUSDT", threshold_usd=1000.0)
    
    # Single small tick -> no completed bar
    bars = generator.process_tick(price=100.0, volume=5.0, side="BUY", ts=1000.0, current_ofi=10.0)
    assert len(bars) == 0
    
    # Large tick that exceeds threshold -> completes one bar and leaves remainder
    # Dollar value of second tick is 200 * 5 = 1000. Cumulative is 1500.
    bars2 = generator.process_tick(price=200.0, volume=5.0, side="SELL", ts=1001.0, current_ofi=-20.0)
    assert len(bars2) == 1
    bar = bars2[0]
    assert bar.symbol == "ETHUSDT"
    assert bar.open == Decimal('100.0')
    assert bar.close == Decimal('200.0')
    assert bar.volume_usd == Decimal('1000.0')
    # First tick volume_usd was 500, second tick volume_usd was 500.
    # Second tick OFI was -20, so proportion is 500 / 1000 = 0.5.
    # Completed bar should have: 10 (from first tick) + (-20 * 0.5) = 10 - 10 = 0.
    assert float(getattr(bar, 'ofi_accum', 0)) == pytest.approx(0.0)

def test_o1_vpin_engine():
    """
    Tests VPIN signal calculation and absorption (iceberg) detection.
    """
    engine = O1VPINEngine(window_size=2, volume_threshold=1000.0, z_threshold=1.0)
    
    # Create dollar bars
    bar1 = DollarBar(
        symbol="BTCUSDT", open=Decimal('100.0'), high=Decimal('105.0'),
        low=Decimal('99.0'), close=Decimal('102.0'),
        buy_volume_usd=Decimal('800.0'), sell_volume_usd=Decimal('200.0'),
        volume_usd=Decimal('1000.0'), volume_crypto=Decimal('10.0')
    )
    # Imbalance: 800 - 200 = 600
    
    bar2 = DollarBar(
        symbol="BTCUSDT", open=Decimal('102.0'), high=Decimal('103.0'),
        low=Decimal('98.0'), close=Decimal('99.0'),
        buy_volume_usd=Decimal('100.0'), sell_volume_usd=Decimal('900.0'),
        volume_usd=Decimal('1000.0'), volume_crypto=Decimal('10.0')
    )
    # Imbalance: abs(100 - 900) = 800
    
    res1 = engine.process_bar(bar1)
    assert res1 is None # Window size is 2, need 2 bars to fill
    
    res2 = engine.process_bar(bar2)
    assert res2 is not None
    # Running imbalance sum = 600 + 800 = 1400
    # Total volume = 1000 * 2 = 2000
    # VPIN = 1400 / 2000 = 0.7
    assert res2.vpin_value == pytest.approx(0.7)
    
    # Z-Score tracking
    # History contains vpin=0.7. Mean=0.7, std=1e-9.
    # Anomaly since std is tiny and Z-Score > z_threshold
    assert res2.is_anomaly is True

def test_o1_welford_cusum():
    """
    Verifies that O1_WelfordCUSUM correctly tracks variance and detects structural breaks.
    """
    cusum = O1_WelfordCUSUM(decay_rate=0.1, threshold=2.0, drift=0.1)
    
    # Initialize
    assert cusum.process_dollar_bar(100.0) == 0
    assert cusum.mean == 100.0
    
    # Warm up variance
    for price in [99.0, 101.0, 99.5, 100.5]:
        cusum.process_dollar_bar(price)
        
    # Large upward movement to trigger bullish break
    triggered = False
    for i in range(10):
        res = cusum.process_dollar_bar(100.0 + i * 2.0)
        if res == 1:
            triggered = True
            break
            
    assert triggered is True

def test_process_ticks_subroutine_integration():
    """
    Verifies process_ticks_subroutine state preservation, rehydration, and execution.
    """
    # Temporarily override alpha configurations for testing
    old_window_size = alpha.VPIN_WINDOW_SIZE
    old_z_thresh = alpha.VPIN_ANOMALY_Z
    alpha.VPIN_WINDOW_SIZE = 2
    alpha.VPIN_ANOMALY_Z = 0.5
    
    try:
        symbol = "BTCUSDT"
        target_volume = 1000.0
        
        # Call 1: Feed 600 USD volume
        batch1 = [{'p': 100.0, 'v': 6.0, 'm': False, 'T': 1000.0}]
        res1 = process_ticks_subroutine(symbol, batch1, target_volume, current_state=None)
        
        assert len(res1["results"]) == 0
        assert "generator" in res1["new_state"]
        assert "engine" in res1["new_state"]
        
        # Call 2: Feed another 600 USD volume to close 1st bar (accumulated 1200)
        batch2 = [{'p': 100.0, 'v': 6.0, 'm': False, 'T': 2000.0}]
        res2 = process_ticks_subroutine(symbol, batch2, target_volume, current_state=res1["new_state"])
        assert len(res2["results"]) == 0 # Only 1 bar closed, engine needs 2
        
        # Call 3: Feed 1200 USD volume to close 2nd bar (accumulated 2400)
        batch3 = [{'p': 101.0, 'v': 12.0, 'm': True, 'T': 3000.0}]
        res3 = process_ticks_subroutine(symbol, batch3, target_volume, current_state=res2["new_state"])
        
        # Now 2 bars closed, VPIN signal should be calculated
        assert len(res3["results"]) > 0
        signal = res3["results"][0]
        assert "vpin" in signal
        assert "z_score" in signal
        assert signal["price"] == 101.0
        
    finally:
        # Restore configurations
        alpha.VPIN_WINDOW_SIZE = old_window_size
        alpha.VPIN_ANOMALY_Z = old_z_thresh
