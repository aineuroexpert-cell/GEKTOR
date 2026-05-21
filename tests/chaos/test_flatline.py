import pytest

# Sentinel imports the infrastructure DatabaseManager which transitively imports
# redis.asyncio. The Advisory radar (v3.6.0 APEX-RADAR) does not require Redis,
# so we skip this entire chaos suite cleanly if the redis package is missing.
pytest.importorskip("redis", reason="redis not installed; sentinel chaos tests deferred")

import time
from src.application.sentinel import FlatlineSentinel

def test_flatline_detection_chaos(monkeypatch):
    """
    [CHAOS TEST] Sentinel Flatline Detection.
    Simulates a 65s+ time jump without ticks.
    Verifies [PARTIAL BLINDNESS] activation.
    """
    sentinel = FlatlineSentinel(threshold_sec=65)
    symbol = "SOLUSDT"
    
    # 1. Start alive
    start_time = 1000.0
    monkeypatch.setattr(time, "monotonic", lambda: start_time)
    sentinel.update_pulse(symbol)
    
    assert sentinel.is_blind(symbol) is False
    
    # 2. Jump time +60s (Below threshold)
    monkeypatch.setattr(time, "monotonic", lambda: start_time + 60.0)
    blind_list = sentinel.check_for_flatlines()
    assert len(blind_list) == 0
    assert sentinel.is_blind(symbol) is False
    
    # 3. Jump time +66s (Above threshold)
    monkeypatch.setattr(time, "monotonic", lambda: start_time + 66.0)
    blind_list = sentinel.check_for_flatlines()
    
    assert len(blind_list) == 1
    assert blind_list[0] == symbol
    assert sentinel.is_blind(symbol) is True
    
    # 4. Recovery
    monkeypatch.setattr(time, "monotonic", lambda: start_time + 70.0)
    sentinel.update_pulse(symbol)
    assert sentinel.is_blind(symbol) is False
