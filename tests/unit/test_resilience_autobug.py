# tests/unit/test_resilience_autobug.py
import asyncio
import time
import pytest
from src.shared.resilience import LoopMonitor, MemoryShield, ComponentHealer

@pytest.mark.asyncio
async def test_loop_monitor_starvation_detection():
    # Set monitor warning threshold to 10ms for quick testing
    monitor = LoopMonitor(warning_threshold_ms=10.0)
    await monitor.start()
    
    # Allow loop monitor to execute one iteration
    await asyncio.sleep(0.02)
    
    # Simulate a blocking call that blocks the thread for 50ms
    time.sleep(0.05)
    
    # Give the monitor thread/task time to process
    await asyncio.sleep(0.02)
    
    # Verify that it detected the block
    assert monitor.starvation_count > 0, "LoopMonitor should detect event loop starvation"
    await monitor.stop()


@pytest.mark.asyncio
async def test_component_healer_recovery_trigger():
    healer = ComponentHealer(failure_limit=3, window_sec=2.0)
    recovery_triggered = False
    
    async def dummy_recovery():
        nonlocal recovery_triggered
        recovery_triggered = True

    # Register 2 failures (under threshold)
    healer.register_failure("BybitWS", dummy_recovery)
    healer.register_failure("BybitWS", dummy_recovery)
    assert not recovery_triggered, "Recovery should not trigger before limit"
    
    # Register 3rd failure (triggers threshold)
    healer.register_failure("BybitWS", dummy_recovery)
    
    # Yield execution to let the async task run
    await asyncio.sleep(0.05)
    assert recovery_triggered, "Recovery callback should be triggered when limit is breached"
