# src/application/sentinel_watchdog.py
import asyncio
import time
from loguru import logger

async def event_loop_monitor(lag_threshold: float = 0.1):
    """
    [GEKTOR SENTINEL] 🛡️
    Detects Event Loop Starvation caused by blocking sync calls.
    If loop lag > threshold, GEKTOR will scream critically.
    """
    logger.info("🛡️ [Sentinel] Event Loop Monitor ARMED.")
    while True:
        start = time.perf_counter()
        # Попытка заснуть на 100мс
        await asyncio.sleep(0.1)
        elapsed = time.perf_counter() - start
        
        loop_lag = elapsed - 0.1
        if loop_lag > lag_threshold:
            logger.critical(f"🚨 [ARCHITECTURE FAULT] Event Loop blocked for {loop_lag:.3f}s! "
                            f"Check for heavy JSON, Pickle serialization, or blocking I/O.")
            # Дополнительное уведомление в лог каждые 10сек при завале
            if loop_lag > 0.5:
                await asyncio.sleep(1.0)
