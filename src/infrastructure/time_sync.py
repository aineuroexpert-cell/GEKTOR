import asyncio
import time
from typing import Optional
from loguru import logger

async def calibrate_exchange_clock(bybit_client, max_offset_ms: float = 500.0) -> float:  # ⚠️ [STAGING] Было 10.0. ВЕРНУТЬ!
    """HFT-grade clock sync. Жёсткий gate. (v21.64.4)"""
    latencies = []
    # Bybit client should have a method to get server time. 
    # In our current BybitRestClient, there isn't one, so we'll need to use any public endpoint 
    # that returns server time, or add get_server_time to the client.
    
    logger.info("🕰️ [PTP] Initiating HFT-grade clock synchronization...")
    
    for i in range(7):  # больше сэмплов = точнее
        try:
            t0 = time.perf_counter_ns()
            # We'll use the Bybit market time endpoint
            server_time_ms = await bybit_client.get_server_time()
            t1 = time.perf_counter_ns()
            
            rtt_ms = (t1 - t0) / 1_000_000
            if rtt_ms > 1000:  # ⚠️ [STAGING] Было 80. Вернуть на 80 для PRODUCTION!
                logger.warning(f"🐢 [PTP] Sample {i+1} rejected: RTT {rtt_ms:.1f}ms too high.")
                continue
                
            local_ms = time.time() * 1000
            estimated_server = server_time_ms + (rtt_ms / 2)
            offset = estimated_server - local_ms
            latencies.append(offset)
            logger.debug(f"📊 [PTP] Sample {i+1}: RTT={rtt_ms:.2f}ms, Offset={offset:.2f}ms")
            await asyncio.sleep(0.1) # Small gap between samples
        except Exception as e:
            logger.error(f"❌ [PTP] Calibration sample {i+1} failed: {e}")
    
    if not latencies:
        raise RuntimeError("CRITICAL: Cannot calibrate clock (high RTT/Network fail)")
    
    avg_offset = sum(latencies) / len(latencies)
    
    if abs(avg_offset) > max_offset_ms:
        logger.critical(f"🚨 [PTP] CLOCK DRIFT UNACCEPTABLE: {avg_offset:.2f}ms (Limit: {max_offset_ms}ms)")
        raise RuntimeError(
            f"CRITICAL CLOCK DRIFT: {avg_offset:.2f}ms. "
            "Run 'sudo chronyc makestep' + restart. HFT impossible."
        )
    
    logger.success(f"✅ [PTP] Clock synchronized. Average offset: {avg_offset:.2f}ms")
    return avg_offset
