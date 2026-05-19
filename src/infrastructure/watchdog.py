import os
import time
import threading
import multiprocessing
from ctypes import c_double, c_bool
from aiohttp import ClientSession
from loguru import logger

class MicrostructureWatchdog:
    """
    [GEKTOR v22.0] Isolated Dead Man's Switch (Thread/Process Isolation).
    """
    def __init__(self, rest_session: ClientSession, api_key: str, api_secret: str):
        self.rest = rest_session
        self.api_key = api_key
        self.api_secret = api_secret
        
        self.last_lob_update = multiprocessing.Value('d', time.monotonic())
        self.is_evacuating = multiprocessing.Value(c_bool, False)
        
        self._watchdog_thread = threading.Thread(target=self._hardware_loop, daemon=True)
        self._watchdog_thread.start()
        logger.success("🛡️ [WATCHDOG] Hardware Sentinel Thread Started.")

    def update_tape_velocity(self, volume_per_second: float):
        pass # Optional logic for dynamic thresholds

    async def arm_exchange_dcp(self, ws):
        payload = {
            "req_id": f"DCP_{int(time.time())}",
            "op": "dcp",
            "args": ["10"]
        }
        try:
            await ws.send_json(payload)
            logger.info("🛡️ [WATCHDOG] EXCHANGE-SIDE DCP ARMED: 10 SECONDS.")
        except Exception as e:
            logger.error(f"⚠️ [WATCHDOG] Failed to arm DCP: {e}")

    def feed(self):
        """Called on every L2 delta to reset the watchdog timer."""
        self.last_lob_update.value = time.monotonic()

    def _hardware_loop(self):
        logger.info("👁️ [WATCHDOG] Isolated Hardware Sentinel watching...")
        while True:
            time.sleep(0.05)
            if self.is_evacuating.value:
                continue

            delta = time.monotonic() - self.last_lob_update.value
            if delta > 1.0: # 1000 ms limit
                logger.critical(f"💀 [WATCHDOG] EVENT LOOP STALL / SILENCE DETECTED: {delta:.3f}s! EXECUTING SUICIDE OS._EXIT(9)")
                os._exit(9)

    async def run_sentinel(self):
        """Deprecated async sentinel. Replaced by hardware loop."""
        pass

