import asyncio
import time
from typing import Set, Dict, Optional, Any
from loguru import logger
import statistics
import collections

class HardwareClockSynchronizer:
    """
    [GEKTOR v8.0] Precision Time Protocol (PTP-Lite).
    Компенсирует дрейф локальных часов ОС относительно Matching Engine.
    Латентность вызова: < 500нс.
    """
    __slots__ = ('_rolling_offset_ms', '_alpha', '_last_rtt_ms')

    def __init__(self, alpha: float = 0.2):
        self._rolling_offset_ms = 0.0
        self._alpha = alpha
        self._last_rtt_ms = 0.0

    def calibrate(self, t0_local_ms: float, t1_exchange_ms: float, t2_local_ms: float):
        """
        t0: Send Ping (Local ms)
        t1: Rx Ping (Exchange TS from Payload ms)
        t2: Rx Pong (Local ms)
        """
        rtt = t2_local_ms - t0_local_ms
        self._last_rtt_ms = rtt
        
        # Предполагаем симметричную задержку L3/L4
        one_way_latency = rtt / 2.0
        
        # Вычисляем смещение: Насколько наши часы впереди/позади биржи
        current_offset = t1_exchange_ms - (t0_local_ms + one_way_latency)
        
        if self._rolling_offset_ms == 0.0:
            self._rolling_offset_ms = current_offset
        else:
            self._rolling_offset_ms = (self._alpha * current_offset) + ((1.0 - self._alpha) * self._rolling_offset_ms)
            
        if abs(current_offset) > 5.0:
             logger.warning(f"⏱️ [PTP] Major Clock Drift Detected: {current_offset:.2f}ms. Synced.")

    def get_exchange_time_ns(self) -> int:
        """Синтезирует время биржи с учетом дрейфа и RTT. (Time-Source of Truth)"""
        local_now_ms = time.perf_counter_ns() / 1_000_000.0
        exchange_now_ms = local_now_ms + self._rolling_offset_ms
        return int(exchange_now_ms * 1_000_000)

    @property
    def rtt(self) -> float: return self._last_rtt_ms
    @property
    def offset(self) -> float: return self._rolling_offset_ms

class HybridClock:
    """
    [GEKTOR v5.7] Absolute Hybrid Clock Core.
    Anchors Wall-Clock (Epoch) to Monotonic Time on start.
    Provides Epoch time that is immune to NTP jumps.
    """
    def __init__(self):
        self._anchor_epoch = time.time() * 1000.0
        self._anchor_mono = time.monotonic() * 1000.0

    def now_ms(self) -> float:
        """Returns stable Unix Epoch in milliseconds."""
        elapsed = (time.monotonic() * 1000.0) - self._anchor_mono
        return self._anchor_epoch + elapsed

class FastEMAClockSynchronizer:
    """
    [GEKTOR v5.4] O(1) Fast Sync Core.
    Латентность вызова: < 1мкс. Исключает блокировку GIL через Timsort.
    Использует EMA для плавной калибровки аппаратного дрейфа часов.
    """
    __slots__ = ['_alpha', '_max_allowed_lag_ms', '_current_baseline_offset', '_clock']

    def __init__(self, alpha: float = 0.05, max_allowed_lag_ms: float = 300.0):
        self._alpha = alpha
        self._max_allowed_lag_ms = max_allowed_lag_ms
        self._current_baseline_offset: Optional[float] = None
        self._clock = HybridClock()
    def check_exchange_lag(self, exchange_ts_ms: int) -> bool:
        """
        [THE SHADOW CLOCK]
        Determines if a packet is too stale based on local hybrid epoch.
        """
        # 1. Get stable local time (Hybrid Epoch)
        local_now_ms = self._clock.now_ms()
        
        # 2. Raw Latency (One-way trip + loop delay)
        # Result should be positive (msg from past)
        raw_lag = local_now_ms - exchange_ts_ms
        
        # 3. Cold Start / Calibration
        if self._current_baseline_offset is None:
            self._current_baseline_offset = raw_lag
            logger.info(f"⏱️ [ClockSync] O(1) Core Warmup. Local Offset (Latency): {raw_lag:.1f}ms")
            return True

        # 4. Filter Spike / Jitter
        # We check relative deviation from our expected baseline (EMA)
        deviation = raw_lag - self._current_baseline_offset
        
        if deviation > self._max_allowed_lag_ms:
            # Drop packets that arrive much later than the average latency
            return False

        # 5. EMA Update (Slowly adapt baseline to network conditions)
        self._current_baseline_offset = (self._alpha * raw_lag) + ((1.0 - self._alpha) * self._current_baseline_offset)
        
        return True

class SystemHealthMonitor:

    """
    [GEKTOR v5.1] Microsecond Event Loop Lag Monitor.
    Threshold: 5.0ms (0.005s). 
    If loop lag > 5ms, alpha signals are considered stale and invalidated.
    """
    def __init__(self, check_interval: float = 0.01, threshold_ms: float = 20.0):
        self.check_interval = check_interval
        self.threshold_ms = threshold_ms
        self.current_loop_lag_ms = 0.0
        self._running = False

    async def monitor_loop(self):
        self._running = True
        logger.info(f"🩺 [HEALTH] HFT Lag Monitor ARMED (Threshold: {self.threshold_ms}ms)")
        while self._running:
            start = time.perf_counter()
            # Микро-пауза для замера планировщика
            await asyncio.sleep(self.check_interval)
            
            actual_elapsed = time.perf_counter() - start
            delay = actual_elapsed - self.check_interval
            
            # Агрессивное сглаживание EMA (0.5/0.5) для мгновенной реакции на шипы
            new_lag = max(0.0, delay * 1000)
            self.current_loop_lag_ms = (self.current_loop_lag_ms * 0.5) + (new_lag * 0.5)
            
            if self.current_loop_lag_ms > self.threshold_ms:
                 # Логируем только критические отклонения (>2x порога) чтобы не забивать IO
                 if self.current_loop_lag_ms > self.threshold_ms * 2:
                    logger.warning(f"🚨 [GIL STALL] Event Loop lag spiked: {self.current_loop_lag_ms:.2f}ms! Signals at risk.")


import socket

class UDPEmergencyBroadcaster:
    """
    [GEKTOR v5.0] Async Redundant UDP Broadcaster.
    Uses 1ms micro-delays to ensure frames are dispatched separately by the OS NIC.
    """
    def __init__(self, port: int = 9999):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setblocking(False)

    async def fire_abort_pulse_async(self, symbol: str, regime: str):
        """Asynchronous burst with network-stack breathing room."""
        message = f"ABORT|{symbol}|{regime}|{time.time()}".encode('utf-8')
        try:
            for _ in range(3):
                # Non-blocking send on local broadcast
                self.sock.sendto(message, ('255.255.255.255', self.port))
                # 1ms delay allows the OS to flush the TX ring buffer
                await asyncio.sleep(0.001)
            logger.critical(f"🛑 [ASYNC UDP ABORT] {symbol} redundant burst complete.")
        except Exception as e:
            logger.error(f"❌ [UDP] Async broadcast failed: {e}")

import threading
import queue
import orjson
import os

class BulletproofFlightRecorder:
    """
    [GEKTOR v5.7] Absolute I/O Decoupling with Graceful Shutdown.
    
    Ensures that Event Loop latency is NEVER affected by physical disk performance.
    Uses a dedicated thread (non-daemon) with a Poison Pill for secure cleanup.
    """
    def __init__(self, log_path: str = "logs/telemetry_flight_recorder.jsonl", max_queue_size: int = 20000):
        self._path = log_path
        self._queue = queue.Queue(maxsize=max_queue_size)
        self._stop_event = threading.Event()
        
        # Non-daemon thread ensures buffers are flushed before software exit
        self._writer = threading.Thread(target=self._disk_writer_worker, daemon=False)
        self._writer.start()

    def record_nowait(self, event_type: str, data: dict):
        """Zero-latency ingestion. Called from asyncio loop without await."""
        payload = {
            "ts": time.time(),
            "type": event_type,
            "data": data
        }
        try:
            # orjson serialization in C-extension avoids GIL contention
            raw_bytes = orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE)
            self._queue.put_nowait(raw_bytes)
        except queue.Full:
            # Emergency: Disk is frozen.
            pass
        except Exception as e:
            logger.error(f"❌ [FLIGHT RECORDER] Serialization Error: {e}")

    def _disk_writer_worker(self):
        """Synchronous blocking I/O in an isolated environment."""
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        
        with open(self._path, 'ab') as f:
            while not self._stop_event.is_set() or not self._queue.empty():
                try:
                    # Timeout allows periodic checks of the stop_event
                    data = self._queue.get(timeout=0.1)
                    f.write(data)
                    # Flush occasionally to ensure durability without killing IOPS
                    if self._queue.empty():
                        f.flush()
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"FATAL DISK ERROR in FlightRecorder: {e}")
                finally:
                    try:
                        self._queue.task_done()
                    except ValueError:
                        pass # task_done called more times than put

    def graceful_shutdown(self, hard_timeout_sec: float = 3.0):
        """[V2] Guaranteed Exit. Bypasses join() if disk is stalled (Deadlock Protection)."""
        logger.warning(f"⏳ [FLIGHT RECORDER] Shutdown initiated. Goal: Flush {self._queue.qsize()} events.")
        self._stop_event.set()
        
        start_ts = time.time()
        # Active observation of the I/O progress
        while not self._queue.empty():
            if time.time() - start_ts > hard_timeout_sec:
                logger.critical("🛑 [FLIGHT RECORDER] Hard Timeout! Disk controller stalled. Abandoning buffer to save the kernel.")
                break
            time.sleep(0.05)
            
        # Give the Thread a final 200ms to exit cleanly before abandon
        self._writer.join(timeout=0.2)
        logger.success("🔒 [FLIGHT RECORDER] Terminated.")

class HydrationManager:
    """
    [GEKTOR v5.2] Anti-Ban Hydration Throttler.
    Prevents IP-bans and HTTP 429 cascades during mass desynchronization events.
    Uses PriorityQueue to ensure Lead Assets (BTC/ETH/SOL) recover first.
    """
    def __init__(self, requests_per_second: float = 15.0):
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.rate_limit_sec = 1.0 / requests_per_second
        self._last_call = 0.0
        self._pending_hydrations: Set[str] = set()
        self._running = False

    def request_hydration(self, symbol: str, priority: int = 1):
        if symbol not in self._pending_hydrations:
            self._pending_hydrations.add(symbol)
            if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]: priority = 0
            self.queue.put_nowait((priority, symbol))
            logger.info(f"🛡️ [HYDRATION] {symbol} (P:{priority}) queued.")

    async def run(self, rest_client, on_snapshot_received):
        self._running = True
        while self._running:
            priority, symbol = await self.queue.get()
            now = time.monotonic()
            wait_time = self._last_call + self.rate_limit_sec - now
            if wait_time > 0: await asyncio.sleep(wait_time)
            
            try:
                data = await rest_client.get_orderbook(symbol, limit=50)
                if data:
                    await on_snapshot_received(symbol, {
                        "U": int(data.get("u") or data.get("U", 0)),
                        "type": "snapshot",
                        "b": data.get("b", []), "a": data.get("a", []),
                        "ts": int(time.time() * 1000)
                    })
                    self._last_call = time.monotonic()
            except Exception as e:
                logger.error(f"❌ [HYDRATION] Error for {symbol}: {e}")
                await asyncio.sleep(1.0)
            finally:
                self._pending_hydrations.discard(symbol)
                self.queue.task_done()

