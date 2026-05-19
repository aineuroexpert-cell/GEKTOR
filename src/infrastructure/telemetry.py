# src/infrastructure/telemetry.py
import asyncio
import os
import sys
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, List
from loguru import logger

class ZeroLatencyLogger:
    """
    [GEKTOR v21.66] Zero-Latency Telemetry & Logging Pipeline.
    
    Architecture:
    1. PRODUCER: The trading Event Loop. It enqueues logs into a bounded memory buffer.
    2. CONSUMER: A dedicated background thread (or process) that batches and flushes logs to disk.
    3. BACKPRESSURE: If the queue is full, logs are dropped (LOSS-OVER-LATENCY) to ensure 
       the Event Loop NEVER blocks on I/O.
    """
    
    def __init__(self, log_path: str, max_queue_size: int = 100_000, batch_size: int = 1000):
        self.log_path = log_path
        self.max_queue_size = max_queue_size
        self.batch_size = batch_size
        
        # Lock-free memory queue (Bounded)
        self._queue: deque = deque(maxlen=max_queue_size)
        
        # Dedicated I/O executor (Pinned core recommended at OS level)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="gektor_telemetry")
        
        self._is_running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._drain_task: Optional[asyncio.Task] = None

    def sink(self, message):
        """
        [HOT PATH] Custom Loguru Sink.
        This must be extremely fast. It only puts the message in the queue.
        """
        record = message.record
        # Minimal processing in the hot path
        formatted = f"{record['time'].strftime('%H:%M:%S.%f')} | {record['level'].name: <8} | {record['message']}\n"
        
        try:
            # Atomic append to deque. If maxlen is reached, it automatically drops the oldest.
            # This is O(1) and never blocks.
            self._queue.append(formatted)
        except Exception:
            # Fallback to stderr if everything fails, but avoid blocking
            pass

    def start(self):
        """Activates the background drain loop."""
        if self._is_running:
            return
            
        self._is_running = True
        self._loop = asyncio.get_running_loop()
        self._drain_task = self._loop.create_task(self._drain_loop())
        
        # Configure Loguru to use this sink
        logger.remove() # Remove default stderr sink
        # Add high-speed non-blocking sink
        logger.add(self.sink, level="DEBUG", format="{message}")
        # Re-add stderr sink with higher level for operator visibility
        logger.add(sys.stderr, level="INFO", colorize=True, 
                   format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")
        
        logger.success(f"🚀 [TELEMETRY] Zero-Latency Pipeline ARMED. Path: {self.log_path}")

    async def _drain_loop(self):
        """
        Background worker that batches messages and writes them to disk.
        Runs in the event loop but offloads the blocking write to an executor.
        """
        while self._is_running:
            try:
                if not self._queue:
                    await asyncio.sleep(0.01) # Yield control
                    continue
                
                # Collect batch
                batch = []
                while self._queue and len(batch) < self.batch_size:
                    batch.append(self._queue.popleft())
                
                if batch:
                    # Offload the blocking I/O to a dedicated thread
                    await self._loop.run_in_executor(self._executor, self._sync_flush, batch)
                    
            except Exception as e:
                # We can't log this normally as it might cause recursion
                print(f"💥 [TELEMETRY ERROR] {e}", file=sys.stderr)
                await asyncio.sleep(0.5)

    def _sync_flush(self, batch: List[str]):
        """Synchronous write + fsync. Executed in the ThreadPool."""
        try:
            # Optimization: Use buffered writing on a high-speed disk (NVMe/RAMDISK)
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write("".join(batch))
                f.flush()
                # Crucial for HFT: Ensure data is physically on disk to survive crash
                # but only if not on a RAMDISK (where fsync is less relevant)
                os.fsync(f.fileno())
        except OSError as e:
            print(f"🚨 [TELEMETRY I/O FATAL] {e}", file=sys.stderr)

    async def stop(self):
        """Graceful shutdown: flush remaining logs."""
        logger.warning("🔌 [TELEMETRY] Flushing buffers and shutting down...")
        self._is_running = False
        if self._drain_task:
            await self._drain_task
            
        # Final flush
        if self._queue:
            batch = list(self._queue)
            self._sync_flush(batch)
            
        self._executor.shutdown(wait=True)
        logger.success("💤 [TELEMETRY] Pipeline closed.")
