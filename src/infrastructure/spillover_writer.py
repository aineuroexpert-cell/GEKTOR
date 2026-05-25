import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from loguru import logger

class DedicatedSpilloverWriter:
    """
    [GEKTOR v21.64.4] Zero-blocking I/O. Single-writer thread. FIFO.
    Изолирует дисковый I/O от основного Event Loop для предотвращения GIL-штормов.
    """
    
    def __init__(self, filepath: str, max_queue: int = 100_000):
        self.filepath = filepath
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue)
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="spillover")
        self._running = True
        self._drain_task: Optional[asyncio.Task] = None

    def start(self):
        """Активирует фоновый поток записи."""
        loop = asyncio.get_running_loop()
        self._drain_task = loop.create_task(self.run_drain_loop())
        logger.info(f"💾 [Spillover] Dedicated writer started for {self.filepath}")

    def _sync_write(self, batch: List[str]):
        """Синхронная запись в файл (выполняется в ThreadPool)."""
        try:
            # Создаем директорию, если её нет
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            
            with open(self.filepath, "a", encoding="utf-8", buffering=8192) as f:
                f.write("\n".join(batch) + "\n")
                f.flush()
                try:
                    os.fsync(f.fileno())  # жёсткая гарантия записи на диск
                except Exception as e:
                    logger.error("🚨 [Spillover] FILE SYNC ERROR: {}", e, exc_info=True)
        except OSError as e:
            logger.error(f"🚨 [Spillover] CRITICAL FILE I/O ERROR: {e}")

    async def run_drain_loop(self):
        """Асинхронный цикл сбора батчей из очереди."""
        while self._running or not self._queue.empty():
            batch: List[str] = []
            try:
                # Ожидаем появления первого элемента (Batch Trigger)
                item = await asyncio.wait_for(self._queue.get(), timeout=0.08)
                batch.append(item)
                
                # Собираем остальные элементы, которые уже в очереди (до 500 штук)
                while len(batch) < 500 and not self._queue.empty():
                    batch.append(self._queue.get_nowait())
                
                if batch:
                    loop = asyncio.get_running_loop()
                    # Отправляем батч в отдельный поток
                    await loop.run_in_executor(self._pool, self._sync_write, batch)
                    for _ in batch:
                        self._queue.task_done()
                        
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"💥 [Spillover] Drain loop error: {e}")
                await asyncio.sleep(0.1)

    def enqueue(self, payload: str):
        """Неблокирующая постановка в очередь."""
        try:
            self._queue.put_nowait(payload)
        except asyncio.QueueFull:
            # Предотвращение блокировки при переполнении (Lossy mode)
            logger.warning("📉 [Spillover] QUEUE FULL — DROPPING STATE (lossy mode)")

    async def stop(self):
        """Грациозное завершение и сброс остатков."""
        logger.info("🔌 [Spillover] Initiating graceful shutdown...")
        self._running = False
        if self._drain_task:
            try:
                await asyncio.wait_for(self._drain_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("⏰ [Spillover] Shutdown timeout. Forcing pool closure.")
        
        self._pool.shutdown(wait=True)
        logger.success("💤 [Spillover] Shutdown complete.")
