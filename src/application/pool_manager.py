import asyncio
import os
import psutil
from concurrent.futures import ProcessPoolExecutor
from loguru import logger
from typing import Any

class PipelineStarvationError(Exception):
    """Raised when worker pool fails to provide a slot in time."""
    pass

class WorkerDispatchController:
    """
    [GEKTOR v2.2] Аппаратная изоляция: Паттерн Bulkhead для защиты CORE-маршрутизации.
    """
    def __init__(self, core_workers: int = 2, noise_workers: int = 4):
        # Выделенный пул ТОЛЬКО для Поводырей (Priority 0).
        self._core_pool = ProcessPoolExecutor(max_workers=core_workers)
        # Пул для периферии (Priority 2)
        self._noise_pool = ProcessPoolExecutor(max_workers=noise_workers)
        
        # Семафоры строго соответствуют размеру пулов
        self._core_semaphore = asyncio.Semaphore(core_workers)
        self._noise_semaphore = asyncio.Semaphore(noise_workers)

    async def execute_with_priority(self, symbol: str, priority: int, coro_func, *args) -> Any:
        # Маршрутизация по независимым магистралям
        if priority == 0:
            return await self._execute_core(symbol, coro_func, *args)
        else:
            return await self._execute_noise(symbol, coro_func, *args)

    async def _execute_core(self, symbol: str, coro_func, *args) -> Any:
        """Зеленая магистраль. Бесконечное ожидание, но пул застрахован от NOISE."""
        await self._core_semaphore.acquire()
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._core_pool, coro_func, symbol, *args)
        finally:
            self._core_semaphore.release()

    async def _execute_noise(self, symbol: str, coro_func, *args) -> Any:
        """Периферия с жестким троттлингом."""
        try:
            # Если шум забил свою часть пула, новый шум эвтаназируется мгновенно
            await asyncio.wait_for(self._noise_semaphore.acquire(), timeout=0.5)
        except asyncio.TimeoutError:
            raise PipelineStarvationError(f"Noise pool starved. Rejecting {symbol} for Euthanasia.")
        
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._noise_pool, coro_func, symbol, *args)
        finally:
            self._noise_semaphore.release()

    def replace_pools(self, core_workers, noise_workers):
        """Soft Recycle functionality: creates new pools."""
        old_core = self._core_pool
        old_noise = self._noise_pool
        
        self._core_pool = ProcessPoolExecutor(max_workers=core_workers)
        self._noise_pool = ProcessPoolExecutor(max_workers=noise_workers)
        
        return old_core, old_noise

    def shutdown(self):
        self._core_pool.shutdown(wait=True)
        self._noise_pool.shutdown(wait=True)


class WorkerPoolManager:
    """
    [GEKTOR v2.2] Orchestrates ProcessPool lifecycle with Strict Semaphore Triage via Bulkheads.
    """
    def __init__(self, core_workers: int = 2, noise_workers: int = 4, memory_threshold_mb: int = 1024):
        self.core_workers = core_workers
        self.noise_workers = noise_workers
        self.memory_threshold = memory_threshold_mb
        self.process = psutil.Process(os.getpid())
        
        # [TRIAGE PIPELINE - BULKHEAD]
        self.dispatcher = WorkerDispatchController(core_workers, noise_workers)
        
        self._lock = asyncio.Lock()
        self._running = True

    def requires_recycle(self) -> bool:
        """DYNAMIC MONITOR RSS physical memory check. O(1) complexity."""
        try:
            current_mem = self.process.memory_info().rss / (1024 * 1024)
            if current_mem > self.memory_threshold:
                logger.warning(f"🚨 [PoolManager] Memory Threshold breach: {current_mem:.1f} MB.")
                return True
        except Exception as e:
            logger.error(f"⚠️ [PoolManager] RSS Check failure: {e}")
        return False

    async def execute_batch(self, target_function, symbol: str, *args, priority: int = 2):
        """
        [TRIAGE FAST PATH]
        priority 0: CORE (BTC/ETH) - Never dropped, allowed to queue on Core Pool
        priority 2: Peripheral (Noise) - Euthanized if Noise pool is starved
        """
        try:
            return await self.dispatcher.execute_with_priority(
                symbol, priority, target_function, *args
            )
        except PipelineStarvationError as e:
            raise RuntimeError(str(e)) # Re-raise as RuntimeError for Orchestrator to catch

    async def memory_watchdog_loop(self):
        """Background RSS Monitor."""
        logger.info("🛡️ [PoolManager] Adaptive RSS Watchdog ARMED.")
        while self._running:
            await asyncio.sleep(10)
            try:
                if self.requires_recycle():
                    async with self._lock:
                        await self._trigger_soft_recycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"⚠️ [PoolManager] Watchdog Error: {e}")

    async def _trigger_soft_recycle(self):
        logger.warning(f"🔄 [PoolManager] Memory-based Soft Recycle initiated...")
        
        old_core, old_noise = self.dispatcher.replace_pools(self.core_workers, self.noise_workers)
        
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, self._shutdown_old_pool, old_core)
        loop.run_in_executor(None, self._shutdown_old_pool, old_noise)
        
        logger.success("✅ [PoolManager] Memory rotation complete (Bulkheads re-established).")

    def _shutdown_old_pool(self, pool: ProcessPoolExecutor):
        try:
            pool.shutdown(wait=True)
        except Exception as e:
            logger.warning(f"[PoolManager] Old pool shutdown error: {e}")

    def shutdown_all(self):
        self._running = False
        self.dispatcher.shutdown()
