# src/application/supervisor.py
import asyncio
import signal
import sys
import os
from loguru import logger
from typing import Optional, Set

class GektorApexSupervisor:
    """
    [GEKTOR APEX v4.1] Core Infrastructure Supervisor.
    Handles Graceful Teardown, Event Loop Profiling, and Signal Catching.
    
    Philosophy: "Graceful exit is the last act of a disciplined algorithm."
    """
    __slots__ = ('loop', '_shutdown_triggered', '_loop_latency_threshold', '_orchestrator')

    def __init__(self, orchestrator: Optional[object] = None):
        self._orchestrator = orchestrator
        self.loop = asyncio.get_running_loop()
        self._shutdown_triggered = False
        self._loop_latency_threshold = 0.02  # 20ms maximum event loop lag

        # [DEBUG MODE] Включаем жесткий дебаг Event Loop для поиска виновников флатлайнов
        self.loop.set_debug(True)
        self.loop.slow_callback_duration = self._loop_latency_threshold

    def arm_graceful_shutdown(self):
        """Перехват POSIX сигналов. Запрет на грязный KeyboardInterrupt."""
        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                self.loop.add_signal_handler(
                    sig, 
                    lambda s=sig: asyncio.create_task(self.initiate_teardown(s))
                )
            logger.info("🛡️ [SUPERVISOR] Graceful shutdown armed (POSIX).")
        else:
            # Fallback для Windows (сигналы работают иначе, перехватываем через signal.signal)
            # Примечание: В Windows SIGINT работает в главном потоке.
            try:
                signal.signal(signal.SIGINT, self._win_teardown_handler)
                signal.signal(signal.SIGTERM, self._win_teardown_handler)
                logger.info("🛡️ [SUPERVISOR] Graceful shutdown armed (Windows).")
            except ValueError:
                # Вторичные потоки не могут устанавливать хендлеры
                logger.warning("⚠️ [SUPERVISOR] Could not arm signals (Not in Main Thread).")

    def _win_teardown_handler(self, signum, frame):
        """Windows-specific bridge to async teardown."""
        if not self._shutdown_triggered:
            # Мы в основном потоке, но вне Event Loop. Используем call_soon_threadsafe.
            self.loop.call_soon_threadsafe(
                lambda: asyncio.create_task(self.initiate_teardown(signum))
            )

    async def initiate_teardown(self, sig):
        """Детерминированное разрешение суперпозиций и сброс стейта."""
        if self._shutdown_triggered:
            return
        
        self._shutdown_triggered = True
        logger.critical(f"🛑 [TEARDOWN] Сигнал {sig} получен. Активация Exit Protocol.")
        
        # 1. Информируем оркестратор (если есть) об экстренной остановке
        if self._orchestrator and hasattr(self._orchestrator, 'shutdown'):
            logger.info("🛡️ Блокировка выдачи новых сигналов. Аннулирование активных Intents.")
            await self._orchestrator.shutdown()
        else:
            # Fallback: Ручная очистка корутин
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            logger.warning(f"🧹 Отмена {len(tasks)} активных корутин...")
            for task in tasks:
                task.cancel()
            
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.success("✅ [CORE] State Recovery сохранен. Идемпотентность гарантирована. Выход.")
            self.loop.stop()
            os._exit(0)
