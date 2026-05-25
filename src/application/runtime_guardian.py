import gc
import os
import psutil
import logging

logger = logging.getLogger("GEKTOR.Runtime")

class RuntimeGuardian:
    """
    [GEKTOR v2.0] Контроль сборщика мусора и защита от фрагментации (RSS-Гвардия).
    """
    __slots__ = ['max_rss_bytes', 'process']

    def __init__(self, max_rss_mb: int = 1024):
        self.max_rss_bytes = max_rss_mb * 1024 * 1024
        self.process = psutil.Process(os.getpid())
        
        # 1. Отключаем автоматический STW (Stop-The-World) сборщик мусора
        # Память будет расти, но мы избегаем рандомных фризов Event Loop
        gc.disable()
        logger.warning("🛡️ [RUNTIME] Automatic Garbage Collection DISABLED. Hot-path secured.")

    def check_memory_and_collect(self, is_flatline: bool) -> bool:
        """
        Вызывается Оркестратором.
        Возвращает False, если воркер необратимо раздут и требует Soft Recycle.
        """
        current_rss = self.process.memory_info().rss
        
        # Если память пробила физический лимит -> Сигнал на перерождение
        if current_rss > self.max_rss_bytes:
            logger.critical(f"⚠️ [RSS GUARD] Memory limit breached: {current_rss / 1024**2:.1f}MB.")
            return False 

        # 2. Ручная сборка запускается ТОЛЬКО когда на рынке штиль (Flatline)
        if is_flatline:
            collected = gc.collect()
            if collected > 0:
                logger.debug(f"🧹 [GC] Manual collection during Flatline completed. Freed {collected} objects.")
        
        return True
