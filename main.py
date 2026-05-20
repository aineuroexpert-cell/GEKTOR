import asyncio
import sys
import signal
import logging
from typing import NoReturn, Optional
from datetime import datetime, timezone
from src.infrastructure.config import settings
from src.infrastructure.database import DatabaseManager
from src.infrastructure.telegram_notifier import TelegramRadarNotifier

# Настройка высокопроизводительного логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GEKTOR_RADAR")

class GektorRadarCore:
    """
    Единая точка входа. Инкапсулирует инициализацию, Event Loop и Graceful Shutdown.
    """
    def __init__(self, env: str = "local"):
        self.env = env
        self._is_running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.db = DatabaseManager()
        self.tg = TelegramRadarNotifier(db_manager=self.db, bot_token=settings.bot_token, chat_id=settings.chat_id)
        
        # [GEKTOR STRIKE] Wiping sensitive data from config/env immediately after TelegramNotifier is initialized
        settings.wipe_sensitive()

        from src.application.outbox_relay import OutboxRepository, TelegramRelayWorker
        self.outbox_repo = OutboxRepository(self.db)
        self.outbox_relay = TelegramRelayWorker(repo=self.outbox_repo, tg_client=self.tg)

    async def _alert_engine(self) -> None:
        """
        Изолированный асинхронный воркер для отправки Telegram-алертов.
        Гарантирует, что сетевое трение API Telegram не заблокирует ингестию котировок.
        """
        logger.info(f"[ALERT ENGINE] Запущен в среде: {self.env}")
        try:
            await self.outbox_relay.run()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[ALERT ENGINE] Сбой Outbox Relay: {e}")

    async def _radar_engine(self) -> None:
        """
        Основной квант-движок. Advisory Mode.
        Никакой интеграции с REST/WS для отправки ордеров.
        """
        logger.info("[RADAR ENGINE] Поиск среднесрочных аномалий активирован.")
        while self._is_running:
            # TODO: Zero-Copy маппинг стаканов и каузальное сжатие (Dollar Bars)
            await asyncio.sleep(0.1)

    async def startup(self) -> None:
        self._is_running = True
        logger.info("[SYSTEM] Инициализация GEKTOR APEX (Advisory Mode)...")
        
        # Инициализация DatabaseManager (WAL)
        await self.db.initialize()
        
        # Инициализация Telegram-нотифиера
        await self.tg.start()
        
        # Отправка стартового оповещения
        await self.tg.notify_manual(
            "🟢 <b>[GEKTOR APEX] Система выведена на орбиту</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📡 L2-Радар активен (Advisory Mode)\n"
            f"🌍 Окружение: <code>{self.env}</code>\n"
            f"⏰ <code>{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>СТАТУС: МОНИТОРИНГ ЗАПУЩЕН</b>",
            "STARTUP"
        )
        
        # Запуск подсистем конкурентно
        await asyncio.gather(
            self._alert_engine(),
            self._radar_engine()
        )

    async def shutdown(self, sig: signal.Signals) -> None:
        logger.warning(f"[SYSTEM] Получен сигнал {sig.name}. Начат Graceful Shutdown.")
        self._is_running = False
        
        # Останавливаем воркер релея
        self.outbox_relay.stop()
        
        # Отправка оповещения о завершении
        await self.tg.notify_manual(
            f"🔴 <b>[GEKTOR APEX] Завершение работы</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🧬 Причина: Сигнал <code>{sig.name}</code>\n"
            f"⏰ <code>{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🛑 <b>СТАТУС: СИСТЕМА ОСТАНОВЛЕНА</b>",
            "SHUTDOWN"
        )
        
        # Ожидаем завершения отправки алертов из очереди
        try:
            await asyncio.timeout(3.0, self.tg._queue.join())
        except Exception:
            pass
        await self.tg.stop()
        await self.db.close()
        
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        
        logger.info(f"[SYSTEM] Ожидание отмены {len(tasks)} фоновых задач...")
        await asyncio.gather(*tasks, return_exceptions=True)
        self._loop.stop()
        logger.info("[SYSTEM] Контур безопасно обесточен.")

    async def hot_reload(self) -> None:
        logger.warning("[SYSTEM] Получен сигнал SIGHUP. Запуск Hot Reload...")
        self._is_running = False
        
        # Останавливаем воркер релея
        self.outbox_relay.stop()
        
        # Отправка оповещения о горячей перезагрузке
        await self.tg.notify_manual(
            "🔄 <b>[GEKTOR APEX] Горячая перезагрузка</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ Выполняется перезапуск процесса (Hot Reload)...\n"
            f"⏰ <code>{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⏳ <b>СТАТУС: ПЕРЕЗАПУСК</b>",
            "SHUTDOWN"
        )
        
        # Ожидаем завершения отправки алертов из очереди
        try:
            await asyncio.timeout(3.0, self.tg._queue.join())
        except Exception:
            pass
        await self.tg.stop()
        await self.db.close()
        
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        
        logger.info(f"[SYSTEM] Ожидание отмены {len(tasks)} фоновых задач перед hot reload...")
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("[SYSTEM] Перезапуск процесса через os.execv...")
        import os
        os.execv(sys.executable, [sys.executable] + sys.argv)

def main() -> NoReturn:
    # Оптимизация Event Loop (uvloop для Linux-сервера)
    if sys.platform != "win32":
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            logger.warning("uvloop не найден. Используется стандартный asyncio.")

    core = GektorRadarCore(env="production" if sys.platform != "win32" else "local")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    core._loop = loop

    # Перехват системных сигналов для предотвращения повреждения стейта
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(core.shutdown(s)))
        # SIGHUP для атомарной Blue-Green перезагрузки без перезапуска systemd юнита
        loop.add_signal_handler(signal.SIGHUP, lambda: asyncio.create_task(core.hot_reload()))
    else:
        # На Windows перехват сигналов ограничен
        pass

    try:
        loop.run_until_complete(core.startup())
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()
        sys.exit(0)

if __name__ == "__main__":
    main()