import asyncio
import sys
import signal
import logging
import os
from decimal import Decimal
from typing import NoReturn, Optional
from datetime import datetime, timezone

from src.application.outbox_alert_sink import OutboxAlertSink
from src.application.radar_pipeline import RadarPipeline
from src.infrastructure.bybit import BybitRestClient
from src.infrastructure.bybit_ws_ingestion import BybitWSIngestion
from src.infrastructure.config import settings
from src.infrastructure.database import DatabaseManager
from src.infrastructure.telegram_notifier import TelegramRadarNotifier
from src.shared.alpha_config import alpha

# Настройка высокопроизводительного логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GEKTOR_RADAR")

WS_URL_LINEAR = "wss://stream.bybit.com/v5/public/linear"
# Cap subscriptions per WS connection to keep below Bybit's per-conn limit.
MAX_SYMBOLS_PER_WS = 180


class GektorRadarCore:
    """
    Единая точка входа. Инкапсулирует инициализацию, Event Loop и Graceful Shutdown.
    """
    def __init__(self, env: str = "local"):
        self.env = env
        self._is_running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.db = DatabaseManager()
        self.tg = TelegramRadarNotifier(
            db_manager=self.db,
            bot_token=settings.bot_token,
            chat_id=settings.chat_id,
            proxy_url=settings.TG_PROXY_URL,
        )

        # [GEKTOR STRIKE] Wiping sensitive data from config/env immediately after TelegramNotifier is initialized
        settings.wipe_sensitive()

        from src.application.outbox_relay import OutboxRepository, TelegramRelayWorker
        self.outbox_repo = OutboxRepository(self.db)
        self.outbox_relay = TelegramRelayWorker(repo=self.outbox_repo, tg_client=self.tg)

        # --- Quantitative core (v3.6.0 APEX-RADAR) ---
        self.bybit_rest = BybitRestClient(proxy_url=settings.PROXY_URL if settings.USE_PROXY_FOR_BYBIT else None)
        threshold_usd_env = float(os.getenv("DOLLAR_THRESHOLD_BASE", "100000"))
        self.alert_sink = OutboxAlertSink(self.db)
        self.radar = RadarPipeline(
            threshold_usd=Decimal(str(threshold_usd_env)),
            alert_sink=self.alert_sink,
            window_size=alpha.VPIN_WINDOW_SIZE,
            z_threshold=alpha.VPIN_ANOMALY_Z if alpha.VPIN_ANOMALY_Z else 2.5,
            z_history_size=500,
            per_symbol_cooldown_sec=float(os.getenv("RADAR_COOLDOWN_SEC", "300")),
        )
        self._ws_tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

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
        """Main quant engine — Advisory Mode pipeline (v3.6.0 APEX-RADAR).

        Discovers active USDT-Linear symbols, chunks them across multiple
        WS connections, and routes each tick through RadarPipeline:
            Bybit WS → RadarPipeline.process_tick()
              → DollarBarEngine.process_tick()
                → on_bar_closed() → O1VPINEngine.process_bar()
                  → [anomaly?] → OutboxAlertSink → outbox_events row
                    → TelegramRelayWorker → Telegram

        No order execution. No REST trade API. Advisory only.
        """
        logger.info("[RADAR ENGINE] Поиск среднесрочных аномалий активирован.")

        symbols = await self.bybit_rest.fetch_active_symbols()
        if not symbols:
            logger.error("[RADAR ENGINE] Discovery вернул пустой список — радар не запустится.")
            return
        logger.info(f"[RADAR ENGINE] Discovery: {len(symbols)} USDT-Linear contracts.")

        # Chunk symbols across WS connections.
        chunks = [
            symbols[i : i + MAX_SYMBOLS_PER_WS]
            for i in range(0, len(symbols), MAX_SYMBOLS_PER_WS)
        ]
        logger.info(f"[RADAR ENGINE] Spawning {len(chunks)} WS connection(s).")

        for chunk in chunks:
            ws = BybitWSIngestion(ws_url=WS_URL_LINEAR, aggregator=self.radar)
            task = asyncio.create_task(ws.run(chunk, self._shutdown_event))
            self._ws_tasks.append(task)

        # Status reporter loop (lightweight, no I/O on hot path).
        while self._is_running:
            await asyncio.sleep(60.0)
            m = self.radar.metrics()
            logger.info(
                f"[RADAR METRICS] ticks={m['tick_count']} bars={m['bar_count']} "
                f"signals={m['signal_count']} alerts={m['alert_count']} "
                f"symbols={m['symbols_tracked']}"
            )

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
        self._shutdown_event.set()

        # Останавливаем WS-задачи радара
        for task in self._ws_tasks:
            task.cancel()

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