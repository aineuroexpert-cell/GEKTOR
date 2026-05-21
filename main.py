import asyncio
import sys
import signal
import logging
from typing import NoReturn, Optional
from datetime import datetime, timezone
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
        [GEKTOR v3.0.0] Predator Radar — Mid-Term Anomaly Scanner.

        Full pipeline:
          1. Discover ALL USDT-Linear futures via Bybit REST API
          2. Subscribe to publicTrade WS streams for all symbols
          3. Route every trade through RadarPipeline:
             Trade → Dollar Bar → VPIN → Anomaly → Telegram Alert

        Runs indefinitely. Auto-refreshes symbol list every 6 hours.
        """
        from src.application.radar_pipeline import RadarPipeline
        from src.infrastructure.bybit import BybitRestClient, BybitIngestor

        logger.info("[RADAR ENGINE] Инициализация хищного радара...")

        # REST client for symbol discovery (no auth needed for public endpoints)
        rest = BybitRestClient(
            proxy_url=settings.TG_PROXY_URL if settings.USE_PROXY_FOR_BYBIT else None
        )

        # Create radar pipeline
        pipeline = RadarPipeline(
            tg_notify_callback=self.tg.notify_manual,
            db_push_callback=self.db.push_query,
            dollar_threshold_usd=float(settings.DOLLAR_THRESHOLD_BASE),
            vpin_window=alpha.VPIN_WINDOW_SIZE if alpha.VPIN_WINDOW_SIZE > 0 else 50,
            vpin_z_threshold=alpha.VPIN_ANOMALY_Z if alpha.VPIN_ANOMALY_Z > 0 else 2.5,
            alert_cooldown_sec=alpha.SIGNAL_COOLDOWN_SEC if alpha.SIGNAL_COOLDOWN_SEC > 0 else 300.0,
        )
        pipeline.start()

        # Discover symbols
        try:
            all_symbols = await rest.fetch_active_symbols()
            if not all_symbols:
                all_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
                logger.warning(f"[RADAR] Discovery failed. Fallback: {all_symbols}")
        except Exception as e:
            all_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
            logger.error(f"[RADAR] REST discovery error: {e}. Fallback: {all_symbols}")

        logger.success(f"[RADAR] 📡 Обнаружено {len(all_symbols)} фьючерсов. Подключаю ленту сделок...")

        # Trade callback wired to pipeline
        async def on_trade(symbol: str, tick: dict) -> None:
            await pipeline.on_trade(symbol, tick)

        async def on_snapshot(symbol: str, data: dict) -> None:
            pass  # L2 not needed for Advisory VPIN radar

        def on_critical_alert(msg: str) -> None:
            logger.error(f"🚨 [INGESTOR] {msg}")

        # Create WS ingestor for ALL symbols
        ingestor = BybitIngestor(
            symbols=all_symbols,
            on_tick_callback=on_trade,
            on_snapshot_callback=on_snapshot,
            alert_callback=on_critical_alert,
            proxy_url=settings.TG_PROXY_URL if settings.USE_PROXY_FOR_BYBIT else None,
        )

        # Send startup confirmation with symbol count
        await self.tg.notify_manual(
            f"🎯 <b>[RADAR] Хищник вышел на охоту</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 Фьючерсов: <code>{len(all_symbols)}</code>\n"
            f"💰 Порог бара: <code>${float(settings.DOLLAR_THRESHOLD_BASE):,.0f}</code>\n"
            f"🧬 VPIN окно: <code>{pipeline._vpin_window}</code>\n"
            f"⚡ Z-порог: <code>{pipeline._vpin_z_threshold}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ <b>СКАНИРОВАНИЕ АКТИВНО</b>",
            "STARTUP"
        )

        # Health report task
        async def _health_report_loop():
            while self._is_running:
                await asyncio.sleep(1800)  # Every 30 minutes
                try:
                    stats = await pipeline.get_stats()
                    await self.tg.notify_manual(
                        f"📊 <b>[RADAR] Статус</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📡 Символов: <code>{stats['symbols_tracked']}</code>\n"
                        f"🔢 Тиков: <code>{stats['total_ticks']:,}</code>\n"
                        f"📦 Баров: <code>{stats['total_bars_closed']:,}</code>\n"
                        f"🎯 Аномалий: <code>{stats['total_anomalies']}</code>\n"
                        f"⚡ Тиков/сек: <code>{stats['ticks_per_sec']}</code>\n"
                        f"⏱ Аптайм: <code>{stats['uptime_hours']}h</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━",
                        "HEALTH"
                    )
                except Exception as e:
                    logger.error(f"[RADAR] Health report error: {e}")

        # Launch health reporter as background task
        health_task = asyncio.create_task(_health_report_loop())

        try:
            # Run ingestor (blocks until shutdown)
            await ingestor.start()
        except asyncio.CancelledError:
            logger.info("[RADAR] Radar engine cancelled.")
        finally:
            health_task.cancel()
            await ingestor.stop()
            await rest.close()
            logger.info("[RADAR] Radar engine stopped.")

    async def startup(self) -> None:
        self._is_running = True
        logger.info("[SYSTEM] Инициализация GEKTOR APEX (Advisory Mode)...")
        
        # Инициализация DatabaseManager (WAL)
        await self.db.initialize()
        
        # Инициализация Telegram-нотифиера
        await self.tg.start()
        
        # [GEKTOR STRIKE] Delayed secret wiping to resolve Race Condition (J).
        # Gives background tasks (WS ingestor/rest client) 5 seconds to boot up and read settings.
        async def delayed_wipe():
            await asyncio.sleep(5.0)
            settings.wipe_sensitive()
            logger.info("🔒 [SECURITY] Sensitive credentials physically wiped from memory.")
        
        asyncio.create_task(delayed_wipe())
        
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
            async with asyncio.timeout(3.0):
                await self.tg._queue.join()
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning("[SYSTEM] Telegram queue drain timeout (3s). Force stopping.")
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
            async with asyncio.timeout(3.0):
                await self.tg._queue.join()
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning("[SYSTEM] Telegram queue drain timeout (3s). Force stopping.")
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