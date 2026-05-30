import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import NoReturn

from src.application.outbox_alert_sink import OutboxAlertSink, OutboxLiquiditySink
from src.application.radar_pipeline import RadarPipeline
from src.application.watchdog import PartialBlindnessWatchdog
from src.domain.liquidity_detectors import (
    LargePrintDetector,
    LiquidityDetectorBank,
    OFIPulseDetector,
    SweepDetector,
)
from src.infrastructure.adaptive_threshold import (
    AdaptiveDollarThresholdProvider,
    adaptive_threshold_refresher,
)
from src.infrastructure.bybit import BybitRestClient
from src.infrastructure.bybit_ws_ingestion import BybitWSIngestion
from src.infrastructure.config import resolve_sensitivity, settings
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
        self._loop: asyncio.AbstractEventLoop | None = None
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

        # --- Quantitative core (v3.6.2 APEX-RADAR) ---
        self.bybit_rest = BybitRestClient(
            proxy_url=settings.PROXY_URL if settings.USE_PROXY_FOR_BYBIT else None
        )

        # Sensitivity tier translation (v3.6.2). Operator picks one of
        # conservative|active|scanner in .env; we resolve to concrete
        # radar params here. AlphaConfig overrides take precedence if
        # the operator has tuned them manually (alpha.VPIN_ANOMALY_Z != 0).
        tier = resolve_sensitivity(settings.SENSITIVITY)
        z_threshold = (
            alpha.VPIN_ANOMALY_Z if alpha.VPIN_ANOMALY_Z else float(tier["z_threshold"])
        )
        vpin_window = (
            alpha.VPIN_WINDOW_SIZE
            if alpha.VPIN_WINDOW_SIZE
            else int(tier["vpin_window"])
        )
        cooldown_sec = float(
            os.getenv("RADAR_COOLDOWN_SEC", str(tier["cooldown_sec"]))
        )
        logger.info(
            f"[CONFIG] Sensitivity tier='{settings.SENSITIVITY}' resolved to "
            f"z={z_threshold}, window={vpin_window}, cooldown={cooldown_sec}s"
        )

        threshold_usd_env = float(
            os.getenv("DOLLAR_THRESHOLD_BASE", str(settings.DOLLAR_THRESHOLD_BASE))
        )

        # Adaptive per-symbol dollar threshold provider (v3.6.2).
        # Empty cache until first refresh() call; threshold_for() falls
        # back to threshold_usd_env until then.
        self.threshold_provider: AdaptiveDollarThresholdProvider | None = None
        if settings.ADAPTIVE_THRESHOLD_ENABLE:
            self.threshold_provider = AdaptiveDollarThresholdProvider(
                rest_client=self.bybit_rest,
                target_bars_per_day=settings.ADAPTIVE_TARGET_BARS_PER_DAY,
                min_usd=settings.ADAPTIVE_MIN_USD,
                max_usd=settings.ADAPTIVE_MAX_USD,
                default_usd=threshold_usd_env,
            )
            logger.info(
                "[CONFIG] Adaptive threshold provider enabled "
                f"(target_bars/day={settings.ADAPTIVE_TARGET_BARS_PER_DAY}, "
                f"min=${settings.ADAPTIVE_MIN_USD:,.0f}, "
                f"max=${settings.ADAPTIVE_MAX_USD:,.0f})"
            )

        # Liquidity detectors (instant-fire, no warmup) — v3.6.2.
        self.liquidity_bank: LiquidityDetectorBank | None = None
        if settings.LIQUIDITY_DETECTORS_ENABLE:
            sweep = SweepDetector(
                min_trades=settings.SWEEP_MIN_TRADES,
                window_sec=settings.SWEEP_WINDOW_SEC,
                min_notional_usd=settings.SWEEP_MIN_NOTIONAL_USD,
                cooldown_sec=cooldown_sec,
            )
            # LargePrintDetector needs a turnover provider. If adaptive
            # threshold provider is on, reuse its cache; else stub returns 0
            # which makes the detector use the absolute floor only.
            if self.threshold_provider is not None:
                turnover_provider = self.threshold_provider.turnover_for
            else:
                def turnover_provider(_symbol: str) -> float:
                    return 0.0

            large_print = LargePrintDetector(
                turnover_provider=turnover_provider,
                pct_threshold=settings.LARGE_PRINT_PCT_THRESHOLD,
                min_notional_usd=settings.LARGE_PRINT_MIN_NOTIONAL_USD,
                cooldown_sec=cooldown_sec,
            )
            ofi_pulse = OFIPulseDetector(
                bucket_sec=settings.OFI_PULSE_BUCKET_SEC,
                history_buckets=settings.OFI_PULSE_HISTORY_BUCKETS,
                k=settings.OFI_PULSE_K,
                min_notional_usd=settings.OFI_PULSE_MIN_NOTIONAL_USD,
                cooldown_sec=cooldown_sec * 2.0,
            )
            self.liquidity_bank = LiquidityDetectorBank(
                sweep=sweep,
                large_print=large_print,
                ofi_pulse=ofi_pulse,
            )
            logger.info(
                "[CONFIG] Liquidity detectors enabled: "
                f"Sweep(N={settings.SWEEP_MIN_TRADES}, ${settings.SWEEP_MIN_NOTIONAL_USD:,.0f}) "
                f"LargePrint({settings.LARGE_PRINT_PCT_THRESHOLD * 100:.2f}% of 24h) "
                f"OFI Pulse(k={settings.OFI_PULSE_K})"
            )

        self.alert_sink = OutboxAlertSink(self.db)
        self.liquidity_sink = OutboxLiquiditySink(self.db)
        self.radar = RadarPipeline(
            threshold_usd=threshold_usd_env,
            alert_sink=self.alert_sink,
            window_size=vpin_window,
            z_threshold=z_threshold,
            z_history_size=500,
            per_symbol_cooldown_sec=cooldown_sec,
            threshold_provider=(
                self.threshold_provider.threshold_for if self.threshold_provider else None
            ),
            liquidity_detectors=self.liquidity_bank,
            liquidity_alert_sink=self.liquidity_sink,
        )
        self._ws_tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

        async def _watchdog_sink(kind: str, metrics: dict) -> None:
            """Bridge watchdog events to the operator via the existing Outbox."""
            try:
                from datetime import datetime
                from datetime import timezone as _tz
                payload_text = (
                    "⚠️ <b>[GEKTOR APEX] PARTIAL BLINDNESS</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Событие: <code>{kind}</code>\n"
                    f"💡 Символов: {metrics.get('symbols_tracked', 0)}\n"
                    f"📦 ticks={metrics.get('tick_count', 0)} bars={metrics.get('bar_count', 0)}\n"
                    f"⏰ {datetime.now(_tz.utc).strftime('%H:%M:%S')} UTC\n"
                    "━━━━━━━━━━━━━━━━━━━━━━"
                )
                await self.tg.notify_manual(payload_text, alert_type=kind)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[WATCHDOG] alert dispatch failed: {exc}")

        self.watchdog = PartialBlindnessWatchdog(
            pipeline=self.radar,
            alert_sink=_watchdog_sink,
            silence_threshold_sec=float(os.getenv("WATCHDOG_SILENCE_SEC", "60")),
            poll_interval_sec=float(os.getenv("WATCHDOG_POLL_SEC", "10")),
        )

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

        # v3.6.2: prime the adaptive threshold cache BEFORE listing
        # symbols, so the first bars closed already use per-symbol
        # sizing. A failure here is non-fatal — provider will fall back
        # to the base threshold and the next refresh tick can recover.
        if self.threshold_provider is not None:
            await self.threshold_provider.refresh()
            asyncio.create_task(
                adaptive_threshold_refresher(
                    self.threshold_provider,
                    interval_sec=settings.ADAPTIVE_REFRESH_SEC,
                    stop_event=self._shutdown_event,
                ),
                name="adaptive_threshold_refresher",
            )

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
                f"liq_alerts={m['liquidity_alert_count']} "
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
            self._radar_engine(),
            self.watchdog.run(self._shutdown_event),
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

        # Ожидаем завершения отправки алертов из очереди.
        # v3.6.2 фикс: используем asyncio.wait_for() для корректного timeout.
        try:
            await asyncio.wait_for(self.tg._queue.join(), timeout=3.0)
        except asyncio.TimeoutError:
            logger.warning("[SYSTEM] Telegram queue drain timed out (shutdown); some alerts may be lost.")
        except Exception as exc:
            logger.error(f"[SYSTEM] Telegram queue drain failed during shutdown: {exc!r}")
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

        # Ожидаем завершения отправки алертов (см. комментарий в shutdown() выше).
        try:
            await asyncio.wait_for(self.tg._queue.join(), timeout=3.0)
        except asyncio.TimeoutError:
            logger.warning("[SYSTEM] Telegram queue drain timed out (hot_reload); some alerts may be lost.")
        except Exception as exc:
            logger.error(f"[SYSTEM] Telegram queue drain failed during hot_reload: {exc!r}")
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
    except KeyboardInterrupt:
        logger.warning("[SYSTEM] KeyboardInterrupt перехвачен. Вызов Graceful Shutdown...")
        loop.run_until_complete(core.shutdown(signal.SIGINT))
    except asyncio.CancelledError:
        pass
    finally:
        try:
            loop.close()
        except Exception:
            pass
        sys.exit(0)

if __name__ == "__main__":
    main()
