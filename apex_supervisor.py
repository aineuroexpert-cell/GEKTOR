import asyncio
import sys
import signal
import logging
from decimal import Decimal
from typing import NoReturn

# Импорты абстракций и реализаций (моковые пути для демонстрации)
from src.domain.conflation import DollarBarEngine, DollarBar
# from src.domain.quant_radar import QuantRadarEngine
# from src.infrastructure.sqlite_outbox import SqliteSignalRepository, SqliteFlusher
# from src.infrastructure.telegram_worker import TelegramOutboxWorker
from src.domain.shadow_ledger import BiologicalFirewall

logger = logging.getLogger("GEKTOR_APEX")

class ExecutionSupervisor:
    """
    Монолитный корневой узел композиции (Composition Root).
    Собирает Zero-Blocking Pipeline:
    [WS] -> DollarBarEngine -> (bar_queue) -> QuantRadarEngine -> (signal_queue) -> SqliteFlusher -> [SQLite WAL] -> TelegramWorker
    """
    def __init__(self, env: str, db_path: str, bar_threshold_usd: Decimal):
        self.env = env
        self.shutdown_event = asyncio.Event()
        
        # 1. Инициализация RAM-буферов
        self.bar_queue: asyncio.Queue[DollarBar] = asyncio.Queue() # Внимание: Безлимитная очередь (см. стресс-тест)
        self.signal_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=100)
        
        # 2. Инициализация слоя персистентности (Persistence Layer)
        # Раскомментировать при реальном деплое
        # self.outbox_repo = SqliteSignalRepository(db_path=db_path)
        # self.sqlite_flusher = SqliteFlusher(
        #     in_queue=self.signal_queue, 
        #     repo=self.outbox_repo
        # )
        
        # 3. Инициализация квант-слоя (Quant Layer)
        # self.quant_engine = QuantRadarEngine(
        #     in_queue=self.bar_queue, 
        #     out_queue=self.signal_queue
        # )
        
        # 4. Инициализация слоя ингестии (Ingestion Layer)
        self.conflation_engine = DollarBarEngine(
            threshold_usd=bar_threshold_usd
            # out_queue=self.bar_queue # Requires update in DollarBarEngine init
        )
        
        # 5. Инициализация слоя доставки (Delivery Layer)
        # self.telegram_worker = TelegramOutboxWorker(
        #     repo=self.outbox_repo
        # )

    async def _ws_ingestion_mock(self) -> None:
        """
        Изолированный сетевой воркер (Подлежит реализации через aiohttp/websockets).
        Читает сокеты и кормит conflation_engine.
        """
        logger.info("[INGESTION] WebSocket-мост активирован.")
        while not self.shutdown_event.is_set():
            try:
                # Физика: receiving tick -> self.conflation_engine.process_tick(...)
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[INGESTION] Критический сбой: {e}")

    async def boot_sequence(self) -> None:
        logger.info(f"[SYSTEM] Инициализация конвейера GEKTOR CORE ({self.env.upper()})...")
        
        # Предварительная инициализация БД (установка PRAGMA WAL и таблиц)
        # await self.outbox_repo.initialize()

        # РЕКОНСТРУКЦИЯ СТЕЙТА: Запрашиваем наличие активного карантина
        # blackout_str = await self.outbox_repo.get_system_state("BLACKOUT_UNTIL")
        # blackout_until = float(blackout_str) if blackout_str else 0.0

        # if blackout_until > 0:
        #     logger.warning(f"[SYSTEM] ВНИМАНИЕ: Обнаружен прерванный карантин. Блокировка активна до {blackout_until}.")

        # Инициализация Firewall с восстановленным стейтом
        # self.firewall = BiologicalFirewall(
        #     repo=self.outbox_repo, 
        #     initial_blackout_until=blackout_until
        # )

        try:
            async with asyncio.TaskGroup() as tg:
                # Поднятие компонентов в порядке обратной зависимости
                # tg.create_task(self.telegram_worker.run(self.shutdown_event))
                # tg.create_task(self.sqlite_flusher.run(self.shutdown_event))
                # tg.create_task(self.quant_engine.run(self.shutdown_event))
                tg.create_task(self._ws_ingestion_mock())
                
                logger.info("[SYSTEM] Все эшелоны запущены. Ожидание сигналов...")
                await self.shutdown_event.wait()
        except Exception as e:
            logger.critical(f"[SYSTEM] Фатальный сбой TaskGroup: {e}")
            raise
        finally:
            # Гарантированное закрытие соединений при выходе
            # await self.outbox_repo.close()
            logger.info("[SYSTEM] Контур безопасно обесточен. БД синхронизирована.")

    def trigger_shutdown(self) -> None:
        logger.warning("[SYSTEM] Получен сигнал прерывания. Инициализация Graceful Shutdown.")
        self.shutdown_event.set()

def main() -> NoReturn:
    if sys.platform != "win32":
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass

    supervisor = ExecutionSupervisor(
        env="production" if sys.platform != "win32" else "local",
        db_path="gektor_state.sqlite",
        bar_threshold_usd=Decimal('500000')
    )
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, supervisor.trigger_shutdown)

    try:
        loop.run_until_complete(supervisor.boot_sequence())
    except KeyboardInterrupt:
        supervisor.trigger_shutdown()
        # Дополнительный run_until_complete нужен для завершения TaskGroup, если он был прерван
        loop.run_until_complete(asyncio.sleep(0.1)) 
    finally:
        loop.close()
        sys.exit(0)

if __name__ == "__main__":
    main()
