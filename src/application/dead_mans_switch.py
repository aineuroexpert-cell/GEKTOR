import asyncio
import signal
import logging
from typing import Set

logger = logging.getLogger("GEKTOR.Terminal")

class GlobalDeadMansSwitch:
    """[GEKTOR v2.0] Атомарный перехватчик смерти процесса. Graceful Shutdown & The Final Order 66."""
    __slots__ = ['orchestrator', 'gateway', '_shutdown_triggered']

    def __init__(self, orchestrator, execution_gateway):
        self.orchestrator = orchestrator
        self.gateway = execution_gateway
        self._shutdown_triggered = False
        self._fatal_context = "UNKNOWN_FATAL_ERROR OR MANUAL_SIGTERM"

    def set_fatal_context(self, reason: str):
        self._fatal_context = reason

    def arm(self):
        """Инъекция в обработчики сигналов POSIX."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.trigger_shutdown(s)))
        logger.info("🛡️ [DEAD MAN SWITCH] ARMED. OS signals intercepted.")

    async def trigger_shutdown(self, sig: signal.Signals):
        if self._shutdown_triggered:
            return
        self._shutdown_triggered = True
        logger.critical(f"⚠️ [SYSTEM] Received OS Signal: {sig.name}. INITIATING GRACEFUL SHUTDOWN.")

        try:
            # Time-To-Die: Даем 8 секунд на закрытие из 10 (перед SIGKILL)
            await asyncio.wait_for(self._execute_shutdown_sequence(sig), timeout=8.0)
        except asyncio.TimeoutError:
            logger.critical("⌛ [FATAL] SHUTDOWN TIMEOUT EXCEEDED! NETWORK PARTITION DETECTED.")
            self._emergency_blackbox_dump()
        finally:
            logger.critical("⬛ [SYSTEM] Terminal execution complete. Going dark.")
            loop = asyncio.get_running_loop()
            loop.stop()

    async def _execute_shutdown_sequence(self, sig: signal.Signals):
        # 1. Мгновенная блокировка Ingress-слоя (остановка парсинга новых тиков)
        if hasattr(self.orchestrator, 'halt_ingress'):
            self.orchestrator.halt_ingress()

        # 2. Предсмертный крик в Телеграм (Строгий await)
        if hasattr(self.orchestrator, 'tg'):
            await self.orchestrator.tg.broadcast_offline_sync(str(sig.name))

        # 3. Вызов Экстренной Ликвидации для всех открытых позиций (The Final Order 66)
        if hasattr(self.orchestrator, 'get_active_positions'):
            active_positions = self.orchestrator.get_active_positions()
            if active_positions:
                logger.critical(f"🔥 [TERMINAL] Liquidating {len(active_positions)} active positions before death...")
                tasks = [
                    self.gateway.execute_liquidation(
                        symbol=pos.symbol,
                        qty=pos.volume,
                        reference_price=pos.current_market_price
                    ) for pos in active_positions
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

        # 4. Атомарный сброс Outbox и остановка пулов
        if hasattr(self.orchestrator, 'flush_state_to_disk'):
            await self.orchestrator.flush_state_to_disk()
            
        if hasattr(self.orchestrator, 'worker_pool'):
            self.orchestrator.worker_pool.shutdown()

    def _emergency_blackbox_dump(self):
        """Синхронный дамп стейта при фатальном таймауте сети."""
        logger.critical("⬛ [BLACKBOX] Dumping raw memory state to local disk...")
        try:
            import json, time
            dump_data = {
                "timestamp": time.time(),
                "fatal_reason": "SIGTERM_TIMEOUT",
                "death_context": getattr(self, '_fatal_context', "UNKNOWN"),
                "macro_states": getattr(self.orchestrator, 'macro_states', {})
            }
            with open("blackbox_dump.json", "w") as f:
                json.dump(dump_data, f)
            logger.critical("⬛ [BLACKBOX] Dump saved.")
        except Exception as e:
            logger.critical(f"⬛ [BLACKBOX] Failed to dump: {e}")
