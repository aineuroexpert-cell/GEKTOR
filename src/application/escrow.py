import asyncio
import time
import logging
from typing import Dict, Tuple, Any, Set

logger = logging.getLogger("GEKTOR.ESCROW")

class LiquidationEchoGuard:
    """
    [ФАЗА 5: ИНВАЛИДАЦИЯ И ЭСКАЛАЦИЯ]
    Асинхронный Эскроу-буфер для защиты от запаздывающих ликвидаций.
    """
    def __init__(self, event_bus: Any = None, escrow_window_ms: int = 150):
        self.bus = event_bus
        self.escrow_window_sec = escrow_window_ms / 1000.0
        # Храним монотонное время последней ликвидации и её объем
        self._recent_liquidations: Dict[str, Tuple[float, str, float]] = {}
        self._mute_threshold_usd = 50_000.0
        
        # [MEMORY SAFETY] Реестр активных спящих задач
        self._pending_tasks: Dict[str, Set[asyncio.Task]] = {}
        self._is_network_healthy: bool = True
        self._health_last_updated: float = 0.0

        if self.bus:
            self.bus.subscribe("LiquidationEvent", self._handle_bus_liquidation)

    def update_network_health(self, is_healthy: bool) -> None:
        """Синхронный метод обновления стейта от HealthMonitor."""
        self._is_network_healthy = is_healthy
        if not is_healthy:
            self.purge_stale_escrow()

    def _handle_bus_liquidation(self, event: dict):
        self.register_liquidation_event(
            event.get("symbol"), 
            event.get("side"), 
            event.get("usd_volume", 0.0)
        )

    def register_liquidation_event(self, symbol: str, side: str, usd_volume: float):
        """Вызывается синхронно из WebSocket-ингестора ликвидаций"""
        if usd_volume < self._mute_threshold_usd:
            return
            
        logger.debug(f"🩸 [LIQUIDATION REGISTERED] {symbol} | {side} | ${usd_volume:,.0f}")
        self._recent_liquidations[symbol] = (time.monotonic(), side, usd_volume)

    def register_task(self, symbol: str, task: asyncio.Task):
        if symbol not in self._pending_tasks:
            self._pending_tasks[symbol] = set()
        self._pending_tasks[symbol].add(task)
        task.add_done_callback(lambda t: self._pending_tasks[symbol].discard(t) if symbol in self._pending_tasks else None)

    def purge_stale_escrow(self, symbol: str = None):
        """Вызывается при дисконнекте для предотвращения хронологического дрифта."""
        targets = [symbol] if symbol else list(self._pending_tasks.keys())
        for sym in targets:
            tasks = self._pending_tasks.get(sym, set())
            count = len(tasks)
            for task in list(tasks):
                if not task.done():
                    task.cancel()
            if count > 0:
                logger.warning(f"🧹 [ESCROW PURGE] Убито {count} зомби-сигналов для {sym} (Network GAP).")

    async def escort_signal(self, symbol: str, signal_side: str, ofi_usd: float) -> bool:
        """
        Пропускает сигнал через временной карантин.
        """
        if not self._is_network_healthy:
            return False

        task = asyncio.current_task()
        self.register_task(symbol, task)

        try:
            # Неблокирующее ожидание, пока прилетят пакеты из смежного сокета
            await asyncio.sleep(self.escrow_window_sec)
        except asyncio.CancelledError:
            logger.debug(f"🛑 [ESCROW ABORT] Сигнал {symbol} отменен: потеря консенсуса времени.")
            raise

        # Проверяем, не прилетел ли маржин-колл за время задержки
        if symbol in self._recent_liquidations:
            last_time, liq_side, liq_vol = self._recent_liquidations[symbol]
            time_since_liq = time.monotonic() - last_time

            # Если ликвидация была менее 1.5 секунд назад
            if time_since_liq < 1.5:
                # Маппинг: Ликвидация Шортистов (BUY) выглядит как ACCUMULATION (BUY)
                if (signal_side.endswith("BUY") and liq_side == "BUY") or \
                   (signal_side.endswith("SELL") and liq_side == "SELL"):
                    logger.warning(
                        f"🛑 [SIGNAL ABORTED] {symbol} | {signal_side} | "
                        f"Опознан ликвидационный каскад (${liq_vol:,.0f}). Сигнал сожжен."
                    )
                    return False

        return True
