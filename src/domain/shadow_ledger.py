import asyncio
import logging
from typing import Protocol
from collections import deque

logger = logging.getLogger("GEKTOR_FIREWALL")

class ISystemStateRepo(Protocol):
    async def upsert_system_state(self, key: str, value: str) -> None: ...
    async def get_system_state(self, key: str) -> str | None: ...

class BiologicalFirewall:
    def __init__(self, repo: ISystemStateRepo, initial_blackout_until: float = 0.0):
        self.repo = repo
        self.blackout_until_ts = initial_blackout_until
        self.is_blackout_active = self.blackout_until_ts > 0
        self.QUARANTINE_DURATION_SEC = 14400.0  # 4 часа жесткого карантина
        
        self.biological_offset_sec = 1.2
        self.tilt_threshold = 3
        self.consecutive_failures = 0
        self.pending_intents: deque = deque()

    async def register_intent(self, intent: dict) -> None:
        if self.is_blackout_active:
            return

        intent['verification_ts'] = intent['timestamp'] + self.biological_offset_sec
        self.pending_intents.append(intent)

    async def process_market_tick(self, symbol: str, current_price: float, exchange_ts: float) -> None:
        """Zero-Blocking оценка истории и проверка карантина (строго по времени биржи)."""
        # 1. Проверка снятия карантина (СТРОГО ПО ВРЕМЕНИ БИРЖИ)
        if self.is_blackout_active:
            if exchange_ts >= self.blackout_until_ts:
                await self._lift_quarantine()
            return  # В карантине не оцениваем новые сигналы

        # 2. Оценка старейшего сигнала
        if not self.pending_intents:
            return

        oldest_intent = self.pending_intents[0]
        if exchange_ts >= oldest_intent['verification_ts']:
            intent = self.pending_intents.popleft()
            await self._evaluate_reality(intent, current_price, exchange_ts)

    async def _evaluate_reality(self, intent: dict, current_price: float, exchange_ts: float) -> None:
        success = self._check_alpha_decay(intent, current_price)

        if success:
            logger.info(f"[SHADOW_LEDGER] Сигнал {intent['symbol']} валиден на T_0+1.2s.")
            self.consecutive_failures = 0
        else:
            logger.warning(f"[SHADOW_LEDGER] Альфа сигнала {intent['symbol']} расщепилась до исполнения.")
            self.consecutive_failures += 1
            
            if self.consecutive_failures >= self.tilt_threshold:
                await self._trigger_blackout(exchange_ts)

    def _check_alpha_decay(self, intent: dict, actual_price: float) -> bool:
        return False  # Симуляция распада альфы

    async def _trigger_blackout(self, exchange_ts: float) -> None:
        """Активация карантина с записью в БД."""
        if not self.is_blackout_active:
            self.is_blackout_active = True
            self.blackout_until_ts = exchange_ts + self.QUARANTINE_DURATION_SEC
            
            # Физическая фиксация стейта на диске
            await self.repo.upsert_system_state("BLACKOUT_UNTIL", str(self.blackout_until_ts))
            
            logger.critical("[FIREWALL] АКТИВИРОВАНО АСИММЕТРИЧНОЕ ОСЛЕПЛЕНИЕ.")
            logger.critical(f"[FIREWALL] Карантин до exchange_ts: {self.blackout_until_ts}")

    async def _lift_quarantine(self) -> None:
        """Снятие карантина."""
        self.is_blackout_active = False
        self.blackout_until_ts = 0.0
        self.consecutive_failures = 0
        await self.repo.upsert_system_state("BLACKOUT_UNTIL", "0")
        logger.info("[FIREWALL] Карантин снят. Когнитивные функции Оператора признаны восстановленными.")
