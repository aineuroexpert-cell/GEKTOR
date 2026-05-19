import asyncio
import time
from enum import Enum
from typing import Dict, Any, Optional
from loguru import logger
from src.application.alpha_decay import ShrapnelEvictor

class IntentStatus(Enum):
    ARMED = 1
    PARTIAL = 2
    REVOKING = 3  # Cancel requested
    RECONCILING = 4 # Exiting fragment
    CLOSED = 5

class ShrapnelRecoveryMachine:
    """
    [GEKTOR v9.5] Институциональный конечный автомат жизненного цикла ордера.
    Обработка Regime Inversion, Partial Fills и таймаутов отмены.
    """
    def __init__(self, api_gateway: Any):
        self.gateway = api_gateway
        self.evictor = ShrapnelEvictor(max_loss_bps=20.0) # Порог боли: 0.2%
        self.active_intents: Dict[str, Dict[str, Any]] = {}
        
    async def on_regime_inversion(self, symbol: str):
        """
        [GLOBAL ABORT] Вызывается при развороте микроструктуры.
        Немедленная очистка "токсичных" лимитов.
        """
        intent = self.active_intents.get(symbol)
        if not intent or intent['status'] in [IntentStatus.REVOKING, IntentStatus.CLOSED]:
            return

        logger.critical(f"⚠️ [REGIME_INVERSION] {symbol}. Активация протокола экстренного отзыва.")
        
        # Переход в состояние отзыва. clOrdID — наш якорь идемпотентности.
        intent['status'] = IntentStatus.REVOKING
        success = await self.gateway.cancel_order(symbol, intent['clOrdID'])
        
        if not success:
            # [ERROR RECOVERY] Если REST упал — не страшно. 
            # Лимит будет признан мертвым по TTL или через WebSocket ExecutionReport.
            logger.error(f"❌ [ABORT_FAILED] {symbol} Cancel timeout. Relying on OrderTopic Oracle.")

    async def on_execution_report(self, report: dict):
        """
        [ORACLE] Единственный источник правды о стейте ордера на бирже (WebSocket).
        """
        symbol = report['symbol']
        intent = self.active_intents.get(symbol)
        if not intent: return

        # Синхронизация объема
        intent['filled_qty'] = float(report['cum_exec_qty'])
        intent['leaves_qty'] = float(report['leaves_qty'])
        
        status = report['order_status']
        
        if status == "Cancelled" or status == "Deactivated":
            if intent['filled_qty'] > 0:
                # [STATE_SHRAPNEL] У нас на руках "осколок" (Partial Fill) в развернутом рынке
                await self._reconcile_fragment(symbol, intent)
            else:
                intent['status'] = IntentStatus.CLOSED
                self.active_intents.pop(symbol)

        elif status == "Filled":
            # Ордер полностью исполнен до инверсии — передаем в ProfitTracker
            intent['status'] = IntentStatus.CLOSED
            self.active_intents.pop(symbol)

    async def handle_lifecycle_tick(self, symbol: str):
        """[TICK] Периодическая проверка жизненного цикла ордера."""
        intent = self.active_intents.get(symbol)
        if not intent: return

        duration_ms = (time.time() - intent['start_ts']) * 1000
        
        # 1. СТРОГИЙ TTL (Манифест v6.0)
        if duration_ms > 5000 and intent['status'] != IntentStatus.CLOSED:
             # Пора ампутировать. 5 секунд истекли.
             await self._amputate_immediately(symbol)
             return

        # 2. Мягкая ампутация осколков в RECONCILING
        if intent['status'] == IntentStatus.RECONCILING:
            # Даем 2 секунды на попытки мягкого IoC выхода
            shrapnel_duration = (time.time() - intent['shrapnel_ts']) * 1000
            if shrapnel_duration > 2000:
                await self._amputate_immediately(symbol)
            else:
                await self._attempt_soft_reconcile(symbol, intent)

    async def _amputate_immediately(self, symbol: str):
        """[TAKER SWEEP] Последний шанс: выход по любой цене."""
        logger.critical(f"✂️ [AMPUTATION] TTL Expired for {symbol}. Forced Taker Sweep.")
        await self.gateway.execute_taker_sweep(symbol)
        self.active_intents.pop(symbol)

    async def _attempt_soft_reconcile(self, symbol: str, intent: dict):
        # ... Реализация мягкого IoC с динамической ценой из Evictor ...
        pass

    async def _reconcile_fragment(self, symbol: str, intent: dict):
        """
        [RECONCILIATION] Начало протокола закрытия осколка.
        """
        if intent['status'] == IntentStatus.RECONCILING: return
        intent['status'] = IntentStatus.RECONCILING
        intent['shrapnel_ts'] = time.time()
        
        logger.warning(f"🩹 [SHRAPNEL] Detected fragment on {symbol} ({intent['filled_qty']}).")
        await self._attempt_soft_reconcile(symbol, intent)

    def _calculate_emergency_exit_price(self, symbol: str, side: str) -> float:
        """Инженерный расчет цены выхода с учетом спреда и волатильности."""
        # Упрощенная логика для прототипа
        return 0.0 # В реальности берется из SHM OrderBook
