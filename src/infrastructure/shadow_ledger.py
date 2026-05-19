import asyncio
import time
import queue
import threading
import json
import logging
from typing import Dict, Any, List

logger = logging.getLogger("GEKTOR.ShadowLedger")

class ShadowLedger:
    """
    [GEKTOR v10.0] Теневой Реестр: Анализ Реалистичности Сигнала.
    Учитывает Биологическую задержку (1.2с) и подавляет сигналы при тильте.
    """
    def __init__(self, log_path: str):
        # 1. I/O Инфраструктура
        self.io_queue = queue.SimpleQueue()
        self.biological_offset_sec = 1.2
        self.maker_fee = -0.0001  # -1 bps (Bybit Maker Rebate/Fee)
        
        # 2. Психологический Монитор (Tilt-Breaker)
        self.is_operator_compromised = False
        self.performance_window: List[float] = [] 
        self.max_drawdown_limit = -100.0 # USD (условный лимит боли)
        
        # 3. Выделенный поток записи. Никаких блокировок Event Loop'а.
        self._writer_thread = threading.Thread(
            target=self._dedicated_io_worker, 
            args=(log_path,), 
            daemon=True
        )
        self._writer_thread.start()

    def _dedicated_io_worker(self, log_path: str):
        """O(1) I/O поток. Асинхронная запись в файл-журнал."""
        with open(log_path, 'a') as f:
            while True:
                record = self.io_queue.get()
                if record is None: break
                f.write(json.dumps(record) + '\n')
                f.flush()

    def submit_intent(self, symbol: str, intent_price: float, side: float, orchestrator: Any):
        """Регистрация сигнала в T_0."""
        # Мы не ждем выполнения, запускаем симуляцию в фоне
        asyncio.create_task(self._simulate_temporal_decay(symbol, intent_price, side, orchestrator))

    async def _simulate_temporal_decay(self, symbol: str, intent_price: float, side: float, orchestrator: Any):
        """
        [GEKTOR v12.9] Анализ временного распада Альфы. 
        O(1) сбор данных в точках: T0, T100ms, T500ms, T1200ms.
        """
        # Паузы между замерами (0.0 -> 0.1 -> 0.4 -> 0.7 = кумулятивно 1.2s)
        intervals = [0.0, 0.1, 0.4, 0.7] 
        realized_mids = []
        
        start_t = time.time()
        book = orchestrator.books.get(symbol)
        if not book: return

        for wait in intervals:
            if wait > 0: await asyncio.sleep(wait)
            
            # Read-Path O(K) из Unified SHM-view
            _, status, bids, asks, is_dirty = book.get_snapshot()
            if status == "SYNCED" and not is_dirty and bids and asks:
                # Mid-price в float64 для анализа PnL (Scaled Int -> Float)
                mid = (float(bids[0][0]) + float(asks[0][0])) / 2 / 100_000_000
                realized_mids.append(mid)
            else:
                realized_mids.append(0.0)

        # Результат через 1.2с (биологический порог)
        final_mid = realized_mids[-1]
        is_alpha_valid = (final_mid > intent_price) if side == 1.0 else (final_mid < intent_price) if final_mid > 0 else False

        record = {
            "ts": start_t,
            "symbol": symbol,
            "intent": intent_price,
            "side": "BUY" if side == 1.0 else "SELL",
            "decay_matrix": realized_mids, # [T0, T100, T500, T1200]
            "alpha_win": 1 if is_alpha_valid else 0
        }
        
        self._update_tilt_metrics(record["alpha_win"])
        self.io_queue.put(record)

    def _update_tilt_metrics(self, last_win: int):
        """
        [PSYCH-GUARD] Анализ серии неудач для активации Blackout.
        Если за последние 10 сигналов более 70% промахов — Оператор в тильте.
        """
        self.performance_window.append(last_win)
        if len(self.performance_window) > 10:
            self.performance_window.pop(0)
            
        if len(self.performance_window) == 10:
            success_rate = sum(self.performance_window) / 10
            if success_rate < 0.3:
                if not self.is_operator_compromised:
                    logger.critical("🚨 [TILT_BREAKER] PERFORMANCE COLLAPSE. OPERATOR BLINDED.")
                    self.is_operator_compromised = True
            else:
                if self.is_operator_compromised:
                    logger.success("📟 [TILT_BREAKER] Stability Restored. Visuals ON.")
                    self.is_operator_compromised = False
