import numpy as np
import numba as nb
import asyncio
import logging
from typing import Dict
from src.domain.conflation import DollarBar

logger = logging.getLogger("GEKTOR_RADAR")

@nb.njit(nogil=True, cache=True)
def compute_ofi_divergence(close_prices: np.ndarray, ofis: np.ndarray, window: int) -> float:
    """
    Расчет дивергенции между кумулятивным OFI и дельтой цены.
    Выполняется без GIL на скорости C.
    """
    if len(close_prices) < window:
        return 0.0
        
    price_delta = close_prices[-1] - close_prices[-window]
    cum_ofi = np.sum(ofis[-window:])
    
    # Нормализация (упрощенная Z-score/MinMax логика для примера)
    # Положительная дивергенция: Цена падает/стоит, но умные деньги агрессивно откупают (OFI > 0)
    return cum_ofi - price_delta 

class NumpyRingBuffer:
    """
    Zero-allocation Ring Buffer для хранения скользящего окна Dollar Bars.
    """
    __slots__ = ['capacity', 'index', 'is_full', 'closes', 'ofis']

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.index = 0
        self.is_full = False
        # Аллокация СТРОГО одного непрерывного блока памяти C
        self.closes = np.zeros(capacity, dtype=np.float64)
        self.ofis = np.zeros(capacity, dtype=np.float64)

    def append(self, close: float, ofi: float) -> None:
        self.closes[self.index] = close
        self.ofis[self.index] = ofi
        self.index = (self.index + 1) % self.capacity
        if self.index == 0:
            self.is_full = True

    def get_ordered_arrays(self) -> tuple[np.ndarray, np.ndarray]:
        """Возвращает плоский массив в правильном хронологическом порядке."""
        if not self.is_full:
            return self.closes[:self.index], self.ofis[:self.index]
        # Матричная склейка
        ord_closes = np.concatenate((self.closes[self.index:], self.closes[:self.index]))
        ord_ofis = np.concatenate((self.ofis[self.index:], self.ofis[:self.index]))
        return ord_closes, ord_ofis

class QuantRadarEngine:
    def __init__(self, in_queue: asyncio.Queue, out_queue: asyncio.Queue, window_size: int = 1000):
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.window_size = window_size
        self.buffers: Dict[str, NumpyRingBuffer] = {}

    async def run(self, shutdown_event: asyncio.Event) -> None:
        logger.info("[RADAR] Квант-движок активирован. Ожидание Dollar Bars...")
        while not shutdown_event.is_set():
            try:
                # Асинхронное извлечение с таймаутом для проверки shutdown_event
                bar: DollarBar = await asyncio.wait_for(self.in_queue.get(), timeout=1.0)
                await self._analyze_alpha(bar)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[RADAR] Внутренний сбой: {e}", exc_info=True)

    async def _analyze_alpha(self, bar: DollarBar) -> None:
        """
        ФАЗА 3: Микроструктурный анализ.
        """
        symbol = bar.symbol
        if symbol not in self.buffers:
            self.buffers[symbol] = NumpyRingBuffer(capacity=self.window_size)
            
        buf = self.buffers[symbol]
        
        # Конвертация Decimal во float только для быстрой математики
        buf.append(float(bar.close), float(bar.order_flow_imbalance))
        
        if not buf.is_full:
            return  # Недостаточно данных для прогрева окна (Burn-in period)

        closes, ofis = buf.get_ordered_arrays()
        
        # Вызов Numba-оптимизированной функции. GIL освобожден!
        divergence = compute_ofi_divergence(closes, ofis, window=100)

        # ПРОТОКОЛ 1 & 2: Оценка аномалии
        if divergence > 5000000.0:  # Порог условный
            intent = {
                "symbol": symbol,
                "type": "HIDDEN_ICEBERG_ACCUMULATION",
                "divergence": divergence,
                "action": "MAKER_LONG_IOC",
                "timestamp": bar.end_ts
            }
            # Передача сигнала в Outbox (Vector A) с паттерном Drop-Head
            self._dispatch_intent(intent)

    def _dispatch_intent(self, intent: dict) -> None:
        try:
            self.out_queue.put_nowait(intent)
            logger.info(f"[RADAR] СИГНАЛ СГЕНЕРИРОВАН: {intent['symbol']} -> {intent['type']}")
        except asyncio.QueueFull:
            _ = self.out_queue.get_nowait() # Drop-Head
            self.out_queue.put_nowait(intent)
            logger.warning("[RADAR] Backpressure очереди сигналов. Старая альфа уничтожена.")
