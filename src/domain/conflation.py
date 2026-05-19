from dataclasses import dataclass, field
from decimal import Decimal
import time
import logging
from typing import Protocol, Callable, Awaitable

logger = logging.getLogger("GEKTOR_CONFLATION")

@dataclass(slots=True)
class DollarBar:
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    
    # Микроструктурные метрики
    volume_crypto: Decimal = Decimal('0')
    volume_usd: Decimal = Decimal('0')
    buy_volume_usd: Decimal = Decimal('0')
    sell_volume_usd: Decimal = Decimal('0')
    tick_count: int = 0
    
    # Временные метки биржи (Exchange Time)
    start_ts: float = field(default_factory=time.time)
    end_ts: float = 0.0

    @property
    def order_flow_imbalance(self) -> Decimal:
        """
        Дельта стакана внутри бара. 
        Положительное значение = доминация рыночных покупателей (Taker Buy).
        """
        return self.buy_volume_usd - self.sell_volume_usd


class IBarAggregator(Protocol):
    async def process_tick(self, symbol: str, price: Decimal, size: Decimal, is_buyer_maker: bool, exchange_ts: float) -> None:
        """Агрегирует входящий тик. Если порог достигнут — закрывает бар."""
        ...

    def set_callback(self, callback: Callable[[DollarBar], Awaitable[None]]) -> None:
        """Регистрация асинхронного коллбэка для передачи закрытого бара в квант-движок."""
        ...
        
    async def handle_resync(self) -> None:
        """Аварийный сброс разорванного стейта при реконнекте сети."""
        ...


class DollarBarEngine(IBarAggregator):
    """
    Машина сборки Dollar Bars.
    """
    def __init__(self, threshold_usd: Decimal):
        self.threshold_usd = threshold_usd
        self._current_bars: dict[str, DollarBar] = {}
        self._on_bar_closed: Callable[[DollarBar], Awaitable[None]] | None = None

    def set_callback(self, callback: Callable[[DollarBar], Awaitable[None]]) -> None:
        self._on_bar_closed = callback

    async def handle_resync(self) -> None:
        """
        Уничтожение отравленного стейта. 
        Очищает все недособранные бары при переподключении сокета.
        """
        purged_count = len(self._current_bars)
        self._current_bars.clear()
        if purged_count > 0:
            logger.warning(f"[CONFLATION] CAUSAL RESYNC: Уничтожено {purged_count} отравленных аккумуляторов.")

    async def process_tick(
        self, symbol: str, price: Decimal, size: Decimal, is_buyer_maker: bool, exchange_ts: float
    ) -> None:
        tick_usd = price * size
        
        # Получаем или инициализируем новый аккумулятор для тикера
        bar = self._current_bars.get(symbol)
        if not bar:
            bar = DollarBar(
                symbol=symbol, open=price, high=price, low=price, close=price, start_ts=exchange_ts
            )
            self._current_bars[symbol] = bar

        # Обновление экстремумов и цены закрытия
        if price > bar.high: bar.high = price
        if price < bar.low: bar.low = price
        bar.close = price

        # Обновление микроструктуры
        bar.volume_crypto += size
        bar.volume_usd += tick_usd
        bar.tick_count += 1

        if is_buyer_maker:
            # Maker был покупателем, значит Taker продал по Bid
            bar.sell_volume_usd += tick_usd
        else:
            # Maker был продавцом, значит Taker купил по Ask
            bar.buy_volume_usd += tick_usd

        # Каузальный триггер: порог долларов превышен
        if bar.volume_usd >= self.threshold_usd:
            bar.end_ts = exchange_ts
            
            # 1. Извлекаем готовый бар
            closed_bar = self._current_bars.pop(symbol)
            
            # 2. Немедленно передаем в квант-движок
            if self._on_bar_closed:
                try:
                    await self._on_bar_closed(closed_bar)
                except Exception as e:
                    logger.error(f"[CONFLATION] Ошибка передачи бара {symbol} в радар: {e}", exc_info=True)
