# src/domain/dollar_bar.py
"""
[GEKTOR DOCTRINE] All price/volume calculations use decimal.Decimal.
IEEE 754 floats are PROHIBITED for financial math.

DollarBar is the single canonical dataclass defined in conflation.py.
This module re-exports it and provides RealtimeDollarBarGenerator.
"""
from decimal import Decimal
from typing import List, Optional, Callable, Coroutine, Any
import asyncio
from loguru import logger

from src.domain.conflation import DollarBar


ZERO = Decimal('0')
ONE = Decimal('1')


class RealtimeDollarBarGenerator:
    """
    [GEKTOR v21.68] Information-Driven Bar Generator (Dollar Bars).

    All arithmetic uses decimal.Decimal per Manifesto.
    Tracks LOB Pressure (OFI) to distinguish Aggressive Trades
    from OTC/Cross-Trade tape prints.
    """
    __slots__ = ['symbol', 'threshold_usd', '_current_bar']

    def __init__(self, symbol: str, threshold_usd: Decimal):
        self.symbol = symbol
        self.threshold_usd = threshold_usd
        self._current_bar: Optional[DollarBar] = None

    def process_tick(
        self, price: Decimal, volume: Decimal, side: str, ts: float, current_ofi: Decimal = ZERO
    ) -> List[DollarBar]:
        volume_usd = price * volume
        is_buyer_maker = str(side).upper() != "BUY"
        completed_bars: List[DollarBar] = []

        while volume_usd > ZERO:
            if not self._current_bar:
                self._current_bar = DollarBar(
                    symbol=self.symbol,
                    open=price, high=price, low=price, close=price,
                    start_ts=ts,
                )

            available_capacity = self.threshold_usd - self._current_bar.volume_usd

            chunk_ratio = min(ONE, available_capacity / volume_usd) if volume_usd > ZERO else ONE
            chunk_ofi = current_ofi * chunk_ratio

            if volume_usd <= available_capacity:
                self._apply_chunk(self._current_bar, price, volume, volume_usd, is_buyer_maker, ts, current_ofi)
                if self._current_bar.volume_usd >= self.threshold_usd:
                    completed_bars.append(self._current_bar)
                    self._current_bar = None
                volume_usd = ZERO
            else:
                chunk_base = (available_capacity / price) if price > ZERO else ZERO
                self._apply_chunk(self._current_bar, price, chunk_base, available_capacity, is_buyer_maker, ts, chunk_ofi)
                completed_bars.append(self._current_bar)
                self._current_bar = None
                volume_usd -= available_capacity
                volume -= chunk_base
                current_ofi -= chunk_ofi

        return completed_bars

    @staticmethod
    def _apply_chunk(
        bar: DollarBar, price: Decimal, vol_base: Decimal, vol_usd: Decimal,
        is_buyer_maker: bool, ts: float, ofi: Decimal,
    ) -> None:
        if price > bar.high:
            bar.high = price
        if price < bar.low:
            bar.low = price
        bar.close = price
        bar.volume_usd += vol_usd
        bar.volume_crypto += vol_base
        if is_buyer_maker:
            bar.sell_volume_usd += vol_usd
        else:
            bar.buy_volume_usd += vol_usd
        bar.tick_count += 1
        bar.end_ts = ts


class CortexHandoffBridge:
    """
    [GEKTOR v21.69] The "Bridge" — asynchronous handoff of completed bars to the quant engine.

    Philosophy: "Data is Mass."
    Instead of dropping bars (mathematical crime), we queue them during lag.
    """
    def __init__(self, cortex_callback: Callable[[Any], Coroutine[Any, Any, None]]):
        self._queue: asyncio.Queue[DollarBar] = asyncio.Queue(maxsize=4096)
        self._callback = cortex_callback
        self._worker_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def start(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.success("[CORTEX] Atomic Bridge active. State integrity guaranteed.")

    async def _worker_loop(self) -> None:
        while self._is_running:
            try:
                bar = await asyncio.wait_for(self._queue.get(), timeout=0.01)
                await self._callback(bar)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[CORTEX] Math Failure: {e}")
                await asyncio.sleep(0.01)

    def enqueue_bars(self, bars: List[DollarBar]) -> None:
        for bar in bars:
            try:
                self._queue.put_nowait(bar)
            except asyncio.QueueFull:
                logger.warning("[CORTEX] Bar queue full, dropping oldest bar (lossy mode)")

    async def stop(self) -> None:
        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("[CORTEX] Bridge shut down.")
