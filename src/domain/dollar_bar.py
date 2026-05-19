# src/domain/dollar_bar.py
from dataclasses import dataclass, field
from typing import List, Optional, Callable, Coroutine, Any
import asyncio
from loguru import logger

@dataclass(slots=True)
class DollarBar:
    symbol: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume_base: float
    volume_quote: float  # The USD volume (Threshold metric)
    buy_volume_quote: float
    sell_volume_quote: float
    tick_count: int
    start_timestamp: float
    end_timestamp: float
    # [GEKTOR v21.68] Microstructure Integrity Metrics
    ofi_accum: float = 0.0  # Order Flow Imbalance (LOB Pressure)
    vpin: float = 0.0       # Volume-synchronous Probability of Informed Trading

class RealtimeDollarBarGenerator:
    """
    [GEKTOR v21.68] Information-Driven Bar Generator (Dollar Bars).
    
    Now tracks LOB Pressure (OFI) to distinguish between Aggressive Trades 
    and OTC/Cross-Trade 'tape prints'.
    """
    __slots__ = ['symbol', 'threshold_usd', '_current_bar']

    def __init__(self, symbol: str, threshold_usd: float):
        self.symbol = symbol
        self.threshold_usd = threshold_usd
        self._current_bar: Optional[DollarBar] = None

    def process_tick(self, price: float, volume: float, side: str, ts: float, current_ofi: float = 0.0) -> List[DollarBar]:
        """
        Processes a single trade tick with synchronous OFI tracking.
        """
        volume_usd = price * volume
        is_buy = str(side).upper() == "BUY"
        completed_bars = []

        while volume_usd > 0:
            if not self._current_bar:
                self._current_bar = DollarBar(
                    symbol=self.symbol,
                    open_price=price, high_price=price, low_price=price, close_price=price,
                    volume_base=0.0, volume_quote=0.0,
                    buy_volume_quote=0.0, sell_volume_quote=0.0,
                    tick_count=0, start_timestamp=ts, end_timestamp=ts,
                    ofi_accum=0.0
                )

            available_capacity = self.threshold_usd - self._current_bar.volume_quote
            
            # Divide OFI proportionally if tick is sliced
            chunk_ratio = min(1.0, available_capacity / volume_usd) if volume_usd > 0 else 1.0
            chunk_ofi = current_ofi * chunk_ratio

            if volume_usd <= available_capacity:
                self._apply_chunk(self._current_bar, price, volume_usd, is_buy, ts, current_ofi)
                if self._current_bar.volume_quote >= self.threshold_usd:
                    completed_bars.append(self._current_bar)
                    self._current_bar = None
                volume_usd = 0 
            else:
                self._apply_chunk(self._current_bar, price, available_capacity, is_buy, ts, chunk_ofi)
                completed_bars.append(self._current_bar)
                self._current_bar = None
                volume_usd -= available_capacity
                current_ofi -= chunk_ofi # Remaining OFI for next bar

        return completed_bars

    def _apply_chunk(self, bar: DollarBar, price: float, vol_usd: float, is_buy: bool, ts: float, ofi: float):
        bar.high_price = max(bar.high_price, price)
        bar.low_price = min(bar.low_price, price)
        bar.close_price = price
        bar.volume_quote += vol_usd
        bar.volume_base += (vol_usd / price) if price > 0 else 0
        if is_buy:
            bar.buy_volume_quote += vol_usd
        else:
            bar.sell_volume_quote += vol_usd
        bar.tick_count += 1
        bar.end_timestamp = ts
        bar.ofi_accum += ofi

from src.domain.conflation import AtomicBarConflator

class CortexHandoffBridge:
    """
    [GEKTOR v21.69] The "Bridge" with Atomic Conflation.
    
    Philosophy: "Data is Mass." 
    Instead of dropping bars (mathematical crime), we fuse them during lag.
    """
    def __init__(self, cortex_callback: Callable[[Any], Coroutine[Any, Any, None]]):
        self._conflator = AtomicBarConflator()
        self._callback = cortex_callback
        self._worker_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._condition = asyncio.Condition()

    async def start(self):
        if self._is_running: return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.success("🧠 [CORTEX] Atomic Bridge active. State integrity guaranteed.")

    async def _worker_loop(self):
        while self._is_running:
            try:
                bar = self._conflator.pop_bar()
                if bar:
                    # Execute math on fused data
                    await self._callback(bar)
                else:
                    await asyncio.sleep(0.001) # Low-latency poll
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"💥 [CORTEX] Math Failure: {e}")
                await asyncio.sleep(0.01)

    def enqueue_bars(self, bars: List[DollarBar]):
        """Non-blocking injection via Conflator."""
        for bar in bars:
            self._conflator.push_bar(bar.close_price, bar.volume_quote, bar.ofi_accum)

    async def stop(self):
        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("🔌 [CORTEX] Bridge shut down.")
