# src/domain/dollar_bar.py
from dataclasses import dataclass, field
from typing import List, Optional, Callable, Coroutine, Any
import asyncio
from loguru import logger

from src.domain.conflation import DollarBar
from decimal import Decimal

class RealtimeDollarBarGenerator:
    """
    [GEKTOR v21.68] Information-Driven Bar Generator (Dollar Bars).
    
    Now tracks LOB Pressure (OFI) to distinguish between Aggressive Trades 
    and OTC/Cross-Trade 'tape prints'.
    Uses unified Decimal-based DollarBar from conflation.py.
    """
    __slots__ = ['symbol', 'threshold_usd', '_current_bar']

    def __init__(self, symbol: str, threshold_usd: float):
        self.symbol = symbol
        self.threshold_usd = Decimal(str(threshold_usd))
        self._current_bar: Optional[DollarBar] = None

    def process_tick(self, price: float, volume: float, side: str, ts: float, current_ofi: float = 0.0) -> List[DollarBar]:
        """
        Processes a single trade tick with synchronous OFI tracking.
        """
        p = Decimal(str(price))
        v = Decimal(str(volume))
        volume_usd = p * v
        is_buy = str(side).upper() == "BUY"
        completed_bars = []
        c_ofi = Decimal(str(current_ofi))

        while volume_usd > Decimal('0'):
            if not self._current_bar:
                self._current_bar = DollarBar(
                    symbol=self.symbol,
                    open=p, high=p, low=p, close=p,
                    volume_crypto=Decimal('0'), volume_usd=Decimal('0'),
                    buy_volume_usd=Decimal('0'), sell_volume_usd=Decimal('0'),
                    tick_count=0, start_ts=ts, end_ts=ts
                )
                self._current_bar.ofi_accum = Decimal('0')

            available_capacity = self.threshold_usd - self._current_bar.volume_usd
            
            # Divide OFI proportionally if tick is sliced
            chunk_ratio = (available_capacity / volume_usd) if volume_usd > Decimal('0') else Decimal('1')
            if chunk_ratio > Decimal('1'): chunk_ratio = Decimal('1')
            chunk_ofi = c_ofi * chunk_ratio

            if volume_usd <= available_capacity:
                self._apply_chunk(self._current_bar, p, volume_usd, is_buy, ts, c_ofi)
                if self._current_bar.volume_usd >= self.threshold_usd:
                    completed_bars.append(self._current_bar)
                    self._current_bar = None
                volume_usd = Decimal('0')
            else:
                self._apply_chunk(self._current_bar, p, available_capacity, is_buy, ts, chunk_ofi)
                completed_bars.append(self._current_bar)
                self._current_bar = None
                volume_usd -= available_capacity
                c_ofi -= chunk_ofi # Remaining OFI for next bar

        return completed_bars

    def _apply_chunk(self, bar: DollarBar, price: Decimal, vol_usd: Decimal, is_buy: bool, ts: float, ofi: Decimal):
        bar.high = max(bar.high, price)
        bar.low = min(bar.low, price)
        bar.close = price
        bar.volume_usd += vol_usd
        bar.volume_crypto += (vol_usd / price) if price > Decimal('0') else Decimal('0')
        if is_buy:
            bar.buy_volume_usd += vol_usd
        else:
            bar.sell_volume_usd += vol_usd
        bar.tick_count += 1
        bar.end_ts = ts
        bar.ofi_accum = getattr(bar, 'ofi_accum', Decimal('0')) + ofi


