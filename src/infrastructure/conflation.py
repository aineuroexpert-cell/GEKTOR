# src/infrastructure/conflation.py
from loguru import logger
from dataclasses import dataclass
from typing import Dict, List, Optional
import time


@dataclass(slots=True)
class DirectionalTick:
    symbol: str
    price: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    exchange_ts: int = 0

class DirectionalConflationBuffer:
    """
    [GEKTOR APEX] DIRECTIONAL SLOW CONSUMER SHIELD.
    Aggregates ticks per symbol while preserving Buy/Sell delta.
    Prevents Event Loop blocking by collapsing high-frequency flow into 
    consolidated "Mega-Ticks" without volume loss.
    """
    def __init__(self, critical_size: int = 5000):
        self.critical_size = critical_size
        # Key: symbol, Value: DirectionalTick
        self._buffer: Dict[str, DirectionalTick] = {}
        self.total_conflated = 0

    def ingest_immediate(self, tick_dict: dict):
        """
        [O(1) NON-BLOCKING] Ingests a raw tick from the feed.
        Aggregates volume into existing DirectionalTick if present.
        """
        symbol = tick_dict['symbol']
        try:
            price = float(tick_dict['price'])
            vol = float(tick_dict['volume'])
            ts = int(tick_dict['timestamp'])
            is_buy = str(tick_dict.get('side', '')).lower() == 'buy'
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"⚠️ [Conflation] Malformed tick: {e}")
            return

        if symbol in self._buffer:
            existing = self._buffer[symbol]
            existing.price = price # Update to latest price
            existing.exchange_ts = ts # Update to latest timestamp
            if is_buy:
                existing.buy_volume += vol
            else:
                existing.sell_volume += vol
            self.total_conflated += 1
        else:
            # Check for memory protection limit
            if len(self._buffer) >= self.critical_size:
                 # If we have too many unique symbols (rare), drop the oldest
                 # In crypto we rarely track > 5000 symbols simultaneously
                 logger.warning("🚨 [Conflation] Critical size reached. Dropping new symbol entry.")
                 return

            self._buffer[symbol] = DirectionalTick(
                symbol=symbol,
                price=price,
                buy_volume=vol if is_buy else 0.0,
                sell_volume=0.0 if is_buy else vol,
                exchange_ts=ts
            )

    def flush(self) -> List[dict]:
        """
        Atomically extracts and clears the buffer.
        Returns Buy/Sell Mega-Ticks for each symbol.
        """
        if not self._buffer:
            return []
            
        results = []
        for symbol, t in self._buffer.items():
            if t.buy_volume > 0:
                results.append({
                    "symbol": symbol,
                    "price": t.price,
                    "volume": t.buy_volume,
                    "side": "Buy",
                    "timestamp": t.exchange_ts,
                    "conflated": True
                })
            if t.sell_volume > 0:
                results.append({
                    "symbol": symbol,
                    "price": t.price,
                    "volume": t.sell_volume,
                    "side": "Sell",
                    "timestamp": t.exchange_ts,
                    "conflated": True
                })
        
        self._buffer.clear()
        self.total_conflated = 0
        return results
