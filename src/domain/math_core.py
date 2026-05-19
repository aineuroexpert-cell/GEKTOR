import time
import logging
from typing import Optional, Dict, List, Tuple
from loguru import logger

from src.domain.dollar_bar import RealtimeDollarBarGenerator
from src.domain.vpin_engine import O1VPINEngine
from src.shared.alpha_config import alpha

class AdaptiveHybridClock:
    """
    [GEKTOR v5.1] Adaptive Hybrid Information Clock.
    Aggregates by Volume (X USD) but enforces a TTL (Time-To-Live) to prevent stale state.
    Solves the "Stale Bucket Decay" problem during low-volatility sessions.
    """
    def __init__(self, symbol: str, target_usd: float = 1000000.0, max_ttl_sec: float = 300.0):
        self.symbol = symbol
        self.target_usd = target_usd
        self.max_ttl_sec = max_ttl_sec
        self._reset()

    def apply_data(self, trade_volume_usd: float, current_ofi: float) -> Optional[dict]:
        import time
        now = time.monotonic()
        time_elapsed = now - self._bucket_start_time
        
        self.current_volume += trade_volume_usd
        self.ofi_sum += current_ofi
        self.bucket_ticks += 1

        # Trigger 1: Physical mass threshold reached (The alpha path)
        volume_triggered = self.current_volume >= self.target_usd
        
        # Trigger 2: Time decay fallback
        # If bucket is > TTL, we check if we have enough data (e.g. 20% of target) to make a call
        # Otherwise, we flush the "stale junk" state.
        time_triggered = (time_elapsed >= self.max_ttl_sec)

        if volume_triggered:
            result = self._harvest_bucket(reason="VOLUME_TARGET")
            self._reset()
            return result
        
        if time_triggered:
            # [VPIN INTEGRITY GUARD]
            # Если корзина закрыта по таймеру, мы проверяем, достаточно ли объема.
            # 80% - это инженерная граница статистической значимости. Меньше - мусор (White Noise).
            if self.current_volume >= self.target_usd * 0.8:
                result = self._harvest_bucket(reason="TTL_EXPIRY")
                self._reset()
                return result
            else:
                # МЫ НЕ ГЕНЕРИРУЕМ РЕЗУЛЬТАТ. Стейт математики замораживается.
                logger.warning(f"🗑️ [CLOCK DECAY] {self.symbol} Bucket scrapped (Insignificant volume: {self.current_volume/self.target_usd*100:.1f}%). VPIN frozen.")
                self._reset()

        
        return None

    def _harvest_bucket(self, reason: str) -> dict:
        vnofi = self.ofi_sum / self.current_volume if self.current_volume > 0 else 0
        return {
            "symbol": self.symbol,
            "vnofi": vnofi,
            "volume": self.current_volume,
            "ticks": self.bucket_ticks,
            "trigger": reason,
            "type": "HYBRID_BUCKET_COMPLETE"
        }

    def _reset(self):
        import time
        self.current_volume = 0.0
        self.ofi_sum = 0.0
        self.bucket_ticks = 0
        self._bucket_start_time = time.monotonic()


def process_ticks_subroutine(symbol: str, batch: List[dict], target_volume: float, current_state: Optional[dict], priority: int = 2) -> dict:
    import time
    """
    [GEKTOR v5.2] Pure-Function Worker Subroutine.
    Executed inside ProcessPoolExecutor to offload math from the Event Loop.
    
    Rehydrates analytical engines, processes a batch of raw ticks into Dollar Bars,
    calculates VPIN metrics, and returns the results + updated state.
    """
    # 1. Rehydrate Engines
    generator = RealtimeDollarBarGenerator(symbol, target_volume)
    window_z = alpha.VPIN_WINDOW_SIZE
    engine = O1VPINEngine(window_size=window_z, volume_threshold=target_volume, z_threshold=alpha.VPIN_ANOMALY_Z)
    
    if current_state:
        # Rehydrate generator state if provided
        if "generator" in current_state:
            gs = current_state["generator"]
            generator._current_bar = gs.get("current_bar")
            
        # Rehydrate engine state
        if "engine" in current_state:
            es = current_state["engine"]
            engine._imbalances = es.get("imbalances", [0.0] * window_z)
            engine._index = es.get("index", 0)
            engine._is_filled = es.get("is_filled", False)
            engine._running_imbalance_sum = es.get("running_imbalance_sum", 0.0)
            engine._vpin_history = es.get("vpin_history", [0.0] * window_z)
            engine._vpin_sum = es.get("vpin_sum", 0.0)
            engine._vpin_sq_sum = es.get("vpin_sq_sum", 0.0)
            engine._price_history = es.get("price_history", [0.0] * window_z)

    # 2. Process Batch
    results = []
    for tick in batch:
        # Expected tick format: {'p': price, 'v': volume, 'm': is_buyer_maker, 'T': ts}
        # Or Bybit format: {'price': p, 'volume': v, 'side': s, 'timestamp': ts}
        p = float(tick.get('p') or tick.get('price', 0))
        v = float(tick.get('v') or tick.get('volume', 0))
        # Bybit 'side' covers 'Buy'/'Sell'. 
        # In Bybit WS, 'm' (isBuyerMaker) is usually true for Sell orders if it's from the trade topic.
        is_m = tick.get('m')
        if is_m is not None:
             # Binance-style: m=True means Sell (buyer is maker)
             side_str = "SELL" if is_m else "BUY"
        else:
             side_str = str(tick.get('side', '')).upper()
             if side_str not in ("BUY", "SELL"):
                 side_str = "BUY"  # Safe default to avoid empty string
             
        ts = float(tick.get('T') or tick.get('timestamp', time.time() * 1000)) / 1000.0
        
        bars = generator.process_tick(p, v, side_str, ts)
        for bar in bars:
            signal = engine.process_bar(bar)
            if signal:
                results.append({
                    "vpin": signal.vpin_value,
                    "z_score": signal.z_score,
                    "is_anomaly": signal.is_anomaly,
                    "absorption": signal.absorption_detected,
                    "price": bar.close_price,
                    "timestamp": bar.end_timestamp
                })

    # 3. Capture New State
    new_state = {
        "generator": {
            "current_bar": generator._current_bar
        },
        "engine": {
            "imbalances": engine._imbalances,
            "index": engine._index,
            "is_filled": engine._is_filled,
            "running_imbalance_sum": engine._running_imbalance_sum,
            "vpin_history": engine._vpin_history,
            "vpin_sum": engine._vpin_sum,
            "vpin_sq_sum": engine._vpin_sq_sum,
            "price_history": engine._price_history
        }
    }
    
    return {
        "results": results,
        "new_state": new_state
    }
