# src/domain/macro_regime.py
from dataclasses import dataclass
from typing import Dict, Optional, List
import time
from collections import deque
from loguru import logger
from src.shared.alpha_config import alpha

@dataclass(slots=True)
class MacroHealth:
    is_panic: bool = False
    btc_vpin: float = 0.0
    btc_delta_pct: float = 0.0
    reason: str = ""

class MacroRegimeFilter:
    """[GEKTOR APEX] MARKET BASELINE (Поводырь) with Windowed ROC."""
    def __init__(self, panic_vpin_threshold: float = 0.85, panic_delta_threshold: float = -0.005):
        self.panic_vpin_threshold = panic_vpin_threshold
        self.panic_delta_threshold = panic_delta_threshold # e.g., -0.5%
        self.current_health = MacroHealth()
        
        # 60-second window for BTC price (O(1) sliding window)
        self._price_window = deque() # List of (timestamp, price)
        self._window_sec = 60
        self._last_panic_time = 0.0
        self._cooldown_sec = 180.0 # 3 minutes

    def update_baseline(self, symbol: str, vpin: float, price: float):
        """[GEKTOR v2.9] Central Gravity Monitor: Track BTC momentum."""
        if symbol != "BTCUSDT": return
        
        now = time.monotonic()
        self._price_window.append((now, price))
        
        # 1. Clean old entries and find window reference
        while self._price_window and now - self._price_window[0][0] > self._window_sec:
            self._price_window.popleft()
            
        if not self._price_window or len(self._price_window) < 2:
            return
        
        # 2. Calculate Windowed ROC (Rate of Change)
        ref_price = self._price_window[0][1]
        delta_pct = (price - ref_price) / ref_price
        
        # 3. Panic Detection & Hysteresis
        is_panic = False
        reason = ""
        
        if vpin > self.panic_vpin_threshold:
            is_panic = True
            reason = f"BTC TOXIC FLOW (VPIN: {vpin:.4f})"
        elif delta_pct < self.panic_delta_threshold:
            is_panic = True
            reason = f"BTC FLASH CRASH (ROC60s: {delta_pct:.2%})"
            
        if is_panic:
            self._last_panic_time = now
            
        # Cooldown guard: Stay in panic mode for _cooldown_sec after last trigger
        in_cooldown = (now - self._last_panic_time) < self._cooldown_sec
        
        self.current_health = MacroHealth(
            is_panic=is_panic or in_cooldown,
            btc_vpin=vpin,
            btc_delta_pct=delta_pct,
            reason=reason if is_panic else (f"COOLDOWN ({int(self._cooldown_sec - (now - self._last_panic_time))}s)" if in_cooldown else "")
        )
        
        if is_panic:
            logger.warning(f"🚨 [MACRO] Market PANIC! {reason}. Global Lock active.")
        elif in_cooldown and not is_panic and int(now) % 30 == 0:
            logger.info(f"⏳ [MACRO] Waiting for stabilization... ROC: {delta_pct:.2%}")

    def should_mute(self, symbol: str) -> bool:
        """Подавлять ли сигнал для данного актива."""
        if symbol == "BTCUSDT": return False # Поводырь никогда не мутится
        return self.current_health.is_panic

    def is_trend_reset(self, symbol: str, current_vpin: float) -> bool:
        """
        [HYSTERESIS RESET]
        Проверка: затух ли предыдущий волатильный кластер?
        Сигнал разрешается только если VPIN упал ниже уровня 'истощения' (0.55).
        """
        # Поводырю разрешено всегда (для апдейта макро), альткоинам нужна 'тишина'
        if symbol == "BTCUSDT": return True
        
        exhaustion_threshold = alpha.VPIN_EXHAUSTION_THRESHOLD
        return current_vpin < exhaustion_threshold
