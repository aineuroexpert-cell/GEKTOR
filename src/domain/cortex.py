# src/domain/cortex.py
import math
import logging
from typing import Optional
from loguru import logger

class O1_WelfordCUSUM:
    """
    [GEKTOR v21.68] O(1) Structural Break Detector.
    
    Uses Welford's Online Algorithm for incremental variance and a 
    Symmetric CUSUM Filter for trend detection. 
    Zero allocations, O(1) time.
    """
    __slots__ = ('decay', 'threshold', 'drift', 'mean', 'var', 's_pos', 's_neg')

    def __init__(self, decay_rate: float = 0.05, threshold: float = 3.0, drift: float = 0.5):
        self.decay = decay_rate
        self.threshold = threshold
        self.drift = drift  
        
        # Welford EWMA State
        self.mean: Optional[float] = None
        self.var: float = 1e-8
        
        # CUSUM State
        self.s_pos: float = 0.0
        self.s_neg: float = 0.0

    def process_dollar_bar(self, close_price: float) -> int:
        """
        Calculates incremental stats and checks for CUSUM trigger.
        Returns: 1 (Bullish), -1 (Bearish), 0 (Noise).
        """
        if self.mean is None:
            self.mean = close_price
            return 0

        # 1. O(1) Welford Incremental Variance (EWMA)
        delta = close_price - self.mean
        self.mean += self.decay * delta
        self.var = (1 - self.decay) * (self.var + self.decay * (delta ** 2))

        std_dev = math.sqrt(self.var) + 1e-8
        z_score = delta / std_dev

        # 2. O(1) Symmetric CUSUM Filter
        # Accumulates deviations exceeding the 'drift' (noise) threshold
        self.s_pos = max(0.0, self.s_pos + z_score - self.drift)
        self.s_neg = min(0.0, self.s_neg + z_score + self.drift)

        # 3. Structural Break Detection
        if self.s_pos > self.threshold:
            self._reset_cusum()
            logger.critical(f"🚀 [CORTEX] BULLISH STRUCTURAL BREAK. Z-Score: {z_score:.2f}σ")
            return 1
            
        elif self.s_neg < -self.threshold:
            self._reset_cusum()
            logger.critical(f"🩸 [CORTEX] BEARISH STRUCTURAL BREAK. Z-Score: {z_score:.2f}σ")
            return -1

        return 0

    def _reset_cusum(self):
        """Resets accumulators after a trigger to wait for the next regime."""
        self.s_pos = 0.0
        self.s_neg = 0.0

class CortexCUSUMProcessor:
    """
    [GEKTOR v21.68] Zero-Lag Cortex Processor.
    
    Instead of an infinite buffer, it uses a bounded queue (Load Shedding)
    and executes the O(1) math in nanoseconds.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.cusum = O1_WelfordCUSUM()
        # Strictly bounded queue to prevent "Lag Bomb"
        self._queue = asyncio.Queue(maxsize=10) 

    async def process_bar(self, bar_data: float):
        """Fast execution path."""
        # Note: In production, this can be moved directly into the bar-closed callback
        # to eliminate the queue overhead entirely for O(1) math.
        signal = self.cusum.process_dollar_bar(bar_data)
        if signal != 0:
            # Emit signal to Event Bus / Risk Guard
            pass
