import numpy as np
from typing import Dict, List
from loguru import logger
from src.infrastructure.shm_layout import SHMOrderBook, get_shm_view_np

class PassiveAuditRingBuffer:
    """
    [GEKTOR v12.5] Пассивное Временное Кольцо.
    Аудит сигналов без аллокации корутин и asyncio.sleep.
    """
    def __init__(self, capacity: int = 10000, num_models: int = 50):
        self.capacity = capacity
        self.head = 0
        self.tail = 0
        # [timestamp, sym_idx, model_idx, intent_price, side]
        self.matrix = np.zeros((capacity, 5), dtype=np.float64)
        self.biological_offset = 1.2

    def push(self, sym_idx: int, mod_idx: int, price: float, side: float):
        self.matrix[self.tail] = [time.time(), sym_idx, mod_idx, price, side]
        self.tail = (self.tail + 1) % self.capacity
        if self.tail == self.head: self.head = (self.head + 1) % self.capacity

    def audit(self, current_mid_prices: np.ndarray, model_pnl: np.ndarray):
        now = time.time()
        while self.head != self.tail:
            if now - self.matrix[self.head, 0] < self.biological_offset: break
            
            # Разгружаем сигнал для аудита
            _, sym_idx, mod_idx, intent_p, side = self.matrix[self.head]
            actual_p = current_mid_prices[int(sym_idx)]
            
            # [ADVERSE SELECTION CHECK]
            # Если купили (side=1) и цена упала - штраф.
            # Если продали (side=-1) и цена выросла - штраф.
            drift = (actual_p - intent_p) / intent_p * side
            model_pnl[int(mod_idx)] += drift
            
            self.head = (self.head + 1) % self.capacity

class PrismTournamentEngine:
    def __init__(self, num_models: int = 50):
        self.num_models = num_models
        self.vpin_thresholds = np.linspace(0.4, 0.8, num_models)
        self.decay_speeds = np.linspace(0.0001, 0.005, num_models)
        
        self.virtual_pnl = np.zeros(num_models, dtype=np.float64)
        self.audit_ring = PassiveAuditRingBuffer(num_models=num_models)
        
        # [TACTICAL WARM-UP] Насыщенность окон
        self.saturation_map: Dict[int, int] = {} # sym_idx -> tick_count
        self.required_saturation = 100 # тиков для прогрева

    def is_symbol_warm(self, sym_idx: int) -> bool:
        """Проверка детерминированной готовности окна по монете."""
        return self.saturation_map.get(sym_idx, 0) >= self.required_saturation

    def evaluate_and_audit(self, sym_idx: int, imbal: float, vpin: float, current_mid: float, mid_prices_all: np.ndarray):
        """
        [ATOMIC TICK]
        1. Прогрев статистического окна.
        2. Аудит созревших виртуальных сделок.
        3. Расчет новых скоров.
        """
        # 1. Прогрев
        self.saturation_map[sym_idx] = self.saturation_map.get(sym_idx, 0) + 1
        
        # 2. Аудит (институциональный PnL)
        self.audit_ring.audit(mid_prices_all, self.virtual_pnl)
        
        # 3. Расчет только если "прогреты"
        if not self.is_symbol_warm(sym_idx):
            return np.zeros(self.num_models)

        # Vectorized Alpha Matrix
        scores = np.where(vpin > self.vpin_thresholds, imbal * np.exp(-self.decay_speeds), 0.0)
        
        # Регистрация виртуального входа (только для топ-модели или для пробного шага)
        # Для турнира мы регистрируем вход каждой модели, пробившей свой порог
        for i in np.where(scores > 0.1)[0]:
            self.audit_ring.push(sym_idx, i, current_mid, 1.0 if imbal > 0 else -1.0)
            
        return scores

# Использование в AlphaEngine:
# Вместо одного скора, вызываем PrismTournamentEngine.evaluate_all()
# и транслируем сигнал только от модели с лучшим Shadow PnL.
