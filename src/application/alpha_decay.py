import numpy as np

class ShrapnelEvictor:
    """
    [GEKTOR v9.6] Протокол экстренной ампутации токсичных лимитов.
    Оперирует пределами VaR и эрозией альфы. Никакой "надежды".
    """
    def __init__(self, max_loss_bps: float = 15.0):
        # Базисные пункты (bps). 15 bps = 0.15% максимального допустимого урона.
        self.max_loss_bps = max_loss_bps

    def calculate_decay_score(self, initial_score: float, time_held_ms: float, volume_imbalance: float) -> float:
        """
        [ALPHA DECAY] Экспоненциальное сгорание валидности сигнала.
        Скорость распада (lambda) растет при давлении против позиции.
        """
        # decay_rate увеличивается, если дисбаланс (OFI) подтверждает разворот
        decay_rate = 0.001 * np.exp(-volume_imbalance * 5) 
        return initial_score * np.exp(-decay_rate * time_held_ms)

    def compute_amputation_price(self, entry_price: Decimal, side: str, current_bbo_price: Decimal) -> Decimal:
        """
        [HARD AMNPUTATION] Расчет цены агрессивного IoC-выхода.
        Никаких float. Работаем в чистом Decimal для финансовой точности.
        """
        # ИНЖЕНЕРНЫЙ СТАНДАРТ: Константы в Decimal инициализируются СТРОГО через str()
        tolerance_offset = entry_price * (Decimal(str(self.max_loss_bps)) / Decimal('10000.0'))
        
        if side.capitalize() == "Buy": # Мы в Long (нужно продать / Hit the Bid)
            hard_floor = entry_price - tolerance_offset
            return max(current_bbo_price, hard_floor)
        else: # Мы в Short (нужно откупить / Lift the Ask)
            hard_ceiling = entry_price + tolerance_offset
            return min(current_bbo_price, hard_ceiling)
