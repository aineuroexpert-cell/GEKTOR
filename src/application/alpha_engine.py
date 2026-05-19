import numpy as np
from src.infrastructure.shm_layout import PRICE_SCALE, VOLUME_SCALE

class ZeroAllocationEngine:
    """
    [GEKTOR v9.0] Векторное ядро Alpha Engine.
    Zero-Allocation, Защита от переполнения (float64 cast) и деления на ноль.
    """
    def __init__(self, max_symbols: int):
        # Предварительно выделенные буферы для работы в ОЗУ без участия GC
        self.imb_buffer = np.zeros(max_symbols, dtype=np.float64)
        self.ema_imb_buffer = np.zeros(max_symbols, dtype=np.float64) 
        self.microprice_buffer = np.zeros(max_symbols, dtype=np.float64)
        self.scores_buffer = np.zeros(max_symbols, dtype=np.float64)
        
        # [GEKTOR v12.12] Time-Weighted Anti-Spoofing
        self.last_update_ts = np.zeros(max_symbols, dtype=np.float64)
        # Период полураспада (ms). Чем выше, тем "ленивее" EMA реагирует на изменения.
        self.tau_ms = np.full(max_symbols, 150.0, dtype=np.float64)
        
        # Константы масштабирования для возврата к реальным единицам в FPU
        self.inv_price_scale = 1.0 / PRICE_SCALE
        self.inv_volume_scale = 1.0 / VOLUME_SCALE

    def compute_imbalance_vectorized(self, symbol_idx: int, bids_vol: np.ndarray, asks_vol: np.ndarray, current_ts_ms: float) -> float:
        """
        O(1) Time-Weighted Anti-Spoofing Imbalance.
        Иммунитет к Tick-Spamming атак. Затухание привязано к времени биржи.
        """
        top_bid_vol = bids_vol[0].astype(np.float64)
        top_ask_vol = asks_vol[0].astype(np.float64)
        
        diff = top_bid_vol - top_ask_vol
        total = top_bid_vol + top_ask_vol
        
        # 1. Расчет сырого дисбаланса
        raw_imb = np.divide(
            diff, total, 
            out=np.zeros_like(diff, dtype=np.float64), 
            where=(total != 0)
        )
        self.imb_buffer[symbol_idx] = raw_imb
        
        # 2. Динамический расчет сглаживания на основе Delta-T
        last_t = self.last_update_ts[symbol_idx]
        delta_t = max(current_ts_ms - last_t, 0.0)
        self.last_update_ts[symbol_idx] = current_ts_ms

        if last_t == 0.0:
            # Холодный запуск
            self.ema_imb_buffer[symbol_idx] = raw_imb
            return float(raw_imb)
        
        # [STRICT PHYSICS] Alpha = 1 - e^(-dt/tau)
        # Если dt -> 0 (спам тиками), alpha -> 0 (стакан игнорируется)
        alpha = 1.0 - np.exp(-delta_t / self.tau_ms[symbol_idx])
        
        prev_ema = self.ema_imb_buffer[symbol_idx]
        new_ema = (raw_imb * alpha) + (prev_ema * (1.0 - alpha))
        self.ema_imb_buffer[symbol_idx] = new_ema
        
        return float(new_ema)

    def force_reset_timing(self, symbol_idx: int):
        """
        [RECOVERY SHIELD] Reset absolute time anchor to prevent EMA whiplash.
        Used when a symbol is restored after Storm Mode.
        """
        self.last_update_ts[symbol_idx] = 0.0
        logger.debug(f"♨️ [ALPHA_ENGINE] Timing reset for index {symbol_idx}. Next tick will be COLD_START.")

    def compute_microprice(self, symbol_idx: int, bids: np.ndarray, asks: np.ndarray) -> float:
        """
        Каст во float64 перед перемножением для защиты от int64 overflow.
        BP * AV + AP * BV может превысить 9.2e18 при масштабе 10^8.
        """
        # Извлекаем значения из структурированного массива (view)
        b_price, b_vol = bids['price'][0], bids['volume'][0]
        a_price, a_vol = asks['price'][0], asks['volume'][0]
        
        # Конвертация в f64 и масштабирование обратно к реальным значениям
        bp_f = b_price.astype(np.float64) * self.inv_price_scale
        bv_f = b_vol.astype(np.float64) * self.inv_volume_scale
        ap_f = a_price.astype(np.float64) * self.inv_price_scale
        av_f = a_vol.astype(np.float64) * self.inv_volume_scale
        
        total_vol = bv_f + av_f
        if total_vol > 0:
            # Математика в float64 на уровне FPU - защита от wrap-around
            mp = (bp_f * av_f + ap_f * bv_f) / total_vol
        else:
            # Fallback к среднему при пустом стакане
            mp = (bp_f + ap_f) * 0.5
            
        self.microprice_buffer[symbol_idx] = mp
        return mp

    def update_alpha_score(self, symbol_idx: int, imbalance: float, impact_ratio: float, sector_heat: float = 0.0) -> float:
        """
        [GEKTOR v13.4] Resonance-Aware Alpha Scoring.
        Score = abs(imbalance) * impact_ratio * confidence_multiplier.
        """
        # Confidence multiplier: 0.5 (Cold) -> 1.5 (Hot)
        # We trust structural moves more than idiosyncratic spikes.
        confidence_multiplier = 0.5 + sector_heat 
        
        score = abs(imbalance) * impact_ratio * confidence_multiplier
        self.scores_buffer[symbol_idx] = score
        return float(score)
