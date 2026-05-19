import numpy as np
import time
from typing import List, Tuple
from loguru import logger

class ShrapnelRouter:
    """
    [GEKTOR v10.1] Shrapnel Protocol: Smart Egress Router.
    Управление лимитами Bybit API и Tilt-Breaker (Информационный блэкаут).
    """
    def __init__(self, api_limit_per_sec: int = 10, max_usd_per_burst: float = 10000.0, shadow_ledger: Any = None):
        self.api_limit = api_limit_per_sec
        self.token_bucket = float(api_limit_per_sec)
        self.last_refill = time.monotonic()
        self.max_usd_per_burst = max_usd_per_burst
        self.shadow_ledger = shadow_ledger
        self.current_rtt_sec: float = 0.015
        
    def _refill_tokens(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        # Пополнение ведра токенов согласно лимиту Bybit
        self.token_bucket = min(float(self.api_limit), self.token_bucket + (elapsed * self.api_limit))
        self.last_refill = now

    def dispatch_to_operator(self, event_type: str, payload: dict) -> None:
        """
        [GEKTOR v10.2] Асимметричный роутинг событий.
        Разделяет сигналы генерации риска и сигналы спасения капитала в состоянии тильта.
        """
        # Если оператор скомпрометирован (Tilt-Breaker в ShadowLedger)
        if self.shadow_ledger and self.shadow_ledger.is_operator_compromised:
            # 1. СТРОГАЯ БЛОКИРОВКА ВХОДА
            # Любые новые сигналы на вход физически уничтожаются.
            if event_type in ("NEW_ALPHA_INTENT", "MAKER_ENTRY_SUGGESTION", "LFI_OPPORTUNITY"):
                return  # Silent discard. Вакуум для новых идей.

            # 2. КАНАЛ ЭКСТРЕННОГО СПАСЕНИЯ (CRITICAL OVERRIDE)
            # Если это статус открытой позиции, осколка или сигнал к ампутации - 
            # пропускаем безусловно и маркируем как критическое.
            if event_type in ("SHRAPNEL_WARNING", "AMPUTATION_TRIGGERED", "POSITION_BLEEDING", "INFRA_DESYNC"):
                payload["CRITICAL_OVERRIDE"] = True
                payload["UI_COLOR"] = "BLOOD_RED"
                self._render_to_screen(event_type, payload)
                return
                
            # Игнорируем фоновый шум (микроструктурные апдейты без позиций)
            return

        # Штатный режим работы радара: пропускаем всё
        self._render_to_screen(event_type, payload)

    def _render_to_screen(self, event_type: str, payload: dict) -> None:
        """Вывод в UI/Telegram логику (mock)."""
        logger.info(f"📺 [UI_RENDER] {event_type} | {payload.get('symbol', 'GLOBAL')}")

    def allocate_combat_capital(
        self, 
        available_usdt: float, 
        scores: np.ndarray, 
        msq_limits: np.ndarray, 
        base_threshold: float
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        [GEKTOR v10.2] O(N) Роутинг: Скрещивание Alpha Score, RTT-пенальти и MSQ-лимитов.
        """
        self._refill_tokens()
        
        # 1. RTT Штраф: Динамическое ужесточение порога при росте пинга
        dynamic_threshold = base_threshold * np.exp(self.current_rtt_sec * 10)
        
        # 2. Фильтрация "мертворожденных" сигналов
        valid_mask = scores > dynamic_threshold
        valid_indices = np.nonzero(valid_mask)[0]
        
        if len(valid_indices) == 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

        # 3. Tournament: Выбираем TOP-K
        k_tokens = min(int(self.token_bucket), len(valid_indices))
        if k_tokens <= 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

        valid_scores = scores[valid_indices]
        if len(valid_scores) > k_tokens:
            top_k_rel_idx = np.argpartition(valid_scores, -k_tokens)[-k_tokens:]
            winners_idx = valid_indices[top_k_rel_idx]
        else:
            winners_idx = valid_indices

        # 4. Капитал Аллокатор: MSQ Hard Capping
        allocated_volumes = np.zeros(len(winners_idx), dtype=np.float64)
        
        # [SAFETY GATE] Принудительный каст в float64 для проекций
        try:
            total_fund_f = float(available_usdt)
        except (TypeError, ValueError):
            total_fund_f = 0.0

        capital_per_winner = total_fund_f / len(winners_idx)
        
        for i, global_idx in enumerate(winners_idx):
            # msq_limits[global_idx] - это уже float64 (из SHM проекции в AlphaEngine)
            safe_volume = min(capital_per_winner, float(msq_limits[global_idx]))
            allocated_volumes[i] = safe_volume

        self.token_bucket -= len(winners_idx)
        return winners_idx, allocated_volumes

class PhantomFillSensor:
    """
    [GEKTOR v10.5] Детектор "Призрачного Исполнения".
    Анализирует каузальный разрыв между REST и WebSocket.
    """
    def __init__(self, causal_threshold_ms: int = 1500):
        self.pending_correlations: Dict[str, float] = {}
        self.threshold = causal_threshold_ms / 1000.0
        self.last_ws_report_ts = time.time()

    def record_rest_call(self, cl_ord_id: str):
        """Запись исходящего приказа."""
        self.pending_correlations[cl_ord_id] = time.time()

    def record_ws_report(self, cl_ord_id: str):
        """Подтверждение из WebSocket-оракула."""
        self.pending_correlations.pop(cl_ord_id, None)
        self.last_ws_report_ts = time.time()

    def check_for_desync(self) -> bool:
        """
        Проверка на "зомби-состояние" биржи.
        Если есть висящие REST-подтверждения, а WS молчит дольше порога - АЛАРМ.
        """
        if not self.pending_correlations:
            return False
            
        now = time.time()
        oldest_pending = min(self.pending_correlations.values())
        
        if (now - oldest_pending) > self.threshold:
            # Мы отправили приказ, получили HTTP 200, но WS не подтвердил выполнение 
            # в течение 1.5 секунд. Каузальная ткань разорвана.
            logger.critical(f"⚠️ [PHANTOM_FILL] Broker desync detected! Deadman's Switch Triggered.")
            return True
        return False
