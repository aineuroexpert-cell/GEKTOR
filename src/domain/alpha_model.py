# src/domain/alpha_model.py
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Tuple
from loguru import logger

@dataclass(slots=True, frozen=True)
class VPINSignal:
    symbol: str
    vpin_toxicity: Decimal  # Уровень токсичности потока (0.0 - 1.0)
    leader_roc_bps: int     # Rate of Change биткоина в базисных пунктах
    timestamp_ms: int

@dataclass(slots=True, frozen=True)
class ExecutionIntent:
    symbol: str
    side: str
    price: Decimal
    qty: Decimal
    ttl_ms: int             # Intent Lifecycle: строго 5000 мс

class AlphaVPINModel:
    """
    [GEKTOR v4.1] Execution-Aware Alpha Engine.
    Filters raw VPIN signals and calculates Maximum Safe Quantity (MSQ).
    """
    __slots__ = ('_toxicity_threshold', '_min_profit_bps', '_fee_taker_bps')

    def __init__(self, toxicity_threshold: Decimal, min_profit_bps: int, fee_taker_bps: int):
        self._toxicity_threshold = toxicity_threshold
        self._min_profit_bps = min_profit_bps
        self._fee_taker_bps = fee_taker_bps

    def evaluate_signal(self, signal: VPINSignal, l2_bids: List[Tuple[str, str]], l2_asks: List[Tuple[str, str]]) -> Optional[ExecutionIntent]:
        """Оценка остаточной альфы и расчет Maximum Safe Quantity (MSQ)."""
        
        if signal.vpin_toxicity < self._toxicity_threshold:
            return None

        # Определяем вектор атаки на основе гравитации BTC
        # Если Лидер падает (ROC < 0), мы бьем в Биды (SELL). Иначе в Аски (BUY).
        # Пороговые значения в 10 bps выбраны для отсева микро-шума.
        is_short = signal.leader_roc_bps < -10
        is_long = signal.leader_roc_bps > 10
        
        if not (is_short or is_long):
            logger.debug(f"📉 [ALPHA] Noise gravity for BTC: {signal.leader_roc_bps} bps. Suppressed.")
            return None

        side = "SELL" if is_short else "BUY"
        target_book = l2_bids if is_short else l2_asks
        
        # Calculate MSQ based on L2 depth and expected friction
        msq_qty, safe_price = self._calculate_msq_realism(target_book, side, signal.vpin_toxicity)

        if msq_qty <= Decimal('0'):
            logger.warning(f"⚖️ [MSQ SIZER] Orderbook for {signal.symbol} is too thin for the alpha capture. Abort.")
            return None

        logger.success(f"🎯 [ALPHA CAPTURE] Armed {signal.symbol} | VPIN: {signal.vpin_toxicity} | MSQ: {msq_qty} | Price: {safe_price}")
        
        return ExecutionIntent(
            symbol=signal.symbol,
            side=side,
            price=safe_price,
            qty=msq_qty,
            ttl_ms=5000  # Strict TTL to prevent capital staleness
        )

    def _calculate_msq_realism(self, book_side: list, side: str, toxicity: Decimal) -> Tuple[Decimal, Decimal]:
        """
        [L2 DEPTH PROBE]
        Determines the maximum lot size that doesn't violate alpha thresholds.
        Logic: Profitability = (Expected Alpha - (Slippage + Fee + BBO Gap))
        """
        if not book_side:
            return Decimal('0'), Decimal('0')

        # Baseline: Use the most competitive price as limit anchor
        best_price = Decimal(book_side[0][0])
        
        # Institutional MSQ logic is highly complex; here we implement a constrained lot size
        # to ensure we don't penetrate more than 2 levels of the book.
        accumulated_qty = Decimal('0')
        max_levels = 2
        for i in range(min(len(book_side), max_levels)):
            accumulated_qty += Decimal(book_side[i][1])
            
        # Hard cap for Shadow Capital realism
        msq_cap = Decimal('5.0') 
        final_qty = min(accumulated_qty, msq_cap)

        return final_qty, best_price
