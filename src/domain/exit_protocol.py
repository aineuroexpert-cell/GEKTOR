# src/domain/exit_protocol.py
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Protocol, Tuple
from loguru import logger

# ==========================================
# 1. БАЗОВЫЕ DTO И СТАРЫЕ СОСТОЯНИЯ
# ==========================================
@dataclass(slots=True, frozen=True)
class L2Level:
    price: float
    volume: float

class SignalState(Enum):
    ACTIVE = auto()
    INVALIDATED_TOXIC_FLOW = auto()
    INVALIDATED_VOLUME_SPIKE = auto()
    INVALIDATED_TIME_STOP = auto()
    CLOSED_MANUALLY = auto()

@dataclass(slots=True, frozen=True)
class MarketTick:
    symbol: str
    price: float
    volume: float
    side: str  
    exchange_ts: int  
    conflated: bool = False

@dataclass(slots=True, frozen=True)
class MarketSnapshot:
    bid_price: float
    ask_price: float
    mid_price: float
    timestamp: float

# ==========================================
# 2. НОВЫЕ СОСТОЯНИЯ (ДВИЖОК ВЫХОДА)
# ==========================================
class ExitReason(Enum):
    NONE = auto()
    TIME_DECAY = auto()         
    WALL_COLLAPSE = auto()      
    DYNAMIC_TARGET = auto()     
    TOXIC_FLOW = auto()         
    MANUAL_OVERRIDE = auto()    

# ==========================================
# 3. ГИБРИДНЫЙ СТЕЙТ СИГНАЛА
# ==========================================
@dataclass(slots=True)
class ActiveSignal:
    signal_id: str
    symbol: str
    entry_ts: float 
    entry_price: float = 0.0
    entry_bid: float = 0.0
    entry_ask: float = 0.0
    entry_vwap: float = 0.0
    direction: int = 1 
    
    # [LATENCY GUARD]
    human_entry_bid: float = 0.0
    human_entry_ask: float = 0.0
    human_entry_vwap: float = 0.0
    exit_vwap: float = 0.0
    
    # Старые поля для SignalTracker
    state: SignalState = SignalState.ACTIVE
    bars_observed: int = 0
    max_vpin: float = 0.0
    
    # [НОВЫЕ ПОЛЯ ДЛЯ MICROSTRUCTURAL EXIT ENGINE]
    anchor_price: float = 0.0
    initial_anchor_volume: float = 0.0
    exit_reason: ExitReason = ExitReason.NONE

# ==========================================
# 4. MICROSTRUCTURAL EXIT ENGINE (БОЕВОЙ ЩИТ)
# ==========================================
class MicrostructuralExitEngine:
    def __init__(self, max_holding_sec: float = 180.0, wall_collapse_pct: float = 0.75):
        self.max_holding_sec = max_holding_sec
        self.wall_collapse_pct = wall_collapse_pct

    def evaluate_position(self, signal: ActiveSignal, bids: List[L2Level], asks: List[L2Level], current_ts: float) -> ExitReason:
        if signal.exit_reason != ExitReason.NONE:
            return signal.exit_reason

        # 1. TIME DECAY
        if current_ts - signal.entry_ts > self.max_holding_sec:
            logger.warning(f"⏳ [EXIT] Time Decay: Сделка {signal.symbol} зависла более {self.max_holding_sec}с. Ликвидация.")
            return ExitReason.TIME_DECAY

        # 2. STRUCTURAL STOP-LOSS
        if signal.anchor_price > 0 and signal.initial_anchor_volume > 0:
            book_side = bids if signal.direction > 0 else asks
            current_anchor_vol = self._get_volume_at_price(signal.anchor_price, book_side)
            
            if current_anchor_vol < (signal.initial_anchor_volume * (1.0 - self.wall_collapse_pct)):
                logger.critical(f"👻 [EXIT] Structural Stop: Плита на {signal.anchor_price} испарилась! (Стало: {current_anchor_vol}). ЭКСТРЕННЫЙ ВЫХОД.")
                return ExitReason.WALL_COLLAPSE

        # 3. DYNAMIC TAKE PROFIT
        if signal.direction > 0 and asks:
            best_ask_vol = asks[0].volume
            if best_ask_vol > signal.initial_anchor_volume * 1.5:
                logger.success(f"🎯 [EXIT] Dynamic Target: Встречная стена на Ask. Фиксируем ПнЛ.")
                return ExitReason.DYNAMIC_TARGET
                
        elif signal.direction < 0 and bids:
            best_bid_vol = bids[0].volume
            if best_bid_vol > signal.initial_anchor_volume * 1.5:
                logger.success(f"🎯 [EXIT] Dynamic Target: Встречная стена на Bid. Фиксируем ПнЛ.")
                return ExitReason.DYNAMIC_TARGET

        return ExitReason.NONE

    def _get_volume_at_price(self, target_price: float, levels: List[L2Level]) -> float:
        for level in levels:
            if abs(level.price - target_price) < 0.00001:
                return level.volume
        return 0.0 

# ==========================================
# 5. СТАРЫЕ ПРАВИЛА И СИМУЛЯТОР (ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ)
# ==========================================
class ExecutionSimulator:
    def __init__(self, taker_fee_bps: float = 4.0):
        self.taker_fee_bps = taker_fee_bps

    def calculate_vwap_execution(self, levels: List[L2Level], target_usd_volume: float) -> float:
        remaining_volume = target_usd_volume
        total_cost = 0.0
        for level in levels:
            if remaining_volume <= 0: break
            fill_qty = min(remaining_volume / level.price, level.volume)
            fill_cost = fill_qty * level.price
            total_cost += fill_cost
            remaining_volume -= fill_cost
        if remaining_volume > 0:
            return float('inf') 
        return total_cost / target_usd_volume

    def calculate_real_markout_bps(self, direction: int, entry_bids: List[L2Level], entry_asks: List[L2Level], exit_bids: List[L2Level], exit_asks: List[L2Level], target_usd: float = 10000.0) -> float:
        if direction > 0:
            entry_price = self.calculate_vwap_execution(entry_asks, target_usd)
            exit_price = self.calculate_vwap_execution(exit_bids, target_usd)
        else:
            entry_price = self.calculate_vwap_execution(entry_bids, target_usd)
            exit_price = self.calculate_vwap_execution(exit_asks, target_usd)

        if entry_price == 0 or entry_price == float('inf') or exit_price == float('inf'): 
            return -999.0 
        gross_pnl_bps = ((exit_price - entry_price) / entry_price) * 10000 * (1 if direction > 0 else -1)
        return gross_pnl_bps - (self.taker_fee_bps * 2)

class InvalidationRule(Protocol):
    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        ...

class TimeStopRule:
    def __init__(self, max_holding_bars: int):
        self.max_holding_bars = max_holding_bars
    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        return None

class VPINDecayRule:
    def __init__(self, decay_factor: float):
        self.decay_factor = decay_factor
    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        if signal.max_vpin > 0:
            threshold = signal.max_vpin * self.decay_factor
            if current_vpin < threshold and signal.bars_observed > 3:
                return SignalState.INVALIDATED_TOXIC_FLOW
        return None

class MicrostructureSpikeRule:
    def __init__(self, critical_vol_mult: float = 5.0):
        self.critical_vol_mult = critical_vol_mult
    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        return None