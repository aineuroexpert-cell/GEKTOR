import time
import collections
import dataclasses
from typing import Optional, Any, Tuple, Dict
from loguru import logger
from src.shared.alpha_config import alpha

@dataclasses.dataclass(slots=True)
class L2Level:
    price: float
    volume: float

@dataclasses.dataclass(slots=True)
class L2Snapshot:
    symbol: str
    best_bid: L2Level
    best_ask: L2Level
    bids: list[L2Level]
    asks: list[L2Level]
    exchange_ts: int


class SpoofingDiscriminator:
    """
    [GEKTOR v8.5] Zero-Allocation Temporal Conflator & CTR Engine.
    Includes Auto-Pruning to prevent RAM exhaustion (OOM).
    """
    __slots__ = ('_level_trades', '_level_cancels', '_decay_factor', '_prune_counter')

    def __init__(self, decay_factor: float = 0.95):
        self._level_trades: Dict[float, float] = collections.defaultdict(float)
        self._level_cancels: Dict[float, float] = collections.defaultdict(float)
        self._decay_factor = decay_factor
        self._prune_counter = 0

    def register_trade(self, price: float, volume: float):
        """O(1) Physical execution observed in publicTrade stream."""
        self._level_trades[price] += volume
        self._check_prune()

    def register_delta(self, price: float, old_vol: float, new_vol: float):
        """O(1) Change in orderbook volume."""
        if new_vol < old_vol:
            self._level_cancels[price] += (old_vol - new_vol)
            self._check_prune()

    def _check_prune(self):
        """[HFT MEM_GUARD] Prevents defaultdict memory leak."""
        self._prune_counter += 1
        if self._prune_counter > 2000:
            self.prune_stale_levels()
            self._prune_counter = 0

    def prune_stale_levels(self):
        """Removes dead price levels from memory."""
        dead_trades = [p for p, v in self._level_trades.items() if v < 1e-5]
        for p in dead_trades: del self._level_trades[p]
        
        dead_cancels = [p for p, v in self._level_cancels.items() if v < 1e-5]
        for p in dead_cancels: del self._level_cancels[p]

    def reconcile_and_get_weight(self, price: float) -> float:
        """
        [HFT Reconciliation] Settle trades vs potential cancels.
        Returns trust weight [0.0 - 1.0].
        """
        trades = self._level_trades.get(price, 0.0)
        cancels_candidate = self._level_cancels.get(price, 0.0)

        # Reconcile: Actual trades subtract from 'cancels_candidate'
        true_cancels = max(0.0, cancels_candidate - trades)
        total_activity = trades + true_cancels
        
        if total_activity < 1e-9:
            return 1.0 # No info -> default trust
        
        # Cancel-to-Trade Ratio (CTR) inversion
        trade_ratio = trades / total_activity
        
        # Memory Decay (Prevents stale spoofing from poisoning price levels)
        if trades > 0: self._level_trades[price] *= self._decay_factor
        if cancels_candidate > 0: self._level_cancels[price] *= self._decay_factor
        
        # Logic: If Trades < 20% of activity -> heavy discount.
        return min(1.0, trade_ratio * 5.0)

class OrderBookSequenceGuard:
    """
    [GEKTOR v8.6] Zero-Tolerance L2 Sequence Validator.
    Protects local orderbook from silent data corruption.
    """
    __slots__ = ('_last_u', '_is_synced')

    def __init__(self):
        self._last_u = -1
        self._is_synced = False

    def validate(self, payload: dict) -> bool:
        """O(1) Validation of causal monotonicity."""
        msg_type = payload.get('type')
        data = payload.get('data', {})
        u_id = data.get('u', -1)

        if msg_type == 'snapshot':
            self._last_u = u_id
            self._is_synced = True
            logger.info(f"🔄 [L2 GUARD] Snapshot synchronized. UID: {u_id}")
            return True

        if msg_type == 'delta':
            if not self._is_synced:
                return False

            if self._last_u > 0 and u_id != self._last_u + 1:
                if u_id <= self._last_u:
                    return False
                logger.critical(
                    f"💥 [L2 GUARD] SEQUENCE GAP! Expected: {self._last_u + 1}, Got: {u_id}."
                )
                self._is_synced = False
                return False

            self._last_u = u_id
            return True
        return False

class MicrostructureAnalyzer:
    """
    [GEKTOR v8.5] Zero-Allocation Microstructure Extractor.
    Calculates BBO Imbalance and Volume-Weighted Microprice.
    """
    __slots__ = ('_bbo_imbalance', '_microprice', '_spread')

    def __init__(self):
        self._bbo_imbalance = 0.0
        self._microprice = 0.0
        self._spread = 0.0

    def extract_features(self, bid_p: float, bid_v: float, ask_p: float, ask_v: float, 
                         bid_weight: float = 1.0, ask_weight: float = 1.0) -> Tuple[float, float, float]:
        
        effective_bid_v = bid_v * bid_weight
        effective_ask_v = ask_v * ask_weight
        
        self._spread = ask_p - bid_p
        total_v = effective_bid_v + effective_ask_v
        
        if total_v < 1e-9:
            return (bid_p + ask_p) / 2.0, 0.0, self._spread

        # BBO Imbalance ([-1.0, 1.0])
        self._bbo_imbalance = (effective_bid_v - effective_ask_v) / total_v
        
        # Microprice calculation
        self._microprice = (bid_p * effective_ask_v + ask_p * effective_bid_v) / total_v
        
        return self._microprice, self._bbo_imbalance, self._spread

class MicrostructureDefender:
    """
    [GEKTOR v6.0] Real-time L2 Anomaly Detection & Normalization.
    Coordinates Analyzer, Discriminator and Sequence Guard.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        self._analyzer = MicrostructureAnalyzer()
        self._discriminator = SpoofingDiscriminator()
        self._sequence_guard = OrderBookSequenceGuard()
        self._imbalance_threshold = float(alpha.MICRO_OFI.get("imbalance_threshold", 0.55))
        
    def reset_state(self):
        self._sequence_guard = OrderBookSequenceGuard()
        self._discriminator = SpoofingDiscriminator()
        self._imbalance_threshold = float(alpha.MICRO_OFI.get("imbalance_threshold", 0.55))
        logger.warning(f"♻️ [DEFENDER] State reset for {self.symbol}")
        
    async def ingest_snapshot(self, snapshot: L2Snapshot) -> dict:
        if not snapshot.bids or not snapshot.asks:
            return {"bbo_imbalance": 0.0, "state": "IDLE", "is_new_impulse": False}
            
        if snapshot.bids[0].price >= snapshot.asks[0].price:
            logger.critical(f"🚨 [DEFENDER] SPREAD INVERSION on {self.symbol}!")
            return {"bbo_imbalance": 0.0, "state": "IDLE", "is_new_impulse": False}

        # Вычисляем вес доверия к Bid/Ask через дискриминатор
        bid_weight = self._discriminator.reconcile_and_get_weight(snapshot.bids[0].price)
        ask_weight = self._discriminator.reconcile_and_get_weight(snapshot.asks[0].price)

        microprice, imbalance, spread = self._analyzer.extract_features(
            snapshot.bids[0].price, snapshot.bids[0].volume,
            snapshot.asks[0].price, snapshot.asks[0].volume,
            bid_weight=bid_weight, ask_weight=ask_weight
        )
        
        state = "IDLE"
        if imbalance > self._imbalance_threshold:
            state = "BUY_IMPULSE"
        elif imbalance < -self._imbalance_threshold:
            state = "SELL_IMPULSE"
        
        return {
            "bbo_imbalance": imbalance, 
            "state": state,
            "is_new_impulse": state != "IDLE",
            "microprice": microprice,
            "spread": spread
        }
