import math
from typing import Optional
from dataclasses import dataclass
from loguru import logger

@dataclass
class SimulatedOrder:
    price: float
    volume: float
    side: str
    v_ahead: float
    filled_volume: float = 0.0
    is_filled: bool = False

class PessimisticFillSimulator:
    """
    [GEKTOR v21.73] Conservative MAKER Backtest Engine.
    Prevents the "Grail Illusion" by mathematically simulating Queue Dynamics 
    and Adverse Selection instead of naive price-touch fills.
    """
    def __init__(self, latency_penalty_ms: float = 50.0):
        self.latency_penalty = latency_penalty_ms
        self.active_order: Optional[SimulatedOrder] = None

    def place_virtual_order(self, price: float, volume: float, side: str, l2_volume_at_price: float):
        """
        Orders are placed at the BACK of the queue.
        We capture the total L2 volume as our starting V_ahead.
        """
        # Conservative Assumption: Everyone else is faster. 
        # We start at the absolute tail of the visible liquidity.
        self.active_order = SimulatedOrder(
            price=price,
            volume=volume,
            side=side,
            v_ahead=l2_volume_at_price
        )
        logger.debug(f"📝 Virtual Order Placed. Queue Position: {l2_volume_at_price:.2f} behind.")

    def process_market_update(self, current_v_l2: float, previous_v_l2: float, trade_price: float, trade_volume: float):
        """
        Processes real historical LOB updates and tape trades.
        Applies Iceberg Detection to penalize our queue advancement.
        """
        order = self.active_order
        if not order or order.is_filled:
            return

        # 1. Absolute Certainty Fill (Price moved THROUGH our level)
        if (order.side == "BUY" and trade_price < order.price) or \
           (order.side == "SELL" and trade_price > order.price):
            order.filled_volume = order.volume
            order.is_filled = True
            logger.success("✅ Level obliterated. 100% Fill Guaranteed.")
            return

        # 2. Queue Depletion at our exact price level
        if trade_price == order.price:
            # The Iceberg Detection Protocol (Reality Check)
            delta_v_l2 = current_v_l2 - previous_v_l2
            
            # If trade was 100, but L2 only dropped by 20, 80 was hidden.
            replenished_hidden_volume = max(0, trade_volume + delta_v_l2)
            
            # We only advance if visible liquidity was actually burned
            effective_trade_impact = max(0, trade_volume - replenished_hidden_volume)
            
            # Burn the queue ahead of us
            if order.v_ahead > 0:
                burn = min(order.v_ahead, effective_trade_impact)
                order.v_ahead -= burn
                effective_trade_impact -= burn
            
            # If we reached the front, start filling our order
            if order.v_ahead <= 0 and effective_trade_impact > 0:
                fill_amount = min(order.volume - order.filled_volume, effective_trade_impact)
                order.filled_volume += fill_amount
                
                if order.filled_volume >= order.volume:
                    order.is_filled = True
                    logger.success("✅ Queue Depleted. Order Filled.")

        # 3. Spoofing Detection (Boundary Condition Correction)
        # Even if trades didn't happen, if someone ahead of us cancelled, we advance.
        if current_v_l2 > 0:
            # We cannot be further back than the total current volume minus what we've simulated as ours
            remaining_virtual_vol = order.volume - order.filled_volume
            order.v_ahead = min(order.v_ahead, current_v_l2 - remaining_virtual_vol)
