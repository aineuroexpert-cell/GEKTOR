import asyncio
from typing import Literal, Dict, Any
from loguru import logger

class BybitPrivateClientProtocol:
    """Type hinting protocol for dependency injection"""
    async def cancel_all_orders(self, symbol: str) -> None: ...
    async def place_order(self, payload: Dict[str, Any]) -> None: ...

class MicrostructureDefender:
    """
    [GEKTOR v14.2 - PROTOCOL ZERO] 
    Autonomous Rescue Mechanism (Circuit Breaker).
    """
    def __init__(self, private_client: Any):
        self.client = private_client
        
    async def trigger_protocol_zero(self, symbol: str, current_price: float, position_size: float, side: Literal["Buy", "Sell"]) -> None:
        """
        Executes Protocol Zero: 
        1. Cancel all existing orders (including Stop Losses).
        2. Fire panic IOC Limit order (0.5% spread penetration) to exit position.
        Uses asyncio.gather for zero-blocking parallel execution.
        """
        logger.critical(f"🚨 [PROTOCOL ZERO] TRIGGERED FOR {symbol}! VPIN COLLAPSE DETECTED.")
        
        # Determine panic price: 0.5% penetration
        # If we are long (side="Buy"), we need to Sell to exit. Panic sell = current_price * 0.995
        # If we are short (side="Sell"), we need to Buy to exit. Panic buy = current_price * 1.005
        exit_side: Literal["Buy", "Sell"] = "Sell" if side == "Buy" else "Buy"
        panic_price = current_price * 0.995 if exit_side == "Sell" else current_price * 1.005
        
        panic_order_payload = {
            "symbol": symbol,
            "side": exit_side,
            "orderType": "Limit",
            "timeInForce": "IOC",
            "qty": str(abs(position_size)),
            "price": f"{panic_price:.4f}",
            "reduceOnly": True
        }
        
        logger.critical(f"🚨 [PROTOCOL ZERO] Sending panic payload: {panic_order_payload}")
        
        # [The Beazley Rule] Parallel execution: Cancel all AND Panic Exit simultaneously
        results = await asyncio.gather(
            self.client.cancel_all_orders(symbol),
            self.client.place_order(panic_order_payload),
            return_exceptions=True
        )
        
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                task_name = "CancelAll" if i == 0 else "PanicExit"
                logger.critical(f"❌ [PROTOCOL ZERO] Task {task_name} failed: {repr(res)}")
        
        if not isinstance(results[1], Exception):
            logger.success(f"🛟 [PROTOCOL ZERO] Panic exit order transmitted successfully for {symbol}.")
