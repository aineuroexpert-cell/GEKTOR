from typing import Dict, Any, TypedDict, Literal
import logging

logger = logging.getLogger("GEKTOR.Executor")

class ExecutionCapsule(TypedDict):
    symbol: str
    orderType: Literal["Limit"]
    timeInForce: Literal["IOC"]
    price: str
    qty: str
    side: Literal["Buy", "Sell"]

def build_execution_capsule(signal_data: Dict[str, Any], side: Literal["Buy", "Sell"], qty: float) -> ExecutionCapsule:
    """
    [GEKTOR v14.2] Execution Capsule Builder.
    Creates a pre-computed JSON payload for manual 1-click execution.
    Enforces IOC Limit orders with exactly 2 bps slippage tolerance.
    """
    t0_price = float(signal_data["price"])
    symbol = str(signal_data["symbol"])
    
    # 2 bps = 0.02% = 0.0002
    # Long (Buy): accept filling up to T0 + 2 bps
    # Short (Sell): accept filling down to T0 - 2 bps
    if side == "Buy":
        limit_price = t0_price * 1.0002
    else:
        limit_price = t0_price * 0.9998
        
    logger.info(f"🛡️ [CAPSULE] Built for {symbol}: {side} {qty} @ Limit {limit_price:.4f} (IOC)")
        
    return {
        "symbol": symbol,
        "orderType": "Limit",
        "timeInForce": "IOC",
        "price": f"{limit_price:.4f}",
        "qty": str(qty),
        "side": side
    }
