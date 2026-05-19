# src/application/inventory_manager.py
from decimal import Decimal
from loguru import logger
from typing import Dict, Any

class InventoryManager:
    """
    [GEKTOR v8.5] Shrapnel & Orphan Position Management.
    Ensures state purity after network blackouts.
    """
    __slots__ = ('_target_sizes', '_active_shrapnel')

    def __init__(self):
        self._target_sizes: Dict[str, Decimal] = {}
        self._active_shrapnel: Dict[str, Any] = {}

    def set_target_size(self, symbol: str, size: Decimal):
        self._target_sizes[symbol] = size

    def audit_position(self, symbol: str, current_size: Decimal):
        """
        Compares Ground Truth (Exchange) with Local Intent.
        If current_size < 80% of target_size -> Mark as SHRAPNEL.
        """
        target = self._target_sizes.get(symbol, Decimal('0'))
        
        if current_size == 0:
            return "CLEAN"
            
        if target == 0:
            logger.warning(f"🚨 [INVENTORY] Orphan Position detected for {symbol}: {current_size} BTC. No target recorded!")
            return "ORPHAN"

        ratio = current_size / target
        if ratio < 0.95:
             logger.warning(f"🩹 [INVENTORY] Shrapnel Position noticed for {symbol}: {current_size}/{target} ({ratio:.1%}).")
             return "SHRAPNEL"
             
        return "HEALTHY"

    def handle_shrapnel(self, symbol: str, current_size: Decimal):
        """
        Protocol: Passive Exit. 
        We don't force 'fill-up' (Adverse Selection risk). 
        We exit at Microprice to preserve fees.
        """
        logger.info(f"🛡️ [INVENTORY] Initiating SHRAPNEL_EXIT for {symbol} ({current_size} units).")
        # In v9.0 this triggers the PassiveAggressiveSlicer
        return {
            "action": "PASSIVE_EXIT",
            "symbol": symbol,
            "qty": current_size,
            "reason": "INVALID_SIZE_AFTER_BLACKOUT"
        }
