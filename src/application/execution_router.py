# src/application/execution_router.py
import asyncio
from enum import Enum
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Dict, Optional, Any
from loguru import logger

class OrderStatus(Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    PENDING_AMEND = "PENDING_AMEND" # Суперпозиция (In-Flight)
    REJECTED = "REJECTED"

@dataclass(slots=True)
class OrderState:
    order_id: str
    symbol: str
    qty: str
    price: str
    order_link_id: str  # Наш Client ID для детерминированной реконсиляции
    status: OrderStatus = OrderStatus.NEW
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def is_terminal(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED)

class SmartExecutionRouter:
    """
    [GEKTOR v8.4] State-Safe Execution Router.
    Uses atomic 'amendOrder' and protects against REST/WS race conditions.
    """
    __slots__ = ('_active_orders', '_api_client', '_link_id_map')

    def __init__(self, api_client: Any):
        self._active_orders: Dict[str, OrderState] = {} # Key: exchange_order_id
        self._link_id_map: Dict[str, str] = {} # order_link_id -> exchange_order_id
        self._api_client = api_client

    async def execute_amend(self, order_id: str, new_price: float) -> bool:
        """Surgical order repositioning with In-Flight guard."""
        order = self._active_orders.get(order_id)
        if not order:
            return False

        async with order._lock:
            # 1. Absorbing State Check (Terminal lockout)
            if order.is_terminal:
                logger.warning(f"⚠️ [ROUTER] Amend aborted: Order {order_id} is already {order.status.name}")
                return False
            
            # 2. In-Flight Protection
            if order.status == OrderStatus.PENDING_AMEND:
                return False

            previous_status = order.status
            order.status = OrderStatus.PENDING_AMEND

        # I/O Phase: Lock released to allow WebSocket status processing
        try:
            logger.info(f"📤 [REST] Atomic Amend: {order_id} -> {new_price:.2f}")
            success = await self._api_client.amend_order(order_id, price=str(new_price), symbol=order.symbol)
        except Exception as e:
            logger.error(f"💥 [REST] Amend I/O Failure: {e}")
            success = False

        # Resolution Phase: Re-lock and reconcile
        async with order._lock:
            if order.is_terminal:
                logger.warning(f"👻 [ROUTER] Ghost Amend: WS filled order {order_id} during REST I/O.")
                return False

            if success:
                order.status = OrderStatus.NEW
                order.price = str(new_price)
                return True
            else:
                order.status = previous_status
                return False

    async def on_ws_execution_report(self, payload: dict):
        """Oracle: Single Source of Truth (Private WSS)."""
        order_id = payload.get('orderId')
        link_id = payload.get('orderLinkId')
        
        # Mapping link_id to exchange_order_id if not known
        if link_id and link_id not in self._link_id_map and order_id:
            self._link_id_map[link_id] = order_id

        order = self._active_orders.get(order_id)
        if not order: 
            return

        async with order._lock:
            status_str = payload.get('orderStatus')
            if status_str == "Filled":
                order.status = OrderStatus.FILLED
                logger.success(f"✅ [WS] Order {order_id} FILLED. Releasing memory.")
                self._cleanup(order_id)
            elif status_str in ("Cancelled", "Deactivated"):
                order.status = OrderStatus.CANCELED
                logger.warning(f"🗑️ [WS] Order {order_id} CANCELED.")
                self._cleanup(order_id)
            elif status_str == "Rejected":
                order.status = OrderStatus.REJECTED
                logger.error(f"❌ [WS] Order {order_id} REJECTED.")
                self._cleanup(order_id)

    def _cleanup(self, order_id: str):
        order = self._active_orders.pop(order_id, None)
        if order and order.order_link_id:
            self._link_id_map.pop(order.order_link_id, None)

    def register_order(self, order: OrderState):
        self._active_orders[order.order_id] = order
        if order.order_link_id:
            self._link_id_map[order.order_link_id] = order.order_id
