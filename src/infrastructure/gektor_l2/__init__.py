"""
GEKTOR APEX L2 Engine — strict modules: universe, ndarray orderbook, WS multiplexer.
"""

from src.infrastructure.gektor_l2.book_state import BookReadiness, BookState
from src.infrastructure.gektor_l2.bybit_orderbook_rest import BybitLinearOrderbookRestSource
from src.infrastructure.gektor_l2.errors import BybitRestRateLimited, SnapshotIsolationError
from src.infrastructure.gektor_l2.nd_orderbook import CrossAssetSnapshot, NdOrderBookStateMachine
from src.infrastructure.gektor_l2.protocols import (
    AbstractOrderBookProcessor,
    AbstractOrderBookResyncSource,
)
from src.infrastructure.gektor_l2.reconnect_throttle import AsyncReconnectTokenBucket
from src.infrastructure.gektor_l2.resync_gate import RestResyncGate
from src.infrastructure.gektor_l2.universe_manager import (
    ActiveUniverse,
    DynamicUniverseManager,
    InstrumentSpec,
    load_universe_books,
)
from src.infrastructure.gektor_l2.ws_multiplexer import L2OrderBookWebSocketMultiplexer

__all__ = [
    "AbstractOrderBookProcessor",
    "AbstractOrderBookResyncSource",
    "ActiveUniverse",
    "AsyncReconnectTokenBucket",
    "BookReadiness",
    "BookState",
    "BybitLinearOrderbookRestSource",
    "BybitRestRateLimited",
    "CrossAssetSnapshot",
    "DynamicUniverseManager",
    "InstrumentSpec",
    "L2OrderBookWebSocketMultiplexer",
    "NdOrderBookStateMachine",
    "RestResyncGate",
    "SnapshotIsolationError",
    "load_universe_books",
]
