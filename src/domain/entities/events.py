# src/domain/entities/events.py
from dataclasses import dataclass, field
from datetime import datetime, timezone
import time
from typing import Dict, Any, Optional

@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """
    [GEKTOR APEX] Atomic Trading Event.
    Strictly Typed according to ArjanCodes/Beazley standards.
    """
    symbol: str
    price: float
    volume: float
    side: str  # 'BUY' | 'SELL'
    order_type: str = "ADVISORY"
    timestamp: float = field(default_factory=time.monotonic)
    metadata: Dict[str, Any] = field(default_factory=dict)
    event_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "volume": self.volume,
            "side": self.side,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }

@dataclass(slots=True)
class ConflatedEvent:
    """
    [GEKTOR APEX] Aggregated Event for Heavy Load.
    Preserves microstructure metrics for OFI Velocity.
    """
    symbol: str
    side: str
    total_volume: float
    tick_count: int
    duration_ms: float
    start_ts: float
    end_ts: float
    avg_price: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "volume": self.total_volume,
            "tick_count": self.tick_count,
            "duration_ms": self.duration_ms,
            "price": self.avg_price,
            "metadata": {**self.metadata, "conflated": True}
        }

@dataclass(slots=True, frozen=True)
class StateInvalidationEvent:
    """
    [GEKTOR APEX] MATH CORRUPTION MARKER.
    Fired when the causality chain is broken (Dropped Ticks).
    Forces immediate cessation of trading for the asset.
    """
    symbol: str
    reason: str
    timestamp: float = field(default_factory=time.monotonic)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True, frozen=True)
class EuthanasiaEvent:
    """
    [GEKTOR APEX] TOMBSTONE EVENT.
    Strict command to amputate asset from network and logic layers.
    """
    symbol: str
    reason: str
    cooldown_seconds: int = 900
    timestamp: float = field(default_factory=time.monotonic)
