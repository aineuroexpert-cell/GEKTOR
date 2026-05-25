# src/application/quarantine.py
import asyncio
from typing import Set, Dict, List
from collections import deque
import time
from loguru import logger
from src.domain.entities.events import EuthanasiaEvent

class QuarantineManager:
    """
    [GEKTOR v2.2] State Replay & Tombstone Coordinator.
    Solves the Event Sourcing paradox of Pit-Stop Hydration and Ghost Subscriptions.
    """
    def __init__(self, event_bus, core_assets: Set[str] = frozenset(["BTCUSDT", "ETHUSDT", "SOLUSDT"])):
        self._corrupted_symbols: Set[str] = set()
        self._quarantine_stash: Dict[str, deque] = {}
        self._tombstones: Dict[str, float] = {}
        self.core_assets = core_assets
        self.event_bus = event_bus

    def is_dead(self, symbol: str) -> bool:
        """[TOMBSTONE GATEKEEPER] Prevents processing of euthanized assets."""
        if symbol not in self._tombstones:
            return False
        if time.monotonic() > self._tombstones[symbol]:
            del self._tombstones[symbol]
            return False
        return True

    def bury(self, symbol: str, cooldown: int = 900) -> None:
        """Physical eviction and burial of an asset."""
        self._tombstones[symbol] = time.monotonic() + cooldown
        self._corrupted_symbols.discard(symbol)
        if symbol in self._quarantine_stash:
            del self._quarantine_stash[symbol]
        logger.warning(f"🪦 [TOMBSTONE] {symbol} is dead. Buried for {cooldown}s.")

    def mark_corrupted(self, symbol: str) -> None:
        """Locks causality and starts stashing live WS stream."""
        if symbol not in self._corrupted_symbols:
            self._corrupted_symbols.add(symbol)
            # Safe boundary. Core assets get larger margin of safety.
            stash_size = 10000 if symbol in self.core_assets else 2000
            self._quarantine_stash[symbol] = deque(maxlen=stash_size)
            logger.warning(f"🔒 [CAUSALITY LOCK] {symbol} locked. Catch-up buffer (max={stash_size}) initialized.")

    def intercept_ws_tick(self, symbol: str, tick_data: dict) -> bool:
        """
        Intercepts raw ticks from Ingress.
        Returns False if intercepted into quarantine, True if clear to proceed.
        """
        if symbol in self._corrupted_symbols:
            if symbol in self._quarantine_stash:
                if len(self._quarantine_stash[symbol]) == self._quarantine_stash[symbol].maxlen:
                    # Stash Overflow - This is the Triage breaking point
                    if symbol not in self.core_assets:
                        logger.critical(f"☠️ [TRIAGE] {symbol} stash overflowing. Executing Order 66.")
                        self.event_bus.publish_fire_and_forget(EuthanasiaEvent(
                            symbol=symbol,
                            reason="QUARANTINE_OVERFLOW",
                            cooldown_seconds=900
                        ))
                        return False # Dropped until Orchestrator executes death
                    else:
                        logger.critical(f"🚨 [CORE INTEGRITY] {symbol} stash overflowing. Data truncation (VPIN drift).")
                        
                self._quarantine_stash[symbol].append(tick_data)
            return False
        
        return True

    def extract_stash(self, symbol: str) -> List[dict]:
        """Retrieves and clears the catch-up buffer."""
        stash = self._quarantine_stash.pop(symbol, deque())
        return list(stash)

    def mark_recovered(self, symbol: str) -> None:
        """Unlocks causality."""
        self._corrupted_symbols.discard(symbol)
        if symbol in self._quarantine_stash:
            del self._quarantine_stash[symbol]
        logger.info(f"🔓 [CAUSALITY UNLOCKED] {symbol} state verified. Stream alive.")
