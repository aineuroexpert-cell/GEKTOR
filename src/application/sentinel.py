import asyncio
import sys
import time
from typing import Dict, List, Optional
from loguru import logger

from src.infrastructure.database import DatabaseManager
from sqlalchemy import text

class BlackoutSentinel:
    """[GEKTOR v2.0] Heartbeat Monitor for Post-Relay Integrity."""
    def __init__(self, db_manager: DatabaseManager, notifier_event: asyncio.Event, timeout_sec: int = 300):
        self.db = db_manager
        self.notifier_event = notifier_event # Reference to _live_allowed
        self.timeout_sec = timeout_sec
        self.last_pending_count = 0
        self.stagnation_time = 0
        self._running = False

    async def watch(self):
        self._running = True
        logger.info("🛡️ [Sentinel] Blackout Heartbeat Guard ARMED.")
        
        while self._running:
            await asyncio.sleep(10)
            
            # Check PENDING count in DB
            try:
                async with self.db.engine.connect() as conn:
                    result = await conn.execute(text("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'"))
                    current_count = result.scalar() or 0
            except Exception as e:
                logger.error(f"⚠️ [Sentinel] DB Healthcheck failed: {e}")
                continue

            if current_count > 0 and current_count >= self.last_pending_count:
                self.stagnation_time += 10
                if self.stagnation_time >= self.timeout_sec:
                    self._trigger_local_alarm(current_count)
                    self.notifier_event.clear() # Emergency Freeze
            else:
                if self.stagnation_time >= self.timeout_sec:
                    logger.success("📡 [Sentinel] Bridge recovered. Resuming flow.")
                self.stagnation_time = 0
            
            self.last_pending_count = current_count

    def _trigger_local_alarm(self, lost_messages: int):
        # System Bell
        sys.stdout.write('\a\a\a')
        sys.stdout.flush()
        
        logger.opt(raw=True).critical(
            f"\n{'='*60}\n"
            f"🚨 [!!! BRIDGE DEAD !!!] ТЕЛЕГРАМ-МОСТ УНИЧТОЖЕН.\n"
            f"Очередь заблокирована: {lost_messages} непереданных аномалий.\n"
            f"Включен протокол сброса в JSONL.\n"
            f"{'='*60}\n"
        )

    def stop(self):
        self._running = False

class FlatlineSentinel:
    """
    [GEKTOR v2.0] Per-Symbol Pulse Monitor.
    Detects if a specific coin has stopped sending ticks (Exchange Freeze).
    Triggers [PARTIAL BLINDNESS] event to prevent trading on stale data.
    """
    def __init__(self, threshold_sec: int = 15):
        self.threshold_sec = threshold_sec
        self._last_ticks: Dict[str, float] = {}
        self._blind_symbols: set[str] = set()

    def update_pulse(self, symbol: str, timestamp_ms: Optional[int] = None):
        """Updates the last alive time for a symbol."""
        # Use monotonic time for internal tracking to be immune to NTP drifts
        self._last_ticks[symbol] = time.monotonic()
        if symbol in self._blind_symbols:
            logger.success(f"📡 [Flatline] {symbol} pulse RECOVERED. Visibility restored.")
            self._blind_symbols.remove(symbol)

    def check_for_flatlines(self, active_symbols: set[str]) -> List[str]:
        """
        Checks all monitored symbols for silence, but only if they are in active_symbols.
        Returns a list of symbols that just entered [PARTIAL BLINDNESS].
        """
        now = time.monotonic()
        newly_blind = []
        
        # [PHANTOM FLATLINE PREVENTION] 
        # Clean up internal registry for any symbol that has been decommissioned
        keys_to_purge = [s for s in self._last_ticks.keys() if s not in active_symbols]
        for s in keys_to_purge:
            self._last_ticks.pop(s, None)
            if s in self._blind_symbols:
                self._blind_symbols.remove(s)

        for symbol in active_symbols:
            last_time = self._last_ticks.get(symbol)
            if not last_time:
                continue # Never saw a tick for this yet
                
            if symbol in self._blind_symbols:
                continue
                
            if now - last_time > self.threshold_sec:
                logger.critical(f"🛑 [FLATLINE] {symbol} is frozen! Gap > {self.threshold_sec}s. [PARTIAL BLINDNESS] active.")
                self._blind_symbols.add(symbol)
                newly_blind.append(symbol)
                
        return newly_blind

    def is_blind(self, symbol: str) -> bool:
        return symbol in self._blind_symbols

    def remove_symbol(self, symbol: str):
        """[RESILIENCE] Completely removes a symbol from watchdog registry."""
        self._last_ticks.pop(symbol, None)
        if symbol in self._blind_symbols:
            self._blind_symbols.remove(symbol)
        logger.info(f"🛡️ [Sentinel] {symbol} removed from Watchdog registry.")
