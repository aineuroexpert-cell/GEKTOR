# src/domain/friction_guard.py
"""
[GEKTOR APEX v4.3] Execution Friction Guard (Peter Brown's Razor).

Mathematical filter for transaction costs. Suppresses signals
where the expected Alpha is consumed by spread + taker fees.

Philosophy: "If you don't account for transaction costs,
they will account for your capital." — Peter Brown, RenTech.
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set
from loguru import logger
from src.shared.alpha_config import alpha


@dataclass(slots=True)
class L1Quote:
    """Atomic L1 quote snapshot for spread calculation."""
    bid: float
    ask: float
    ts: float  # monotonic timestamp of last update


class ExecutionFrictionGuard:
    """
    [GEKTOR v4.3] Transaction Cost Aware Signal Filter.

    Before any signal reaches the Operator, this guard verifies that
    the expected Alpha (derived from VPIN Z-Score magnitude) exceeds
    the total round-trip friction (spread cost + 2x taker fee).

    If friction >= alpha => signal is SUPPRESSED (not worth executing).
    """
    __slots__ = ['taker_fee_bps', 'min_alpha_bps', '_quotes', '_stale_threshold_sec']

    def __init__(self, taker_fee_bps: float = 6.0, min_alpha_bps: float = 5.0):
        """
        Args:
            taker_fee_bps: Bybit taker fee in basis points (default 6.0 = 0.06%).
            min_alpha_bps: Minimum net alpha after friction to pass filter.
        """
        self.taker_fee_bps = taker_fee_bps
        self.min_alpha_bps = min_alpha_bps
        self._quotes: Dict[str, L1Quote] = {}
        self._stale_threshold_sec = alpha.STALE_QUOTE_SEC

    def update_quote(self, symbol: str, bid: float, ask: float) -> None:
        """Ingest latest L1 BBO from orderbook snapshots (called from ingest_snapshot)."""
        self._quotes[symbol] = L1Quote(bid=bid, ask=ask, ts=time.monotonic())

    def is_tradable(self, symbol: str, vpin: float, dynamic_threshold: float) -> bool:
        """
        Determines if the signal has enough Alpha to survive friction.

        Expected Alpha is estimated from the VPIN excess above the dynamic threshold.
        A VPIN of 0.85 against a threshold of 0.65 means ~30% excess, which translates
        to roughly 30 bps of expected directional edge (heuristic scaling).

        Args:
            symbol: Trading pair.
            vpin: Current VPIN value.
            dynamic_threshold: Current adaptive threshold.

        Returns:
            True if signal is worth executing, False if friction kills it.
        """
        quote = self._quotes.get(symbol)

        # [FAILSAFE] If no L1 data available, let the signal through
        # (conservative approach: don't suppress valid signals due to data gaps)
        if not quote:
            logger.debug(f"⚠️ [FrictionGuard] No L1 data for {symbol}. Passing signal through.")
            return True

        # [STALE CHECK] Old quotes are unreliable
        age_sec = time.monotonic() - quote.ts
        if age_sec > self._stale_threshold_sec:
            logger.debug(f"⚠️ [FrictionGuard] L1 stale for {symbol} ({age_sec:.1f}s). Passing signal through.")
            return True

        # 1. Spread in basis points
        mid_price = (quote.ask + quote.bid) / 2.0
        if mid_price <= 0:
            return True  # Prevent division by zero

        spread_bps = ((quote.ask - quote.bid) / mid_price) * 10_000

        # 2. Total round-trip friction: spread crossing + 2x taker fee (entry + exit)
        friction_bps = spread_bps + (self.taker_fee_bps * 2)

        # 3. Expected Alpha (heuristic): excess VPIN as % of threshold, scaled to bps
        # If VPIN = 0.85, threshold = 0.65 => excess_ratio = 0.308 => ~30.8 bps expected move
        if dynamic_threshold > 0:
            excess_ratio = (vpin - dynamic_threshold) / dynamic_threshold
        else:
            excess_ratio = 0.0

        expected_alpha_bps = excess_ratio * 100.0  # Scale to basis points

        # 4. Net Alpha after friction
        net_alpha_bps = expected_alpha_bps - friction_bps

        if net_alpha_bps < self.min_alpha_bps:
            logger.warning(
                f"🛡️ [FRICTION GUARD] {symbol} Signal SUPPRESSED. "
                f"Alpha: {expected_alpha_bps:.1f} bps | "
                f"Spread: {spread_bps:.1f} bps | "
                f"Fees: {self.taker_fee_bps * 2:.1f} bps | "
                f"Friction: {friction_bps:.1f} bps | "
                f"Net: {net_alpha_bps:.1f} bps (min: {self.min_alpha_bps:.1f})"
            )
            return False

        logger.info(
            f"✅ [FRICTION GUARD] {symbol} Signal CLEARED. "
            f"Net Alpha: {net_alpha_bps:.1f} bps > {self.min_alpha_bps:.1f} bps min."
        )
        return True

@dataclass
class PostTradeAudit:
    symbol: str
    exec_price: float
    direction: str
    ts: float
    mtm_1s: Optional[float] = None
    mtm_10s: Optional[float] = None
    mtm_60s: Optional[float] = None

class PostTradeToxicityMonitor:
    """
    [GEKTOR v5.7] Autonomous Zero-I/O MTM Auditor.
    
    Tracks virtual trade performance at 1s, 10s, and 60s intervals.
    Uses the Orchestrator's shared local state cache to avoid network requests.
    If 1s MTM is sharply negative, the symbol is flagged as 'TOXIC' (Market Maker Spoofed).
    """
    def __init__(self, state_cache: Dict[str, dict]):
        self._state_cache = state_cache
        self.toxicity_scores: Dict[str, float] = {} # Symbol -> Multiplier (1.0 = clean)
        self._active_audits: Set[asyncio.Task] = set()

    # ═══════════════════════════════════════════════════════════════════
    # [GEKTOR v5.22] EVENT BUS ADAPTER (Typed Contract)
    # ═══════════════════════════════════════════════════════════════════
    async def handle_execution_event(self, event):
        """
        [EventBus Subscriber] Thin adapter that unpacks the ExecutionEvent DTO
        and delegates to the internal fire-and-forget audit lifecycle.
        
        This is the ONLY entry point from the EventBus. The EventBus calls this
        method asynchronously; internally we spawn a background task to avoid
        blocking event delivery.
        """
        symbol = getattr(event, 'symbol', None) or event.get('symbol', 'UNKNOWN') if isinstance(event, dict) else event.symbol
        price = getattr(event, 'price', 0.0) if not isinstance(event, dict) else event.get('price', 0.0)
        side = getattr(event, 'side', 'BUY') if not isinstance(event, dict) else event.get('side', 'BUY')
        
        logger.debug(f"📊 [ToxicityMonitor] Received ExecutionEvent for {symbol}. Starting MTM tracking.")
        self.register_trade(symbol, price, side)

    def register_trade(self, symbol: str, entry_price: float, direction: str):
        """Fire-and-forget initialization of the audit lifecycle."""
        task = asyncio.create_task(self._audit_lifecycle(symbol, entry_price, direction))
        self._active_audits.add(task)
        task.add_done_callback(self._active_audits.discard)

    async def _audit_lifecycle(self, symbol: str, entry_price: float, direction: str):
        checkpoints = [1.0, 9.0, 50.0] # 1s, 10s (1+9), 60s (1+9+50)
        mtm_results = {}
        
        for wait_time in checkpoints:
            await asyncio.sleep(wait_time)
            
            # ZERO I/O Price Fetch from local cache
            state = self._state_cache.get(symbol)
            if not state or not state.get('is_synchronized'):
                logger.debug(f"⚠️ [MTM] {symbol} state stale/desynced. Skipping checkpoint.")
                continue

            # We use mid_price for unbiased MTM tracking
            current_price = state.get('mid_price', entry_price)
            pnl_bps = self._calc_pnl_bps(entry_price, current_price, direction)
            
            elapsed = sum(checkpoints[:checkpoints.index(wait_time)+1])
            mtm_results[int(elapsed)] = pnl_bps
            
            # [TOXICITY TRIGGER]
            # If 1s MTM is < -15bps, the fill was likely toxic (Winner's Curse)
            if int(elapsed) == 1 and pnl_bps < alpha.TOXICITY_1S_THRESHOLD_BPS:
                self._penalize_symbol(symbol, pnl_bps)

    def _calc_pnl_bps(self, entry: float, current: float, direction: str) -> float:
        pnl = (current - entry) / entry
        if direction == 'SELL': pnl = -pnl
        return pnl * 10_000

    def _penalize_symbol(self, symbol: str, pnl_bps: float):
        """Increase the selectivity threshold for toxic symbols."""
        current = self.toxicity_scores.get(symbol, 1.0)
        # Exponential penalty: threshold multiplier grows by 20% per toxic event
        self.toxicity_scores[symbol] = current * alpha.TOXICITY_PENALTY_FACTOR
        logger.error(
            f"🚨 [AUDIT TOXICITY] {symbol} penalty applied! "
            f"1s MTM: {pnl_bps:.1f} bps. New Threshold Multiplier: {self.toxicity_scores[symbol]:.2f}x"
        )

    def get_selectivity_multiplier(self, symbol: str) -> float:
        """Returns the current VPIN threshold multiplier for the symbol."""
        multiplier = self.toxicity_scores.get(symbol, 1.0)
        # Decay penalty over time (TODO: Implement temporal decay)
        return multiplier

