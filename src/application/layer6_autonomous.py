import uuid
import time
import asyncio
from loguru import logger
from typing import Set, Dict, Any, Optional
from src.shared.resilience import DivergenceMonitor, CrossVolumeVerifier

class AutonomousExecutionGateway:
    """
    [GEKTOR v12.17] LAYER 6: THE AUTONOMOUS BRIDGE.
    
    Zero-Human Execution Gateway with strictly idempotent clOrdID tracking.
    Bypasses the 1.2s biological bottleneck entirely.
    """
    def __init__(self, rest_client: Any, shadow_ledger: Any, rate_limiter: Any, flight_recorder: Any):
        self.rest = rest_client
        self.shadow = shadow_ledger
        self.limiter = rate_limiter
        self.flight_recorder = flight_recorder
        
        # [GEKTOR v13.7] Echelon 7 Engine
        self.verifier = CrossVolumeVerifier()
        self._shared_slots = asyncio.Semaphore(40)
        self._cortex_slots = asyncio.Semaphore(10)
        self.cortex_list = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
        
        # [GEKTOR v13.7.1] Pulse Monopolization Guard
        self._symbol_wait_counts: Dict[str, int] = {}
        self._wait_lock = asyncio.Lock()
        
        # O(1) in-memory gate to prevent duplicate strikes for the same signal pulse
        self._active_intents: Set[str] = set()
        # [GEKTOR v12.19] Loopback Storm Protection: TS of the last strike per symbol
        self._strike_timestamps: Dict[str, float] = {}
        # [GEKTOR v12.21] Zombie Locks: symbol -> set(orderLinkId)
        # Prevents new strikes until indeterminate orders are confirmed dead.
        self._zombie_locks: Dict[str, Set[str]] = {}
        # Mapping clOrdID -> symbol for reconciliation
        self._cl_ord_cache: Dict[str, str] = {}

    def is_locked_by_zombie(self, symbol: str) -> bool:
        """Returns True if there are pending indeterminate cancellations for this symbol."""
        return len(self._zombie_locks.get(symbol, set())) > 0

    def is_in_quiet_period(self, symbol: str, window_ms: float = 1000.0) -> bool:
        """
        Returns True if a strike was dispatched recently OR symbol is locked by zombie.
        """
        if self.is_locked_by_zombie(symbol): return True
        last_strike = self._strike_timestamps.get(symbol, 0.0)
        return (time.monotonic() - last_strike) < (window_ms / 1000.0)

    async def execute_strike(
        self,
        symbol: str,
        price: float,
        side: str,
        qty: float,
        *,
        pre_flight_book: Any = None,
        signal_msq: Optional[tuple] = None,
        slippage_tolerance_bps: int = 25,
        gravitational_anchor: Any = None,
        intent_version: Optional[int] = None,
        intent_ledger: Any = None,
    ) -> None:
        """
        [ATOMIC STRIKE] with Pre-Flight Check (JIT Validation).

        Persistent dispatch via Flight Recorder (WAL).

        Validation cascade (synchronous, no awaits):
          Priority 0: GravitationalAnchor.is_blackout (macro regime)
          Priority 1: IntentLedger.validate_click (version gate)
          Priority 2: MSQ drift check (microstructure)

        Args:
            symbol: Trading pair.
            price: Reference price from signal.
            side: BUY or SELL.
            qty: Order quantity.
            pre_flight_book: Optional NdOrderBookStateMachine for JIT validation.
            signal_msq: Original (qty, avg_px) from the signal snapshot.
            slippage_tolerance_bps: Max acceptable price drift in basis points.
            gravitational_anchor: Optional GravitationalAnchor for macro blackout.
            intent_version: Version from operator click (must match ledger).
            intent_ledger: Optional IntentLedger for version validation.
        """
        if symbol in self._active_intents or self.is_locked_by_zombie(symbol):
            return 

        # ── PRIORITY 0: GRAVITATIONAL ANCHOR (macro regime) ──
        # O(1) monotonic comparison — blocks ALL altcoin trading during leader shock
        if gravitational_anchor is not None and gravitational_anchor.is_blackout:
            leader, drift = gravitational_anchor.last_shock_info
            logger.warning(
                "🌑 [PRE-FLIGHT] {} BLOCKED by Systemic Blackout. "
                "Leader {} shocked {}bps. Remaining: {:.1f}s",
                symbol, leader, drift,
                gravitational_anchor.blackout_remaining_sec,
            )
            return

        # ── PRIORITY 1: INTENT VERSION GATE (zero-trust) ──
        # O(1) integer comparison — rejects ghost clicks from stale UI
        if intent_ledger is not None and intent_version is not None:
            validated = intent_ledger.validate_click(intent_version)
            if validated is None:
                return  # Logging handled inside validate_click

        # ── PRIORITY 2: MSQ PRE-FLIGHT CHECK (microstructure) ──
        if pre_flight_book is not None and signal_msq is not None:
            current_msq = pre_flight_book.calculate_msq(signal_msq[0])
            if current_msq is None:
                logger.warning(
                    "🛑 [PRE-FLIGHT] {} ABORTED: book invalidated during "
                    "operator reaction window", symbol,
                )
                return

            _, signal_avg_px = signal_msq
            _, current_avg_px = current_msq

            if signal_avg_px > 0:
                drift_bps = abs(current_avg_px - signal_avg_px) * 10_000 // signal_avg_px
                if drift_bps > slippage_tolerance_bps:
                    logger.warning(
                        "🛑 [PRE-FLIGHT] {} ABORTED: price drift {}bps > {}bps tolerance. "
                        "Signal MSQ: {} → Current MSQ: {}",
                        symbol, drift_bps, slippage_tolerance_bps,
                        signal_avg_px, current_avg_px,
                    )
                    return

            logger.debug(
                "✅ [PRE-FLIGHT] {} CLEARED: price drift {}bps within tolerance",
                symbol,
                abs(current_avg_px - signal_avg_px) * 10_000 // signal_avg_px
                if signal_avg_px > 0 else 0,
            )
        # ── END PRE-FLIGHT CHECK ──

        await self.limiter.request_rest_call(f"EXEC_{symbol}", priority=0)

        cl_ord_id = f"GKT_{symbol}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:4]}"

        # ── IOC FRICTION FIREWALL ──
        # Calculate worst acceptable price from Pre-Flight MSQ.
        # If liquidity vanishes during T_transit (20-80ms), IOC auto-cancels.
        # Protection transferred to exchange matching engine hardware.
        worst_price = price
        pre_flight_px = 0.0
        if signal_msq is not None:
            _, signal_avg_px = signal_msq
            pre_flight_px = signal_avg_px
            if str(side).upper() == "BUY":
                worst_price = signal_avg_px * (10_000 + slippage_tolerance_bps) / 10_000
            else:
                worst_price = signal_avg_px * (10_000 - slippage_tolerance_bps) / 10_000

        # [IMMORTAL CAUSALITY] Step 1: Record Intent on Disk BEFORE network dispatch
        self.flight_recorder.log_intent(
            cl_ord_id, symbol, side, qty, worst_price,
            pre_flight_msq_price=pre_flight_px,
        )
        
        self._active_intents.add(symbol)
        self._strike_timestamps[symbol] = time.monotonic()
        self._cl_ord_cache[cl_ord_id] = symbol

        logger.critical(
            f"⚡ [L6_STRIKE] IOC Intent: {side} {qty} {symbol} @ "
            f"worst_price={worst_price:.4f} (pre_flight={pre_flight_px:.4f}) | "
            f"cl_ord_id: {cl_ord_id}"
        )

        try:
            res = await self.rest.place_order(
                category="linear", symbol=symbol, side=side.capitalize(),
                orderType="Limit", qty=str(qty), price=str(worst_price),
                timeInForce="IOC", orderLinkId=cl_ord_id
            )
            
            if res.get("retCode") == 0:
                self.flight_recorder.mark_dispatched(cl_ord_id)
            else:
                logger.error(f"❌ [L6_API_REJECT] {symbol}: {res.get('retMsg')}")
                self.release_intent(symbol, force=True, cl_ord_id=cl_ord_id)
                
        except Exception as e:
            # ── SCHRÖDINGER'S ORDER ──
            # TCP died mid-flight. Status is UNKNOWN.
            # FlightRecorder has PENDING intent. Reconciler will resolve on reconnect.
            if symbol not in self._zombie_locks: self._zombie_locks[symbol] = set()
            self._zombie_locks[symbol].add(cl_ord_id)
            asyncio.create_task(self._confirmed_kill_task(symbol, cl_ord_id))
            logger.warning(f"🧟 [ZOMBIE] Strike {cl_ord_id} 504/Timeout: {e}")

    async def execute_legacy_strike(self, symbol: str, ref_price: float, side: str, qty_usd: float, 
                                    b_l2_imbalance: float, b_l1_vol_delta: int, avg_vol: int,
                                    l1_ts: int, l2_ts: int,
                                    divergence_mgr: DivergenceMonitor) -> None:
        """
        [GEKTOR v13.7.1] STRIKE FROM THE FUTURE.
        Executes a strike based on Binance data with Causal Wait & Slot Limiting.
        """
        # 1. Verify Reality Desync
        if not divergence_mgr.detect_desync():
            return 
            
        # 2. Echelon 7: Cross-Volume Verification (Sync Check)
        if self.verifier.is_pulse_real(b_l2_imbalance, b_l1_vol_delta, avg_vol, l2_ts):
            # Instant confirmation: Execute immediately
            await self._dispatch_legacy(symbol, ref_price, side, qty_usd)
            return

        # 3. [CAUSAL WAIT] Reality Gap - Tape might be lagging
        # Admission Control: Route to pool based on asset tier [cite: 521]
        is_cortex = symbol in self.cortex_list
        slot_pool = self._cortex_slots if is_cortex else self._shared_slots
        
        # [PULSE LIMIT] Prevent one symbol from clogging the pipes
        if self._symbol_wait_counts.get(symbol, 0) >= 3:
            return

        if slot_pool.locked():
            # [FIRE_EXIT] Queue is full for this tier. Shed load to protect core stability.
            if not is_cortex:
                return # Sensory shedding (Normal)
            else:
                logger.error(f"⚠️ [CORTEX_BUSY] Leader {symbol} skipped: Wait slots saturated!")
                return

        asyncio.create_task(self._causal_wait_task(
            symbol, ref_price, side, qty_usd, b_l2_imbalance, avg_vol, l2_ts, slot_pool, 
            divergence_mgr.primary.global_heat
        ))

    async def _causal_wait_task(self, symbol, price, side, qty_usd, imbalance, avg_vol, l2_ts, slot_pool, global_heat):
        """
        Non-blocking micro-wait with Adaptive TTL Pulse.
        """
        # [GEKTOR v13.7.1] Dual-Resolution Infrastructure Guard [cite: 523]
        if not self.verifier.primary.check_infra_health():
            logger.warning(f"🚨 [INFRA_BREAKER] {symbol} strike aborted. Wide P99 instability detected.")
            return
            
        # [STORM ADAPTIVE TTL] If entropy > 0.9, shorten wait to 5ms for higher throughput
        is_cortex = symbol in self.cortex_list
        wait_time = 0.005 if (is_cortex and global_heat > 0.9) else 0.01

        async with self._wait_lock:
            self._symbol_wait_counts[symbol] = self._symbol_wait_counts.get(symbol, 0) + 1

        try:
            async with slot_pool:
                # Sleep for the Coherence Window
                await asyncio.sleep(wait_time) 
                
                # Re-verify with updated Tape 
                if self.verifier.is_pulse_real(imbalance, self.verifier._seq_guard._last_l1_volume, avg_vol, l2_ts):
                    await self._dispatch_legacy(symbol, price, side, qty_usd)
        finally:
            async with self._wait_lock:
                self._symbol_wait_counts[symbol] -= 1

    async def _dispatch_legacy(self, symbol, ref_price, side, qty_usd):
        """
        [GEKTOR v13.7.1] DYNAMIC PESSIMIZATION.
        Adjusts entry threshold based on Dual-Resolution Jitter Filter.
        """
        # Fetching pre-calculated penalty from entropy manager (Scaled Integers)
        total_bps = self.verifier.primary.get_jitter_penalty()
        pessimization_factor = 1.0 + (total_bps / 10000.0) if str(side).upper() == "BUY" else 1.0 - (total_bps / 10000.0)
        
        pessimized_price = ref_price * pessimization_factor
        qty = qty_usd / pessimized_price
        
        logger.critical(f"🚀 [LEGACY_STRIKE] Hitting {symbol} | Target: {pessimized_price} | Pess: {total_bps:.1f}bps.")
        await self.execute_strike(symbol, pessimized_price, side, qty)

    async def _confirmed_kill_task(self, symbol: str, cl_ord_id: str):
        """
        [VERIFIED DEATH RITUAL]
        Resolves Case Alpha/Beta/Gamma: Never Created vs Just Filled vs Cancelled.
        """
        attempts = 0
        while cl_ord_id in self._zombie_locks.get(symbol, set()):
            attempts += 1
            try:
                res = await self.rest.cancel_order(symbol, order_link_id=cl_ord_id)
                ret_code = res.get("retCode", -1)
                
                if ret_code == 0:
                    logger.info(f"🎯 [KILL_SUCCESS] Zombie {cl_ord_id} terminated.")
                    self._zombie_locks[symbol].discard(cl_ord_id)
                    break
                
                # [CANCEL-FILL RACE PROTECTION v2: FINAL JUDGEMENT]
                # 10001 = Order not found. 
                # This could mean it NEVER EXISTED, was JUST FILLED, or JUST CANCELLED.
                if ret_code == 10001:
                    # 1. Check Trade History (Medium Lag)
                    hist = await self.rest.get_trade_history(symbol, limit=10)
                    trades = hist.get("result", {}).get("list", [])
                    was_filled = any(t.get("orderLinkId") == cl_ord_id for t in trades)
                    
                    if was_filled:
                        logger.critical(f"🏁 [RACE_WON_BY_MARKET] Zombie {cl_ord_id} was FILLED. Execution detected in History.")
                        self._zombie_locks[symbol].discard(cl_ord_id)
                        break
                    
                    # 2. POSITION ARBITRATION (Absolute Anchor)
                    # If we can't find the trade, check if the actual position size has moved.
                    pos_list = await self.rest.get_active_positions(symbol)
                    p_info = next((p for p in pos_list if p["symbol"] == symbol), None)
                    current_size = float(p_info["size"]) if p_info else 0.0
                    local_size = self.shadow.get_symbol_exposure(symbol).get("size", 0.0)
                    
                    if abs(current_size - local_size) > 1e-8:
                        # Position moved! Something executed even if history is lagging.
                        # WE DO NOT UNLOCK. We wait for WS or for history to catch up.
                        logger.warning(f"⚖️ [INVENTORY_ARBITRAGE] {symbol} size mismatch ({current_size} != {local_size}). "
                                       f"Zombie {cl_ord_id} suspected alive. Holding lock.")
                        await asyncio.sleep(1.0) # Penalty sleep
                        continue

                    # 3. Final Judgement
                    if attempts > 30: # ~3 seconds of silence and no position change
                        logger.warning(f"💀 [FINAL_JUDGEMENT] Zombie {cl_ord_id} confirmed DEAD. No trade, no position delta.")
                        self._zombie_locks[symbol].discard(cl_ord_id)
                        break

            except Exception as e:
                logger.error(f"⚠️ [KILL_ERROR] {cl_ord_id}: {e}")
            
            await asyncio.sleep(0.1)

        if not self.is_locked_by_zombie(symbol):
            self.release_intent(symbol, force=True, cl_ord_id=cl_ord_id)

    def release_intent(self, symbol: str, force: bool = False, cl_ord_id: Optional[str] = None):
        """
        [ATOMIC TERMINAL]
        Cleans both in-memory and on-disk intent logs.
        """
        if symbol in self._active_intents:
            self._active_intents.discard(symbol)
            self._strike_timestamps[symbol] = 0.0
            
            # [MEMORY PURGE] Clean the clOrdID cache
            if cl_ord_id:
                self.flight_recorder.mark_terminal(cl_ord_id)
                self._cl_ord_cache.pop(cl_ord_id, None)
            
            logger.info(f"🔓 [L6_GATE] Released {symbol} (Immortal Memory Cleaned).")

    def reset_quiet_period(self, symbol: str):
        self._strike_timestamps[symbol] = 0.0
