import asyncio
import time
from loguru import logger
from typing import Any, Dict, Set, Optional

class L6StateHealer:
    """
    [GEKTOR v12.21] LAYER 6 ATOMIC RECONCILIATION ENGINE.
    
    Evidence-Based Reconciliation with Negative Trust Protection.
    Synchronizes Positions + Open Orders with causal proof from WebSocket.
    """
    def __init__(self, shadow_ledger: Any, rest_client: Any, l6_gateway: Any):
        self.shadow = shadow_ledger
        self.rest = rest_client
        self.gateway = l6_gateway
        self._is_healing = False
        
        # High-Water Mark: symbol -> last_exch_ts_from_ws
        self._last_ws_ts: Dict[str, int] = {}
        # Scaled Integer Precision
        self._qty_scale = 1_000_000
        # [GEKTOR v12.20] Persistence of Disbelief: symbol -> consecutive_empty_results
        self._suspected_glitch_count: Dict[str, int] = {}
        
        # [EVENT SOURCING TAINT]
        self.is_tainted = False
        self.ws_fallback_active = False

    async def execute_oracle_reconciliation(self) -> None:
        """
        Абсолютный источник правды (Oracle).
        Стресс-Тест Решение: Защита от 502/RateLimit через Private WS Fallback.
        """
        logger.warning("🛡️ [ORACLE RECON] Инициирован принудительный аудит стейта.")
        try:
            # 1. Попытка запросить REST Оракул
            raw_pos, raw_ord = await asyncio.wait_for(
                asyncio.gather(
                    self.rest.get_active_positions(),
                    self.rest.get_open_orders("")
                ), timeout=3.0
            )
            
            exchange_truth = {p["symbol"]: p for p in raw_pos if float(p["size"]) > 0}
            
            # Rebuild Shadow Ledger from Truth
            self.shadow._exposures.clear()
            for symbol, p in exchange_truth.items():
                self.shadow.set_symbol_exposure(symbol, float(p["size"]), float(p["avgPrice"]), str(p.get("side", "")).upper())
                
            logger.success("✅ [ORACLE RECON] Стейт синхронизирован по REST. Амнезия устранена.")
            self.is_tainted = False
            self.ws_fallback_active = False
            
        except (asyncio.TimeoutError, Exception) as e:
            # [ВЫЖИВАНИЕ КАПИТАЛА] Не бросаем RuntimeError.
            # Биржа Bybit при подключении Private WS (канал 'position') мгновенно пушит снапшот всех позиций.
            logger.critical(f"🛑 [ORACLE RECON] REST Оракул мертв (502/Timeout): {e}.")
            logger.warning("🦇 [WS FALLBACK] Активация режима Летучей Мыши (Радар только по Private WS). Ждем пуш-снапшот от биржи...")
            self.ws_fallback_active = True
            self.is_tainted = True
            # Торговые входы будут заблокированы оркерстратором, пока is_tainted == True
            # Как только Private WS пришлет ивент 'position' (он это делает при реконнекте), мы снимем Taint.

    def update_ws_high_water_mark(self, symbol: str, exch_ts: int):
        """Called by Private WS to update the last known exchange time."""
        if exch_ts > self._last_ws_ts.get(symbol, 0):
            self._last_ws_ts[symbol] = exch_ts
            self._suspected_glitch_count[symbol] = 0

    async def boot_sync(self):
        """
        [BOOT-TIME PURGATORY]
        Resolves unfinished business from previous sessions before arming the system.
        """
        logger.warning("🦴 [BOOT] Initiating Purgatory Scan (Unresolved Intent Recovery)...")
        pending = self.gateway.flight_recorder.get_unresolved_intents()
        
        for intent in pending:
            cl_ord_id = intent["cl_ord_id"]
            symbol = intent["symbol"]
            logger.info(f"🕵️ [PURGATORY] Recovering intent {cl_ord_id} for {symbol}...")
            
            # Lock the gate for this symbol during recovery
            self.gateway._active_intents.add(symbol)
            if symbol not in self.gateway._zombie_locks: self.gateway._zombie_locks[symbol] = set()
            self.gateway._zombie_locks[symbol].add(cl_ord_id)
            
            # Start the killer task for this orphaned intent
            asyncio.create_task(self.gateway._confirmed_kill_task(symbol, cl_ord_id))

        # Perform initial full audit
        # Если при загрузке реестр был порван — is_tainted уже установлен в True
        if self.is_tainted:
            await self.execute_oracle_reconciliation()
        else:
            await self.reconcile_all()
            
        logger.success("🏁 [BOOT] Purgatory Cleared. Operational Readiness: HIGH.")

    async def reconnect_reconciliation(self) -> None:
        """
        [SCHRÖDINGER'S ORDER RESOLUTION]

        Deterministic 3-phase protocol for resolving in-flight orders
        after network reconnection. Guarantees:

          1. No manual intervention (fully autonomous)
          2. No double-execution (orderLinkId idempotency key on exchange side)
          3. Deterministic terminal state for every intent

        Called by the orchestrator immediately after Private WS reconnects.

        Resolution matrix:
          PENDING  + not found on exchange → NEVER_EXISTED (safe to re-arm)
          PENDING  + found FILLED          → adopt position (Shadow Ledger update)
          DISPATCHED + not found           → check trade history → position arbitration
          DISPATCHED + found FILLED        → adopt position
          DISPATCHED + found CANCELLED     → IOC expired (no fill)
        """
        pending = self.gateway.flight_recorder.get_unresolved_intents()
        if not pending:
            return

        logger.warning(
            "🔬 [SCHRÖDINGER] Reconnect reconciliation: {} unresolved intents",
            len(pending),
        )

        for intent in pending:
            cl_ord_id = intent["cl_ord_id"]
            symbol = intent["symbol"]
            status = intent["status"]

            try:
                resolved = await self._resolve_single_intent(
                    cl_ord_id, symbol, status, intent,
                )
                if resolved:
                    logger.success(
                        "✅ [SCHRÖDINGER] {} resolved as {} for {}",
                        cl_ord_id, resolved, symbol,
                    )
            except Exception as e:
                logger.error(
                    "💀 [SCHRÖDINGER] Failed to resolve {}: {}",
                    cl_ord_id, e,
                )

        # Final: full position reconciliation as ground truth arbiter
        await self.reconcile_all()

    async def _resolve_single_intent(
        self,
        cl_ord_id: str,
        symbol: str,
        status: str,
        intent: dict,
    ) -> str:
        """
        Three-phase resolution for a single Schrödinger's Order.

        Phase 1: REST Query by orderLinkId (direct ask)
        Phase 2: Trade History Scan (execution evidence)
        Phase 3: Position Arbitration (absolute anchor)

        Returns terminal state: 'FILLED', 'CANCELLED', 'NEVER_EXISTED'
        """
        # ── Phase 1: Direct REST Query ──
        try:
            order_resp = await asyncio.wait_for(
                self.rest.get_order_history(symbol, client_order_id=cl_ord_id),
                timeout=3.0,
            )
            orders = order_resp.get("result", {}).get("list", [])

            if orders:
                order = orders[0]
                exchange_status = str(order.get("orderStatus", "")).upper()

                if exchange_status == "FILLED":
                    exec_price = float(order.get("avgPrice", 0.0))
                    exec_qty = float(order.get("cumExecQty", 0.0))
                    self.gateway.flight_recorder.mark_filled(cl_ord_id, exec_price)
                    self.shadow.set_symbol_exposure(
                        symbol, exec_qty, exec_price,
                        str(intent.get("side", "")).upper(),
                    )
                    self._unlock_symbol(symbol, cl_ord_id)
                    return "FILLED"

                if exchange_status in ("CANCELLED", "DEACTIVATED", "REJECTED"):
                    self.gateway.flight_recorder.mark_terminal(cl_ord_id)
                    self._unlock_symbol(symbol, cl_ord_id)
                    return "CANCELLED"

                if exchange_status in ("PARTIALLYFILLED", "NEW", "UNTRIGGERED"):
                    # Order is still alive — let zombie killer handle it
                    logger.warning(
                        "⏳ [SCHRÖDINGER] {} is {}, deferring to zombie killer",
                        cl_ord_id, exchange_status,
                    )
                    return f"DEFERRED_{exchange_status}"

        except asyncio.TimeoutError:
            logger.warning("⏱️ [SCHRÖDINGER] REST timeout for {}. Proceeding to Phase 2.", cl_ord_id)
        except Exception as e:
            logger.warning("⚠️ [SCHRÖDINGER] Phase 1 error for {}: {}. Proceeding.", cl_ord_id, e)

        # ── Phase 2: Trade History Scan ──
        if status == "DISPATCHED":
            try:
                hist = await asyncio.wait_for(
                    self.rest.get_trade_history(symbol, limit=20),
                    timeout=3.0,
                )
                trades = hist.get("result", {}).get("list", [])
                matching = [t for t in trades if t.get("orderLinkId") == cl_ord_id]

                if matching:
                    exec_price = float(matching[0].get("execPrice", 0.0))
                    exec_qty = sum(float(t.get("execQty", 0.0)) for t in matching)
                    self.gateway.flight_recorder.mark_filled(cl_ord_id, exec_price)
                    self.shadow.set_symbol_exposure(
                        symbol, exec_qty, exec_price,
                        str(intent.get("side", "")).upper(),
                    )
                    self._unlock_symbol(symbol, cl_ord_id)
                    return "FILLED"

            except (asyncio.TimeoutError, Exception) as e:
                logger.warning("⚠️ [SCHRÖDINGER] Phase 2 error for {}: {}", cl_ord_id, e)

        # ── Phase 3: Position Arbitration (Absolute Anchor) ──
        try:
            pos_list = await asyncio.wait_for(
                self.rest.get_active_positions(symbol),
                timeout=3.0,
            )
            p_info = next((p for p in pos_list if p["symbol"] == symbol), None)
            exchange_qty = float(p_info["size"]) if p_info else 0.0
            local_qty = float(
                self.shadow._exposures.get(symbol, {}).get("size", 0.0)
            )

            if abs(exchange_qty - local_qty) > 1e-8 and exchange_qty > 0:
                # Position moved — something executed even if history is lagging
                avg_price = float(p_info.get("avgPrice", 0.0)) if p_info else 0.0
                self.gateway.flight_recorder.mark_filled(cl_ord_id, avg_price)
                self.shadow.set_symbol_exposure(
                    symbol, exchange_qty, avg_price,
                    str(p_info.get("side", "")).upper() if p_info else "BUY",
                )
                self._unlock_symbol(symbol, cl_ord_id)
                return "FILLED"

        except (asyncio.TimeoutError, Exception) as e:
            logger.warning("⚠️ [SCHRÖDINGER] Phase 3 error for {}: {}", cl_ord_id, e)

        # If PENDING and not found anywhere → request never reached exchange
        if status == "PENDING":
            self.gateway.flight_recorder.mark_terminal(cl_ord_id)
            self._unlock_symbol(symbol, cl_ord_id)
            return "NEVER_EXISTED"

        # DISPATCHED but no evidence anywhere — conservative: assume cancelled (IOC expired)
        self.gateway.flight_recorder.mark_terminal(cl_ord_id)
        self._unlock_symbol(symbol, cl_ord_id)
        return "CANCELLED"

    def _unlock_symbol(self, symbol: str, cl_ord_id: str) -> None:
        """Atomically unlock symbol gate and clean zombie state."""
        if symbol in self.gateway._zombie_locks:
            self.gateway._zombie_locks[symbol].discard(cl_ord_id)
        self.gateway._cl_ord_cache.pop(cl_ord_id, None)
        if not self.gateway.is_locked_by_zombie(symbol):
            self.gateway.release_intent(symbol, force=True, cl_ord_id=cl_ord_id)

    async def reconcile_all(self):
        """
        [EVIDENCE-BASED RECON]
        Synchronizes Risk with hard Timeouts and WebSocket Proof-of-Closure.
        """
        if self._is_healing: return
        self._is_healing = True
        
        try:
            # 1. Atomic parallel fetch with hard 500ms timeout
            pos_task = self.rest.get_active_positions()
            ord_task = self.rest.get_open_orders("") 

            raw_positions, raw_orders = await asyncio.wait_for(
                asyncio.gather(pos_task, ord_task),
                timeout=0.5
            )
            
            # 2. Reconcile Positions
            exchange_truth = {p["symbol"]: p for p in raw_positions if float(p["size"]) > 0}
            local_symbols = set(self.shadow._exposures.keys())
            exchange_symbols = set(exchange_truth.keys())
            all_symbols = local_symbols | exchange_symbols
            
            # [STORM_MODE TRIAGE] Prioritize Risk
            if getattr(self.gateway, '_load_shedding_active', False):
                majors = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
                essential = majors | local_symbols | exchange_symbols
                all_symbols = [s for s in all_symbols if s in essential]

            for symbol in all_symbols:
                await self._resolve_symbol_gap(
                    symbol, 
                    self.shadow._exposures.get(symbol), 
                    exchange_truth.get(symbol)
                )

            # 3. [ORPHAN_CLEANING] with Guard Logic
            await self._cleanup_orphans(raw_orders.get("result", {}).get("list", []))

        except asyncio.TimeoutError:
            logger.error("⏱️ [RECON_TIMEOUT] Bybit API degraded (>500ms). Skipping audit cycle.")
        except Exception as e:
            logger.critical(f"💀 [HEALER_FATAL] Atomic reconciliation collapsed: {e}")
        finally:
            self._is_healing = False

    async def _cleanup_orphans(self, orders_list: list):
        """Terminated orders that exist on exchange but NOT in our local intent-gate."""
        for o in orders_list:
            cl_ord_id = o.get("orderLinkId", "")
            symbol = o.get("symbol", "")
            if cl_ord_id not in self.gateway._cl_ord_cache:
                if not self.gateway.is_in_quiet_period(symbol):
                    logger.critical(f"🚨 [ORPHAN_ORDER] Found untracked order {o['orderId']} on {symbol}. ABORTING.")
                    await self.rest.cancel_order(symbol, order_id=o["orderId"])

    async def _resolve_symbol_gap(self, symbol: str, local: Optional[dict], exchange: Optional[dict]):
        """
        [EVIDENCE-BASED RESOLUTION]
        If WS proves a closure happened, we bypass 'Negative Trust' quarantine.
        """
        if self.gateway.is_in_quiet_period(symbol):
            return

        ex_qty = float(exchange.get("size", 0.0)) if isinstance(exchange, dict) else 0.0
        loc_qty = float(local.get("size", 0.0)) if isinstance(local, dict) else 0.0
        
        # GAP 1: GHOST POSITION (Local has it, Exchange does not)
        if ex_qty == 0 and loc_qty != 0:
            ws_proof = self.gateway.shadow._ingestor.has_recent_terminal_event(symbol)
            if ws_proof:
                logger.info(f"✅ [EVIDENCE_HEAL] {symbol} death confirmed via WS Proof. Purging.")
                self._force_purge(symbol)
                return

            glitch_count = self._suspected_glitch_count.get(symbol, 0)
            if glitch_count < 2:
                self._suspected_glitch_count[symbol] = glitch_count + 1
                logger.warning(f"🤔 [NEGATIVE_TRUST] No WS proof for {symbol} closure. Trial #{glitch_count+1}/3.")
                return
            
            self._force_purge(symbol)

        # GAP 2: ALIEN POSITION 
        elif ex_qty != 0 and loc_qty == 0:
            logger.success(f"👽 [GAP_B] Alien Position detected for {symbol}. Adopting.")
            avg_price = await self._fetch_real_avg_price(symbol)
            self.shadow.set_symbol_exposure(symbol, ex_qty, avg_price, str(exchange.get("side", "")).upper())
            self.gateway._active_intents.add(symbol)
            self._suspected_glitch_count[symbol] = 0

        # GAP 3: DRIFT
        elif abs(ex_qty - loc_qty) > 1e-8:
            logger.warning(f"📉 [GAP_C] {symbol} Size Drift: Local {loc_qty} != Ex {ex_qty}. Correcting.")
            avg_price = float(exchange.get("avgPrice", 0.0))
            if avg_price <= 0: avg_price = await self._fetch_real_avg_price(symbol)
            self.shadow.set_symbol_exposure(symbol, ex_qty, avg_price, str(exchange.get("side", "")).upper())
            self._suspected_glitch_count[symbol] = 0

    def _force_purge(self, symbol: str):
        self.shadow.clear_symbol_exposure(symbol)
        self.gateway.release_intent(symbol, force=True)
        self._suspected_glitch_count[symbol] = 0

    async def _fetch_real_avg_price(self, symbol: str) -> float:
        """Consults Execution History to find ground-truth Entry Price."""
        try:
            history = await self.rest.get_trade_history(symbol, limit=5)
            if history.get("retCode") == 0:
                trades = history.get("result", {}).get("list", [])
                if trades:
                    return float(trades[0].get("execPrice", 0.0))
            return 0.0
        except Exception as e:
            logger.error(f"⚠️ [HEALER_HISTORY] Failed to fetch basis for {symbol}: {e}")
            return 0.0
