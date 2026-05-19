import asyncio
import json
import time
from typing import List, Dict, Optional, Tuple
from loguru import logger
from sqlalchemy import text

from src.domain.exit_protocol import ActiveSignal, MarketTick, InvalidationRule, SignalState, VPINDecayRule, TimeStopRule, ExecutionSimulator
from src.application.microstructure import L2Level
from src.infrastructure.config import settings
from src.application.outbox_relay import OutboxRepository

class SignalTracker:
    """[GEKTOR APEX] Reactive Signal Monitoring & Invalidation Engine."""
    def __init__(self, db, tg):
        self.db = db # Expects DatabaseManager
        self.tg = tg # Expects TelegramRadarNotifier
        self.active_signals: Dict[str, ActiveSignal] = {}
        self.latest_depth: Dict[str, Tuple[List[L2Level], List[L2Level]]] = {}
        self.rules = [
            VPINDecayRule(settings.EXIT_VPIN_DECAY_FACTOR),
            TimeStopRule(settings.EXIT_TIME_MAX_BARS)
        ]
        self._lock = asyncio.Lock()
        self.spillover_path = "artifacts/spillover.jsonl"
        
        # [GEKTOR v5.24] Atomic Disk I/O Offloading (Zero-Blocking)
        import queue
        import threading
        self._disk_queue = queue.Queue()
        self._disk_thread = threading.Thread(
            target=self._disk_writer_worker, 
            daemon=True, 
            name="ExitProtocolDiskWriter"
        )
        self._disk_thread.start()

    def add_rule(self, rule: InvalidationRule):
        self.rules.append(rule)

    async def hydrate_signals(self):
        """[RESILIENCE] Reconstructs active premises from Redis (Primary) or Spillover (Secondary)."""
        try:
            # 1. Primary Path: Redis In-Memory State
            key = "gektor:active_signals"
            data = await self.db.buffer.redis.get(key)
            if data:
                signals_data = json.loads(data)
                self._apply_hydration_data(signals_data)
                logger.success(f"📟 [ExitProtocol] Hydrated {len(self.active_signals)} premises from REDIS.")
                return True
            
            # 2. Secondary Path: Spillover JSONL (Local Disk)
            import os
            if os.path.exists(self.spillover_path):
                logger.warning(f"🗂️ [ExitProtocol] Redis empty/offline. Falling back to spillover: {self.spillover_path}")
                with open(self.spillover_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._apply_hydration_data(data)
                logger.success(f"📟 [ExitProtocol] Hydrated {len(self.active_signals)} premises from SPILLOVER.")
                return True
            
            logger.info("📟 [ExitProtocol] No prior state found (Clean start).")
            return True
        except Exception as e:
            logger.error(f"⚠️ [ExitProtocol] Full hydration crash: {e}")
            return False

    def _apply_hydration_data(self, signals_data: dict):
        for sid, sdata in signals_data.items():
            self.active_signals[sid] = ActiveSignal(
                signal_id=sid,
                symbol=sdata["symbol"],
                entry_ts=sdata["entry_ts"],
                entry_price=sdata["entry_price"],
                direction=sdata["direction"],
                state=SignalState[sdata["state"]],
                bars_observed=sdata.get("bars_observed", 0),
                max_vpin=sdata.get("max_vpin", 0.0)
            )

    async def register_signal(self, symbol: str, price: float, vpin: float, direction: int, timestamp: int, 
                              entry_bid: float = 0.0, entry_ask: float = 0.0, entry_vwap: float = 0.0):
        """Идемпотентная регистрация новой торговой гипотезы с учетом спреда и проскальзывания."""
        signal_id = f"{symbol}_{timestamp}"
        async with self._lock:
            if signal_id not in self.active_signals:
                signal = ActiveSignal(
                    signal_id=signal_id,
                    symbol=symbol,
                    entry_ts=timestamp,
                    entry_price=price,
                    entry_bid=entry_bid,
                    entry_ask=entry_ask,
                    entry_vwap=entry_vwap,
                    direction=direction,
                    max_vpin=vpin
                )
                self.active_signals[signal_id] = signal
                await self._persist_all_signals()
                logger.info(f"🔍 [ExitProtocol] Registered tracking for {symbol} ({'LONG' if direction > 0 else 'SHORT'})")

    async def update_math_state(self, symbol: str, current_vpin: float, current_price: float, timestamp: int,
                                bid: float = 0.0, ask: float = 0.0,
                                bids: List[L2Level] = None, asks: List[L2Level] = None):
        """Обновление макро-параметров и L2 Snapshot Depth."""
        async with self._lock:
             for sig in self.active_signals.values():
                 if sig.symbol == symbol:
                     sig.bars_observed += 1
                     sig.max_vpin = max(sig.max_vpin, current_vpin)
                     
                     # Fill latency baseline if it's the first update after 1200ms
                     if sig.human_entry_bid == 0 and bid > 0:
                          if (timestamp - sig.entry_ts) > 1200:
                               sig.human_entry_bid = bid
                               sig.human_entry_ask = ask
                               
                               # [REALITY CHECK] Calculate Human VWAP if depth available
                               if bids and asks:
                                   sim = ExecutionSimulator()
                                   sig.human_entry_vwap = sim.calculate_vwap_execution(
                                       asks if sig.direction > 0 else bids,
                                       target_usd_volume=10000.0 # Default benchmark
                                   )
                               logger.debug(f"⏱️ [LATENCY] Captured human baseline for {symbol} | VWAP: {sig.human_entry_vwap:.2f}")

             await self._persist_all_signals()

    def _disk_writer_worker(self):
        """[RESILIENCE] Isolated blocking I/O worker."""
        import os
        self._writer_alive = True
        while self._writer_alive:
            try:
                data = self._disk_queue.get()
                if data is None: break
                
                tmp_path = self.spillover_path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.spillover_path)
            except Exception as e:
                logger.error(f"🚨 [DISK_FATAL] Atomic write failed: {e}")
                self._writer_alive = False # Panic: stop the loop
            finally:
                self._disk_queue.task_done()

    async def _persist_all_signals(self):
        """Asynchronously push state to Redis and the Disk worker thread."""
        if not getattr(self, "_writer_alive", False):
            logger.warning("⚠️ [ExitProtocol] Disk Writer is DEAD. Persistent state is STALE.")
            return

        try:
            data = {sid: {
                "symbol": s.symbol,
                "entry_ts": s.entry_ts,
                "entry_price": s.entry_price,
                "entry_bid": s.entry_bid,
                "entry_ask": s.entry_ask,
                "entry_vwap": s.entry_vwap,
                "human_entry_bid": s.human_entry_bid,
                "human_entry_ask": s.human_entry_ask,
                "human_entry_vwap": s.human_entry_vwap,
                "direction": s.direction,
                "state": s.state.name,
                "bars_observed": s.bars_observed,
                "max_vpin": s.max_vpin
            } for sid, s in self.active_signals.items()}
            
            # 1. Non-blocking Redis call
            await self.db.buffer.redis.set("gektor:active_signals", json.dumps(data))
            
            # 2. Zero-blocking Queue push to Disk Writer
            self._disk_queue.put_nowait(data)
            
        except Exception as e:
            logger.error(f"❌ [ExitProtocol] State persistence dispatch failure: {e}")

    async def process_l2_snapshot(self, symbol: str, bids: List[L2Level], asks: List[L2Level]):
        """Update local depth cache for exit VWAP calculation."""
        async with self._lock:
            self.latest_depth[symbol] = (bids, asks)

    async def process_tick(self, tick: MarketTick, current_vpin: float):
        """Реактивная проверка приходящих тиков."""
        to_remove = []
        async with self._lock:
            for sid, sig in self.active_signals.items():
                if sig.symbol != tick.symbol: continue
                
                for rule in self.rules:
                    invalidation_state = rule.check(sig, tick, current_vpin)
                    if invalidation_state:
                         sig.state = invalidation_state
                         bids, asks = self.latest_depth.get(tick.symbol, (None, None))
                         await self._trigger_abort(sig, current_vpin, tick.price, bids, asks)
                         to_remove.append(sid)
                         break
            
            if to_remove:
                for sid in to_remove: self.active_signals.pop(sid)
                await self._persist_all_signals()

    async def _trigger_abort(self, signal: ActiveSignal, vpin: float, price: float, 
                             exit_bids: List[L2Level] = None, exit_asks: List[L2Level] = None):
        """[NON-BLOCKING] Atomic emission of state and alert via Redis WAL."""
        
        # [SLIPPAGE CALCULATION]
        exit_vwap = price
        if exit_bids and exit_asks:
            sim = ExecutionSimulator()
            target_usd = 10000.0 # Standard benchmark for slippage estimation
            # If we were Long (direction > 0), we SELL now (into Bids)
            exit_vwap = sim.calculate_vwap_execution(
                exit_bids if signal.direction > 0 else exit_asks, 
                target_usd
            )

        signal_query = """
            INSERT INTO signals (
                signal_id, symbol, state, entry_bid, entry_ask, entry_vwap,
                exit_bid, exit_ask, exit_vwap, human_entry_bid, human_entry_ask, 
                human_entry_vwap, exit_vpin
            ) VALUES (:sid, :symbol, :state, :eb, :ea, :evw, :xb, :xa, :xvw, :heb, :hea, :hvw, :ev)
        """
        signal_params = {
            "sid": signal.signal_id,
            "symbol": signal.symbol,
            "state": signal.state.name,
            "eb": signal.entry_bid,
            "ea": signal.entry_ask,
            "evw": signal.entry_vwap,
            "xb": price, # Exit Bid (approximation)
            "xa": price, # Exit Ask (approximation)
            "xvw": exit_vwap,
            "heb": signal.human_entry_bid,
            "hea": signal.human_entry_ask,
            "hvw": signal.human_entry_vwap,
            "ev": vpin
        }
        
        # 1. Push Signal Record to WAL (Redis)
        await self.db.push_query_to_wal(signal_query, signal_params)

        # 2. Push Alert to Outbox via WAL (Redis)
        event_dict = {
            "symbol": signal.symbol,
            "price": price,
            "vpin": vpin,
            "timestamp": int(time.time() * 1000), 
            "abort_mission": True,
            "abort_reason": signal.state.name
        }
        tg_payload = self.tg._format_message(event_dict)
        
        outbox_query = "INSERT INTO outbox_events (payload) VALUES (:payload)"
        await self.db.push_query_to_wal(outbox_query, {"payload": tg_payload})

        logger.warning(f"🚨 [ExitProtocol] PREMISE INVALIDATED (Pushed to WAL): {signal.symbol} -> {signal.state.name}")
