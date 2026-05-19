# -*- coding: utf-8 -*-
# src/application/orchestrator.py

import asyncio
import bisect
import math
import time
import os
import uuid
import json
import threading
import queue
from typing import Dict, List, Optional, Any, Set, Tuple, Union, Callable, Coroutine
import collections
from loguru import logger

from src.infrastructure.config import settings
from src.infrastructure.database import DatabaseManager
from src.infrastructure.telegram_notifier import TelegramRadarNotifier
from src.infrastructure.bybit import BybitIngestor, PrivateBybitWSIngestor
from src.infrastructure.bybit import BybitRestClient
from src.infrastructure.event_bus import EventBus
from src.infrastructure.flight_recorder import AtomicFlightRecorder

from src.application.pool_manager import WorkerPoolManager
from src.application.sentinel_watchdog import event_loop_monitor
from src.application.vanguard import VanguardScanner
from src.application.microstructure import MicrostructureDefender, L2Snapshot, L2Level
from src.application.quarantine import QuarantineManager
from src.application.escrow import LiquidationEchoGuard
from src.application.alpha_engine import ZeroAllocationEngine
from src.application.sentry_brain import SentryBrain
from src.application.exit_protocol_service import SignalTracker
from src.application.outbox_relay import OutboxRepository, TelegramRelayWorker

from src.domain.math_core import process_ticks_subroutine
from src.domain.exit_protocol import MarketTick as ExitTick
from src.domain.macro_regime import MacroRegimeFilter
from src.domain.friction_guard import ExecutionFrictionGuard, PostTradeToxicityMonitor
from src.domain.entities.events import ExecutionEvent, ConflatedEvent, StateInvalidationEvent, EuthanasiaEvent
# [НОВЫЙ ИМПОРТ: ДВИЖОК ВЫХОДА]
from src.domain.exit_protocol import MicrostructuralExitEngine, ActiveSignal, ExitReason

from src.shared.resilience import GlobalResilienceManager, HydrationPriority, EntropyManager
from src.shared.alpha_config import alpha
from decimal import Decimal, ROUND_DOWN, ROUND_FLOOR, ROUND_CEILING

# --- Вспомогательные классы (Normalizer, Debouncer, Batcher, RiskGuard, ShadowLedger, Genesis) ---
class AbsolutePrecisionNormalizer:
    @staticmethod
    def normalize_price(price: float, tick_size: float) -> float:
        if tick_size <= 0: return price
        d_price = Decimal(str(price))
        d_tick = Decimal(str(tick_size))
        return float((d_price / d_tick).quantize(Decimal('1'), rounding=ROUND_FLOOR) * d_tick)

    @staticmethod
    def normalize_quantity(quantity: float, step_size: float) -> float:
        if step_size <= 0: return quantity
        d_qty = Decimal(str(quantity))
        d_step = Decimal(str(step_size))
        return float((d_qty / d_step).quantize(Decimal('1'), rounding=ROUND_DOWN) * d_step)


class ThrottleDebouncer:
    def __init__(self, cooldown_sec: float):
        self.cooldown_sec = cooldown_sec
        self._last_call: Dict[str, float] = {}

    def allow(self, key: str, current_ts: float) -> bool:
        if key not in self._last_call or (current_ts - self._last_call[key]) > self.cooldown_sec:
            self._last_call[key] = current_ts
            return True
        return False


class TickBatcher:
    def __init__(self, max_batch_size: int = 100, max_delay_sec: float = 0.5):
        self.max_batch_size = max_batch_size
        self.max_delay_sec = max_delay_sec
        self.buffer = []
        self.last_flush = time.monotonic()

    def add(self, tick):
        self.buffer.append(tick)

    def should_flush(self, current_ts: float) -> bool:
        return len(self.buffer) >= self.max_batch_size or (current_ts - self.last_flush) > self.max_delay_sec

    def flush(self):
        res = self.buffer
        self.buffer = []
        self.last_flush = time.monotonic()
        return res


class GlobalRiskGuard:
    def __init__(self, shadow_ledger, max_equity_usd: float = 10000.0, reserve_pct: float = 0.15):
        self.ledger = shadow_ledger
        self.max_equity = max_equity_usd
        self.reserve = reserve_pct

    def calculate_msq(self, symbol: str, bbo_price: float, step_size: float) -> float:
        available_usd = self.max_equity * (1.0 - self.reserve)
        if available_usd <= 0 or bbo_price <= 0: return 0.0
        raw_qty = available_usd / bbo_price
        return AbsolutePrecisionNormalizer.normalize_quantity(raw_qty, step_size)


class RealismShadowLedger:
    def __init__(self, db_manager, orchestrator):
        self.db = db_manager
        self.orchestrator = orchestrator


class GenesisConflationEngineV4:
    def __init__(self, macro_states, defenders, event_bus, watchdog):
        pass


class ReactivePremiseWatchdog:
    def __init__(self):
        pass


# ==========================================
# ОСНОВНОЙ КЛАСС ОРКЕСТРАТОРА
# ==========================================
class GektorOrchestrator:
    def __init__(self, symbols: Optional[List[str]] = None, clock_offset: float = 0.0, spillover: Optional[Any] = None):
        self._shutdown_event = asyncio.Event()
        self.clock_offset = clock_offset
        self.spillover = spillover

        if symbols is None:
            symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.symbols = list(symbols)

        self._last_book_update: Dict[str, float] = {s: time.monotonic() for s in self.symbols}
        self._last_alert_time: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self._last_public_ts: Dict[str, int] = {s: 0 for s in self.symbols}

        self.db = DatabaseManager()
        self.event_bus = EventBus(max_queue_size=5000)
        self.quarantine = QuarantineManager(event_bus=self.event_bus)
        self.pool_manager = WorkerPoolManager()
        self.bybit_proxy = settings.TG_PROXY_URL if settings.USE_PROXY_FOR_BYBIT else None

        self.rest_client = BybitRestClient(
            api_key=settings.BYBIT_API_KEY,
            api_secret=settings.BYBIT_API_SECRET,
            proxy_url=self.bybit_proxy
        )

        self.macro_states: Dict[str, dict] = {}
        self._daemon_tasks: Set[asyncio.Task] = set()
        self._background_tasks: Set[asyncio.Task] = set()
        self._active_micro_tasks: Dict[str, asyncio.Task] = {}

        self.books: Dict[str, Any] = {}
        self.volume_clocks: Dict[str, Any] = {}
        self.batchers: Dict[str, Any] = {}
        self.micro_defenders: Dict[str, Any] = {}

        # [БОЕВОЙ АРСЕНАЛ ВЫХОДА]
        self.exit_engine = MicrostructuralExitEngine(max_holding_sec=180.0, wall_collapse_pct=0.75)
        self.active_signals: Dict[str, ActiveSignal] = {}

        self.latest_bbo: Dict[str, Tuple[float, float]] = {s: (0.0, 0.0) for s in self.symbols}
        self.latest_depth: Dict[str, Tuple[List[Any], List[Any]]] = {s: ([], []) for s in self.symbols}

        self._alert_conflation_buffer: List[str] = []
        self._conflation_lock = asyncio.Lock()
        self._genesis_node: Optional[str] = None
        self._pending_signal_regimes: Dict[str, str] = {}
        self._bars_since_signal: Dict[str, int] = {s: 100 for s in self.symbols}
        
        self._pending_intents: Dict[str, dict] = {}
        self._recon_debouncers: Dict[str, Any] = {}

        self._causal_buf_size = getattr(alpha, 'CAUSAL_RING_BUFFER_SIZE', 1000)
        self._l2_head: Dict[str, int] = {s: 0 for s in self.symbols}
        self._l2_count: Dict[str, int] = {s: 0 for s in self.symbols}
        self._l2_ts_buf: Dict[str, Any] = {s: [0]*self._causal_buf_size for s in self.symbols}
        self._l2_depth_buf: Dict[str, Any] = {s: [0.0]*self._causal_buf_size for s in self.symbols}
        self._shm_queue = queue.Queue(maxsize=5000)
        self._lfi_vector: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.max_loop_lag = 50.0
        self.human_latency_ms = 1200.0
        self._rules = alpha.TRADING_RULES

        self._screener_ema: Dict[str, float] = {}
        self._screener_alpha = 0.05
        self._sniper_last_activity: Dict[str, float] = {}

        self.flight_recorder = AtomicFlightRecorder()
        self._limbic_symbols: Set[str] = set()
        self._tick_counters: Dict[str, int] = collections.defaultdict(int)
        self._warmup_required: Dict[str, int] = {}
        self._sentry_trigger_vol = 50000.0
        self._awakening_tokens = 5.0
        self._last_awakening_refill = time.monotonic()

        self.sentry_brain = SentryBrain(self.symbols, self._get_default_sector_map())

        self.resilience = GlobalResilienceManager.get_instance()
        self.resilience.bind_sentry(self.sentry_brain)
        self.entropy_mgr = getattr(self.resilience, 'entropy', None)
        self.cortex_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        self.alpha_engine = ZeroAllocationEngine(max_symbols=500)
        self._symbol_to_idx = {s: i for i, s in enumerate(self.symbols)}
        self._load_shedding_active = False
        self._jettisoned_symbols: Set[str] = set()
        self._snapshot_count = 0

        self.shadow_ledger = RealismShadowLedger(self.db, self)
        self.watchdog = ReactivePremiseWatchdog()
        self.toxicity_monitor = PostTradeToxicityMonitor(self.macro_states)

        self.tg = TelegramRadarNotifier(db_manager=self.db, bot_token=settings.bot_token, chat_id=settings.chat_id, event_bus=self.event_bus)

        self.outbox_repo = OutboxRepository(self.db)
        self.outbox_relay = TelegramRelayWorker(self.outbox_repo, self.tg)

        from src.application.sentinel import BlackoutSentinel, FlatlineSentinel
        self.sentinel = BlackoutSentinel(self.db, self.tg._live_allowed)
        self.flatline_sentinel = FlatlineSentinel(threshold_sec=15.0)

        self.signal_tracker = SignalTracker(self.db, self.tg)
        self.macro_filter = MacroRegimeFilter(panic_vpin_threshold=alpha.PANIC_VPIN_THRESHOLD, panic_delta_threshold=alpha.PANIC_DELTA_THRESHOLD)
        self.friction_guard = ExecutionFrictionGuard(taker_fee_bps=alpha.TAKER_FEE_BPS, min_alpha_bps=alpha.MIN_ALPHA_BPS)
        self.escrow_guard = LiquidationEchoGuard(self.event_bus)

        self.risk_guard = GlobalRiskGuard(shadow_ledger=self.shadow_ledger, max_equity_usd=alpha.MAX_EQUITY_USD, reserve_pct=0.15)
        self.defenders = self.micro_defenders
        self.genesis_conflator = GenesisConflationEngineV4(self.macro_states, self.defenders, self.event_bus, self.watchdog)

        # [DOCTRINE] AutonomousExecutionGateway and L6StateHealer REMOVED.
        # GEKTOR is advisory-only. No auto-execution capability.

    def _launch_daemon(self, name: str, coro) -> None:
        """[SUPERVISION] Изолированный запуск бесконечных циклов."""
        task = asyncio.create_task(coro, name=name)
        self._daemon_tasks.add(task)
        task.add_done_callback(self._handle_daemon_death)
        logger.info(f"⚙️ [SUPERVISOR] Daemon '{name}' launched.")

    def _handle_daemon_death(self, task: asyncio.Task):
        """[FAIL-FAST] Erlang 'Let it Crash' Pattern."""
        self._daemon_tasks.discard(task)
        try:
            exc = task.exception()
            if exc and not isinstance(exc, asyncio.CancelledError):
                logger.critical(f"☠️ [FATAL] Daemon '{task.get_name()}' crashed: {type(exc).__name__} - {str(exc)}")
                self._shutdown_event.set()
        except asyncio.CancelledError:
            pass 

    async def _flatline_monitor_loop(self):
        """Background loop to check for exchange freezes."""
        while not self._shutdown_event.is_set():
            await asyncio.sleep(10)
            blind_symbols = self.flatline_sentinel.check_for_flatlines(self.symbols)
            if blind_symbols:
                for symbol in blind_symbols:
                    asyncio.create_task(self.tg.notify_manual(f"🛑 <b>[ЧАСТИЧНАЯ СЛЕПОТА]</b> {symbol} flatlined."))

    def send_critical_alert(self, message: str):
        """Callback for ingestor to report critical network/API failures."""
        logger.error(f"🚨 [INGESTOR] {message}")
        asyncio.create_task(self.tg.notify_manual(f"🚨 <b>[СБОЙ ИНФРАСТРУКТУРЫ]</b>\n{message}"))

    async def start(self):
        """[GEKTOR v14.0] Ignite all core infrastructure with Zombie Signal Exorcism."""
        logger.info("🔥 [CORE] Gektor APEX v14.0 Monolith starting...")
        
        # 1. Start Event Bus & Infrastructure
        await self.event_bus.start()
        await self.db.initialize()
        await self.tg.start()
        
        # 2. [EXORCISM] Bury zombie PENDING signals that drifted beyond threshold
        await self._exorcise_zombie_signals()
        
        # 3. Start Ingestor 
        from src.infrastructure.bybit import BybitIngestor
        self.ingestor = BybitIngestor(
            symbols=self.symbols,
            on_tick_callback=self.ingest_tick,
            on_snapshot_callback=self.ingest_snapshot,
            alert_callback=self.send_critical_alert,
            proxy_url=self.bybit_proxy
        )
        
        # 4. Process Supervisors
        self._launch_daemon("BlackoutSentinel", self.sentinel.watch())
        self._launch_daemon("WebSocketIngestor", self.ingestor.run())
        self._launch_daemon("FlatlineMonitor", self._flatline_monitor_loop())
        
        logger.success("🚀 [CORE] System ARMED. Daemons and Supervisor running.")
        try:
            await self.tg.notify_manual("🟢 [GEKTOR APEX] Система выведена на орбиту. Монолит стабилен. L2-Радар активирован.", "STARTUP")
        except Exception as e:
            logger.error(f"🚨 [Telegram] Ошибка отправки стартового алерта: {e}")

    async def _exorcise_zombie_signals(self) -> None:
        """
        [GEKTOR v14.0] Zombie Signal Burial Protocol.
        
        Problem: After a restart, the Transactional Outbox may contain PENDING
        signals that were queued before shutdown. During the downtime, the market
        may have moved significantly. Dispatching these stale signals would send
        the Operator a false call-to-action (Protocol 2 violation).
        
        Solution: Fetch current spot price via REST. For every PENDING outbox event
        that contains a price, compute delta. If drift > ZOMBIE_DRIFT_THRESHOLD,
        mark the signal as BURIED (dead) instead of dispatching it.
        
        Determinism: This runs BEFORE TelegramRelayWorker starts processing,
        so there is zero race condition — zombies are buried atomically.
        """
        import json
        from sqlalchemy import text
        
        _ZOMBIE_DRIFT_PCT: float = getattr(alpha, 'ZOMBIE_DRIFT_THRESHOLD_PCT', 0.5)
        
        try:
            # 1. Fetch current reference price (BTC as market anchor)
            current_price: float = 0.0
            try:
                current_price = float(await self.rest_client.get_tickers("BTCUSDT"))
            except Exception as e:
                logger.warning(f"⚠️ [EXORCISM] Failed to fetch reference price: {e}. Skipping exorcism.")
                return
            
            if current_price <= 0:
                logger.warning("⚠️ [EXORCISM] Invalid reference price. Skipping.")
                return
            
            # 2. Scan outbox for PENDING events
            async with self.db.SessionLocal() as session:
                result = await session.execute(
                    text("SELECT id, payload, created_at FROM outbox_events WHERE status IN ('PENDING', 'PROCESSING')")
                )
                rows = result.fetchall()
                
                if not rows:
                    logger.info("🧹 [EXORCISM] Outbox clean. No zombie signals.")
                    return
                
                buried_count: int = 0
                for row in rows:
                    msg_id, payload_str, created_at = row[0], row[1], row[2]
                    try:
                        payload = json.loads(payload_str)
                        signal_price = float(payload.get("price", 0))
                        if signal_price <= 0:
                            continue
                        
                        # 3. Compute price drift
                        drift_pct = abs(current_price - signal_price) / signal_price * 100.0
                        
                        if drift_pct > _ZOMBIE_DRIFT_PCT:
                            # BURY: Mark as ZOMBIE — never dispatch
                            await session.execute(
                                text("UPDATE outbox_events SET status = 'ZOMBIE_BURIED' WHERE id = :id"),
                                {"id": msg_id}
                            )
                            symbol = payload.get("symbol", "?")
                            logger.warning(
                                f"🪦 [EXORCISM] Buried zombie signal #{msg_id} ({symbol}): "
                                f"signal_price=${signal_price:.2f}, current=${current_price:.2f}, "
                                f"drift={drift_pct:.2f}% > {_ZOMBIE_DRIFT_PCT}%"
                            )
                            buried_count += 1
                    except (json.JSONDecodeError, ValueError, TypeError):
                        # Non-parseable payload — not a price-bearing signal, leave it
                        continue
                
                if buried_count > 0:
                    await session.commit()
                    logger.success(f"🧹 [EXORCISM] Buried {buried_count} zombie signals. Operator protected.")
                else:
                    logger.info(f"🧹 [EXORCISM] {len(rows)} PENDING signals validated. All within drift threshold.")
                    
        except Exception as e:
            logger.error(f"⚠️ [EXORCISM] Zombie scan failed (non-fatal): {e}")

    async def ingest_tick(self, symbol: str, tick_data: dict):
        """[HFT] O(1) tick router."""
        self.flatline_sentinel.update_pulse(symbol)
        
        if symbol in self.micro_defenders:
            self.micro_defenders[symbol].update_execution(tick_data["volume"], tick_data["side"])
            
        if symbol not in self.batchers:
            # We initialize batchers or route to the gateway here
            pass

    async def ingest_snapshot(self, symbol: str, snapshot_data: dict):
        """[HFT] Snapshot ingestion."""
        self.flatline_sentinel.update_pulse(symbol)
        pass

    def _get_default_sector_map(self) -> dict:
        """
        [GEKTOR v13.4] Default symbol → sector mapping for SentryBrain SSM.
        Maps tracked symbols to crypto sector groups.
        """
        sector_defaults = {
            "BTCUSDT": "BTC", "ETHUSDT": "L1", "SOLUSDT": "L1",
            "BNBUSDT": "CEX", "XRPUSDT": "PAYMENTS", "ADAUSDT": "L1",
            "DOGEUSDT": "MEME", "SHIBUSDT": "MEME", "PEPEUSDT": "MEME",
            "AVAXUSDT": "L1", "DOTUSDT": "L1", "MATICUSDT": "L2",
            "LINKUSDT": "ORACLE", "UNIUSDT": "DEFI", "AAVEUSDT": "DEFI",
            "ARBUSDT": "L2", "OPUSDT": "L2", "APTUSDT": "L1",
            "SUIUSDT": "L1", "NEARUSDT": "L1", "ATOMUSDT": "L1",
            "FTMUSDT": "L1", "INJUSDT": "DEFI", "TIAUSDT": "MODULAR",
            "WLDUSDT": "AI", "FETUSDT": "AI", "RENDERUSDT": "AI",
            "LTCUSDT": "PAYMENTS", "BCHUSDT": "PAYMENTS",
        }
        # Map only symbols we're actually tracking
        return {s: sector_defaults.get(s, "OTHER") for s in self.symbols}

    # ═══════════════════════════════════════════════════════════════════
    # [PATCH 1] GRACEFUL SHUTDOWN & SHM CLEANUP
    # ═══════════════════════════════════════════════════════════════════

    async def shutdown(self):
        """
        [GEKTOR v12.12] Deterministic Graceful Shutdown.
        Execution order:
        1. Halt ingress (stop accepting new WS data)
        2. Cancel all daemon/background tasks
        3. Flush state to disk (EventBus outbox, flight recorder)
        4. Cleanup Shared Memory segments (CRITICAL — prevents leaked SHM)
        5. Close REST singleton session
        """
        logger.critical("🛑 [SHUTDOWN] Initiating Graceful Teardown Protocol...")

        # 1. Signal all loops to stop
        self._shutdown_event.set()

        # 2. Cancel daemon tasks with grace period
        all_tasks = list(self._daemon_tasks) + list(self._background_tasks)
        for task in all_tasks:
            if not task.done():
                task.cancel()

        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
            logger.info(f"🧹 [SHUTDOWN] Cancelled {len(all_tasks)} tasks.")

        # 3. Flush EventBus outbox
        try:
            await self.event_bus.stop()
            logger.info("✅ [SHUTDOWN] EventBus outbox flushed.")
        except Exception as e:
            logger.error(f"⚠️ [SHUTDOWN] EventBus flush error: {e}")

        # 4. Flush flight recorder
        if hasattr(self, 'flight_recorder') and hasattr(self.flight_recorder, 'graceful_shutdown'):
            self.flight_recorder.graceful_shutdown()

        # 5. Cleanup Shared Memory (CRITICAL)
        await self._cleanup_shared_memory()

        # 6. Close REST singleton session
        try:
            await self.rest_client.close()
            logger.info("✅ [SHUTDOWN] REST session closed.")
        except Exception as e:
            logger.error(f"⚠️ [SHUTDOWN] REST session close error: {e}")

        # 7. Close Telegram notifier session
        if hasattr(self.tg, 'close'):
            try:
                await self.tg.close()
            except Exception as e:
                logger.warning(f"[SHUTDOWN] Telegram close error (non-fatal): {e}")

        logger.critical("⬛ [SHUTDOWN] Teardown complete. All resources released.")

    async def _cleanup_shared_memory(self):
        """
        [PATCH 1] Deterministic SHM segment cleanup.
        Iterates the books registry and calls close() on each StateMachineOrderBook,
        which triggers shm.close() + shm.unlink(). This prevents the
        'resource_tracker: 6 leaked shared_memory objects' error.
        """
        cleaned = 0
        failed = 0
        for symbol, book in self.books.items():
            try:
                if hasattr(book, 'close'):
                    book.close()
                    cleaned += 1
            except Exception as e:
                logger.error(f"⚠️ [SHM] Cleanup error for {symbol}: {e}")
                failed += 1

        if cleaned > 0:
            logger.info(f"🗑️ [SHM] Cleaned up {cleaned} segments. Failed: {failed}.")
        elif not self.books:
            logger.info("🗑️ [SHM] No active books to cleanup.")

    def halt_ingress(self):
        """[PATCH 1] Emergency ingress halt. Called by DeadMansSwitch."""
        self._shutdown_event.set()
        logger.critical("🛑 [INGRESS] Data ingestion HALTED.")