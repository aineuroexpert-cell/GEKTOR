# src/shared/resilience.py
import asyncio
import time
import random
import numpy as np
from typing import Final, Optional, List, Set, Dict, Any, Tuple
from enum import IntEnum
from loguru import logger

# [GEKTOR v13.7.1] IMMORTAL STANDARDS
SCALING_FACTOR: Final[int] = 10**8
WIDE_WINDOW: Final[int] = 1000
SHORT_WINDOW: Final[int] = 200
COHERENCE_WINDOW_MS: Final[int] = 10
REARM_THRESHOLD_MS: Final[int] = 100
INFRA_BREAKER_MS: Final[int] = 150

class HydrationPriority(IntEnum):
    CRITICAL_RISK = 0
    GRAVITY_LEADER = 1
    VIP = 2
    SHARED = 3
    DEFAULT = 4

class SystemState(IntEnum):
    COLD = 0
    WARMUP = 1
    ARMED = 2
    STORM = 3
    SHUTDOWN = 4

class EntropyManager:
    """
    [GEKTOR v13.7.1] IMMORTAL JITTER GUARD.
    O(1) Ring Buffer implementation with Dual-Resolution (Wide/Short) P99 metrics.
    """
    def __init__(self, sentry_brain=None):
        self.sentry = sentry_brain
        self.tunnel_vision_active = False
        self.global_heat = 0.0
        
        # [cite: 522] High-Performance Ring Buffer
        self._buffer = np.zeros(WIDE_WINDOW, dtype=np.int32)
        self._ptr = 0
        self._is_warm = False
        
        # Dual-Resolution Metrics
        self.wide_p99 = 20
        self.short_p99 = 20
        self.rtt_ms = 20
        
        # [GEKTOR v13.7.1] Liveness Guard (Throughput Monitoring)
        self.current_tps = 0
        self._tps_lock = 0
        self._last_tps_reset = time.time()

    def update_metrics(self, heat: float, current_rtt: float, tick_pulse: bool = True):
        """Update metrics with Liveness Monitoring."""
        self.global_heat = heat
        self.rtt_ms = current_rtt
        
        if tick_pulse:
            self._tps_lock += 1
            
        # Throughput resolution (1s window)
        now = time.time()
        if now - self._last_tps_reset >= 1.0:
            self.current_tps = self._tps_lock
            self._tps_lock = 0
            self._last_tps_reset = now
        
        # Ring Buffer Ingestion
        self._buffer[self._ptr] = int(current_rtt)
        self._ptr = (self._ptr + 1) % WIDE_WINDOW
        if self._ptr == 0: self._is_warm = True
            
        # Batch Calculation (Every 10 samples)
        if self._ptr % 10 == 0:
            self._update_resolutions()

    def _update_resolutions(self):
        """Dual-Resolution P99 Calculation."""
        data = self._buffer if self._is_warm else self._buffer[:self._ptr]
        if len(data) < SHORT_WINDOW: return

        # 1. Wide resolution (1000 samples)
        self.wide_p99 = int(np.percentile(data, 99))
        
        # 2. Short resolution (200 samples) with wrap-around logic
        if self._ptr >= SHORT_WINDOW:
            short_data = self._buffer[self._ptr - SHORT_WINDOW : self._ptr]
        else:
            tail = self._buffer[-(SHORT_WINDOW - self._ptr):]
            head = self._buffer[:self._ptr]
            short_data = np.concatenate((tail, head))
            
        self.short_p99 = int(np.percentile(short_data, 99))

    def check_infra_health(self) -> bool:
        """[cite: 523] Triage Recovery Logic."""
        if self.wide_p99 <= INFRA_BREAKER_MS:
            return True # Stable State
            
        return self.short_p99 < REARM_THRESHOLD_MS

    def is_healthy(self) -> bool:
        """Alias for GlobalResilienceManager check."""
        return self.check_infra_health()

    def get_jitter_penalty(self) -> int:
        """[GEKTOR v13.7.1] STORM DECAY PENALTY (Throughput-Aware)."""
        is_starving = self.current_tps < 10
        if self.short_p99 < self.wide_p99 * 0.4 and not is_starving:
            effective_rtt = int(self.short_p99 * 0.7 + self.wide_p99 * 0.3)
        else:
            effective_rtt = self.wide_p99
            
        extra_lag = max(0, effective_rtt - 20)
        return 2 + (extra_lag // 25)

    def check_global_pressure(self) -> bool:
        """O(1) Audit of global market temperature."""
        if self.sentry is None or not hasattr(self.sentry, 'sector_heat'):
            return False
            
        self.global_heat = float(np.mean(self.sentry.sector_heat))
        if self.global_heat > 0.85 and not self.tunnel_vision_active:
            logger.critical(f"🔥 [ENTROPY] Global Heat Overload ({self.global_heat:.2f})! Engaging Tunnel Vision Mode.")
            self.tunnel_vision_active = True
        elif self.global_heat < 0.4 and self.tunnel_vision_active:
            logger.success(f"🌤️ [ENTROPY] Market cooling down ({self.global_heat:.2f}). Disengaging Tunnel Vision.")
            self.tunnel_vision_active = False
        return self.tunnel_vision_active

    def should_shed_tick(self, symbol: str, is_private: bool, cortex_list: list) -> bool:
        if is_private: return False
        if not self.tunnel_vision_active: return False
        if symbol in cortex_list: return False
        return True

class GhostQuotaManager:
    """[GEKTOR v13.7.1] ZQI Guard: Prevents memory admission failure."""
    def __init__(self, limit: int = 50):
        self.limit = limit
        self._zombies = 0
    
    def register_task(self): self._zombies += 1
    def release_task(self): self._zombies = max(0, self._zombies - 1)
    
    def is_admission_allowed(self) -> bool:
        return self._zombies < self.limit

class ShadowVerificationGuard:
    """[GEKTOR v13.7.1] SVM: Disabled. System goes directly to Live Execution."""
    def __init__(self, parent, duration: int = 0):
        self.parent = parent
        self.duration = duration
        self.start_time = time.time()
        self.state = SystemState.ARMED
        logger.info(f"⚔️ [SVM] Shadow Verification Disabled. System is ARMED for Live Execution.")

    def update(self):
        """Poll state to transition."""
        pass

class StealthAdmissionManager:
    """[GEKTOR v13.7.1] Stealth Admission: Randomizes entry to mask signature."""
    def __init__(self):
        self.jitter_range = (0.5, 5.0)
    
    async def wait_for_admission(self):
        jitter = random.uniform(*self.jitter_range)
        await asyncio.sleep(jitter)

class PTPClock:
    """
    Эшелон 21: Precision Time Protocol Emulator.
    Обеспечивает микросекундную точность и расчет дрейфа относительно биржи.
    """
    def __init__(self):
        self._offset_ns: int = 0

    def calibrate(self, exchange_ts_ms: int):
        """Синхронизация с серверным временем биржи (Bybit REST/WS)."""
        if exchange_ts_ms <= 0: return
        local_now_ns = time.time_ns()
        self._offset_ns = (exchange_ts_ms * 1_000_000) - local_now_ns

    def now_ms(self) -> int:
        """Возвращает синхронизированное время в мс."""
        return (time.time_ns() + self._offset_ns) // 1_000_000

    def now_ns(self) -> int:
        """Возвращает синхронизированное время в наносекундах."""
        return time.time_ns() + self._offset_ns

# ─────────────────────────────────────────────────────────
# Echelon 22: AutoBug Shield (Self-Healing & Loop Starvation Monitoring)
# ─────────────────────────────────────────────────────────
class LoopMonitor:
    """
    [GEKTOR v16.0] Event Loop Starvation Monitor.
    Measures event loop scheduling latency. If tasks block the loop for > 50ms,
    triggers warning logs and alerts.
    """
    def __init__(self, warning_threshold_ms: float = 50.0):
        self.threshold_sec = warning_threshold_ms / 1000.0
        self.starvation_count = 0
        self._monitor_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def start(self):
        if self._is_running: return
        self._is_running = True
        self._monitor_task = asyncio.create_task(self._loop())
        logger.info("🛡️ [AutoBugShield] Loop Starvation Monitor ACTIVE.")

    async def stop(self):
        self._is_running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        while self._is_running:
            start = time.monotonic()
            await asyncio.sleep(0.01)  # Yield execution
            elapsed = time.monotonic() - start - 0.01
            if elapsed > self.threshold_sec:
                self.starvation_count += 1
                logger.critical(
                    f"⚠️ [EVENT LOOP STARVATION] Task blocked the event loop for {elapsed*1000:.1f}ms! "
                    f"Starvation Count: {self.starvation_count}"
                )


class MemoryShield:
    """
    [GEKTOR v16.0] Runtime Memory Guard.
    Audits RAM usage. Force-triggers garbage collection if RSS exceeds limit.
    """
    def __init__(self, limit_mb: float = 1024.0):
        self.limit_mb = limit_mb
        self._is_running = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start(self):
        if self._is_running: return
        self._is_running = True
        self._monitor_task = asyncio.create_task(self._loop())
        logger.info(f"🛡️ [AutoBugShield] Memory Shield ACTIVE (Limit: {self.limit_mb}MB).")

    async def stop(self):
        self._is_running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        import gc
        try:
            import psutil
            process = psutil.Process()
        except ImportError:
            process = None
            logger.warning("psutil not available. Memory Shield operating in mock mode.")

        while self._is_running:
            await asyncio.sleep(5.0)
            if process:
                rss_mb = process.memory_info().rss / (1024 * 1024)
                if rss_mb > self.limit_mb:
                    logger.warning(
                        f"🚨 [MEMORY OVERLIMIT] RAM usage {rss_mb:.1f}MB exceeds limit {self.limit_mb}MB. "
                        "Executing force garbage collection."
                    )
                    gc.collect()


class ComponentHealer:
    """
    [GEKTOR v16.0] Self-Correction & Recovery Engine.
    Tracks failure rates for critical resources (BybitWS, Database, Redis).
    Executes mitigation scripts if failures exceed safety thresholds.
    """
    def __init__(self, failure_limit: int = 5, window_sec: float = 10.0):
        self.limit = failure_limit
        self.window = window_sec
        self._failures: Dict[str, List[float]] = {}
        self._healing_locks: Set[str] = set()

    def register_failure(self, component: str, recovery_callback=None) -> bool:
        """
        Record a failure for a component. If failure limit is breached,
        runs recovery_callback (if provided and not already healing).
        Returns True if healing was triggered.
        """
        now = time.monotonic()
        if component not in self._failures:
            self._failures[component] = []
        
        # Clean expired failures
        self._failures[component] = [t for t in self._failures[component] if now - t <= self.window]
        self._failures[component].append(now)

        if len(self._failures[component]) >= self.limit:
            if component not in self._healing_locks:
                self._healing_locks.add(component)
                logger.critical(
                    f"💥 [SELF-HEALING] Component '{component}' breached safety threshold with "
                    f"{len(self._failures[component])} failures in {self.window}s. Initiating recovery..."
                )
                if recovery_callback:
                    asyncio.create_task(self._run_healing(component, recovery_callback))
                return True
        return False

    async def _run_healing(self, component: str, callback):
        try:
            await callback()
            logger.success(f"✅ [SELF-HEALING] Recovery completed successfully for component '{component}'.")
        except Exception as e:
            logger.error(f"❌ [SELF-HEALING] Recovery failed for component '{component}': {e}")
        finally:
            self._failures[component] = []
            self._healing_locks.discard(component)


class GlobalResilienceManager:
    """
    Эшелон 20: Единый Командный Центр.
    Исправленная версия с поддержкой регистрации отказов, PTP Clock, и AutoBug Shield.
    """
    _instance: Optional['GlobalResilienceManager'] = None

    def __init__(self):
        # This will be called by get_instance or explicitly
        self.entropy = EntropyManager()  # TPS, P99, Gap
        self.ghost_quota = GhostQuotaManager() # ZQI
        self.svm = ShadowVerificationGuard(self) # 60s ReadOnly
        self.stealth = StealthAdmissionManager() # Jittered Activation
        
        # Эшелон 21: Синхронизация времени
        self.ptp_clock = PTPClock()

        # Echelon 22: Self-Healing & Diagnostics (AutoBug Shield)
        self.loop_monitor = LoopMonitor()
        self.memory_shield = MemoryShield()
        self.healer = ComponentHealer()
        
        # Реестры для трекинга попыток и отказов
        self._failure_registry: Dict[str, int] = {} 
        self._resource_attempts: Dict[str, int] = {
            "BybitWS_Public": 0,
            "BybitWS_Private": 0,
            "Redis": 0,
            "Telegram": 0
        }
        
        logger.info("🛡️ GEKTOR v13.7.1: GLOBAL RESILIENCE MANAGER INITIALIZED")

    async def start_shields(self):
        """Start background loop and memory monitors."""
        await self.loop_monitor.start()
        await self.memory_shield.start()

    async def stop_shields(self):
        """Stop background loop and memory monitors."""
        await self.loop_monitor.stop()
        await self.memory_shield.stop()

    def register_failure(self, symbol: str, recovery_callback=None):
        """
        Регистрирует сбой для символа или ресурса.
        """
        self._failure_registry[symbol] = self._failure_registry.get(symbol, 0) + 1
        self._resource_attempts[symbol] = self._resource_attempts.get(symbol, 0) + 1
        
        if self._failure_registry[symbol] > 3:
            logger.warning(f"⚠️ [RESILIENCE] Extreme failure rate for {symbol} ({self._failure_registry[symbol]} attempts).")
            
        # Bind failure to ComponentHealer to trigger recovery scripts
        self.healer.register_failure(symbol, recovery_callback)

    def register_success(self, symbol: str):
        """
        Регистрирует успех для символа или ресурса.
        """
        self._failure_registry.pop(symbol, None)
        self._resource_attempts.pop(symbol, None)

    async def request_rest_call(self, symbol: str, priority: HydrationPriority = HydrationPriority.DEFAULT):
        """
        [GEKTOR v12.15] Sequential wait via Priority Token Bucket.
        Ensures Core Assets (GRAVITY_LEADER) get bandwidth first.
        """
        # VIP/Critical Risk jump the queue
        if priority <= HydrationPriority.VIP:
             await asyncio.sleep(0) # Immediate yield
        else:
             # Standard throttle for micro-universe to prevent 429
             await asyncio.sleep(0.05) 

    @classmethod
    def get_instance(cls) -> 'GlobalResilienceManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def bind_sentry(self, sentry_brain):
        """Late binding of SentryBrain to EntropyManager."""
        self.entropy.sentry = sentry_brain

    def is_armed(self) -> bool:
        """Проверка готовности всех систем к удару."""
        self.svm.update()
        return (
            self.svm.state.name == "ARMED" and 
            self.ghost_quota.is_admission_allowed() and
            self.entropy.is_healthy()
        )

class DivergenceMonitor:
    """
    [GEKTOR v13.7] ENTROPY DIVERGENCE & SPOOF GUARD.
    Compares primary ingestor heat with a reference stream (e.g., Binance) 
    to detect exchange-specific lag or infrastructure choke.
    """
    def __init__(self, primary_entropy: EntropyManager, ref_entropy: EntropyManager):
        self.primary = primary_entropy
        self.ref = ref_entropy
        self.is_lagging = False
        self.divergence_threshold = 0.5

    def detect_desync(self) -> bool:
        """
        O(1) Reality Check.
        If Bybit Entropy >> Binance Entropy, Bybit is in an infrastructure choke.
        """
        divergence = self.primary.global_heat - self.ref.global_heat
        
        if divergence > self.divergence_threshold:
            if not self.is_lagging:
                logger.warning(f"⚠️ [DIVERGENCE] Entropy Sync Lost. Delta: {divergence:.2f}. Source: INFRA_CHOKE.")
                self.is_lagging = True
            return True
        
        self.is_lagging = False
        return False

class BinanceSequenceGuard:
    def __init__(self):
        self._last_l1_ts: int = 0
        self._last_l1_volume: int = 0

    def update_l1(self, exchange_ts: int, volume: int):
        self._last_l1_ts = exchange_ts
        self._last_l1_volume = volume

    def verify_sync(self, l2_ts: int) -> bool:
        delta = l2_ts - self._last_l1_ts
        return 0 <= delta <= COHERENCE_WINDOW_MS

class CrossVolumeVerifier:
    def __init__(self, toir_threshold: float = 1.5):
        self.toir_threshold_scaled = int(toir_threshold * SCALING_FACTOR)
        self._seq_guard = BinanceSequenceGuard()

    def update_reference_tape(self, ts: int, vol: int):
        self._seq_guard.update_l1(ts, vol)

    def is_pulse_real(self, l2_imbalance: float, l1_vol: int, avg_vol: int, l2_ts: int) -> bool:
        if abs(l2_imbalance) < 0.8: return False
        if not self._seq_guard.verify_sync(l2_ts): return False
        vol_intensity = (l1_vol * SCALING_FACTOR) // (avg_vol + 1)
        return vol_intensity > self.toir_threshold_scaled
