# src/presentation/ws_tilt_handler.py
"""
[GEKTOR v15.0] WebSocket Tilt Frame Handler (Presentation Layer).

Receives heartbeat frames from the React frontend via WebSocket.
Routes them to CognitiveSentinel (Domain) and dispatches
state updates back to the frontend.

Also publishes TILT_ACTIVATED events to the EventBus,
which the L6 Gateway subscribes to for execution suppression.

Protocol:
  Client → Server: {"type": "heartbeat", "reaction_ms": 450, "clicks": 2, "errors": 0, "successes": 1}
  Server → Client: {"type": "tilt_state", "state": "CLEAR", "score": 0.12, "generation": 42, ...}
  Server → Client: {"type": "tilt_lock", "locked_until_epoch": 1716834000, "reason": "COGNITIVE_OVERLOAD"}
"""

from __future__ import annotations

import time
import asyncio
from typing import Optional, Any, Dict
from loguru import logger

from src.domain.tilt_breaker import CognitiveSentinel, TiltState, TiltMetrics


class TiltWebSocketHandler:
    """
    Manages one operator's tilt state over a persistent WebSocket connection.

    Lifecycle:
      1. Created when operator connects to signal_stream WS
      2. Receives heartbeat frames every 1s from frontend
      3. Dispatches tilt_state frames back
      4. Publishes TILT_ACTIVATED to EventBus on state transitions
      5. Destroyed when WS closes

    Thread Safety: Single-writer (the WS handler task). No locks needed.
    """
    __slots__ = (
        '_sentinel', '_event_bus', '_ws', '_blind_check_task',
        '_operator_id', '_last_dispatched_state',
    )

    def __init__(
        self,
        event_bus: Any,
        websocket: Any,
        operator_id: str = "primary",
        cooldown_sec: float = 60.0,
    ):
        self._sentinel = CognitiveSentinel(cooldown_sec=cooldown_sec)
        self._event_bus = event_bus
        self._ws = websocket
        self._operator_id = operator_id
        self._last_dispatched_state: TiltState = TiltState.CLEAR
        self._blind_check_task: Optional[asyncio.Task] = None

    async def start_monitoring(self) -> None:
        """Launch the blind-check background loop."""
        self._blind_check_task = asyncio.create_task(
            self._blind_monitor_loop(),
            name=f"TiltBlindMonitor_{self._operator_id}",
        )
        logger.info(f"🧠 [TILT] CognitiveSentinel armed for operator '{self._operator_id}'")

    async def stop_monitoring(self) -> None:
        """Cleanup on WS disconnect."""
        if self._blind_check_task and not self._blind_check_task.done():
            self._blind_check_task.cancel()
            try:
                await self._blind_check_task
            except asyncio.CancelledError:
                pass
        logger.info(f"🧠 [TILT] CognitiveSentinel disarmed for '{self._operator_id}'")

    async def handle_heartbeat(self, frame: Dict[str, Any]) -> None:
        """
        Process an incoming heartbeat frame from the frontend.

        Expected frame structure:
        {
            "type": "heartbeat",
            "reaction_ms": float,   # 0 if no signal was displayed
            "clicks": int,          # execution clicks since last heartbeat
            "errors": int,          # bad decisions since last heartbeat
            "successes": int        # good decisions since last heartbeat
        }
        """
        try:
            reaction_ms = float(frame.get("reaction_ms", 0))
            clicks = int(frame.get("clicks", 0))
            errors = int(frame.get("errors", 0))
            successes = int(frame.get("successes", 0))
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ [TILT] Malformed heartbeat frame: {e}")
            return

        metrics = self._sentinel.ingest_heartbeat(
            reaction_ms=reaction_ms,
            click_count=clicks,
            error_delta=errors,
            success_delta=successes,
        )

        # Dispatch state update to frontend
        await self._dispatch_tilt_state(metrics)

        # Detect state transitions for EventBus
        if metrics.state != self._last_dispatched_state:
            await self._handle_state_transition(self._last_dispatched_state, metrics)
            self._last_dispatched_state = metrics.state

    def is_execution_allowed(self) -> bool:
        """Gate check for L6 Gateway integration."""
        return self._sentinel.is_execution_allowed()

    async def force_unlock(self) -> None:
        """Administrative unlock via Telegram 2FA."""
        metrics = self._sentinel.force_unlock()
        await self._dispatch_tilt_state(metrics)
        self._last_dispatched_state = TiltState.CLEAR
        logger.warning(f"🔓 [TILT] FORCE UNLOCK by admin for '{self._operator_id}'")

    # ──────────────────────────────────────────────────
    # PRIVATE
    # ──────────────────────────────────────────────────

    async def _dispatch_tilt_state(self, metrics: TiltMetrics) -> None:
        """Send tilt state update to frontend via WS."""
        payload = {
            "type": "tilt_state",
            "state": metrics.state.name,
            "score": metrics.composite_score,
            "reaction_drift": metrics.reaction_drift,
            "error_streak": metrics.error_streak,
            "spam_intensity": metrics.spam_intensity,
            "generation": metrics.generation,
            "locked_until_epoch": (
                time.time() + (metrics.locked_until_mono - time.monotonic())
                if metrics.locked_until_mono > 0
                else 0
            ),
        }

        try:
            await self._ws.send_json(payload)
        except Exception as e:
            logger.error(f"⚠️ [TILT] WS dispatch failed: {e}")

    async def _handle_state_transition(
        self, old_state: TiltState, metrics: TiltMetrics
    ) -> None:
        """Publish state transitions to EventBus for L6 integration."""
        new_state = metrics.state

        if new_state in (TiltState.LOCKED, TiltState.CRITICAL):
            logger.critical(
                f"🔒 [TILT] OPERATOR LOCKED | "
                f"Score={metrics.composite_score:.2f} | "
                f"ReactionDrift={metrics.reaction_drift:.2f} | "
                f"ErrorStreak={metrics.error_streak} | "
                f"SpamIntensity={metrics.spam_intensity:.2f}"
            )
            await self._event_bus.publish(
                "TILT_ACTIVATED",
                {
                    "operator_id": self._operator_id,
                    "tilt_score": metrics.composite_score,
                    "state": new_state.name,
                    "generation": metrics.generation,
                },
            )

            # Send explicit lock frame to frontend
            try:
                await self._ws.send_json({
                    "type": "tilt_lock",
                    "locked_until_epoch": (
                        time.time() + (metrics.locked_until_mono - time.monotonic())
                        if metrics.locked_until_mono > 0
                        else time.time() + 60
                    ),
                    "reason": "COGNITIVE_OVERLOAD",
                    "score": metrics.composite_score,
                })
            except Exception as e:
                logger.error(f"[TILT] Failed to publish lockout event: {e}")

        elif new_state == TiltState.BLIND:
            logger.warning(f"👁️ [TILT] Operator '{self._operator_id}' went BLIND (WS silent)")
            await self._event_bus.publish(
                "OPERATOR_BLIND",
                {"operator_id": self._operator_id},
            )

        elif new_state == TiltState.CLEAR and old_state != TiltState.CLEAR:
            logger.success(
                f"✅ [TILT] Operator '{self._operator_id}' recovered to CLEAR state"
            )
            await self._event_bus.publish(
                "TILT_CLEARED",
                {"operator_id": self._operator_id, "generation": metrics.generation},
            )

    async def _blind_monitor_loop(self) -> None:
        """Background task: detect operator disappearance."""
        while True:
            await asyncio.sleep(1.0)
            is_blind = self._sentinel.check_blind()

            if is_blind and self._last_dispatched_state != TiltState.BLIND:
                metrics = TiltMetrics(
                    reaction_drift=0.0,
                    error_streak=0,
                    spam_intensity=0.0,
                    composite_score=0.0,
                    state=TiltState.BLIND,
                    locked_until_mono=0.0,
                    generation=self._sentinel.get_generation(),
                )
                await self._handle_state_transition(self._last_dispatched_state, metrics)
                self._last_dispatched_state = TiltState.BLIND
