"""
[GEKTOR APEX v3.6.0] Alert sink adapter — RadarAlert -> Transactional Outbox.

Bridges the pure application-layer RadarPipeline with the infrastructure
Outbox. The RadarPipeline does NOT depend on this module directly; it
receives a callable. This keeps the radar pipeline unit-testable without
a database.

Payload schema (canonical, do not change without bumping version):
    {
        "event_type": "RADAR_ALERT",
        "version": 1,
        "symbol": str,
        "direction": "long" | "short",
        "vpin": float,                   # [0, 1]
        "z_score": float,
        "absorption": bool,
        "bar_close": float,
        "bar_open": float,
        "timestamp": float               # exchange ts (epoch seconds)
    }
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from src.application.radar_pipeline import RadarAlert


class OutboxAlertSink:
    """Persist RadarAlert as a row in outbox_events.

    Works with both SQLite (default local dev) and PostgreSQL (production)
    because the INSERT does not use any dialect-specific syntax.
    """

    def __init__(self, db_manager) -> None:
        self._db = db_manager

    async def __call__(self, alert: RadarAlert) -> None:
        payload = json.dumps(self._format(alert))
        now = datetime.now(timezone.utc)
        async with self._db.SessionLocal() as session:
            await session.execute(
                text(
                    "INSERT INTO outbox_events "
                    "(payload, status, created_at, execute_after, retry_count, priority) "
                    "VALUES (:payload, 'PENDING', :now, :now, 0, 1)"
                ),
                {"payload": payload, "now": now},
            )
            await session.commit()
        logger.debug(
            f"[OutboxAlertSink] Persisted {alert.symbol} {alert.direction} "
            f"z={alert.z_score:.2f}"
        )

    @staticmethod
    def _format(alert: RadarAlert) -> dict:
        return {
            "event_type": "RADAR_ALERT",
            "version": 1,
            "symbol": alert.symbol,
            "direction": alert.direction,
            "vpin": alert.vpin,
            "z_score": alert.z_score,
            "absorption": alert.absorption,
            "bar_close": alert.bar_close,
            "bar_open": alert.bar_open,
            "timestamp": alert.timestamp,
        }
