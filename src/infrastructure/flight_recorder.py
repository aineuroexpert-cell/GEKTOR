import asyncio
import sqlite3
import time
from typing import List, Dict, Any
from loguru import logger


class AtomicFlightRecorder:
    """
    [GEKTOR v14.0 IMMORTAL CAUSALITY]

    Persistence layer for in-flight execution intents.
    Ensures 'Zombie Orders' are remembered across system restarts.
    Uses SQLite WAL mode for low-latency atomic writes.

    All public methods are async — SQLite I/O is offloaded via
    asyncio.to_thread() to prevent Event Loop blocking.

    Extended schema tracks:
      - pre_flight_msq_price: MSQ avg price at JIT validation moment
      - execution_price: actual fill price from exchange (for Network Decay audit)
      - dispatched_at: monotonic-ish timestamp for stale detection on reconnect
    """
    def __init__(self, db_path: str = "flight_log.db"):
        self.db_path = db_path
        self._init_db_sync()

    def _init_db_sync(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS intents (
                    cl_ord_id TEXT PRIMARY KEY,
                    symbol TEXT,
                    side TEXT,
                    qty REAL,
                    price REAL,
                    status TEXT, -- 'PENDING', 'DISPATCHED', 'FILLED', 'TERMINAL'
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    dispatched_at TIMESTAMP,
                    pre_flight_msq_price REAL,
                    execution_price REAL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON intents(status)")
            for col, col_type in [
                ("dispatched_at", "TIMESTAMP"),
                ("pre_flight_msq_price", "REAL"),
                ("execution_price", "REAL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE intents ADD COLUMN {col} {col_type}")
                except sqlite3.OperationalError:
                    pass  # Column already exists

    # ── Sync helpers (run inside thread) ──────────────────────

    def _log_intent_sync(
        self,
        cl_ord_id: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        pre_flight_msq_price: float,
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO intents "
                "(cl_ord_id, symbol, side, qty, price, status, pre_flight_msq_price) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (cl_ord_id, symbol, side, qty, price, "PENDING", pre_flight_msq_price),
            )

    def _mark_dispatched_sync(self, cl_ord_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE intents SET status = 'DISPATCHED', dispatched_at = ? "
                "WHERE cl_ord_id = ?",
                (time.time(), cl_ord_id),
            )

    def _mark_filled_sync(self, cl_ord_id: str, execution_price: float) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE intents SET status = 'FILLED', execution_price = ? "
                "WHERE cl_ord_id = ?",
                (execution_price, cl_ord_id),
            )

    def _mark_terminal_sync(self, cl_ord_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM intents WHERE cl_ord_id = ?", (cl_ord_id,))

    def _get_unresolved_sync(self) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM intents WHERE status IN ('PENDING', 'DISPATCHED')"
            ).fetchall()
            return [dict(row) for row in rows]

    def _graceful_shutdown_sync(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    # ── Async public API (offloads to thread) ─────────────────

    async def log_intent(
        self,
        cl_ord_id: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        *,
        pre_flight_msq_price: float = 0.0,
    ) -> None:
        """Pre-flight recording. Must happen BEFORE the API call."""
        try:
            await asyncio.to_thread(
                self._log_intent_sync,
                cl_ord_id, symbol, side, qty, price, pre_flight_msq_price,
            )
        except Exception as e:
            logger.error(f"[FLIGHT_RECORDER] Write Failure: {e}")

    async def mark_dispatched(self, cl_ord_id: str) -> None:
        """Update status when API returns a definitive response."""
        try:
            await asyncio.to_thread(self._mark_dispatched_sync, cl_ord_id)
        except Exception as e:
            logger.error(f"[FLIGHT_RECORDER] Update Failure: {e}")

    async def mark_filled(self, cl_ord_id: str, execution_price: float) -> None:
        """
        Record actual fill price for Network Decay audit.
        Drift = |execution_price - pre_flight_msq_price| reveals systematic alpha erosion.
        """
        try:
            await asyncio.to_thread(self._mark_filled_sync, cl_ord_id, execution_price)
        except Exception as e:
            logger.error(f"[FLIGHT_RECORDER] Fill Record Failure: {e}")

    async def mark_terminal(self, cl_ord_id: str) -> None:
        """Remove or mark as finished when WS confirms the order is done."""
        try:
            await asyncio.to_thread(self._mark_terminal_sync, cl_ord_id)
        except Exception as e:
            logger.error(f"[FLIGHT_RECORDER] Cleanup Failure: {e}")

    async def get_unresolved_intents(self) -> List[Dict[str, Any]]:
        """Used during Boot-Time Purgatory and Reconnect Reconciliation."""
        try:
            return await asyncio.to_thread(self._get_unresolved_sync)
        except Exception as e:
            logger.error(f"[FLIGHT_RECORDER] Read Failure: {e}")
            return []

    async def graceful_shutdown(self) -> None:
        """Flush WAL on clean shutdown."""
        try:
            await asyncio.to_thread(self._graceful_shutdown_sync)
        except Exception as e:
            logger.error(f"[FLIGHT_RECORDER] Shutdown Failure: {e}")
