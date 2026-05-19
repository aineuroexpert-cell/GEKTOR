import sqlite3
import os
import time
from typing import List, Dict, Any
from loguru import logger

class AtomicFlightRecorder:
    """
    [GEKTOR v14.0 IMMORTAL CAUSALITY]
    
    Persistence layer for in-flight execution intents.
    Ensures 'Zombie Orders' are remembered across system restarts.
    Uses SQLite WAL mode for low-latency atomic writes.

    Extended schema tracks:
      - pre_flight_msq_price: MSQ avg price at JIT validation moment
      - execution_price: actual fill price from exchange (for Network Decay audit)
      - dispatched_at: monotonic-ish timestamp for stale detection on reconnect
    """
    def __init__(self, db_path: str = "flight_log.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
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
            # Schema migration: add columns if missing (backward compat)
            for col, col_type in [
                ("dispatched_at", "TIMESTAMP"),
                ("pre_flight_msq_price", "REAL"),
                ("execution_price", "REAL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE intents ADD COLUMN {col} {col_type}")
                except sqlite3.OperationalError:
                    pass  # Column already exists

    def log_intent(
        self,
        cl_ord_id: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        *,
        pre_flight_msq_price: float = 0.0,
    ):
        """Pre-flight recording. Must happen BEFORE the API call."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO intents "
                    "(cl_ord_id, symbol, side, qty, price, status, pre_flight_msq_price) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (cl_ord_id, symbol, side, qty, price, "PENDING", pre_flight_msq_price),
                )
        except Exception as e:
            logger.error(f"💀 [FLIGHT_RECORDER] Write Failure: {e}")

    def mark_dispatched(self, cl_ord_id: str):
        """Update status when API returns a definitive response."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE intents SET status = 'DISPATCHED', dispatched_at = ? "
                    "WHERE cl_ord_id = ?",
                    (time.time(), cl_ord_id),
                )
        except Exception as e:
            logger.error(f"💀 [FLIGHT_RECORDER] Update Failure: {e}")

    def mark_filled(self, cl_ord_id: str, execution_price: float):
        """
        Record actual fill price for Network Decay audit (López de Prado).
        Drift = |execution_price - pre_flight_msq_price| reveals systematic alpha erosion.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE intents SET status = 'FILLED', execution_price = ? "
                    "WHERE cl_ord_id = ?",
                    (execution_price, cl_ord_id),
                )
        except Exception as e:
            logger.error(f"💀 [FLIGHT_RECORDER] Fill Record Failure: {e}")

    def mark_terminal(self, cl_ord_id: str):
        """Remove or mark as finished when WS confirms the order is done."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # We delete to keep the DB small and the 'Purgatory' scan fast
                conn.execute("DELETE FROM intents WHERE cl_ord_id = ?", (cl_ord_id,))
        except Exception as e:
            logger.error(f"💀 [FLIGHT_RECORDER] Cleanup Failure: {e}")

    def get_unresolved_intents(self) -> List[Dict[str, Any]]:
        """Used during Boot-Time Purgatory and Reconnect Reconciliation."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT * FROM intents WHERE status IN ('PENDING', 'DISPATCHED')"
                ).fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"💀 [FLIGHT_RECORDER] Read Failure: {e}")
            return []

    def graceful_shutdown(self):
        """Flush WAL on clean shutdown."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception as e:
            logger.error(f"💀 [FLIGHT_RECORDER] Shutdown Failure: {e}")
