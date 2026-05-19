import asyncio
import aiosqlite
import json
import logging
from typing import Dict, Any, List, Protocol

logger = logging.getLogger("GEKTOR_OUTBOX")

class ISignalRepository(Protocol):
    async def initialize(self) -> None: ...
    async def save_signal(self, idempotency_key: str, symbol: str, signal_type: str, payload: Dict[str, Any], created_at: float, ttl_seconds: int) -> None: ...
    async def get_pending_signals(self) -> List[Dict[str, Any]]: ...
    async def mark_as_sent(self, signal_id: int) -> None: ...
    async def drop_expired(self, current_time: float) -> int: ...
    async def close(self) -> None: ...

class SQLiteSignalRepository:
    def __init__(self, db_path: str = "gektor_outbox.db"):
        self.db_path = db_path

    async def initialize(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            # Настройка строгих PRAGMA для Zero-Blocking Pipeline (WAL)
            await db.execute("PRAGMA journal_mode = WAL;")
            await db.execute("PRAGMA synchronous = NORMAL;")
            await db.execute("PRAGMA temp_store = MEMORY;")
            
            await db.execute("""
            CREATE TABLE IF NOT EXISTS outbox_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT UNIQUE NOT NULL,
                symbol TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                payload JSON NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                status TEXT DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'SENT', 'EXPIRED_DROPPED', 'FAILED')),
                retry_count INTEGER DEFAULT 0
            );
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_signals(status) WHERE status = 'PENDING';")
            
            # K-V хранилище системного стейта
            await db.execute("""
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            """)
            await db.commit()
            logger.info(f"[OUTBOX] SQLite WAL хранилище инициализировано: {self.db_path}")

    async def upsert_system_state(self, key: str, value: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO system_state (key, value, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP
                """, 
                (key, value)
            )
            await db.commit()

    async def get_system_state(self, key: str) -> str | None:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM system_state WHERE key = ?", (key,))
            row = await cursor.fetchone()
            return row[0] if row else None

    async def save_signal(self, idempotency_key: str, symbol: str, signal_type: str, payload: Dict[str, Any], created_at: float, ttl_seconds: int) -> None:
        expires_at = created_at + ttl_seconds
        payload_str = json.dumps(payload)
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute("""
                    INSERT INTO outbox_signals (idempotency_key, symbol, signal_type, payload, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (idempotency_key, symbol, signal_type, payload_str, created_at, expires_at))
                await db.commit()
            except aiosqlite.IntegrityError:
                # Защита от дублей (Idempotency) - тихо дропаем
                logger.debug(f"[OUTBOX] Дубликат сигнала проигнорирован: {idempotency_key}")

    async def get_pending_signals(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM outbox_signals WHERE status = 'PENDING' ORDER BY created_at ASC")
            rows = await cursor.fetchall()
            
            results = []
            for row in rows:
                row_dict = dict(row)
                row_dict["payload"] = json.loads(row_dict["payload"])
                results.append(row_dict)
            return results

    async def mark_as_sent(self, signal_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE outbox_signals SET status = 'SENT' WHERE id = ?", (signal_id,))
            await db.commit()

    async def drop_expired(self, current_time: float) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                UPDATE outbox_signals 
                SET status = 'EXPIRED_DROPPED' 
                WHERE status = 'PENDING' AND expires_at < ?
            """, (current_time,))
            await db.commit()
            return cursor.rowcount

    async def close(self) -> None:
        """Соединения открываются и закрываются локально в методах (context manager)."""
        pass
