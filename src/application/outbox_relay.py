import asyncio
import logging
import json
from typing import List, Optional, Protocol, Any
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from sqlalchemy import text

logger = logging.getLogger("GEKTOR.Outbox")

@dataclass
class OutboxMessage:
    id: int
    payload: str
    priority: int
    status: str
    created_at: datetime
    execute_after: datetime
    retry_count: int

class RetryableError(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded. Retry after {retry_after}s.")


class INotifier(Protocol):
    async def notify(self, payload: dict[str, Any]) -> bool:
        ...

    @property
    def last_retry_after_sec(self) -> int:
        ...

class OutboxRepository:
    """[GEKTOR v2.0] Абстракция Transactional Outbox для атомарной записи."""
    def __init__(self, db_manager):
        self.db = db_manager

    async def save_alert_atomically(self, payload: str, db_transaction) -> None:
        """
        Запись алерта происходит строго ВНУТРИ транзакции базы данных, 
        где обновляется торговый стейт.
        """
        query = """
            INSERT INTO outbox_events (payload, status, created_at, execute_after, retry_count)
            VALUES (:payload, 'PENDING', :now, :now, 0)
        """
        now = datetime.now(timezone.utc)
        await db_transaction.execute(text(query), {
            "payload": payload,
            "now": now
        })

    async def fetch_pending(self, batch_size: int = 10) -> List[OutboxMessage]:
        """Claim a batch of pending events atomically.

        Two-step claim that works on BOTH SQLite (no FOR UPDATE) and PostgreSQL:
            1. SELECT candidate ids.
            2. UPDATE those ids from 'PENDING' to 'PROCESSING' in a single
               WHERE id IN (...) AND status = 'PENDING' (status guard
               prevents a stale claim if another relay raced us).
            3. Return only the rows we successfully transitioned.

        On SQLite this is safe because the engine serialises writers (WAL
        single-writer). On PostgreSQL the per-row UPDATE … WHERE status=
        'PENDING' acts as an optimistic lock without long-held row locks.
        """
        async with self.db.SessionLocal() as session:
            now = datetime.now(timezone.utc)
            select_query = """
                SELECT id, payload, priority, status, created_at, execute_after, retry_count
                FROM outbox_events
                WHERE status = 'PENDING' AND execute_after <= :now
                ORDER BY priority ASC, created_at ASC
                LIMIT :limit
            """
            result = await session.execute(
                text(select_query), {"limit": batch_size, "now": now}
            )
            rows = result.fetchall()
            if not rows:
                return []

            ids = [r[0] for r in rows]
            # Bind each id explicitly so we get portable SQL (no IN :tuple).
            placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
            update_query = (
                f"UPDATE outbox_events SET status = 'PROCESSING' "
                f"WHERE id IN ({placeholders}) AND status = 'PENDING'"
            )
            params = {f"id_{i}": ids[i] for i in range(len(ids))}
            update_result = await session.execute(text(update_query), params)
            await session.commit()

            claimed_count = update_result.rowcount or 0
            messages: List[OutboxMessage] = []
            for r in rows[:claimed_count]:
                messages.append(
                    OutboxMessage(
                        id=r[0],
                        payload=r[1],
                        priority=r[2],
                        status="PROCESSING",
                        created_at=r[4],
                        execute_after=r[5],
                        retry_count=r[6],
                    )
                )
            return messages

    async def mark_delivered(self, message_id: int) -> None:
        """Решение проблемы MVCC Bloat: Физическое удаление (DELETE) после доставки."""
        query = "DELETE FROM outbox_events WHERE id = :id"
        async with self.db.SessionLocal() as session:
            await session.execute(text(query), {"id": message_id})
            await session.commit()

    async def mark_failed_for_retry(self, message_id: int, delay_sec: int = 10) -> None:
        query = """
            UPDATE outbox_events 
            SET status = 'PENDING', 
                retry_count = retry_count + 1,
                execute_after = :new_time
            WHERE id = :id
        """
        new_time = datetime.now(timezone.utc) + timedelta(seconds=delay_sec)
        async with self.db.SessionLocal() as session:
            await session.execute(text(query), {"id": message_id, "new_time": new_time})
            await session.commit()

class TelegramRelayWorker:
    """[GEKTOR v2.0] Изолированный демон Relay. Никакого сетевого блокирования Ядра."""
    def __init__(self, repo: OutboxRepository, tg_client: INotifier):
        self.repo = repo
        self.tg = tg_client
        self._running = False

    async def run(self):
        self._running = True
        logger.info("📡 [RELAY] Outbox Worker Started.")
        while self._running:
            # Transactions are executed fully internally in `fetch_pending` and locks released immediately upon SET PROCESSING
            messages = await self.repo.fetch_pending(batch_size=5)
            if not messages:
                await asyncio.sleep(0.5) # Fallback polling
                continue
            
            for msg in messages:
                try:
                    payload = self._decode_payload(msg.payload)
                    sent = await self.tg.notify(payload)
                    if sent:
                        await self.repo.mark_delivered(msg.id)
                    else:
                        delay_sec = max(1, int(getattr(self.tg, "last_retry_after_sec", 5)))
                        logger.warning(f"⏳ [RELAY] Notify deferred for msg {msg.id} by {delay_sec}s.")
                        await self.repo.mark_failed_for_retry(msg.id, delay_sec=delay_sec)
                except Exception as e:
                    logger.error(f"☠️ [RELAY] Fatal delivery error: {e}")
                    # Вернуть в PENDING для базового ретрая
                    await self.repo.mark_failed_for_retry(msg.id, delay_sec=30)

    @staticmethod
    def _decode_payload(raw_payload: str) -> dict[str, Any]:
        """
        Outbox stores either JSON payloads or preformatted strings.
        For legacy plain text, wrap into a raw text payload.
        """
        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
        return {"event_type": "RAW_TEXT", "symbol": "GLOBAL", "reason": "", "fact": raw_payload, "limit": ""}

    def stop(self):
        self._running = False
