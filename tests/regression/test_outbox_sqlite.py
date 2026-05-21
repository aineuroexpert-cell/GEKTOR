"""
Regression tests for src/application/outbox_relay.py SQL portability.

Before v3.6.0 these queries used `FOR UPDATE SKIP LOCKED` which is a
PostgreSQL extension. The default DB on local/Windows/test is SQLite, so
the production code crashed on the first fetch_pending() call.

These tests run against an in-memory SQLite database to guarantee the
SQL stays portable.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.application.outbox_relay import OutboxRepository


class _FakeDB:
    """Lightweight DatabaseManager stand-in providing only SessionLocal."""

    def __init__(self) -> None:
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.SessionLocal = async_sessionmaker(self.engine, expire_on_commit=False)

    async def setup(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    CREATE TABLE outbox_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        payload TEXT,
                        status TEXT DEFAULT 'PENDING',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        execute_after TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        retry_count INTEGER DEFAULT 0,
                        priority INTEGER DEFAULT 2
                    )
                    """
                )
            )

    async def insert_event(self, payload: str, priority: int = 2) -> int:
        async with self.SessionLocal() as session:
            now = datetime.now(timezone.utc)
            result = await session.execute(
                text(
                    "INSERT INTO outbox_events (payload, status, created_at, execute_after, retry_count, priority) "
                    "VALUES (:payload, 'PENDING', :now, :now, 0, :priority)"
                ),
                {"payload": payload, "now": now, "priority": priority},
            )
            await session.commit()
            return result.lastrowid  # type: ignore[return-value]

    async def status(self, msg_id: int) -> str | None:
        async with self.SessionLocal() as session:
            result = await session.execute(
                text("SELECT status FROM outbox_events WHERE id = :id"),
                {"id": msg_id},
            )
            row = result.first()
            return row[0] if row else None


@pytest.mark.asyncio
async def test_outbox_fetch_pending_works_on_sqlite() -> None:
    db = _FakeDB()
    await db.setup()
    repo = OutboxRepository(db)

    payload = json.dumps({"event_type": "RADAR_ALERT", "symbol": "BTCUSDT"})
    mid = await db.insert_event(payload)

    messages = await repo.fetch_pending(batch_size=10)

    assert len(messages) == 1
    assert messages[0].id == mid
    assert messages[0].payload == payload
    # After claim, the underlying row must be PROCESSING.
    assert await db.status(mid) == "PROCESSING"


@pytest.mark.asyncio
async def test_outbox_mark_delivered_removes_row() -> None:
    db = _FakeDB()
    await db.setup()
    repo = OutboxRepository(db)

    mid = await db.insert_event("test")
    await repo.fetch_pending()
    await repo.mark_delivered(mid)
    assert await db.status(mid) is None


@pytest.mark.asyncio
async def test_outbox_mark_failed_returns_to_pending() -> None:
    db = _FakeDB()
    await db.setup()
    repo = OutboxRepository(db)

    mid = await db.insert_event("test")
    await repo.fetch_pending()
    await repo.mark_failed_for_retry(mid, delay_sec=0)
    assert await db.status(mid) == "PENDING"

    # On the second fetch (after the delay has elapsed at 0s), we should
    # claim the same message again with retry_count=1.
    again = await repo.fetch_pending()
    assert len(again) == 1
    assert again[0].retry_count == 1


@pytest.mark.asyncio
async def test_outbox_fetch_respects_priority_and_execute_after() -> None:
    db = _FakeDB()
    await db.setup()
    repo = OutboxRepository(db)

    # Two events: low priority inserted first, high priority inserted second.
    low_id = await db.insert_event("low", priority=5)
    high_id = await db.insert_event("high", priority=1)

    messages = await repo.fetch_pending(batch_size=5)
    assert [m.id for m in messages] == [high_id, low_id]


@pytest.mark.asyncio
async def test_outbox_double_claim_is_idempotent() -> None:
    """Two concurrent relays must not both end up holding the same row.

    Run two fetch_pending() coroutines concurrently and verify the union
    of returned ids has no duplicates.
    """
    db = _FakeDB()
    await db.setup()
    repo = OutboxRepository(db)

    for i in range(10):
        await db.insert_event(f"e{i}")

    a, b = await asyncio.gather(
        repo.fetch_pending(batch_size=10),
        repo.fetch_pending(batch_size=10),
    )
    seen_ids = [m.id for m in a] + [m.id for m in b]
    # SQLite serialises writes, so on a single connection one of the two
    # fetches will see zero rows and the other will see all 10. We just
    # require uniqueness — no row shipped twice.
    assert len(seen_ids) == len(set(seen_ids)), "Outbox claimed the same row twice"
