import asyncio
import json
import os
import time
from datetime import datetime
from typing import Any

from loguru import logger
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.infrastructure.config import settings


class ReliableIngestionBuffer:
    """
    Nerve Center Reliable Queue (GEKTOR v2.0 CLEAN)
    Implements At-Least-Once delivery via modern Redis BLMOVE.
    """

    def __init__(self, db_manager: "DatabaseManager"):
        self.db = db_manager
        self.redis = Redis(
            unix_socket_path=os.getenv("REDIS_SOCKET_PATH", None),
            host=settings.REDIS_HOST if not os.getenv("REDIS_SOCKET_PATH") else None,
            port=settings.REDIS_PORT if not os.getenv("REDIS_SOCKET_PATH") else None,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=20,
            socket_connect_timeout=10,
            health_check_interval=10,
            retry_on_timeout=True,
            protocol=2,
        )
        self.queue_key = "gektor:ingest:queue"
        self.processing_key = "gektor:ingest:processing"
        self.dlq_key = "gektor:ingest:dlq"
        self.spillover_file = "artifacts/spillover.jsonl"
        self._spill_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=100000)
        self._spill_batch_size = 500
        self._spill_daemon_task: asyncio.Task[None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._sweeper_task: asyncio.Task[None] | None = None
        self._spillover_drain_task: asyncio.Task[None] | None = None
        self._running = False
        self._redis_available = False

    async def start(self) -> None:
        try:
            info = await self.redis.info("server")
            version = info.get("redis_version", "0.0.0")
            logger.info(f"✅ [DB] Redis Version Check: {version}")
            self._redis_available = True
        except Exception as e:
            logger.error(f"⚠️ [DB] Redis Offline at start. Using Local Spillover path. Error: {e}")
            self._redis_available = False

        if not os.path.exists("artifacts"):
            os.makedirs("artifacts")

        if not self._running:
            self._running = True
            self._spill_daemon_task = asyncio.create_task(self._spill_to_disk_daemon())

            if self._redis_available:
                await self._recover_stranded_tasks()
                self._worker_task = asyncio.create_task(self._process_queue())
                self._sweeper_task = asyncio.create_task(self._active_sweeper())

            self._spillover_drain_task = asyncio.create_task(self._spillover_sentinel())
            logger.info(f"🚀 [DB] Reliable Buffer active (Spillover path: {self.spillover_file}).")

    async def _recover_stranded_tasks(self) -> None:
        try:
            items = await self.redis.lrange(self.processing_key, 0, -1)
            if not items:
                return

            logger.warning(f"♻️ [DB] Found {len(items)} stranded tasks. Reclaiming to main queue...")
            pipe = self.redis.pipeline()
            for item in items:
                pipe.lrem(self.processing_key, 1, item)
                pipe.lpush(self.queue_key, item)
            await pipe.execute()
        except Exception as e:
            logger.error(f"❌ [DB] Stranded task recovery failed: {e}")

    async def _check_redis_memory(self) -> bool:
        try:
            info = await self.redis.info("memory")
            used = info.get("used_memory", 0)
            total = info.get("maxmemory", 0)
            if total > 0 and (used / total) > 0.8:
                logger.warning(f"🚨 [DB] Redis Memory Pressure (>80%): {used}/{total}. Activating Spillover.")
                return False
            return True
        except Exception:
            return False

    async def push_query(self, query: str, params: Any = None) -> None:
        payload = json.dumps(
            {
                "query": query,
                "params": params,
                "ts": time.time(),
            },
            default=str,
        )

        is_healthy = self._redis_available and await self._check_redis_memory()

        if is_healthy:
            try:
                await self.redis.lpush(self.queue_key, payload)
                return
            except Exception as e:
                logger.error(f"🚨 [DB] Redis Push Failed: {e}. Diverting to Queue.")

        try:
            self._spill_queue.put_nowait(payload)
        except asyncio.QueueFull:
            logger.critical("🔥 [CATASTROPHE] Spillover Queue overflow. Data loss in progress.")

    async def _spill_to_disk_daemon(self) -> None:
        loop = asyncio.get_running_loop()
        while self._running:
            batch: list[str] = []
            try:
                item = await self._spill_queue.get()
                batch.append(item)

                for _ in range(self._spill_batch_size - 1):
                    if self._spill_queue.empty():
                        break
                    batch.append(self._spill_queue.get_nowait())

                await loop.run_in_executor(None, self._write_batch_sync, batch)

                for _ in batch:
                    self._spill_queue.task_done()

            except Exception as e:
                logger.error(f"❌ [DB] Disk Write Failed: {e}")
                await asyncio.sleep(1)

    def _write_batch_sync(self, batch: list[str]) -> None:
        try:
            with open(self.spillover_file, "a", encoding="utf-8") as f:
                f.writelines([line + "\n" for line in batch])
        except Exception as e:
            logger.critical(f"☠️ [DB] PHYSWAL COLLAPSE (Disk Error): {e}")

    async def _spillover_sentinel(self) -> None:
        while self._running:
            await asyncio.sleep(60)
            if not os.path.exists(self.spillover_file) or os.path.getsize(self.spillover_file) == 0:
                continue

            if not self._redis_available:
                try:
                    await self.redis.ping()
                    self._redis_available = True
                    logger.success("✅ [DB] Redis re-established. Draining spillover...")
                    if not self._worker_task:
                        self._worker_task = asyncio.create_task(self._process_queue())
                except Exception:
                    continue

            if not await self._check_redis_memory():
                continue

            temp_file = self.spillover_file + ".recovering"
            try:
                os.rename(self.spillover_file, temp_file)
                with open(temp_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            json.loads(line)
                        except json.JSONDecodeError:
                            logger.error("💀 [DB] Corrupted Spillover line detected (OOM Torn Write). Skipping.")
                            continue

                        if self._redis_available and await self._check_redis_memory():
                            await self.redis.lpush(self.queue_key, line)
                        else:
                            await self._process_direct(line)

                os.remove(temp_file)
                logger.success("📦 [DB] Successfully drained L3 spillover.")
            except Exception as e:
                logger.error(f"❌ [DB] Rehydration failed: {e}")
                if os.path.exists(temp_file):
                    os.rename(temp_file, self.spillover_file)

    async def _process_queue(self) -> None:
        while self._running:
            try:
                payload_str = await self.redis.lmove(self.queue_key, self.processing_key, "LEFT", "RIGHT")
                if not payload_str:
                    await asyncio.sleep(0.05)
                    continue

                data = json.loads(payload_str)
                params = data["params"]
                if isinstance(params, dict):
                    for key, value in params.items():
                        if isinstance(value, str) and len(value) >= 19:
                            try:
                                params[key] = datetime.fromisoformat(value.replace(" ", "T"))
                            except (ValueError, TypeError) as exc:
                                logger.debug(f"[DB] Skipping datetime parse for {key}={value!r}: {exc!r}")

                async with self.db.SessionLocal() as session:
                    try:
                        await session.execute(text(data["query"]), params)
                        await session.commit()
                        await self.redis.lrem(self.processing_key, 1, payload_str)
                    except Exception as e:
                        await session.rollback()
                        logger.error(f"❌ [DB] Write Failed: {e}. Moving to DLQ.")
                        pipe = self.redis.pipeline()
                        pipe.lrem(self.processing_key, 1, payload_str)
                        pipe.lpush(self.dlq_key, payload_str)
                        await pipe.execute()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"⚠️ [DB] Worker Error: {e}")
                self._redis_available = False
                await asyncio.sleep(5)

    async def _active_sweeper(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(300)
                if not self._redis_available:
                    continue

                try:
                    processing_items = await asyncio.wait_for(
                        self.redis.lrange(self.processing_key, 0, -1),
                        timeout=5.0,
                    )
                    if not processing_items:
                        continue

                    now = time.time()
                    for payload_str in processing_items:
                        try:
                            data = json.loads(payload_str)
                            if now - data.get("ts", now) > 60:
                                pipe = self.redis.pipeline()
                                pipe.lrem(self.processing_key, 1, payload_str)
                                data["ts"] = now
                                pipe.lpush(self.queue_key, json.dumps(data))
                                await pipe.execute()
                                logger.warning("♻️ [DB] Reclaimed stalled task.")
                        except json.JSONDecodeError:
                            logger.error("💀 [DB] Corrupted payload in Redis processing list. Removing.")
                            await self.redis.lrem(self.processing_key, 1, payload_str)
                except Exception as sub_e:
                    logger.debug(f"⚠️ [DB] Sweeper Jitter: {sub_e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"⚠️ [DB] Sweeper Task Error: {e}")

    async def _process_direct(self, payload_str: str) -> None:
        try:
            try:
                data = json.loads(payload_str)
            except json.JSONDecodeError:
                logger.error("💀 [DB] Corrupted payload in _process_direct (OOM Torn Write). Purging.")
                return

            params = data["params"]
            if isinstance(params, dict):
                for key, value in params.items():
                    if isinstance(value, str) and len(value) >= 19:
                        try:
                            params[key] = datetime.fromisoformat(value.replace(" ", "T"))
                        except (ValueError, TypeError) as exc:
                            logger.debug(f"[DB] Skipping datetime parse for {key}={value!r}: {exc!r}")

            async with self.db.SessionLocal() as session:
                await session.execute(text(data["query"]), params)
                await session.commit()
        except Exception as e:
            logger.error(f"❌ [DB] Direct execution failed: {e}")
            self._spill_queue.put_nowait(payload_str)

    async def stop(self) -> None:
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
        if self._sweeper_task:
            self._sweeper_task.cancel()
        if self._spillover_drain_task:
            self._spillover_drain_task.cancel()
        if self._spill_daemon_task:
            self._spill_daemon_task.cancel()
        await self.redis.close()


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = 0.0
        self.state = "CLOSED"

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.critical("🚨 [CIRCUIT BREAKER] Database connection is OPEN (Failing Fast).")

    def record_success(self) -> None:
        self.failure_count = 0
        self.state = "CLOSED"

    @property
    def is_available(self) -> bool:
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        return True


class DatabaseManager:
    """SQLAlchemy 2.0 + asyncpg Database Manager with Circuit Breaker."""

    def __init__(self):
        import sys

        # v3.6.1 deploy fix: dispatch by URL scheme, not by host OS. The
        # asyncpg-specific kwargs (pool_size, max_overflow, command_timeout)
        # cause `TypeError: Invalid argument(s) ... sent to create_engine`
        # when the URL points at SQLite/aiosqlite (which uses NullPool and
        # does not accept those args). Previously Linux hosts on SQLite
        # would crash at startup.
        url = (
            "sqlite+aiosqlite:///gektor.db"
            if sys.platform == "win32"
            else settings.ASYNC_DATABASE_URL
        )

        if url.startswith("sqlite"):
            # NullPool is used automatically for sqlite+aiosqlite; do NOT
            # pass pool_size/max_overflow/pool_recycle/command_timeout.
            self.engine = create_async_engine(
                url,
                pool_pre_ping=True,
            )
        else:
            # PostgreSQL via asyncpg.
            self.engine = create_async_engine(
                url,
                pool_size=50,
                max_overflow=20,
                pool_recycle=300,
                pool_pre_ping=True,
                connect_args={"command_timeout": 5},
            )
            
        self.SessionLocal = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
        self.buffer = ReliableIngestionBuffer(self)
        self.cb = CircuitBreaker()

    async def initialize(self) -> None:
        await self.buffer.start()

        import sys
        if sys.platform == "win32":
            ddl_commands = [
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT UNIQUE,
                    symbol TEXT,
                    state TEXT,
                    entry_bid DOUBLE PRECISION,
                    entry_ask DOUBLE PRECISION,
                    entry_vwap DOUBLE PRECISION,
                    exit_bid DOUBLE PRECISION,
                    exit_ask DOUBLE PRECISION,
                    exit_vwap DOUBLE PRECISION,
                    human_entry_bid DOUBLE PRECISION,
                    human_entry_ask DOUBLE PRECISION,
                    human_entry_vwap DOUBLE PRECISION,
                    exit_vpin DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT,
                    status TEXT DEFAULT 'PENDING',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    execute_after TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 2
                );
                """,
            ]
        else:
            ddl_commands = [
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id SERIAL PRIMARY KEY,
                    signal_id TEXT UNIQUE,
                    symbol TEXT,
                    state TEXT,
                    entry_bid DOUBLE PRECISION,
                    entry_ask DOUBLE PRECISION,
                    entry_vwap DOUBLE PRECISION,
                    exit_bid DOUBLE PRECISION,
                    exit_ask DOUBLE PRECISION,
                    exit_vwap DOUBLE PRECISION,
                    human_entry_bid DOUBLE PRECISION,
                    human_entry_ask DOUBLE PRECISION,
                    human_entry_vwap DOUBLE PRECISION,
                    exit_vpin DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id SERIAL PRIMARY KEY,
                    payload TEXT,
                    status TEXT DEFAULT 'PENDING',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    execute_after TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 2
                );
                """,
            ]

        try:
            async with self.engine.begin() as conn:
                for cmd in ddl_commands:
                    await conn.execute(text(cmd.strip()))
            logger.success("✅ [DB] Infrastructure stabilized.")
        except Exception as e:
            logger.error(f"🚨 [DB] Schema initialization failed: {e}")
            raise

    async def push_query_to_wal(self, query: str, params: dict[str, Any] | None = None) -> None:
        await self.buffer.push_query(query, params)

    async def push_query(self, query: str, params: dict[str, Any] | None = None) -> None:
        await self.push_query_to_wal(query, params)

    async def execute_with_circuit_breaker(self, query: str, params: dict[str, Any] | None = None) -> None:
        if not self.cb.is_available:
            raise RuntimeError("Database Unavailable (Circuit Breaker OPEN)")
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text(query), params or {})
                self.cb.record_success()
        except Exception:
            self.cb.record_failure()
            raise

    async def close(self) -> None:
        await self.buffer.stop()
        await self.engine.dispose()
