# src/infrastructure/event_bus.py
import asyncio
import time
import json
import sqlite3
import uuid
import os
import queue
import threading
from typing import Set, Any, Callable, Dict, List, Awaitable, Optional, Union
from loguru import logger
from src.domain.entities.events import ExecutionEvent, ConflatedEvent, StateInvalidationEvent

class EventBus:
    """
    [GEKTOR APEX] Institutional Event Bus v3.0 (The Kleppmann Protocol).
    Architecture: Transactional Outbox + Producer-Consumer.
    
    Safety Protocol:
    1. Zero Data Loss: Critical events are written to the SQLite WAL buffer queue.
    2. Crash Recovery: Relay re-queues PENDING events on startup after an unexpected crash.
    3. Non-Blocking Dispatch: Dedicated Single-Writer Thread handles all SQLite I/O to prevent Event Loop blocking.
    """
    def __init__(self, max_queue_size: int = 5000):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._background_tasks: Set[asyncio.Task[Any]] = set()
        self._consumer_task: Optional[asyncio.Task] = None
        
        self._corrupted_symbols: Set[str] = set()
        self._subscribers: Dict[str, List[Callable[[Any], Awaitable[None]]]] = {}
        self._running = False
        
        # Dedicated Thread-safe SQLite single-writer queue
        self._write_queue: queue.Queue = queue.Queue()
        self._writer_thread: Optional[threading.Thread] = None
        self._writer_running = False
        
        # [KLEPPMANN OUTBOX] Локальный буфер на диске для гарантии ACID
        self._init_outbox()

    def _init_outbox(self):
        """Инициализация сверхбыстрого SQLite WAL для хранения событий до их отправки"""
        os.makedirs("artifacts", exist_ok=True)
        # Инициализируем схему синхронно при старте
        conn = sqlite3.connect("artifacts/gektor_outbox.db")
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox (
                    id TEXT PRIMARY KEY,
                    event_type TEXT,
                    payload TEXT,
                    status TEXT,
                    ts REAL
                )
            """)
        finally:
            conn.close()
            
        # Запуск выделенного фонового потока для записи в SQLite
        self._writer_running = True
        self._writer_thread = threading.Thread(
            target=self._sqlite_writer_loop, 
            name="Gektor-SQLite-Writer", 
            daemon=True
        )
        self._writer_thread.start()
        logger.info("🛡️ [EventBus] Transactional Outbox (SQLite WAL) ARMED on dedicated writer thread.")

    def _sqlite_writer_loop(self):
        """Dedicated SQLite Single-Writer Loop running on a background thread."""
        conn = sqlite3.connect("artifacts/gektor_outbox.db", isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        
        while self._writer_running or not self._write_queue.empty():
            try:
                item = self._write_queue.get(timeout=0.1)
            except queue.Empty:
                continue
                
            op_type, data = item
            try:
                if op_type == "INSERT":
                    event_id, event_type, payload_str, ts = data
                    conn.execute(
                        "INSERT INTO outbox (id, event_type, payload, status, ts) VALUES (?, ?, ?, 'PENDING', ?)",
                        (event_id, event_type, payload_str, ts)
                    )
                elif op_type == "UPDATE":
                    outbox_id = data
                    conn.execute("UPDATE outbox SET status='PROCESSED' WHERE id=?", (outbox_id,))
                elif op_type == "UPDATE_CORRUPTED":
                    row_id = data
                    conn.execute("UPDATE outbox SET status='CORRUPTED' WHERE id=?", (row_id,))
                elif op_type == "DELETE":
                    conn.execute("DELETE FROM outbox WHERE status='PROCESSED'")
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except Exception as e:
                logger.error(f"💥 [OUTBOX WRITER THREAD] SQLite write failed: {e}")
            finally:
                self._write_queue.task_done()
                
        conn.close()

    def _serialize_event(self, obj: Any) -> dict:
        """Безопасная сериализация объектов событий в JSON"""
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        if hasattr(obj, '__slots__'):
            return {s: getattr(obj, s) for s in obj.__slots__}
        return {"data": str(obj)}

    async def start(self):
        if not self._consumer_task:
            self._running = True
            # [CRASH RECOVERY] Поднимаем из могилы события, которые не успели уйти из-за сбоя
            await self._run_recovery_relay()
            
            self._consumer_task = asyncio.create_task(self._consume_loop())
            logger.info("🎛️ [EventBus] Tier-3 Decoupled Core v3.0 ACTIVE.")

    def _read_pending_events_sync(self) -> List[tuple]:
        conn = sqlite3.connect("artifacts/gektor_outbox.db")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, event_type, payload FROM outbox WHERE status = 'PENDING'")
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ [OUTBOX] Failed to read pending events: {e}")
            return []
        finally:
            conn.close()

    async def _run_recovery_relay(self):
        """Реанимирует статусы PENDING после краша системы"""
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(None, self._read_pending_events_sync)
        
        if rows:
            logger.warning(f"🧟 [OUTBOX RELAY] Обнаружено {len(rows)} брошенных событий после сбоя. Запуск реанимации...")
            for row_id, evt_type, payload_str in rows:
                try:
                    payload = json.loads(payload_str)
                    # Восстанавливаем объект события на основе его типа
                    if evt_type == "ExecutionEvent":
                        evt = ExecutionEvent(**payload)
                    elif evt_type == "ConflatedEvent":
                        evt = ConflatedEvent(**payload)
                    else:
                        continue # Пропускаем неизвестные типы
                        
                    setattr(evt, "_outbox_id", row_id)
                    self._queue.put_nowait(evt)
                except Exception as e:
                    logger.error(f"❌ [OUTBOX] Ошибка реанимации события {row_id}: {e}")
                    self._write_queue.put(("UPDATE_CORRUPTED", row_id))

    def subscribe(self, event_name: str, callback: Callable[[Any], Awaitable[None]]):
        if event_name not in self._subscribers:
            self._subscribers[event_name] = []
        self._subscribers[event_name].append(callback)

    def publish_nowait(self, event: Any):
        """
        [ATOMIC PUBLISH] 
        Событие ставится в очередь фонового потока записи (занимает <1 микросекунды),
        затем кладется в In-Memory очередь для асинхронной рассылки.
        """
        if asyncio.iscoroutine(event):
            task = asyncio.create_task(event)
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
            return

        symbol = getattr(event, 'symbol', 'UNKNOWN')
        if symbol in self._corrupted_symbols and type(event).__name__ != "StateInvalidationEvent":
            return

        event_type = type(event).__name__
        
        # [TRANSACTIONAL OUTBOX] Сохраняем только критические события (Сделки и Сигналы)
        if event_type in ("ExecutionEvent", "ConflatedEvent", "SignalEvent"):
            event_id = uuid.uuid4().hex
            payload_str = json.dumps(self._serialize_event(event), default=str)
            self._write_queue.put(("INSERT", (event_id, event_type, payload_str, time.time())))
            setattr(event, "_outbox_id", event_id) # Маркируем объект для последующего удаления

        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.critical("🚨 [LOAD SHEDDING] EventBus Queue Full! Очередь переполнена, события сбрасываются!")

    async def publish(self, event: Any):
        self.publish_nowait(event)

    def publish_fire_and_forget(self, event: Any):
        self.publish_nowait(event)

    def mark_recovered(self, symbol: str):
        if symbol in self._corrupted_symbols:
            self._corrupted_symbols.remove(symbol)
            logger.success(f"🔄 [EventBus] {symbol} recovered.")

    async def _consume_loop(self):
        """Background worker. Рассылает события подписчикам и отмечает их как PROCESSED в базе."""
        while self._running:
            try:
                event = await self._queue.get()
                
                # Создаем задачу на доставку
                task = asyncio.create_task(self._process_and_ack(event))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"⚠️ [EventBus] Consumer Loop Error: {e}")
                await asyncio.sleep(0.1)

    async def _process_and_ack(self, event: Any):
        """Атомарная рассылка всем подписчикам. После успеха — смена статуса в БД."""
        try:
            event_name = type(event).__name__
            if event_name in self._subscribers:
                handlers = [cb(event) for cb in self._subscribers[event_name]]
                if handlers:
                    # [GEKTOR v3.0.0] return_exceptions=True prevents cascading failure:
                    # If Telegram handler crashes, Outbox acknowledgment still proceeds.
                    results = await asyncio.gather(*handlers, return_exceptions=True)
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            logger.error(f"💥 [EventBus] Handler #{i} for {event_name} failed: {type(r).__name__}: {r}")
            
            # [OUTBOX ACKNOWLEDGEMENT] Событие доставлено, помечаем в базе
            outbox_id = getattr(event, "_outbox_id", None)
            if outbox_id:
                self._write_queue.put(("UPDATE", outbox_id))
                
        except Exception as e:
            logger.error(f"💥 [EventBus] Ошибка доставки события {type(event).__name__}: {e}")

    async def stop(self):
        self._running = False
        if self._consumer_task: 
            self._consumer_task.cancel()
        
        # Останавливаем фоновый поток записи
        self._writer_running = False
        if self._writer_thread:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._writer_thread.join)

        # Очищаем старые обработанные события при шатдауне, чтобы БД не пухла
        try:
            conn = sqlite3.connect("artifacts/gektor_outbox.db")
            conn.execute("DELETE FROM outbox WHERE status='PROCESSED'")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.close()
        except (sqlite3.Error, OSError) as exc:
            logger.debug(f"[EventBus] Outbox cleanup on shutdown failed (non-critical): {exc!r}")