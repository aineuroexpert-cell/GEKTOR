# src/infrastructure/telegram_notifier.py
from sqlalchemy import text
import asyncio
import aiohttp
import json
import hashlib
import os
import time
from collections import deque
from datetime import datetime, timezone
from aiohttp_socks import ProxyConnector
from loguru import logger
from typing import Any, Optional
from src.application.formatters import TelegramMessageFormatter
from src.infrastructure.config import settings
from src.infrastructure.database import DatabaseManager
from src.domain.entities.events import ExecutionEvent, ConflatedEvent

class AlertDebouncer:
    """[GEKTOR v5.24] O(1) Spam Suppression for Infrastructure Alerts."""
    __slots__ = ('_cooldown_seconds', '_last_alert_time')

    def __init__(self, cooldown_seconds: int = 300):
        self._cooldown_seconds = cooldown_seconds
        self._last_alert_time: dict[str, float] = {}

    def should_notify(self, alert_signature: str) -> bool:
        """Determines if enough time has passed to re-send a similar alert."""
        now = time.time()
        last_time = self._last_alert_time.get(alert_signature, 0.0)
        
        if (now - last_time) < self._cooldown_seconds:
            return False
            
        self._last_alert_time[alert_signature] = now
        return True

class TelegramRadarNotifier:
    """[GEKTOR v2.0] Institutional Telegram Client with PSR Protocol & Self-Healing Bridge."""
    def __init__(self, db_manager: DatabaseManager, bot_token: str, chat_id: str, event_bus: Any = None, proxy_url: Optional[str] = None):
        self.db = db_manager
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.proxy_url = proxy_url or settings.TELEGRAM_PROXY
        self.bus = event_bus
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        
        logger.info(f"🔑 [Telegram] Token loaded: {bool(self.bot_token)}, Chat ID loaded: {bool(self.chat_id)}")
        
        # [PSR 4.0] TRANSACTIONAL OUTBOX (Persistence > Speed)
        self._wakeup_event = asyncio.Event()
        self._live_allowed = asyncio.Event()
        self._live_allowed.set()
        self._is_throttled = False
        
        # [NECROMANCER] Self-Healing Bridge State
        self.is_bridge_alive = False
        
        # Idempotency: 1000 event IDs
        self._sent_cache = deque(maxlen=1000)
        self.fallback_file = "failed_alerts.jsonl"
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        # [GEKTOR v5.24] ZERO-BLOCKING EGRESS
        self._queue = asyncio.Queue(maxsize=100)
        self.debouncer = AlertDebouncer(cooldown_seconds=300)
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self.formatter = TelegramMessageFormatter()
        self._last_retry_after_sec = 5

    def _generate_event_id(self, event_data: dict) -> str:
        raw = f"{event_data.get('symbol')}_{event_data.get('timestamp')}_{event_data.get('price')}"
        return hashlib.sha256(raw.encode()).hexdigest()

    async def start(self):
        self._running = True
        self._worker_task = asyncio.create_task(self._process_worker())
        logger.info("📡 [Telegram] Secure tunnel initialized. PSR Protocol ARMED.")

    async def _process_worker(self):
        """[GEKTOR v5.25] Nuclear-Isolated Egress. No network exception escapes."""
        import aiohttp
        from aiohttp_socks import ProxyConnector

        while self._running:
            # --- SESSION CREATION (with proxy fallback) ---
            client_timeout = aiohttp.ClientTimeout(total=3.0, connect=2.0)
            try:
                if self.proxy_url:
                    logger.info(f"🌐 [Telegram] Egress routed via Proxy: {self.proxy_url[:32]}...")
                async with aiohttp.ClientSession(timeout=client_timeout) as session:
                    self._session = session
                    while self._running:
                        alert_type = "UNKNOWN"
                        try:
                            text, alert_type = await self._queue.get()

                            if not self.debouncer.should_notify(alert_type):
                                self._queue.task_done()
                                continue

                            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
                            try:
                                async with asyncio.timeout(3.0):
                                    async with session.post(self.api_url, json=payload, proxy=self.proxy_url) as resp:
                                        if resp.status != 200:
                                            err_body = await resp.text()
                                            logger.error(f"⚠️ [TG_API] Dispatch Error {resp.status}: {err_body}")
                                        else:
                                            logger.success(f"📱 [Telegram] Alert delivered: {alert_type}")
                            except (TimeoutError, asyncio.TimeoutError):
                                logger.warning(f"⏰ [TG_WORKER] Timeout (3s). Dropping: {alert_type}")
                            except (ConnectionRefusedError, ConnectionResetError, OSError) as e:
                                logger.warning(f"🔌 [TG_WORKER] Network refused: {e}. Dropping: {alert_type}")
                            except aiohttp.ClientProxyConnectionError as e:
                                self._last_retry_after_sec = 5
                                logger.warning(f"🌐 [TG_WORKER] Proxy unavailable: {e}. Requeue in 5s: {alert_type}")
                                try:
                                    self._queue.put_nowait((text, alert_type))
                                except asyncio.QueueFull:
                                    logger.error("🛑 [TG_DRAIN] Queue overflow while requeueing proxy-failed packet.")
                                await asyncio.sleep(5)
                            except Exception as e:
                                # Catch ProxyConnectionError and any aiohttp transport errors
                                logger.error(f"🚨 [Telegram] Alert failed! Exception: {repr(e)}")

                            try:
                                self._queue.task_done()
                            except ValueError as ve:
                                logger.error(f"🚨 [TG_WORKER] Value error on task done: {ve}")
                            await asyncio.sleep(0.3)

                        except asyncio.CancelledError:
                            return
                        except Exception as e:
                            logger.error(f"💥 [TG_WORKER] Inner loop error: {type(e).__name__}: {e}")
                            await asyncio.sleep(2.0)

            except asyncio.CancelledError:
                return
            except Exception as e:
                # Session creation itself failed (dead proxy, DNS, etc.)
                logger.error(f"🛑 [TG_WORKER] Session creation failed: {type(e).__name__}: {e}. Retrying in 10s...")
                await asyncio.sleep(10.0)

    async def notify_manual(self, text: str, alert_type: str = "DEFAULT"):
        """[PSR 4.1] Instant System Alert. Returns coroutine for create_task compatibility."""
        try:
            # We still use put_nowait to avoid blocking the caller, but the method is now async
            self._queue.put_nowait((text, alert_type))
        except asyncio.QueueFull:
            logger.error("🛑 [TG_DRAIN] Alert Queue Overload! Dropping packet.")
        except Exception as e:
            logger.error(f"TG Error: {e}")

    @property
    def last_retry_after_sec(self) -> int:
        return self._last_retry_after_sec

    async def notify(self, payload: dict[str, Any]) -> bool:
        """Protocol-compatible notifier entrypoint for Outbox relay."""
        message = self.formatter.format(payload)
        return await self._send_text_with_retry(message)

    async def _send_text_with_retry(self, message: str) -> bool:
        """Sends one HTML message with retry semantics and 429 backoff."""
        body = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}
        timeout = aiohttp.ClientTimeout(total=5.0, connect=3.0)

        for attempt in range(3):
            try:
                if self._session and not self._session.closed:
                    async with self._session.post(self.api_url, json=body, proxy=self.proxy_url) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 5))
                            self._last_retry_after_sec = retry_after
                            logger.warning(f"🚨 [TG_API] 429 Too Many Requests. retry_after={retry_after}s")
                            await asyncio.sleep(retry_after)
                            return False
                        if resp.status != 200:
                            logger.error(f"⚠️ [TG_API] Dispatch Error {resp.status}: {await resp.text()}")
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return True

                async with aiohttp.ClientSession(timeout=timeout) as temp_session:
                    async with temp_session.post(self.api_url, json=body, proxy=self.proxy_url) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 5))
                            self._last_retry_after_sec = retry_after
                            logger.warning(f"🚨 [TG_API] 429 Too Many Requests. retry_after={retry_after}s")
                            await asyncio.sleep(retry_after)
                            return False
                        if resp.status != 200:
                            logger.error(f"⚠️ [TG_API] Dispatch Error {resp.status}: {await resp.text()}")
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return True
            except aiohttp.ClientProxyConnectionError as e:
                self._last_retry_after_sec = 5
                logger.warning(f"🌐 [TG_API] Proxy connection failed: {e}. retry_after=5s")
                await asyncio.sleep(self._last_retry_after_sec)
                return False
            except Exception as e:
                logger.error(f"🚨 [Telegram] Alert failed! Exception: {repr(e)}")
                await asyncio.sleep(2 ** attempt)
        return False

    async def _run_recovery_phase(self):
        if not os.path.exists(self.fallback_file) or os.path.getsize(self.fallback_file) == 0:
            return

        logger.warning("🔄 [Recovery] Запуск дренажа отложенных сигналов (JSONL)...")
        self._live_allowed.clear()
        
        remaining_lines = []
        try:
            with open(self.fallback_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                
            for line in lines:
                if not line.strip(): continue
                event_data = json.loads(line.strip())
                event_id = self._generate_event_id(event_data)
                
                if event_id in self._sent_cache: continue
                    
                message = self._format_message(event_data, is_delayed=True)
                success = await self._send_with_retry(message, event_data, is_recovery=True)
                
                if success:
                    self._sent_cache.append(event_id)
                    await asyncio.sleep(1.5) # Anti-Spam
                else:
                    remaining_lines.append(line)
        except Exception as e:
            logger.error(f"🚨 [Recovery] Ошибка парсинга дампа: {e}")
        finally:
            with open(self.fallback_file, "w", encoding="utf-8") as f:
                f.writelines(remaining_lines)
            
            if not remaining_lines:
                logger.success("🟢 [Recovery] Дренаж завершен. Файл очищен. Разморозка живой очереди.")
                self._live_allowed.set()

    async def _send_with_retry(self, message: str, raw_data: dict, is_recovery: bool = False) -> bool:
        if not self._session: return False
        
        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}
        for attempt in range(3):
            try:
                async with self._session.post(self.api_url, json=payload) as resp:
                    if resp.status == 429:
                        self._is_throttled = True
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        logger.warning(f"🚨 [Telegram] Rate Limit! Backing off for {retry_after}s.")
                        await asyncio.sleep(retry_after)
                    if resp.status != 200:
                        logger.error(f"🚨 [Telegram] API Error {resp.status}: {await resp.text()}")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    self._is_throttled = False
                    logger.success("📱 [Telegram] Отчет доставлен.")
                    return True
            except Exception as e:
                logger.error(f"🚨 [Telegram] Alert failed! Exception: {repr(e)}")
                await asyncio.sleep(2 ** attempt)

        if not is_recovery:
            self._dump_to_fallback(raw_data)
        return False

    def _dump_to_fallback(self, event_data: dict):
        event_data["_dump_ts"] = datetime.now(timezone.utc).isoformat()
        with open(self.fallback_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event_data) + "\n")

    def notify_event(self, event_data: dict):
        """[PSR 4.0] Transactional Atomic Alerting."""
        payload = dict(event_data)
        payload.setdefault("event_type", "APPROVED")
        event_type = str(payload.get("event_type", "APPROVED")).upper()

        if event_type in {"APPROVED", "ABORT"} or payload.get("abort_mission"):
            priority = 1
        elif event_type in {"REJECTED", "HEARTBEAT"}:
            priority = 10
        else:
            priority = 2
        
        # [GEKTOR v5.24] SPAM SUPPRESSION
        alert_type = f"EVENT_{event_data.get('symbol', 'GLOBAL')}"
        if not self.debouncer.should_notify(alert_type):
            return
            
        # Write to Outbox (Atomic Disk Persistence)
        if self.bus:
            self.bus.publish_fire_and_forget(self.db.push_query(
                "INSERT INTO outbox_events (payload, priority, status) VALUES (:msg, :pri, 'PENDING')",
                {"msg": json.dumps(payload, ensure_ascii=False, default=str), "pri": priority}
            ))
        else:
            asyncio.create_task(self.db.push_query(
                "INSERT INTO outbox_events (payload, priority, status) VALUES (:msg, :pri, 'PENDING')",
                {"msg": json.dumps(payload, ensure_ascii=False, default=str), "pri": priority}
            ))
        self._wakeup_event.set()

    async def handle_execution_event(self, event: ExecutionEvent):
        """[EventBus Handler] Converts ExecutionEvent to Telegram notification."""
        data = event.to_dict()
        data.update(event.metadata)
        self.notify_event(data)

    async def handle_conflated_event(self, event: ConflatedEvent):
        """[EventBus Handler] Converts ConflatedEvent to Telegram notification."""
        data = event.to_dict()
        data["is_conflated"] = True
        self.notify_event(data)

    async def broadcast_offline_sync(self, reason: str) -> None:
        """
        Предсмертный крик. Вызывается из GlobalDeadMansSwitch.
        ОБЯЗАН быть await, никаких fire_and_forget.
        """
        sig_map = {
            "SIGINT": "Прерывание (Ctrl+C)",
            "SIGTERM": "Команда завершения (Terminate)",
            "SIGABRT": "Аварийный сбой (Abort)"
        }
        friendly_reason = sig_map.get(reason, reason)
        payload = f"🔌 [ОФФЛАЙН] Система завершает работу. Причина: {friendly_reason}"
        
        # Строгое ожидание записи в БД. Транзакция должна завершиться до остановки Loop'а.
        try:
            await self.db.push_query(
                "INSERT INTO outbox_events (payload, priority, status) VALUES (:msg, 1, 'PENDING')",
                {"msg": payload}
            )
            self._wakeup_event.set()
        except Exception as e:
            logger.error(f"☠️ [FATAL] Failed to save terminal alert to Outbox: {e}")
    async def _send_raw_text(self, text: str):
        # Wait up to 5 seconds for session if not initialized
        for _ in range(10):
            if self._session: break
            await asyncio.sleep(0.5)

        if not self._session:
            logger.error("❌ [Telegram] Cannot send manual alert: No active session.")
            return

        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self._session.post(self.api_url, json=payload, proxy=self.proxy_url) as resp:
                if resp.status != 200:
                    logger.error(f"🚨 [Telegram] API Error {resp.status}: {await resp.text()}")
                    return
                logger.success("🔔 [Telegram] Manual alert sent.")
        except aiohttp.ClientProxyConnectionError as e:
            logger.warning(f"🌐 [Telegram] Proxy connection failed (manual alert): {e}")
        except Exception as e:
            logger.error(f"🚨 [Telegram] Alert failed! Exception: {repr(e)}")

    def _format_message(self, event: dict, is_delayed: bool = False) -> str:
        """[GEKTOR v2.0] Профессиональный формат торговых алертов (Локализация: RU)."""
        symbol = event.get('symbol', 'UNKNOWN')
        price = event.get('price', 0.0)
        vpin = event.get('vpin')
        vpin_str = f"{vpin:.4f}" if vpin is not None else "Н/Д"
        ts_utc = datetime.fromtimestamp(event.get('timestamp', time.time()*1000)/1000, tz=timezone.utc).strftime('%H:%M:%S')
        
        # [GEKTOR v2.1] Conflation Data Injection
        conflation_tag = ""
        if event.get("is_conflated"):
            count = event.get("tick_count", 0)
            duration = event.get("duration_ms", 0)
            conflation_tag = f"\n📦 <b>Агрегация:</b> <code>{count} тиков / {duration:.1f}ms</code>"

        # [TYPE 1: ABORT MISSION] Экстренный выход / Невалидная посылка
        if event.get("abort_mission"):
            reason = event.get("abort_reason", "Нарушение структуры")
            # Map technical reasons to user-friendly Russian
            reason_map = {
                "VPIN_DECAY": "Затухание токсичности / Потеря интереса",
                "STOP_LOSS": "Срабатывание ценового стопа",
                "VOLATILITY_EXPANSION": "Взрывной рост волатильности (Риск)",
                "TIME_DECAY": "Истечение времени актуальности"
            }
            friendly_reason = reason_map.get(reason, reason)
            
            return (
                f"🛡️ <b>[ЗАЩИТА КАПИТАЛА: СБРОС]</b>\n"
                f"Пресечение риска по активу <b>{symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
                f"📉 <b>Тип:</b> Снятие торговой гипотезы\n"
                f"🧬 <b>Причина:</b> <i>{friendly_reason}</i>\n"
                f"📊 <b>VPIN:</b> <code>{vpin_str}</code>\n"
                f"💵 <b>Цена:</b> <code>${price:,.2f}</code>\n"
                f"⏰ <b>Время:</b> <code>{ts_utc} UTC</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🛑 <b>СТАТУС: ВЫХОД / OFF-MARKET</b>"
            )

        # [TYPE 2: MICROSTRUCTURE IMPULSE] (OFI)
        if event.get("type") == "MICRO_IMPULSE":
            ofi = event.get("ofi", 0.0)
            side = event.get("side", "NEUTRAL")
            emoji = "📈" if side == "ACCUMULATION" else "📉"
            title = "НАКОПЛЕНИЕ" if side == "ACCUMULATION" else "РАСПРЕДЕЛЕНИЕ"
            description = (
                "Обнаружено доминирование лимитных покупателей (Iceberg/Aggressive)." 
                if side == "ACCUMULATION" else 
                "Обнаружено доминирование лимитных продавцов (Distribution/Pressure)."
            )
            
            return (
                f"{emoji} <b>[МИКРОСТРУКТУРА: {title}]</b>\n"
                f"{description}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
                f"📊 <b>OFI Delta:</b> <code>{ofi:+.2f}</code>\n"
                f"💵 <b>Цена:</b> <code>${price:,.2f}</code>\n"
                f"⏰ <b>Время:</b> <code>{ts_utc} UTC</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📡 <b>СТАТУС: МОНИТОРИНГ ПОДТВЕРЖДЕНИЯ</b>"
            )

        # [TYPE 3: ENTRY SIGNAL] (VPIN Anomaly) - Основной сигнал Альфы
        header = "⏳ [РЕЗЕРВНЫЙ КАНАЛ]" if is_delayed else "⚡ <b>ОБНАРУЖЕНА АНОМАЛИЯ (ALPHA)</b>"
        
        return (
            f"{header}\n"
            f"Институциональное смещение вероятности\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
            f"🎯 <b>Сигнал:</b> ВЕРОЯТНЫЙ ИМПУЛЬС\n"
            f"📊 <b>VPIN (Информационный риск):</b> <code>{vpin_str}</code>\n"
            f"💵 <b>Цена:</b> <code>${price:,.2f}</code>{conflation_tag}\n"
            f"⏰ <b>Время:</b> <code>{ts_utc} UTC</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 <b>СТАТУС: ГОТОВНОСТЬ К ИСПОЛНЕНИЮ</b>"
        )

    async def stop(self):
        self._running = False
        if self._worker_task: self._worker_task.cancel()
        logger.info("🔌 [Telegram] Offline.")
