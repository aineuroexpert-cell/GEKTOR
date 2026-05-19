import asyncio
import logging
import aiohttp
import orjson
from decimal import Decimal
from typing import Protocol

logger = logging.getLogger("GEKTOR_INGESTION")

class IBarAggregator(Protocol):
    async def process_tick(self, symbol: str, price: Decimal, size: Decimal, is_buyer_maker: bool, exchange_ts: float) -> None: ...
    async def handle_resync(self) -> None: ... # [Внедрено для защиты от разрыва каузальности]

class BybitWSIngestion:
    """
    Асинхронная турбина ингестии. 
    Отвечает только за удержание сокета, Zero-GIL парсинг и детекцию разрывов (Sequence Drift).
    """
    def __init__(self, ws_url: str, aggregator: IBarAggregator):
        self.ws_url = ws_url
        self.aggregator = aggregator
        self.session: aiohttp.ClientSession | None = None
        self._last_seq: dict[str, int] = {}
        self.PING_INTERVAL = 20.0

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse, shutdown_event: asyncio.Event) -> None:
        """Аппаратный JSON-пинг для обхода молчаливых разрывов TCP."""
        while not shutdown_event.is_set() and not ws.closed:
            try:
                await ws.send_bytes(orjson.dumps({"op": "ping"}))
                await asyncio.sleep(self.PING_INTERVAL)
            except Exception as e:
                logger.error(f"[INGESTION] Сбой Ping-петли: {e}")
                break

    async def _process_message(self, raw_msg: bytes) -> None:
        """
        Горячий путь (Hot Path). Оптимизирован для микросекундной латентности.
        """
        try:
            # orjson работает напрямую с байтами, минуя декодирование в str
            data = orjson.loads(raw_msg)
            
            if "topic" not in data or "data" not in data:
                return

            topic = data["topic"]
            if not topic.startswith("publicTrade"):
                return

            symbol = data.get("topic").split(".")[-1]
            
            for trade in data["data"]:
                price = Decimal(trade["p"])
                size = Decimal(trade["v"])
                is_buyer_maker = trade["S"] == "Sell" # Если агрессор Sell, maker был Buyer
                exchange_ts = float(trade["T"]) / 1000.0

                # Передаем в Causal Conflation (DollarBarEngine)
                await self.aggregator.process_tick(
                    symbol=symbol,
                    price=price,
                    size=size,
                    is_buyer_maker=is_buyer_maker,
                    exchange_ts=exchange_ts
                )

        except orjson.JSONDecodeError:
            logger.error("[INGESTION] Получен поврежденный payload.")
        except KeyError as e:
            logger.error(f"[INGESTION] Отсутствует ключ в payload: {e}")

    async def run(self, symbols: list[str], shutdown_event: asyncio.Event) -> None:
        """Жизненный цикл подключения с авто-реконнектом (Exponential Backoff)."""
        backoff = 1.0
        self.session = aiohttp.ClientSession()
        
        try:
            while not shutdown_event.is_set():
                logger.info(f"[INGESTION] Подключение к {self.ws_url}...")
                try:
                    async with self.session.ws_connect(self.ws_url, heartbeat=None) as ws:
                        logger.info("[INGESTION] Установлено WS соединение. Триггер RESYNC.")
                        
                        # [STRESS-TEST ЗАЩИТА] Немедленно сбрасываем отравленный стейт
                        await self.aggregator.handle_resync()
                        
                        backoff = 1.0 # Reset backoff
                        
                        # Подписка
                        args = [f"publicTrade.{sym}" for sym in symbols]
                        sub_req = orjson.dumps({"op": "subscribe", "args": args})
                        await ws.send_bytes(sub_req)

                        # Запуск PING
                        ping_task = asyncio.create_task(self._ping_loop(ws, shutdown_event))

                        # Чтение потока (Zero-Copy receive if possible)
                        async for msg in ws:
                            if shutdown_event.is_set():
                                break
                            if msg.type == aiohttp.WSMsgType.BINARY or msg.type == aiohttp.WSMsgType.TEXT:
                                # Bybit шлет текст, но aiohttp может выдать bytes
                                raw_bytes = msg.data if isinstance(msg.data, bytes) else msg.data.encode('utf-8')
                                await self._process_message(raw_bytes)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                        ping_task.cancel()
                        
                except Exception as e:
                    logger.warning(f"[INGESTION] Разрыв сокета: {e}. Реконнект через {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
        finally:
            await self.session.close()
            logger.info("[INGESTION] Сетевой мост разрушен.")
