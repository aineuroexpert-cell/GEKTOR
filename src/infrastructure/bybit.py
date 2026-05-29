import asyncio
import aiohttp
import time
import random
from loguru import logger
from typing import List, Callable, Optional, Dict, Any, Union
import hmac
import hashlib
import orjson

from src.application.microstructure import MicrostructureAnalyzer, OrderBookSequenceGuard, SpoofingDiscriminator
from src.domain.exit_protocol import MarketTick
from aiohttp_socks import ProxyConnector
from .config import settings
from src.shared.resilience import GlobalResilienceManager

def safe_float(val: Any) -> float:
    """[GEKTOR v2.6] Robust float casting for inconsistent Exchange APIs."""
    if not val or str(val).strip() == "": return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

class BybitRestClient:
    """[GEKTOR v2.0] Lightweight REST client for Vanguard Hydration."""
    def __init__(self, proxy_url: Optional[str] = None, requests_per_second: float = 15.0,
                 api_key: Optional[str] = None, api_secret: Optional[str] = None):
        self.proxy_url = proxy_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.bybit.com"
        self.rate_limiter = asyncio.Semaphore(int(requests_per_second))
        # [PATCH 5] Singleton session — created lazily, reused across all calls.
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        [PATCH 5] Lazy singleton session factory.
        Creates one TCP connector + connection pool, reused for all REST calls.
        Eliminates per-call session/connector allocation (DNS, SSL handshake savings).
        """
        if self._session is None or self._session.closed:
            connector = ProxyConnector.from_url(self.proxy_url) if self.proxy_url else None
            self._session = aiohttp.ClientSession(
                connector=connector, trust_env=False,
                timeout=aiohttp.ClientTimeout(total=10.0, connect=5.0)
            )
        return self._session

    async def close(self):
        """[PATCH 5] Graceful session teardown. Must be awaited during shutdown."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _generate_signature(self, timestamp: str, payload: str) -> str:
        """Standard Bybit V5 HMAC-SHA256 signature."""
        recv_window = "5000"
        param_str = timestamp + (self.api_key or "") + recv_window + payload
        hash = hmac.new(bytes(self.api_secret or "", "utf-8"), param_str.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()

    async def get_server_time(self) -> int:
        """[HFT] High-speed server time fetch for PTP synchronization."""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/v5/market/time"
            async with session.get(url, timeout=3) as resp:
                raw = await resp.read()
                data = orjson.loads(raw)
                return int(data["time"])
        except Exception as e:
            logger.error(f"💥 [BybitREST] Server time fetch failed: {e}")
            raise

    async def get_wallet_balance_raw(self, accountType: str = "UNIFIED") -> dict:
        """[HFT] Raw balance fetch for UTA-only restriction."""
        timestamp = str(int(time.time() * 1000))
        params = f"accountType={accountType}"
        signature = self._generate_signature(timestamp, params)
        headers = {
            "X-BAPI-API-KEY": self.api_key or "",
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "5000"
        }
        session = await self._get_session()
        url = f"{self.base_url}/v5/account/wallet-balance?{params}"
        async with session.get(url, headers=headers, timeout=5) as resp:
            raw = await resp.read()
            return orjson.loads(raw)

    async def get_active_positions(self) -> List[Dict[str, Any]]:
        """[ENVIRONMENT AWARENESS] Fetches current open positions."""
        if not self.api_key or not self.api_secret:
            return []
        try:
            timestamp = str(int(time.time() * 1000))
            params = "category=linear&settleCoin=USDT"
            signature = self._generate_signature(timestamp, params)
            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": "5000"
            }
            session = await self._get_session()
            url = f"{self.base_url}/v5/position/list?{params}"
            async with session.get(url, headers=headers, timeout=10) as resp:
                raw = await resp.read()
                data = orjson.loads(raw)
                if data.get("retCode") == 0:
                    return data.get("result", {}).get("list", [])
                else:
                    logger.error(f"❌ [BybitREST] Auth Error: {data.get('retMsg')}")
                    return []
        except Exception as e:
            logger.error(f"⚠️ [BybitREST] Position fetch failed: {repr(e)}")
            return []

    async def amend_order(self, order_id: str, price: str, symbol: str) -> bool:
        """[HFT] Atomic Order repositioning."""
        try:
            timestamp = str(int(time.time() * 1000))
            payload = orjson.dumps({
                "category": "linear",
                "symbol": symbol,
                "orderId": order_id,
                "price": price
            }).decode('utf-8')
            signature = self._generate_signature(timestamp, payload)
            headers = {
                "X-BAPI-API-KEY": self.api_key or "",
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": "5000",
                "Content-Type": "application/json"
            }
            session = await self._get_session()
            url = f"{self.base_url}/v5/order/amend"
            async with session.post(url, headers=headers, data=payload, timeout=5) as resp:
                raw = await resp.read()
                data = orjson.loads(raw)
                if data.get("retCode") == 0:
                    return True
                else:
                    logger.error(f"❌ [Bybit] Amend Rejected: {data.get('retMsg')}")
                    return False
        except Exception as e:
            logger.error(f"💥 [Bybit] Amend I/O Fallback: {e}")
            return False

    async def get_positions(self, symbol: str) -> dict:
        """[RAM-Truth] Atomic Position Snapshot."""
        try:
            timestamp = str(int(time.time() * 1000))
            params = f"category=linear&symbol={symbol}"
            signature = self._generate_signature(timestamp, params)
            headers = {
                "X-BAPI-API-KEY": self.api_key or "", "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-RECV-WINDOW": "5000"
            }
            session = await self._get_session()
            url = f"{self.base_url}/v5/position/list?{params}"
            async with session.get(url, headers=headers, timeout=5) as resp:
                raw = await resp.read()
                return orjson.loads(raw)
        except Exception as e:
            logger.error(f"💥 [REST] Position fetch failure: {e}")
            return {}



    async def get_trade_history(self, symbol: str, limit: int = 5) -> dict:
        """[CAUSAL RECOVERY] Fetches recent execution history for a symbol."""
        try:
            timestamp = str(int(time.time() * 1000))
            params = f"category=linear&symbol={symbol}&limit={limit}"
            signature = self._generate_signature(timestamp, params)
            headers = {
                "X-BAPI-API-KEY": self.api_key or "",
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": "5000"
            }
            session = await self._get_session()
            url = f"{self.base_url}/v5/execution/list?{params}"
            async with session.get(url, headers=headers, timeout=5) as resp:
                raw = await resp.read()
                return orjson.loads(raw)
        except Exception as e:
            logger.error(f"💥 [Bybit] Trade History I/O Fallback: {e}")
            return {"retCode": -1, "retMsg": str(e)}

    async def cancel_order(self, symbol: str, order_id: Optional[str] = None, order_link_id: Optional[str] = None) -> dict:
        """[ATOMIC ABORT] Cancels an active order on the exchange."""
        try:
            timestamp = str(int(time.time() * 1000))
            payload_dict = {"category": "linear", "symbol": symbol}
            if order_id: payload_dict["orderId"] = order_id
            if order_link_id: payload_dict["orderLinkId"] = order_link_id
            
            payload = orjson.dumps(payload_dict).decode('utf-8')
            signature = self._generate_signature(timestamp, payload)
            headers = {
                "X-BAPI-API-KEY": self.api_key or "",
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": "5000",
                "Content-Type": "application/json"
            }
            session = await self._get_session()
            url = f"{self.base_url}/v5/order/cancel"
            async with session.post(url, headers=headers, data=payload, timeout=5) as resp:
                raw = await resp.read()
                return orjson.loads(raw)
        except Exception as e:
            logger.error(f"💥 [Bybit] Cancel I/O Fatal: {e}")
            return {"retCode": -1, "retMsg": str(e)}

    async def get_open_orders(self, symbol: str) -> dict:
        """[RAM-Truth] Atomic Open Orders Snapshot."""
        try:
            timestamp = str(int(time.time() * 1000))
            params = f"category=linear&symbol={symbol}&openOnly=0&limit=50"
            signature = self._generate_signature(timestamp, params)
            headers = {
                "X-BAPI-API-KEY": self.api_key or "", "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-RECV-WINDOW": "5000"
            }
            session = await self._get_session()
            url = f"{self.base_url}/v5/order/realtime?{params}"
            async with session.get(url, headers=headers, timeout=5) as resp:
                raw = await resp.read()
                return orjson.loads(raw)
        except Exception as e:
            logger.error(f"💥 [REST] Open Orders fetch failure: {e}")
            return {}

    async def get_wallet_balance(self, coin: str = "USDT") -> float:
        """[GLOBAL EQUITY SYNC] Fetches 'Available Balance' from Bybit."""
        if not self.api_key or not self.api_secret:
            return 1000.0 # Sandbox default if no keys
            
        acc_type = "UNIFIED"
        try:
            timestamp = str(int(time.time() * 1000))
            params = f"accountType={acc_type}&coin={coin}"
            signature = self._generate_signature(timestamp, params)
            headers = {
                "X-BAPI-API-KEY": self.api_key, "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-RECV-WINDOW": "5000"
            }
            session = await self._get_session()
            url = f"{self.base_url}/v5/account/wallet-balance?{params}"
            async with session.get(url, headers=headers, timeout=10) as resp:
                raw = await resp.read()
                data = orjson.loads(raw)
                ret_code = data.get("retCode")
                if ret_code == 0:
                    acc_list = data.get("result", {}).get("list", [])
                    if not acc_list: 
                        logger.warning(f"🔍 [Bybit] {acc_type} returned EMPTY results list.")
                    else:
                        acc_data = acc_list[0]
                        coin_list = acc_data.get("coin", [])
                        balance = 0.0
                        for c_data in coin_list:
                            if c_data.get("coin") == coin:
                                balance = (
                                    safe_float(c_data.get("availableToWithdraw")) or 
                                    safe_float(c_data.get("walletBalance")) or
                                    safe_float(c_data.get("totalWalletBalance"))
                                )
                                if balance > 0:
                                    logger.success(f"✅ [Bybit] {coin} found in {acc_type}: ${balance:,.2f}")
                                break
                        
                        if balance <= 0:
                            balance = (
                                safe_float(acc_data.get("totalAvailableBalance")) or 
                                safe_float(acc_data.get("totalWalletBalance")) or
                                safe_float(acc_data.get("totalEquity"))
                            )
                        if balance > 0:
                            return balance
                        else:
                            msg = data.get("retMsg", "No Msg")
                            logger.warning(f"⚠️ [Bybit] Zero balance in {acc_type} (Msg: {msg}).")
                else:
                    msg = data.get("retMsg", "Unknown Error")
                    logger.error(f"❌ [Bybit] {acc_type} REJECTED: Code {ret_code} ({msg})")
        except Exception as e:
            logger.error(f"⚠️ [BybitREST] Wallet fetch failed ({acc_type}): {repr(e)}")
            
        virtual_balance = 1000.0
        logger.warning(f"🎭 [VIRTUAL] Shadow Capital activated for Advisory Mode: ${virtual_balance:,.2f}")
        return virtual_balance

    async def get_tickers(self, symbol: Optional[str] = None) -> Union[List[dict], float]:
        """Fetches 24h tickers. If symbol is provided, returns its lastPrice as float."""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/v5/market/tickers?category=linear"
            if symbol:
                url += f"&symbol={symbol}"
            async with session.get(url, timeout=10) as resp:
                raw = await resp.read()
                resp_data = orjson.loads(raw)
                lst = resp_data.get("result", {}).get("list", [])
                
                if symbol:
                    if not lst:
                        return 0.0
                    return float(lst[0].get("lastPrice", 0.0))
                
                return lst
        except Exception as e:
            logger.error(f"❌ [Bybit] Tickers API error: {e}")
            return 0.0 if symbol else []

    async def fetch_active_symbols(self) -> List[str]:
        """[GEKTOR v5.50] Discovery Protocol."""
        try:
            tickers = await self.get_tickers()
            if isinstance(tickers, list):
                symbols = [t["symbol"] for t in tickers if t["symbol"].endswith("USDT")]
            else:
                symbols = []
            logger.success(f"📡 [Bybit] Found {len(symbols)} active USDT-Linear contracts.")
            return symbols
        except Exception as e:
            logger.error(f"❌ [Bybit] Discovery Error: {e}")
            return ["BTCUSDT", "ETHUSDT"]

    async def get_recent_trades(self, symbol: str, limit: int = 1000) -> List[dict]:
        """Inline orjson parsing."""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/v5/market/recent-trade?category=linear&symbol={symbol}&limit={limit}"
            async with session.get(url, timeout=10) as resp:
                raw = await resp.read()
                return orjson.loads(raw).get("result", {}).get("list", [])
        except Exception as e:
            logger.error(f"⚠️ [BybitREST] Trade fetch failed ({symbol}): {repr(e)}")
            return []

    async def get_orderbook(self, symbol: str, limit: int = 50) -> dict:
        """Inline orjson parsing (~15us)."""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/v5/market/orderbook?category=linear&symbol={symbol}&limit={limit}"
            async with session.get(url, timeout=5) as resp:
                raw = await resp.read()
                return orjson.loads(raw).get("result", {})
        except Exception as e:
            logger.error(f"⚠️ [BybitREST] Orderbook fetch failed ({symbol}): {repr(e)}")
            return {}

# ═══════════════════════════════════════════════════════════════════
# [GEKTOR v5.0] ULTRA-LOW LATENCY ORDERBOOK (O(1) Hash Map)
# ═══════════════════════════════════════════════════════════════════
class HighPerformanceL2Book:
    __slots__ = ('_bids', '_asks', 'symbol')
    def __init__(self, symbol: str):
        self.symbol = symbol
        # [HFT FIX] Changed from List to Hash Map (Dict) to eliminate O(N) memory shifts.
        self._bids: Dict[float, float] = {} 
        self._asks: Dict[float, float] = {}

    def apply_delta(self, side: str, price: float, volume: float):
        """O(1) Nano-second delta application."""
        book = self._bids if side == "bids" else self._asks
        if volume == 0.0:
            book.pop(price, None)
        else:
            book[price] = volume

class BybitIngestor:
    """[GEKTOR v6.0] Zero-Latency WS Ingestor. LATENCY SHIELD: DISABLED"""
    def __init__(self, symbols: List[str], on_tick_callback: Callable, on_snapshot_callback: Callable, on_ticker_callback: Optional[Callable] = None, on_reconnect_callback: Optional[Callable] = None, alert_callback: Optional[Callable] = None, proxy_url: Optional[str] = None):
        self.symbols = symbols
        self.screener_symbols: List[str] = []
        self.on_tick = on_tick_callback
        self.on_snapshot = on_snapshot_callback
        self.on_ticker = on_ticker_callback
        self.on_reconnect = on_reconnect_callback
        self.alert_callback = alert_callback
        self.proxy_url = proxy_url
        self._running = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._bbo_only_mode = False
        self._l2_books: Dict[str, HighPerformanceL2Book] = {}
        self._analyzers: Dict[str, MicrostructureAnalyzer] = {}
        self._discriminators: Dict[str, SpoofingDiscriminator] = {}
        self._sequence_guards: Dict[str, OrderBookSequenceGuard] = {}
        self._last_symbol_tick: dict[str, float] = {s: time.monotonic() for s in symbols}
        
        self._ingest_queue: asyncio.Queue = asyncio.Queue(maxsize=75_000)
        self._processor_task: Optional[asyncio.Task] = None
        self.clock_offset = 0.0
        self._clock_calibrated = False
        self._ping_sent_at: Optional[float] = None
        self.last_tick_wall = time.monotonic()

    def enable_backpressure(self, enabled: bool):
        self._bbo_only_mode = enabled
        logger.warning(f"🛡️ [LOD] Backpressure mode: {'ON (BBO Only)' if enabled else 'OFF'}")

    async def _ping_loop(self):
        logger.info("📡 [PING LOOP] Heartbeat thread initialized.")
        while self._active:
            try:
                await asyncio.sleep(10)
                if self._ws and not self._ws.closed:
                    logger.info("➡️ [PING] Transmitting heartbeat to Bybit...")
                    await self._ws.send_json({"op": "ping"})
                    self._last_ping_sent_at = time.monotonic()
            except asyncio.CancelledError:
                logger.info("🛑 [PING LOOP] Task cancelled gracefully.")
                break
            except Exception as e:
                logger.exception(f"💥 [PING LOOP] Fatal error: {e}")

    async def _force_kill_socket(self):
        """[GEKTOR v14.0] Socket annihilation + L2 state purge."""
        if self._ws:
            try:
                await self._ws.close()
            except (RuntimeError, ConnectionError) as exc:
                logger.debug(f"[Ingestor] Socket close error (non-critical): {exc!r}")
            self._ws = None
        self._invalidate_all_l2()
        logger.warning("🔌 [Ingestor] Socket force-killed. L2 state purged.")

    def _invalidate_all_l2(self) -> None:
        """
        [GEKTOR v14.0] Deterministic L2 State Invalidation.
        After any connection loss, ALL cached orderbook state is toxic
        (Schrödinger's Fill). We nuke it and force fresh snapshot download.
        Zero-allocation: dict.clear() is O(1) amortized.
        """
        count = len(self._l2_books)
        self._l2_books.clear()
        self._analyzers.clear()
        self._discriminators.clear()
        self._sequence_guards.clear()
        self._first_msg_logged = False
        if count > 0:
            logger.warning(f"🗑️ [L2] Invalidated {count} cached orderbooks. Fresh snapshots required.")

    async def _watchdog_loop(self):
        # DIAGNOSTIC MODE: Watchdog logs and resets
        """
        [GEKTOR v14.0 PATCH 2] Aggressive Force-Reconnect Watchdog.
        Two independent failure detectors with tightened thresholds:
        1. Data Silence (>5s no ticks/deltas) -> Force kill socket + L2 purge.
        2. Pong Silence (>20s no pong) -> Half-open TCP detected, force kill.
        Zero-allocation: no new objects created per iteration.
        """
        _DATA_SILENCE_THRESHOLD: float = 5.0    # seconds — crypto market is NEVER silent for 5s
        _PONG_SILENCE_THRESHOLD: float = 20.0   # seconds — half-open TCP detector

        # Initialize pong tracker (updated in _handle_update on pong receipt)
        self._last_pong_ts: float = time.monotonic()

        while self._running:
            try:
                await asyncio.sleep(1.0)  # Check every second for faster reaction
            except asyncio.CancelledError:
                break
            now = time.monotonic()

            # --- Check 1: Data Silence ---
            data_silence = now - self.last_tick_wall
            if data_silence > _DATA_SILENCE_THRESHOLD:
                logger.critical(
                    f"💀 [WATCHDOG] Data silence: {data_silence:.1f}s > {_DATA_SILENCE_THRESHOLD}s. "
                    f"FORCE KILLING SOCKET for reconnect."
                )
                await self._force_kill_socket()
                self.last_tick_wall = now
                self._last_pong_ts = now
                continue

            # --- Check 2: Pong Silence (Half-Open TCP Detection) ---
            pong_silence = now - self._last_pong_ts
            if pong_silence > _PONG_SILENCE_THRESHOLD:
                logger.critical(
                    f"💀 [WATCHDOG] Pong silence: {pong_silence:.1f}s > {_PONG_SILENCE_THRESHOLD}s. "
                    f"HALF-OPEN CONNECTION DETECTED. FORCE KILL."
                )
                await self._force_kill_socket()
                self._last_pong_ts = now
                self.last_tick_wall = now

    def parse_bybit_trade(self, payload: dict) -> Optional[MarketTick]:
        self.last_tick_wall = time.monotonic()
        try:
            return MarketTick(
                symbol=payload['s'],
                exchange_ts=int(payload['T']),
                price=float(payload['p']),
                volume=float(payload['v']),
                side=payload['S']
            )
        except: return None

    async def subscribe(self, symbols: List[str]):
        if not self._ws: return
        args = []
        for s in symbols: args.extend([f"publicTrade.{s}", f"orderbook.50.{s}"])
        payload = {"op": "subscribe", "args": args}
        logger.info(f"👀 [SUBSCRIBE PAYLOAD] {payload}")
        await self._ws.send_str(orjson.dumps(payload).decode('utf-8'))

    async def subscribe_screener(self, symbols: List[str]):
        if not self._ws: return
        self.screener_symbols = list(set(self.screener_symbols + symbols))
        args = [f"tickers.{s}" for s in symbols]
        await self._ws.send_str(orjson.dumps({"op": "subscribe", "args": args}).decode('utf-8'))

    async def subscribe_to_symbol(self, symbol: str):
        """Динамическая подписка на инструмент без рестарта WS."""
        if symbol not in self.symbols:
            self.symbols.append(symbol)
        if not self._ws or self._ws.closed:
            logger.error(f"❌ [Bybit] Не удается подписаться на {symbol}: WS не подключен")
            return

        payload = {
            "op": "subscribe",
            "args": [f"publicTrade.{symbol}", f"orderbook.50.{symbol}"]
        }
        await self._ws.send_str(orjson.dumps(payload).decode("utf-8"))
        logger.success(f"📡 [Bybit] Отправлен запрос на динамическую подписку: {symbol}")

    async def _on_ws_message(self, raw: bytes | str):
        if not getattr(self, '_first_msg_logged', False):
            logger.info(f"👀 [RAW TRAFFIC] {str(raw)[:200]}")
            self._first_msg_logged = True
        try:
            self._ingest_queue.put_nowait(raw)
        except asyncio.QueueFull:
            logger.warning("📉 [Ingestor] WS QUEUE FULL — DROPPING DATA")

    async def _controlled_drain(self):
        """[HFT FIX] Absolute GIL Protection via strict time & batch limits."""
        logger.info("⚡ [Ingestor] Controlled Drain loop active.")
        while self._running:
            batch = []
            try:
                first = await asyncio.wait_for(self._ingest_queue.get(), timeout=2.0)
                batch.append(first)
                
                # Жесткий лимит на батч: не более 15 мс или 100 элементов.
                deadline = time.perf_counter() + 0.015
                while len(batch) < 100 and time.perf_counter() < deadline:
                    try:
                        batch.append(self._ingest_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                
                if batch:
                    await self._process_batch(batch)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"💥 [Ingestor] Controlled drain failure: {e}")
                await asyncio.sleep(0.1)

    async def _process_batch(self, batch: list):
        """[HFT FIX] Zero-latency synchronous parsing with micro-yielding."""
        for i, raw in enumerate(batch):
            try:
                data = orjson.loads(raw)
                await self._handle_update(data)
            except Exception as e:
                logger.error(f"💥 [Ingestor Drain Error] {repr(e)}")
            
            # [ДЕТЕРМИНИРОВАННОЕ ДЫХАНИЕ]: Отдаем контроль чаще
            if i > 0 and i % 5 == 0:
                await asyncio.sleep(0)

    async def _handle_update(self, payload: dict):
        if any(k in payload for k in ("op", "success", "ret_msg")):
            return

        topic = payload.get("topic", "")
        data = payload.get("data")
        if not topic or data is None: 
            return

        if topic.startswith("publicTrade"):
            if not isinstance(data, list): return
            for t in data:
                tick = self.parse_bybit_trade(t)
                if tick:
                    if tick.symbol in self._discriminators:
                        self._discriminators[tick.symbol].register_trade(tick.price, tick.volume)
                    await self.on_tick(tick.symbol, {"symbol": tick.symbol, "price": tick.price, "volume": tick.volume, "side": tick.side, "ts": tick.exchange_ts})
        
        elif topic.startswith("orderbook"):
            if "b" not in data and "a" not in data: return

            symbol = topic.split(".")[-1]
            if symbol not in self._l2_books: 
                self._l2_books[symbol] = HighPerformanceL2Book(symbol)
                self._analyzers[symbol] = MicrostructureAnalyzer()
                self._discriminators[symbol] = SpoofingDiscriminator()
                self._sequence_guards[symbol] = OrderBookSequenceGuard()

            # [PATCH 4] Enforce causal monotonicity via SequenceGuard.
            # validate() is O(1), no allocations. Returns False on sequence gap.
            guard = self._sequence_guards[symbol]
            if not guard.validate(payload):
                logger.critical(f"🛑 [SEQ_GUARD] {symbol} sequence gap detected! Dropping connection to protect state.")
                
                if self.alert_callback:
                    self.alert_callback(f"🛑 SEQUENCE_BREAK on {symbol}. Forcing WS reconnect.")
                    
                # Nuke from orbit: жестко рвем сокет. Ingestor сам переподключится и скачает чистые стаканы.
                await self._force_kill_socket()
                return
            self.last_tick_wall = time.monotonic()  # Orderbook deltas feed watchdog
            await self.on_snapshot(symbol, payload)
        elif topic.startswith("tickers") and self.on_ticker:
            await self.on_ticker(topic.split(".")[-1], data)
        
        else:
            # [DIAGNOSTIC] Log unhandled topics for infrastructure visibility
            logger.debug(f"🔍 [Bybit] UNHANDLED TOPIC: {topic}")

    async def start(self):
        """
        [GEKTOR v14.0] Zero-Leak WS Lifecycle.
        Session encapsulated via `async with` — guaranteed cleanup on ALL exit paths.
        Aggressive sock_read=3s — no 16-second silences.
        L2 State invalidated on EVERY reconnect — no Schrödinger's Fills.
        """
        self._running = True
        self._active = True
        self._bg_tasks: set[asyncio.Task] = set()

        t1 = asyncio.create_task(self._watchdog_loop())
        ping_task = asyncio.create_task(self._ping_loop())
        self._processor_task = asyncio.create_task(self._controlled_drain())

        for t in (t1, ping_task, self._processor_task):
            self._bg_tasks.add(t)
            t.add_done_callback(self._bg_tasks.discard)

        # ⚠️ [STAGING] Mock Server — ВЕРНУТЬ НА ПРОДАКШЕН ПОСЛЕ ТЕСТОВ!
        url = "wss://stream.bybit.com/v5/public/linear"  # PRODUCTION
        # url = "ws://localhost:8765/v5/public/linear"  # MOCK SERVER

        # [Beazley Rule] Aggressive I/O timeout: 3s sock_read
        _ws_timeout = aiohttp.ClientTimeout(total=None, sock_connect=5.0, sock_read=3.0)

        reconnect_attempt = 0

        while self._running:
            try:
                # [Kleppmann] L2 State is TOXIC after any disconnect — purge before reconnect
                self._invalidate_all_l2()

                connector = ProxyConnector.from_url(self.proxy_url) if self.proxy_url else None
                # [CRITICAL] async with guarantees session.close() on ANY exit path
                async with aiohttp.ClientSession(
                    connector=connector, trust_env=False, timeout=_ws_timeout
                ) as session:
                    self._ws = await session.ws_connect(url)
                    try:
                        self._last_pong_ts = time.monotonic()
                        if self.on_reconnect:
                            await self.on_reconnect()
                        await self.subscribe(self.symbols)
                        if self.screener_symbols:
                            await self.subscribe_screener(self.screener_symbols)

                        logger.success(
                            f"🟢 [Bybit] WS Connected (Gektor v14.0). "
                            f"Egress: {self.proxy_url or 'DIRECT'}"
                        )

                        msg_count: int = 0
                        reconnect_attempt = 0
                        async for msg in self._ws:
                            msg_count += 1
                            if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                data_str = msg.data if isinstance(msg.data, str) else msg.data.decode('utf-8', errors='ignore')
                                
                                # ЖЕСТКИЙ ПЕРЕХВАТЧИК PONG
                                if "pong" in data_str.lower() or 'op":"pong"' in data_str.replace(" ", ""):
                                    self._last_pong_ts = time.monotonic()
                                    logger.info("🟢 [PONG] Intercepted! Watchdog timer reset.")
                                    continue # Не пускаем служебный пакет дальше по конвейеру

                            if self._bbo_only_mode and '"topic":"orderbook.50' in msg.data:
                                continue
                            await self._on_ws_message(msg.data)

                            # [HFT FIX] Yield control to event loop periodically
                            if msg_count % 20 == 0:
                                await asyncio.sleep(0)
                    except asyncio.CancelledError:
                        logger.info("🛑 [Ingestor] Graceful cancellation received.")
                        return
                    finally:
                        self._ws = None

            except asyncio.CancelledError:
                logger.info("🛑 [Ingestor] Outer cancellation received.")
                return
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                logger.error(f"🔌 [Bybit] WS transport error: {type(e).__name__}: {e}")
                GlobalResilienceManager.get_instance().register_failure("BybitWS_Public")
            except Exception as e:
                logger.error(f"🔌 [Bybit] WS reconnecting: {e}")
                GlobalResilienceManager.get_instance().register_failure("BybitWS_Public")

            if self._running:
                delay = min(60.0, 1.0 * (2 ** reconnect_attempt))
                jitter = random.uniform(0.8, 1.2)
                sleep_time = delay * jitter
                logger.warning(f"🔌 [Ingestor] Reconnecting in {sleep_time:.2f}s (Attempt {reconnect_attempt})...")
                await asyncio.sleep(sleep_time)
                reconnect_attempt += 1

    async def run(self):
        """Backward-compatible alias for legacy callers."""
        await self.start()

    async def stop(self):
        """
        [GEKTOR v14.0] CancelledError-safe shutdown.
        1. Signal main loop to stop.
        2. Kill socket to break `async for msg` immediately.
        3. Cancel background tasks with grace period.
        """
        self._running = False
        self._active = False
        # Kill socket to unblock the `async for` iterator
        if self._ws and not self._ws.closed:
            try:
                await self._ws.close()
            except (RuntimeError, ConnectionError) as exc:
                logger.debug(f"[Ingestor] Socket close error during stop (non-critical): {exc!r}")
            self._ws = None

        # Cancel background tasks
        for task in list(self._bg_tasks):
            if not task.done():
                task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

class PrivateBybitWSIngestor:
    """[GEKTOR v6.0] Zero-Latency Private WebSocket Listener."""
    def __init__(self, shadow_ledger: Any, api_key: str, api_secret: str, 
                 proxy_url: Optional[str] = None, on_order_update: Optional[Callable] = None, on_reconnect: Optional[Callable] = None):
        self._shadow = shadow_ledger
        self._api_key = api_key
        self._api_secret = api_secret
        self._proxy = proxy_url
        self._on_order_update = on_order_update
        self._on_reconnect = on_reconnect
        self._running = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.last_rx_time = time.monotonic()
        self._terminal_events: Dict[str, float] = {}

    def has_recent_terminal_event(self, symbol: str, window_sec: float = 10.0) -> bool:
        ts = self._terminal_events.get(symbol, 0.0)
        return (time.monotonic() - ts) < window_sec

    def _register_terminal_event(self, symbol: str):
        self._terminal_events[symbol] = time.monotonic()

    def _generate_signature(self, timestamp: str) -> str:
        param_str = f"GET/realtime{timestamp}5000"
        hash = hmac.new(bytes(self._api_secret, "utf-8"), param_str.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()

    async def _heartbeat_loop(self):
        ping_payload = orjson.dumps({"req_id": "GEKTOR_PING", "op": "ping"}).decode('utf-8')
        while self._running:
            try:
                await asyncio.sleep(20.0)
                if self._ws and not self._ws.closed:
                    await self._ws.send_str(ping_payload)
            except asyncio.CancelledError: break
            except Exception as e: 
                logger.error(f"🛑 [PrivateWS] Keepalive Crash: {e}")
                break

    async def _watchdog_loop(self):
        while self._running:
            await asyncio.sleep(2)
            if time.monotonic() - self.last_rx_time > 60.0:
                logger.warning("💀 [PrivateWS] Rx-Silence > 60s! Resetting tunnel.")
                if self._ws: await self._ws.close()
                self.last_rx_time = time.monotonic()

    async def run(self):
        if not self._api_key or not self._api_secret:
            logger.warning("🗝️ [PrivateWS] Read-Only API keys missing. Stream skipped.")
            return

        self._running = True
        self._bg_tasks = set()
        
        t1 = asyncio.create_task(self._heartbeat_loop())
        t2 = asyncio.create_task(self._watchdog_loop())
        self._bg_tasks.add(t1)
        self._bg_tasks.add(t2)
        t1.add_done_callback(self._bg_tasks.discard)
        t2.add_done_callback(self._bg_tasks.discard)
        
        url = "wss://stream.bybit.com/v5/private"
        
        retry_count = 0
        while self._running:
            session = None
            try:
                connector = ProxyConnector.from_url(self._proxy) if self._proxy else None
                session = aiohttp.ClientSession(connector=connector, trust_env=False)
                self._ws = await session.ws_connect(url, heartbeat=10.0)
                
                timestamp = str(int(time.time() * 1000))
                signature = self._generate_signature(timestamp)
                
                auth_msg = {
                    "op": "auth",
                    "args": [self._api_key, 5000, timestamp, signature],
                    "header": {
                        "X-BAPI-TIMESTAMP": timestamp,
                        "X-BAPI-RECV-WINDOW": "5000",
                        "cancelOnDisconnect": 1,
                        "cancelOnDisconnectTime": 10
                    }
                }
                await self._ws.send_str(orjson.dumps(auth_msg).decode('utf-8'))
                await self._ws.send_str(orjson.dumps({"op": "subscribe", "args": ["position", "order"]}).decode('utf-8'))
                logger.success("🟢 [PrivateWS] Connected & Authenticated (CoD: ACTIVE).")
                
                if self._on_reconnect:
                    t3 = asyncio.create_task(self._on_reconnect())
                    self._bg_tasks.add(t3)
                    t3.add_done_callback(self._bg_tasks.discard)
                
                msg_count = 0
                async for msg in self._ws:
                    self.last_rx_time = time.monotonic()
                    msg_count += 1
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        payload = orjson.loads(msg.data)
                        topic = payload.get("topic")
                        
                        if topic == "position":
                            for pos in payload.get("data", []):
                                symbol = pos["symbol"]
                                size = float(pos["size"])
                                entry_price = float(pos["avgPrice"])
                                side = str(pos.get("side", "")).upper() 
                                
                                if size == 0.0:
                                    self._shadow.clear_symbol_exposure(symbol)
                                    self._register_terminal_event(symbol)
                                else:
                                    ts = int(pos.get("updatedTime", payload.get("ts", 0)))
                                    self._shadow.set_symbol_exposure(symbol, size, entry_price, side, exch_ts=ts)
                         
                        elif topic == "order":
                            for order in payload.get("data", []):
                                status = order.get("orderStatus")
                                symbol = order.get("symbol")
                                if status in ("Filled", "Cancelled", "Rejected", "Deactivated"):
                                    self._register_terminal_event(symbol)
                                
                                if self._on_order_update:
                                    await self._on_order_update({
                                        "symbol": symbol,
                                        "order_id": order["orderId"],
                                        "order_link_id": order.get("orderLinkId"),
                                        "status": status,
                                        "side": order["side"],
                                        "price": float(order.get("price", 0)),
                                        "cum_exec_qty": float(order.get("cumExecQty", 0)),
                                        "cum_exec_value": float(order.get("cumExecValue", 0)),
                                        "order_type": order.get("orderType", ""),
                                        "reduce_only": order.get("reduceOnly", False),
                                        "updatedTime": order.get("updatedTime", payload.get("ts", 0))
                                    })
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                        
                    # [HFT FIX] Микро-дыхание приватного контура
                    if msg_count % 10 == 0:
                        await asyncio.sleep(0)
                        
            except Exception as e:
                retry_count += 1
                sleep_time = 2.0 if retry_count <= 3 else min(10.0, 2.0 * (1.2 ** (retry_count - 3)))
                logger.error(f"❌ [PrivateWS] Reconnect #{retry_count} (Sleep: {sleep_time:.1f}s): {e}")
                GlobalResilienceManager.get_instance().register_failure("BybitWS_Private")
                if self._running: await asyncio.sleep(sleep_time)
            finally:
                if session: await session.close()

    async def stop(self):
        self._running = False
        if self._ws: await self._ws.close()