"""Shard Bybit linear L2: one WS per shard, hot subscribe/unsubscribe, throttled REST resync."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any, Final

import aiohttp
import orjson
from loguru import logger

from src.infrastructure.gektor_l2.book_state import BookState
from src.infrastructure.gektor_l2.errors import BybitRestRateLimited
from src.infrastructure.gektor_l2.protocols import AbstractOrderBookProcessor, AbstractOrderBookResyncSource
from src.infrastructure.gektor_l2.reconnect_throttle import AsyncReconnectTokenBucket
from src.infrastructure.gektor_l2.resync_gate import RestResyncGate
from src.infrastructure.gektor_l2.wire_parse import optional_cross_id, parse_bids, parse_asks

def _chunk_symbols(symbols: Sequence[str], chunk_size: int) -> list[tuple[str, ...]]:
    syms = [s.strip().upper() for s in symbols if s.strip()]
    out: list[tuple[str, ...]] = []
    for i in range(0, len(syms), chunk_size):
        out.append(tuple(syms[i : i + chunk_size]))
    return out


def _shard_index_for_symbol(layout: Sequence[tuple[str, ...]], sym: str) -> int | None:
    s = sym.strip().upper()
    for i, chunk in enumerate(layout):
        if s in chunk:
            return i
    return None


class L2OrderBookWebSocketMultiplexer:
    """
    One `aiohttp` WebSocket per shard; `asyncio.wait` between `ws.receive()` and a command queue
    for hot subscribe/unsubscribe without tearing down TCP. L2 mutations stay synchronous in the loop.
    REST resync is serialized through `RestResyncGate` + a single queue pump (no per-symbol task storm).
    """

    __slots__ = (
        "_processors",
        "_states",
        "_depth",
        "_ws_url",
        "_session_owner",
        "_session",
        "_bucket",
        "_chunk_size",
        "_resync",
        "_resync_gate",
        "_resync_queue",
        "_resync_pending",
        "_resync_pump_task",
        "_lifecycle",
        "_shard_layout",
        "_shard_queues",
        "_shard_tasks",
        "_connection_epoch",
        "_pending_acks",
    )

    def __init__(
        self,
        processors: Mapping[str, AbstractOrderBookProcessor],
        *,
        depth: int = 50,
        ws_url: str = "wss://stream.bybit.com/v5/public/linear",
        session: aiohttp.ClientSession | None = None,
        reconnect_bucket: AsyncReconnectTokenBucket | None = None,
        chunk_size: int = 20,
        resync_source: AbstractOrderBookResyncSource | None = None,
        resync_gate: RestResyncGate | None = None,
    ) -> None:
        if chunk_size <= 0 or chunk_size > 50:
            raise ValueError("chunk_size must be in 1..50 for operational safety")
        self._processors: dict[str, AbstractOrderBookProcessor] = {
            k.strip().upper(): v for k, v in processors.items()
        }
        self._states: dict[str, BookState] = {s: BookState.SYNCED for s in self._processors}
        self._depth: Final[int] = int(depth)
        self._ws_url: Final[str] = ws_url
        self._session: aiohttp.ClientSession | None = session
        self._session_owner = session is None
        self._bucket: Final[AsyncReconnectTokenBucket] = reconnect_bucket or AsyncReconnectTokenBucket()
        self._chunk_size: Final[int] = int(chunk_size)
        self._resync: AbstractOrderBookResyncSource | None = resync_source
        self._resync_gate: RestResyncGate | None = resync_gate if resync_source else None
        if resync_source and self._resync_gate is None:
            self._resync_gate = RestResyncGate()
        self._resync_queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
        self._resync_pending: set[str] = set()
        self._resync_pump_task: asyncio.Task[None] | None = None
        self._lifecycle = asyncio.Lock()
        self._shard_layout: list[tuple[str, ...]] = []
        self._shard_queues: list[asyncio.Queue[dict[str, Any]]] = []
        self._shard_tasks: list[asyncio.Task[None]] = []
        self._connection_epoch: int = 0
        self._pending_acks: set[asyncio.Future[Any]] = set()

    def replace_processors(self, processors: Mapping[str, AbstractOrderBookProcessor]) -> None:
        """Synchronous map replace; use `hot_swap_processors` to sync WS subscriptions."""
        self._processors = {k.strip().upper(): v for k, v in processors.items()}
        self._states = {s: BookState.SYNCED for s in self._processors}

    def book_state(self, symbol: str) -> BookState:
        return self._states.get(symbol.strip().upper(), BookState.DESYNCED)

    @property
    def connection_epoch(self) -> int:
        """Current connection generation; bumps on every shard reconnect."""
        return self._connection_epoch

    def request_resync(self, symbol: str) -> None:
        if self._resync is None:
            logger.error("L2 resync requested for {} but no resync_source configured", symbol)
            self._states[symbol.strip().upper()] = BookState.DESYNCED
            return
        sym = symbol.strip().upper()
        if sym in self._resync_pending:
            return
        self._resync_pending.add(sym)
        epoch = self._connection_epoch
        try:
            self._resync_queue.put_nowait((sym, epoch))
        except Exception:
            self._resync_pending.discard(sym)

    def _register_ack(self, cmd: dict[str, Any]) -> asyncio.Future[Any] | None:
        """Track ACK future for lifecycle management on crash."""
        ack: asyncio.Future[Any] | None = cmd.get("_ack")
        if ack is not None and not ack.done():
            self._pending_acks.add(ack)
        return ack

    def _complete_cmd_ack(self, cmd: dict[str, Any]) -> None:
        ack: asyncio.Future[Any] | None = cmd.get("_ack")
        if ack is not None and not ack.done():
            ack.set_result(True)
        if ack is not None:
            self._pending_acks.discard(ack)

    def _purge_pending_acks(self, reason: str = "WebSocket connection lost") -> None:
        """Reject all pending ACK futures on crash — prevents UniverseManager deadlock."""
        purged = 0
        for fut in self._pending_acks:
            if not fut.done():
                fut.set_exception(ConnectionError(f"ACK rejected: {reason}"))
                purged += 1
        self._pending_acks.clear()
        if purged > 0:
            logger.warning("L2 purged {} pending ACK futures: {}", purged, reason)

    def _purge_resync_queue(self) -> None:
        """Atomically drain resync queue and pending set on epoch change."""
        drained = 0
        while not self._resync_queue.empty():
            try:
                self._resync_queue.get_nowait()
                drained += 1
            except Exception:
                break
        self._resync_pending.clear()
        if drained > 0:
            logger.warning("L2 drained {} stale resync entries on epoch change", drained)

    async def _apply_shard_command(self, ws: aiohttp.ClientWebSocketResponse, cmd: dict[str, Any]) -> str:
        self._register_ack(cmd)
        if cmd.get("action") == "_shutdown":
            self._complete_cmd_ack(cmd)
            return "stop"
        action = str(cmd.get("action", ""))
        raw_syms = cmd.get("symbols") or []
        if not isinstance(raw_syms, list) or action not in ("subscribe", "unsubscribe"):
            self._complete_cmd_ack(cmd)
            return "ok"
        args = [f"orderbook.{self._depth}.{str(s).strip().upper()}" for s in raw_syms if str(s).strip()]
        if not args:
            self._complete_cmd_ack(cmd)
            return "ok"
        payload = orjson.dumps({"op": action, "args": args})
        try:
            await ws.send_bytes(payload)
        except Exception as exc:
            # ACK must be rejected if send fails — caller must not hang
            ack = cmd.get("_ack")
            if ack is not None and not ack.done():
                ack.set_exception(exc)
            self._pending_acks.discard(ack)
            raise
        self._complete_cmd_ack(cmd)
        return "ok"

    def _ingest_ws_payload(
        self,
        sym: str,
        proc: AbstractOrderBookProcessor,
        msg_type: str,
        first: dict[str, Any],
    ) -> None:
        st = self._states.setdefault(sym, BookState.SYNCED)
        if st == BookState.RECOVERING:
            return
        if st == BookState.DESYNCED:
            self._states[sym] = BookState.RECOVERING
            self.request_resync(sym)
            return
        bids = parse_bids(first.get("b"))
        asks = parse_asks(first.get("a"))
        update_id = int(first.get("u", 0))
        seq_val = optional_cross_id(first.get("seq"))
        u_lo = optional_cross_id(first.get("U"))

        if msg_type == "snapshot":
            proc.ingest_snapshot(update_id, bids, asks, seq=seq_val)
            return

        if msg_type == "delta":
            applied = proc.ingest_delta(update_id, bids, asks, range_start=u_lo, seq=seq_val)
            if applied:
                return
            reason = getattr(proc, "last_reject_reason", None)
            logger.warning(
                "L2 delta not applied symbol={} u={} U={} seq={} reason={}",
                sym,
                update_id,
                u_lo,
                seq_val,
                reason,
            )
            if reason in ("sequence_gap", "invalid_u_range", "no_snapshot_anchor"):
                self._states[sym] = BookState.RECOVERING
                self.request_resync(sym)
            return

    async def _resync_pump(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                entry = await asyncio.wait_for(self._resync_queue.get(), timeout=0.35)
            except asyncio.TimeoutError:
                continue
            sym, entry_epoch = entry
            self._resync_pending.discard(sym)
            # Epoch guard: discard stale entries from dead connections (Kleppmann)
            if entry_epoch != self._connection_epoch:
                logger.debug(
                    "L2 resync skipped stale symbol={} entry_epoch={} current_epoch={}",
                    sym, entry_epoch, self._connection_epoch,
                )
                continue
            if stop.is_set():
                break
            proc = self._processors.get(sym)
            if proc is None:
                continue
            src = self._resync
            if src is None:
                continue
            try:
                gate = self._resync_gate

                async def _fetch() -> tuple[int, list[tuple[int, int]], list[tuple[int, int]]]:
                    return await src.fetch_linear_orderbook(sym, limit=self._depth)

                if gate is not None:
                    u, bids, asks = await gate.run_throttled(_fetch)
                else:
                    u, bids, asks = await _fetch()
                # Double-check epoch after async REST call — connection may have died mid-flight
                if self._connection_epoch != entry_epoch:
                    logger.warning(
                        "L2 REST resync result discarded symbol={}: epoch changed during fetch",
                        sym,
                    )
                    continue
                proc.ingest_snapshot(u, bids, asks)
                self._states[sym] = BookState.SYNCED
                logger.success("L2 REST resync complete symbol={} u={} epoch={}", sym, u, entry_epoch)
            except asyncio.CancelledError:
                raise
            except BybitRestRateLimited:
                logger.warning("L2 REST rate limited symbol={} — re-queue after gate backoff", sym)
                self._states[sym] = BookState.RECOVERING
                self.request_resync(sym)
            except Exception as exc:
                logger.error("L2 REST resync failed symbol={}: {!r}", sym, exc)
                self._states[sym] = BookState.DESYNCED
                await asyncio.sleep(0.5)
                self.request_resync(sym)

    async def hot_swap_processors(
        self,
        processors: Mapping[str, AbstractOrderBookProcessor],
        stop: asyncio.Event,
    ) -> None:
        """
        Update books + route subscribe/unsubscribe on **live** sockets (no TCP recycle when shard count matches).
        """
        async with self._lifecycle:
            if not self._shard_tasks:
                self.replace_processors(processors)
                logger.warning("hot_swap_processors: run() not started — map updated only")
                return

            old_n = len(self._shard_tasks)
            old_keys = frozenset(self._processors.keys())
            new_map = {k.strip().upper(): v for k, v in processors.items()}
            new_keys = frozenset(new_map.keys())
            removed = old_keys - new_keys
            added = new_keys - old_keys

            old_layout = list(self._shard_layout) if self._shard_layout else _chunk_symbols(sorted(old_keys), self._chunk_size)
            layout_snap = list(old_layout)

            self._processors = dict(new_map)
            for s in removed:
                self._states.pop(s, None)
            for s in added:
                self._states[s] = BookState.SYNCED

            new_layout = _chunk_symbols(sorted(self._processors.keys()), int(self._chunk_size))
            if not new_layout:
                logger.warning("hot_swap_processors: empty universe")
                return

            for sym in removed:
                idx = _shard_index_for_symbol(old_layout, sym)
                if idx is not None and idx < len(self._shard_queues):
                    fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                    await self._shard_queues[idx].put({"action": "unsubscribe", "symbols": [sym], "_ack": fut})
                    await asyncio.wait_for(fut, timeout=5.0)

            for sym in added:
                idx = _shard_index_for_symbol(new_layout, sym)
                if idx is None:
                    continue
                if idx >= old_n:
                    continue
                while len(self._shard_queues) <= idx:
                    self._shard_queues.append(asyncio.Queue())
                fut2: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                await self._shard_queues[idx].put({"action": "subscribe", "symbols": [sym], "_ack": fut2})
                await asyncio.wait_for(fut2, timeout=5.0)

            new_n = len(new_layout)

            while len(self._shard_queues) < new_n:
                self._shard_queues.append(asyncio.Queue())

            if new_n > old_n:
                for j in range(old_n, new_n):
                    shard_syms = new_layout[j]
                    q = self._shard_queues[j]
                    t = asyncio.create_task(
                        self._shard_worker(j, shard_syms, q, stop),
                        name=f"l2-ws-{j}",
                    )
                    self._shard_tasks.append(t)

            if new_n < old_n:
                for j in range(new_n, old_n):
                    syms = layout_snap[j] if j < len(layout_snap) else ()
                    if syms:
                        fut3: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                        await self._shard_queues[j].put({"action": "unsubscribe", "symbols": list(syms), "_ack": fut3})
                        await asyncio.wait_for(fut3, timeout=5.0)
                    fut4: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                    await self._shard_queues[j].put({"action": "_shutdown", "_ack": fut4})
                    await asyncio.wait_for(fut4, timeout=5.0)
                    self._shard_tasks[j].cancel()
                await asyncio.gather(*self._shard_tasks[new_n:], return_exceptions=True)
                self._shard_tasks = self._shard_tasks[:new_n]
                self._shard_queues = self._shard_queues[:new_n]

            self._shard_layout = list(new_layout)

    async def rebind_shards(self, processors: Mapping[str, AbstractOrderBookProcessor], stop: asyncio.Event) -> None:
        """Prefer `hot_swap_processors` — keeps TCP when shard topology allows."""
        await self.hot_swap_processors(processors, stop)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None),
                auto_decompress=True,
            )
        return self._session

    async def _shard_worker(
        self,
        shard_id: int,
        initial_symbols: tuple[str, ...],
        cmd_queue: asyncio.Queue[dict[str, Any]],
        stop: asyncio.Event,
    ) -> None:
        if not initial_symbols and stop.is_set():
            return
        session = await self._ensure_session()
        while not stop.is_set():
            await self._bucket.acquire(1.0)
            try:
                ws = await session.ws_connect(self._ws_url, heartbeat=20.0, autoping=True)
            except Exception as exc:
                logger.warning("L2 shard {} connect failed: {!r}", shard_id, exc)
                continue
            topics = [f"orderbook.{self._depth}.{s}" for s in initial_symbols]
            try:
                if topics:
                    await ws.send_bytes(orjson.dumps({"op": "subscribe", "args": topics}))
            except Exception as exc:
                logger.error("L2 shard {} subscribe failed: {!r}", shard_id, exc)
                await ws.close()
                continue

            try:
                while not stop.is_set():
                    recv_task = asyncio.create_task(ws.receive())
                    cmd_task = asyncio.create_task(cmd_queue.get())
                    done, _pending = await asyncio.wait(
                        {recv_task, cmd_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if recv_task in done:
                        cmd_task.cancel()
                        try:
                            await cmd_task
                        except asyncio.CancelledError:
                            pass
                        try:
                            msg = recv_task.result()
                        except Exception as exc:
                            logger.warning("L2 shard {} recv error: {!r}", shard_id, exc)
                            break
                    else:
                        recv_task.cancel()
                        try:
                            await recv_task
                        except asyncio.CancelledError:
                            pass
                        cmd = cmd_task.result()
                        if await self._apply_shard_command(ws, cmd) == "stop":
                            break
                        continue

                    if stop.is_set():
                        break
                    if msg.type == aiohttp.WSMsgType.CLOSE or msg.type == aiohttp.WSMsgType.CLOSED:
                        break
                    if msg.type == aiohttp.WSMsgType.ERROR:
                        break
                    if msg.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        continue
                    raw = msg.data
                    try:
                        payload = orjson.loads(raw)
                    except orjson.JSONDecodeError as exc:
                        logger.error("L2 shard {} decode error: {!r}", shard_id, exc)
                        continue

                    if payload.get("op") == "subscribe":
                        continue
                    topic = str(payload.get("topic", ""))
                    if not topic.startswith("orderbook."):
                        continue
                    msg_type = str(payload.get("type", "")).lower()
                    rows = payload.get("data")
                    if not isinstance(rows, list) or not rows:
                        continue
                    first = rows[0]
                    if not isinstance(first, dict):
                        continue
                    sym = str(first.get("s", "")).strip().upper()
                    proc = self._processors.get(sym)
                    if proc is None:
                        continue
                    self._ingest_ws_payload(sym, proc, msg_type, first)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("L2 shard {} loop error: {!r}", shard_id, exc)
            finally:
                # ── Connection Epoch Invalidation (Beazley + Kleppmann) ──
                # 1. Bump epoch: all resync entries from this generation become stale
                self._connection_epoch += 1
                # 2. Reject all pending ACK futures: UniverseManager must not deadlock
                self._purge_pending_acks(f"shard {shard_id} TCP lost, epoch={self._connection_epoch}")
                # 3. Drain stale resync queue entries
                self._purge_resync_queue()
                # 4. Mark all books for this shard as DESYNCED
                for sym in initial_symbols:
                    if sym in self._states:
                        self._states[sym] = BookState.DESYNCED
                logger.warning(
                    "L2 shard {} epoch bumped to {} — all books DESYNCED, acks purged",
                    shard_id, self._connection_epoch,
                )
                await ws.close()

    async def run(self, stop: asyncio.Event) -> None:
        empty = False
        async with self._lifecycle:
            shards = _chunk_symbols(tuple(sorted(self._processors.keys())), int(self._chunk_size))
            if not shards:
                logger.warning("L2OrderBookWebSocketMultiplexer: empty processor map")
                empty = True
            else:
                self._shard_layout = list(shards)
                self._shard_queues = [asyncio.Queue() for _ in shards]
                self._shard_tasks = [
                    asyncio.create_task(self._shard_worker(i, shard, self._shard_queues[i], stop), name=f"l2-ws-{i}")
                    for i, shard in enumerate(shards)
                ]
                if self._resync is not None:
                    self._resync_pump_task = asyncio.create_task(self._resync_pump(stop), name="l2-resync-pump")
        if empty:
            await stop.wait()
            return
        try:
            await stop.wait()
        finally:
            async with self._lifecycle:
                if self._resync_pump_task is not None:
                    self._resync_pump_task.cancel()
                    await asyncio.gather(self._resync_pump_task, return_exceptions=True)
                    self._resync_pump_task = None
                for t in self._shard_tasks:
                    t.cancel()
                await asyncio.gather(*self._shard_tasks, return_exceptions=True)
                self._shard_tasks.clear()
                self._shard_queues.clear()
                self._shard_layout.clear()
                if self._session_owner and self._session is not None and not self._session.closed:
                    await self._session.close()
                    self._session = None
