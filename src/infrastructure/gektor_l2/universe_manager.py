"""Dynamic linear universe from Bybit v5/market/tickers + atomic JSON snapshot."""

from __future__ import annotations

import asyncio
import os
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

import aiohttp
import orjson
from loguru import logger

from src.infrastructure.gektor_l2.constants import MIN_TURNOVER_USDT, SCALE
from src.infrastructure.gektor_l2.nd_orderbook import NdOrderBookStateMachine
from src.infrastructure.gektor_l2.scaling import decimal_from_str, to_scaled_int


def _atomic_write_json_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".active_universe.", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        os.replace(str(tmp_path), str(path))
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    symbol: str
    tick_size_scaled: int
    qty_step_scaled: int
    min_order_qty_scaled: int
    turnover24h_usd: Decimal
    status: str


@dataclass(frozen=True, slots=True)
class ActiveUniverse:
    instruments: tuple[InstrumentSpec, ...]
    raw_ret_code: int
    raw_ret_msg: str


class DynamicUniverseManager:
    """
    Loads `v5/market/tickers?category=linear`, filters `Trading` and turnover,
    persists atomically to JSON for crash-safe restarts.
    """

    def __init__(self, output_path: Path | None = None) -> None:
        self._output_path = output_path or Path("artifacts/active_universe.json")

    @staticmethod
    def _parse_instrument(row: Mapping[str, Any]) -> InstrumentSpec | None:
        status = str(row.get("status", ""))
        if status != "Trading":
            return None
        turnover_raw = row.get("turnover24h")
        if turnover_raw is None:
            return None
        turnover = decimal_from_str(turnover_raw)
        if turnover <= Decimal(MIN_TURNOVER_USDT):
            return None
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            return None
        lot = row.get("lotSizeFilter") or {}
        price = row.get("priceFilter") or {}
        tick = to_scaled_int(decimal_from_str(price.get("tickSize", "0")))
        qstep = to_scaled_int(decimal_from_str(lot.get("qtyStep", "0")))
        min_q = to_scaled_int(decimal_from_str(lot.get("minOrderQty", "0")))
        if tick <= 0 or qstep <= 0 or min_q <= 0:
            logger.warning("Universe skip {}: invalid filters tick={} step={} min={}", symbol, tick, qstep, min_q)
            return None
        return InstrumentSpec(
            symbol=symbol,
            tick_size_scaled=tick,
            qty_step_scaled=qstep,
            min_order_qty_scaled=min_q,
            turnover24h_usd=turnover,
            status=status,
        )

    async def fetch_universe(
        self,
        session: aiohttp.ClientSession,
        *,
        base_url: str = "https://api.bybit.com",
    ) -> ActiveUniverse:
        url = f"{base_url.rstrip('/')}/v5/market/tickers"
        params = {"category": "linear"}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            raw = await resp.read()
        payload = orjson.loads(raw)
        ret_code = int(payload.get("retCode", -1))
        ret_msg = str(payload.get("retMsg", ""))
        result = payload.get("result") or {}
        rows: Sequence[Mapping[str, Any]] = result.get("list") or []
        instruments: list[InstrumentSpec] = []
        for row in rows:
            spec = self._parse_instrument(row)
            if spec is not None:
                instruments.append(spec)
        instruments.sort(key=lambda s: s.symbol)
        return ActiveUniverse(
            instruments=tuple(instruments),
            raw_ret_code=ret_code,
            raw_ret_msg=ret_msg,
        )

    @staticmethod
    def serialize(universe: ActiveUniverse) -> dict[str, Any]:
        return {
            "scale": SCALE,
            "retCode": universe.raw_ret_code,
            "retMsg": universe.raw_ret_msg,
            "symbols": [i.symbol for i in universe.instruments],
            "instruments": {
                i.symbol: {
                    "tick_size_scaled": i.tick_size_scaled,
                    "qty_step_scaled": i.qty_step_scaled,
                    "min_order_qty_scaled": i.min_order_qty_scaled,
                    "turnover24h_usd": str(i.turnover24h_usd),
                    "status": i.status,
                }
                for i in universe.instruments
            },
        }

    async def refresh_and_persist(
        self,
        session: aiohttp.ClientSession,
        *,
        base_url: str = "https://api.bybit.com",
    ) -> ActiveUniverse:
        universe = await self.fetch_universe(session, base_url=base_url)
        blob = orjson.dumps(self.serialize(universe))
        out = self._output_path.resolve()
        await asyncio.to_thread(_atomic_write_json_bytes, out, blob)
        logger.info(
            "Active universe persisted: {} symbols (retCode={}) -> {}",
            len(universe.instruments),
            universe.raw_ret_code,
            out,
        )
        return universe


def load_universe_books(
    path: Path,
    *,
    max_levels: int = 8192,
) -> dict[str, NdOrderBookStateMachine]:
    """
    Build one `NdOrderBookStateMachine` per symbol from persisted `active_universe.json`.
    Used at radar startup; pair with `L2OrderBookWebSocketMultiplexer.hot_swap_processors(processors, stop)`.
    """
    blob = path.read_bytes()
    data = orjson.loads(blob)
    instruments = data.get("instruments") or {}
    out: dict[str, NdOrderBookStateMachine] = {}
    for sym in sorted(instruments.keys()):
        ss = str(sym).strip().upper()
        if not ss:
            continue
        out[ss] = NdOrderBookStateMachine(ss, max_levels=int(max_levels))
    return out
