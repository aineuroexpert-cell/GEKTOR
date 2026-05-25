"""REST snapshot source for linear orderbook (v5/market/orderbook)."""

from __future__ import annotations

from typing import Final

import aiohttp
import orjson

from src.infrastructure.gektor_l2.errors import BybitRestRateLimited
from src.infrastructure.gektor_l2.protocols import AbstractOrderBookResyncSource
from src.infrastructure.gektor_l2.wire_parse import parse_levels

_BYBIT_RATE_LIMIT_RETCODES: frozenset[int] = frozenset({10006, 10018, 10024})


class BybitLinearOrderbookRestSource(AbstractOrderBookResyncSource):
    def __init__(self, session: aiohttp.ClientSession, *, base_url: str = "https://api.bybit.com") -> None:
        self._session: Final[aiohttp.ClientSession] = session
        self._base: Final[str] = base_url.rstrip("/")

    async def fetch_linear_orderbook(
        self,
        symbol: str,
        *,
        limit: int,
    ) -> tuple[int, list[tuple[int, int]], list[tuple[int, int]]]:
        sym = symbol.strip().upper()
        url = f"{self._base}/v5/market/orderbook"
        params = {"category": "linear", "symbol": sym, "limit": str(int(limit))}
        async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 429:
                raise BybitRestRateLimited("http_429")
            raw = await resp.read()
        payload = orjson.loads(raw)
        ret_code = int(payload.get("retCode", -1))
        if ret_code in _BYBIT_RATE_LIMIT_RETCODES:
            raise BybitRestRateLimited(f"retCode={ret_code}")
        if ret_code != 0:
            raise RuntimeError(f"Bybit orderbook retCode={ret_code} msg={payload.get('retMsg')}")
        result = payload.get("result") or {}
        if not result:
            raise RuntimeError("empty orderbook result")
        u = int(result.get("u", 0))
        bids = parse_levels(result.get("b"))
        asks = parse_levels(result.get("a"))
        return u, bids, asks
