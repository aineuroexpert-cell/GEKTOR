"""Abstract interfaces for WS ↔ order-book wiring (test doubles / mocks)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence


class AbstractOrderBookResyncSource(ABC):
    """REST anchor for L2 recovery (async I/O only; never mutates the book)."""

    @abstractmethod
    async def fetch_linear_orderbook(
        self,
        symbol: str,
        *,
        limit: int,
    ) -> tuple[int, list[tuple[int, int]], list[tuple[int, int]]]:
        """Return `(u, bids, asks)` with scaled ints from `v5/market/orderbook` (linear)."""


class AbstractOrderBookProcessor(ABC):
    """Contract consumed by the public WS multiplexer (ingest only, no I/O)."""

    @property
    @abstractmethod
    def symbol(self) -> str:
        """Linear contract symbol, e.g. BTCUSDT."""

    @abstractmethod
    def ingest_snapshot(
        self,
        update_id: int,
        bids: Sequence[tuple[int, int]],
        asks: Sequence[tuple[int, int]],
        *,
        seq: int | None = None,
    ) -> None:
        """Replace book state; `update_id` is exchange crossing id `u` after snapshot."""

    @abstractmethod
    def ingest_delta(
        self,
        update_id: int,
        bids: Sequence[tuple[int, int]],
        asks: Sequence[tuple[int, int]],
        *,
        range_start: int | None = None,
        seq: int | None = None,
    ) -> bool:
        """
        Apply delta rows (price scaled, qty scaled; qty==0 removes).
        `range_start` maps to Bybit `U` (start id); `seq` is the monotonic feed sequence when present.
        Returns False if ignored (stale/duplicate, gap vs `U`, or regressed `seq`).
        """

    @abstractmethod
    def last_update_id(self) -> int:
        """Last applied crossing sequence id (`u`)."""
