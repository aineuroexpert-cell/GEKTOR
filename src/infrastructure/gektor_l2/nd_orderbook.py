"""Numpy-backed L2 state machine (no dict levels); scaled int64 prices/qty."""

from __future__ import annotations

import time
import collections
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, Sequence

import numpy as np
from loguru import logger

from src.infrastructure.gektor_l2.book_state import BookReadiness, BookState
from src.infrastructure.gektor_l2.constants import SCALE, TAKER_FEE_DENOMINATOR, TAKER_FEE_NUMERATOR
from src.infrastructure.gektor_l2.protocols import AbstractOrderBookProcessor


import ctypes

CACHE_LINE_SIZE = 64
BUFFER_CAPACITY = 65536  # Strictly 2^16
BUFFER_MASK = BUFFER_CAPACITY - 1

class SHMDeltaLevel(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("price", ctypes.c_int64),
        ("volume", ctypes.c_int64)
    ]

class SHMDeltaFrame(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("u_id", ctypes.c_uint64),
        ("timestamp", ctypes.c_int64),
        ("range_start", ctypes.c_int64), # -1 if None
        ("seq", ctypes.c_uint64),        # 0 if None
        ("bid_count", ctypes.c_uint32),
        ("ask_count", ctypes.c_uint32),
        ("bids", SHMDeltaLevel * 50),
        ("asks", SHMDeltaLevel * 50),
        ("_pad", ctypes.c_byte * 24)      # Pad to align to 64 bytes
    ]

class SHMDeltaRingBuffer(ctypes.Structure):
    """
    Zero-Copy Ring Buffer aligned to L1 Cache Lines.
    """
    _pack_ = CACHE_LINE_SIZE
    _fields_ = [
        # Writer cache line
        ("head", ctypes.c_uint64),
        ("_pad1", ctypes.c_byte * (CACHE_LINE_SIZE - 8)),
        
        # Reader cache line
        ("tail", ctypes.c_uint64),
        ("_pad2", ctypes.c_byte * (CACHE_LINE_SIZE - 8)),
        
        # Data block
        ("frames", SHMDeltaFrame * BUFFER_CAPACITY)
    ]

    def fast_forward_splice(self, snapshot_u_id: int) -> bool:
        """O(1) or O(N_buffered) check of state splice feasibility"""
        if self.head == self.tail:
            return False
            
        current_tail_u_id = self.frames[self.tail & BUFFER_MASK].u_id
        current_head_u_id = self.frames[(self.head - 1) & BUFFER_MASK].u_id
        
        if snapshot_u_id < current_tail_u_id:
            # Snapshot too old, data already dropped by Tail Drop
            return False
        if snapshot_u_id > current_head_u_id:
            # Snapshot from the future, missing data in transit
            return False
            
        # Scan from tail to head to find the frame matching snapshot_u_id
        curr = self.tail
        limit = self.head
        found = False
        while curr != limit:
            idx = curr & BUFFER_MASK
            if self.frames[idx].u_id >= snapshot_u_id:
                # If we found the exact sync point or the next frame
                self.tail = curr
                found = True
                break
            curr += 1
            
        return found

class NdOrderBookStateMachine(AbstractOrderBookProcessor):
    """
    Fixed-capacity sides; bids stored as ascending `-price` for `np.searchsorted`.
    Asks stored as ascending `price`.
    Ingest/read run in the asyncio event loop: synchronous code between awaits is atomic;
    `read_epoch` supports OCC across awaits in the Signal Engine.
    """

    __slots__ = (
        "_symbol",
        "_max_levels",
        "_max_age_sec",
        "_u",
        "_last_seq",
        "_last_reject",
        "_consistency_ok",
        "_read_epoch",
        "_last_update_mono",
        "_bid_neg",
        "_bid_q",
        "_bid_n",
        "_ask_p",
        "_ask_q",
        "_ask_n",
        "_delta_buffer",
        "_survival_lut",
        "_ask_ctr_fp",
        "_ask_lambda_fp",
        "_bid_ctr_fp",
        "_bid_lambda_fp",
    )

    def __init__(self, symbol: str, *, max_levels: int = 8192, max_age_ms: float = 2500.0) -> None:
        if max_levels <= 2:
            raise ValueError("max_levels too small")
        self._symbol: Final[str] = symbol.upper()
        self._max_levels: Final[int] = int(max_levels)
        self._max_age_sec: Final[float] = float(max_age_ms) / 1000.0
        self._u: int = 0
        self._last_seq: int = 0
        self._last_reject: str | None = None
        self._consistency_ok: bool = False
        self._read_epoch: int = 0
        self._last_update_mono: float = 0.0
        self._bid_neg = np.zeros(self._max_levels, dtype=np.int64)
        self._bid_q = np.zeros(self._max_levels, dtype=np.int64)
        self._bid_n: int = 0
        self._ask_p = np.zeros(self._max_levels, dtype=np.int64)
        self._ask_q = np.zeros(self._max_levels, dtype=np.int64)
        self._ask_n: int = 0
        self._delta_buffer = SHMDeltaRingBuffer()
        self._delta_buffer.head = 0
        self._delta_buffer.tail = 0

        # Fixed-Point L2-LMPM structures
        self._survival_lut = (ctypes.c_uint16 * 1024)()
        self._ask_ctr_fp = (ctypes.c_uint16 * self._max_levels)()
        self._ask_lambda_fp = (ctypes.c_uint16 * self._max_levels)()
        self._bid_ctr_fp = (ctypes.c_uint16 * self._max_levels)()
        self._bid_lambda_fp = (ctypes.c_uint16 * self._max_levels)()
        self._precompute_lut(delta_t=1.2)

    def _precompute_lut(self, delta_t: float = 1.2) -> None:
        """Background precomputation of exp decay. Never called on the Hot Path."""
        for i in range(1024):
            real_prob = float(np.exp(-(i / 100.0) * delta_t))
            self._survival_lut[i] = int(real_prob * 1024)

    def update_pessimisation_parameters(self, delta_t: float) -> None:
        """Background parameter update. Never called on the hot path."""
        self._precompute_lut(delta_t=delta_t)

    def recalculate_pessimisation_factors(
        self,
        alpha_0: float = 1.0,
        beta: float = 0.5,
        gamma: float = 10.0,
    ) -> None:
        """
        Background calculation of CTR and Lambda fixed-point values.
        Run every 10s or per macro-regime shift. Offloaded from hot path.
        """
        if not self._consistency_ok or self._ask_n == 0 or self._bid_n == 0:
            return

        best_bid = -int(self._bid_neg[0])
        best_ask = int(self._ask_p[0])
        mid_price = (best_bid + best_ask) / 2.0
        if mid_price <= 0:
            return

        v_bid_0 = int(self._bid_q[0])
        v_ask_0 = int(self._ask_q[0])
        denom = v_bid_0 + v_ask_0
        bbo_imbalance = (v_bid_0 - v_ask_0) / denom if denom > 0 else 0.0
        sign_imbalance = np.sign(bbo_imbalance)

        # Recalculate asks
        for k in range(self._ask_n):
            price = int(self._ask_p[k])
            ctr = 0.3 * float(np.exp(-k / 10.0))
            self._ask_ctr_fp[k] = int(ctr * 1024)

            d_k = abs(price - mid_price) / mid_price
            f_dk = 1.0 / (1.0 + gamma * d_k)
            g_bbo = 1.0 + beta * sign_imbalance * 1.0
            
            lam = alpha_0 * f_dk * g_bbo
            self._ask_lambda_fp[k] = min(1023, max(0, int(lam * 100.0)))

        # Recalculate bids
        for k in range(self._bid_n):
            price = -int(self._bid_neg[k])
            ctr = 0.3 * float(np.exp(-k / 10.0))
            self._bid_ctr_fp[k] = int(ctr * 1024)

            d_k = abs(price - mid_price) / mid_price
            f_dk = 1.0 / (1.0 + gamma * d_k)
            g_bbo = 1.0 + beta * sign_imbalance * (-1.0)

            lam = alpha_0 * f_dk * g_bbo
            self._bid_lambda_fp[k] = min(1023, max(0, int(lam * 100.0)))

    @property
    def symbol(self) -> str:
        return self._symbol

    def last_update_id(self) -> int:
        return int(self._u)

    @property
    def last_reject_reason(self) -> str | None:
        return self._last_reject

    @property
    def read_epoch(self) -> int:
        """Bumps on snapshot and on hard invalidation — OCC vs Signal Engine across awaits."""
        return int(self._read_epoch)

    @property
    def is_consistent(self) -> bool:
        """False during gaps / before first anchored snapshot — MSQ must not trade on this."""
        return bool(self._consistency_ok) and self._u > 0 and self._bid_n > 0 and self._ask_n > 0

    @property
    def last_update_mono(self) -> float:
        """Monotonic timestamp of last applied snapshot/delta — for staleness checks."""
        return self._last_update_mono

    @property
    def data_age_sec(self) -> float:
        """Seconds since last applied L2 update. Inf if never updated."""
        if self._last_update_mono == 0.0:
            return float("inf")
        return time.monotonic() - self._last_update_mono

    def readiness(
        self,
        book_state: BookState,
        *,
        circuit_breaker_open: bool = False,
    ) -> BookReadiness:
        """
        Unambiguous radar status for Signal Engine.
        Synthesizes FOUR orthogonal signals (Kleppmann + López de Prado):

          1. Infrastructure: Circuit Breaker blocks REST recovery?
          2. WS connection: BookState from multiplexer (SYNCED/DESYNCED/RECOVERING)
          3. Internal consistency: anchored snapshot with valid u-ID?
          4. Temporal freshness: data_age < max_age_sec? (Silent Stale State defense)

        Returns BookReadiness that the Signal Engine MUST branch on:
          - BLIND_NETWORK:   infrastructure failure (CB / DESYNCED) — suppress signal
          - BLIND_STALE:     data is old, exchange Kafka bridge may be frozen — TOXIC
          - RECOVERING:      REST resync in-flight — data is stale, suppress signal
          - EMPTY_BUT_VALID: genuinely thin market (alpha logic decides)
          - READY:           fresh, consistent, has depth — MSQ is trustworthy
        """
        # Priority 1: Circuit Breaker kills all trust
        if circuit_breaker_open:
            return BookReadiness.BLIND_NETWORK

        # Priority 2: WS connection state
        if book_state == BookState.DESYNCED:
            return BookReadiness.BLIND_NETWORK
        if book_state == BookState.RECOVERING:
            return BookReadiness.RECOVERING

        # Priority 3: Internal consistency (even if WS says SYNCED, book may lack anchor)
        if not self._consistency_ok or self._u == 0:
            return BookReadiness.RECOVERING

        # Priority 4: Temporal Watchdog — Silent Stale State defense
        # TCP alive, ping/pong OK, BookState SYNCED, consistency_ok — BUT
        # exchange Kafka bridge for THIS SPECIFIC symbol may be frozen.
        # "Absence of events is also an event." (Kleppmann)
        if self._last_update_mono > 0.0:
            age = time.monotonic() - self._last_update_mono
            if age > self._max_age_sec:
                return BookReadiness.BLIND_STALE

        # Priority 5: Book is consistent, fresh, and synced — check depth
        if self._bid_n <= 0 or self._ask_n <= 0:
            # We have a valid snapshot with zero depth on one side.
            # This is a REAL market condition, not a data gap.
            return BookReadiness.EMPTY_BUT_VALID

        return BookReadiness.READY

    def _hard_reset(self) -> None:
        self._bid_n = 0
        self._ask_n = 0
        self._bid_neg.fill(0)
        self._bid_q.fill(0)
        self._ask_p.fill(0)
        self._ask_q.fill(0)
        self._u = 0
        self._last_seq = 0
        self._consistency_ok = False
        self._read_epoch += 1
        self._delta_buffer.head = 0
        self._delta_buffer.tail = 0

    def ingest_snapshot(
        self,
        update_id: int,
        bids: Sequence[tuple[int, int]],
        asks: Sequence[tuple[int, int]],
        *,
        seq: int | None = None,
    ) -> None:
        _ = seq
        self._last_reject = None
        self._bid_n = 0
        self._ask_n = 0
        self._fill_side_from_snapshot(
            bids,
            self._bid_neg,
            self._bid_q,
            key_neg=True,
        )
        self._fill_side_from_snapshot(
            asks,
            self._ask_p,
            self._ask_q,
            key_neg=False,
        )
        self._u = int(update_id)
        self._last_seq = 0
        self._consistency_ok = True
        self._last_update_mono = time.monotonic()
        self._read_epoch += 1
        
        # [SEQUENCE SYNC] Быстрая склейка дельт через fast_forward_splice
        # Пытаемся перемотать tail к точке синхронизации снапшота
        self._delta_buffer.fast_forward_splice(self._u)
        
        # Применяем все оставшиеся отложенные дельты, которые новее снапшота
        while self._delta_buffer.tail != self._delta_buffer.head:
            frame = self._delta_buffer.frames[self._delta_buffer.tail & BUFFER_MASK]
            self._delta_buffer.tail += 1
            
            d_u = frame.u_id
            if d_u <= self._u:
                continue
            
            d_lo = frame.range_start if frame.range_start != -1 else None
            d_seq = frame.seq if frame.seq != 0 else None
            
            # Проверка разрыва
            if d_lo is not None and self._u > 0 and d_lo > self._u + 1:
                self._last_reject = "sequence_gap"
                self._hard_reset()
                break
                
            for i in range(frame.bid_count):
                self._apply_row_bid(frame.bids[i].price, frame.bids[i].volume)
            for i in range(frame.ask_count):
                self._apply_row_ask(frame.asks[i].price, frame.asks[i].volume)
                
            self._u = d_u
            if d_seq is not None:
                self._last_seq = int(d_seq)
        
        # Обновляем факторы пессимизации при получении новой опорной точки стакана
        self.recalculate_pessimisation_factors()

    def _fill_side_from_snapshot(
        self,
        rows: Sequence[tuple[int, int]],
        px_buf: np.ndarray,
        q_buf: np.ndarray,
        *,
        key_neg: bool,
    ) -> None:
        n_in = len(rows)
        if n_in == 0:
            return
        tmp_px = np.empty(min(n_in, self._max_levels), dtype=np.int64)
        tmp_q = np.empty_like(tmp_px)
        k = 0
        for price, qty in rows:
            if qty <= 0:
                continue
            key = -int(price) if key_neg else int(price)
            tmp_px[k] = key
            tmp_q[k] = int(qty)
            k += 1
            if k >= self._max_levels:
                break
        if k == 0:
            return
        view_px = tmp_px[:k]
        view_q = tmp_q[:k]
        order = np.argsort(view_px, kind="mergesort")
        sorted_px = view_px[order]
        sorted_q = view_q[order]
        limit = min(k, self._max_levels)
        px_buf[:limit] = sorted_px[:limit]
        q_buf[:limit] = sorted_q[:limit]
        if key_neg:
            self._bid_n = int(limit)
        else:
            self._ask_n = int(limit)

    def ingest_delta(
        self,
        update_id: int,
        bids: Sequence[tuple[int, int]],
        asks: Sequence[tuple[int, int]],
        *,
        range_start: int | None = None,
        seq: int | None = None,
    ) -> bool:
        if self._u == 0:
            # [SEQUENCE SYNC] Буферизация до получения снапшота
            head = self._delta_buffer.head
            tail = self._delta_buffer.tail
            # Tail Drop: if full, advance tail to overwrite oldest frame
            if head - tail >= BUFFER_CAPACITY:
                self._delta_buffer.tail = tail + 1
            
            frame = self._delta_buffer.frames[head & BUFFER_MASK]
            frame.u_id = int(update_id)
            frame.range_start = int(range_start) if range_start is not None else -1
            frame.seq = int(seq) if seq is not None else 0
            
            bid_count = min(len(bids), 50)
            frame.bid_count = bid_count
            for i in range(bid_count):
                frame.bids[i].price = int(bids[i][0])
                frame.bids[i].volume = int(bids[i][1])
                
            ask_count = min(len(asks), 50)
            frame.ask_count = ask_count
            for i in range(ask_count):
                frame.asks[i].price = int(asks[i][0])
                frame.asks[i].volume = int(asks[i][1])
                
            self._delta_buffer.head = head + 1
            self._last_reject = "no_snapshot_anchor"
            return False

        new_u = int(update_id)
        u_lo: int | None = int(range_start) if range_start is not None else None

        if u_lo is not None and new_u < u_lo:
            self._last_reject = "invalid_u_range"
            self._hard_reset()
            return False

        if new_u <= self._u:
            self._last_reject = "stale_or_duplicate_u"
            return False

        if u_lo is not None and self._u > 0 and u_lo > self._u + 1:
            self._last_reject = "sequence_gap"
            self._hard_reset()
            return False

        if seq is not None:
            s_val = int(seq)
            if self._last_seq > 0 and s_val <= self._last_seq:
                self._last_reject = "stale_or_duplicate_seq"
                return False

        self._last_reject = None
        for price, qty in bids:
            self._apply_row_bid(int(price), int(qty))
        for price, qty in asks:
            self._apply_row_ask(int(price), int(qty))
        self._u = new_u
        self._last_update_mono = time.monotonic()
        if seq is not None:
            self._last_seq = int(seq)
        return True

    def _apply_row_bid(self, price: int, qty: int) -> None:
        key = -price
        idx_l = int(np.searchsorted(self._bid_neg[: self._bid_n], key, side="left"))
        exists = idx_l < self._bid_n and int(self._bid_neg[idx_l]) == key
        if qty <= 0:
            if not exists:
                return
            self._remove_bid_index(idx_l)
            return
        if exists:
            self._bid_q[idx_l] = qty
            return
        self._insert_bid_index(idx_l, key, qty)

    def _apply_row_ask(self, price: int, qty: int) -> None:
        idx_l = int(np.searchsorted(self._ask_p[: self._ask_n], price, side="left"))
        exists = idx_l < self._ask_n and int(self._ask_p[idx_l]) == price
        if qty <= 0:
            if not exists:
                return
            self._remove_ask_index(idx_l)
            return
        if exists:
            self._ask_q[idx_l] = qty
            return
        self._insert_ask_index(idx_l, price, qty)

    def _insert_bid_index(self, idx: int, key: int, qty: int) -> None:
        if self._bid_n >= self._max_levels:
            logger.error("NdOrderBook {}: bid capacity exceeded ({})", self._symbol, self._max_levels)
            return
        n = self._bid_n
        if idx < n:
            self._bid_neg[idx + 1 : n + 1] = self._bid_neg[idx:n]
            self._bid_q[idx + 1 : n + 1] = self._bid_q[idx:n]
        self._bid_neg[idx] = key
        self._bid_q[idx] = qty
        self._bid_n = n + 1

    def _remove_bid_index(self, idx: int) -> None:
        n = self._bid_n
        if idx >= n:
            return
        if idx < n - 1:
            self._bid_neg[idx : n - 1] = self._bid_neg[idx + 1 : n]
            self._bid_q[idx : n - 1] = self._bid_q[idx + 1 : n]
        self._bid_n = n - 1
        self._bid_neg[n - 1] = 0
        self._bid_q[n - 1] = 0

    def _insert_ask_index(self, idx: int, price: int, qty: int) -> None:
        if self._ask_n >= self._max_levels:
            logger.error("NdOrderBook {}: ask capacity exceeded ({})", self._symbol, self._max_levels)
            return
        n = self._ask_n
        if idx < n:
            self._ask_p[idx + 1 : n + 1] = self._ask_p[idx:n]
            self._ask_q[idx + 1 : n + 1] = self._ask_q[idx:n]
        self._ask_p[idx] = price
        self._ask_q[idx] = qty
        self._ask_n = n + 1

    def _remove_ask_index(self, idx: int) -> None:
        n = self._ask_n
        if idx >= n:
            return
        if idx < n - 1:
            self._ask_p[idx : n - 1] = self._ask_p[idx + 1 : n]
            self._ask_q[idx : n - 1] = self._ask_q[idx + 1 : n]
        self._ask_n = n - 1
        self._ask_p[n - 1] = 0
        self._ask_q[n - 1] = 0

    def get_cumulative_depth(self, depth_bps: int = 15) -> tuple[int, int] | None:
        """
        USD-notional (scaled 1e8) within ±`depth_bps` of mid.
        Returns None if the book is not consistency-gated (DIRTY / no anchor).
        """
        if not self._consistency_ok or self._bid_n <= 0 or self._ask_n <= 0:
            return None
        best_bid = int(-self._bid_neg[0])
        best_ask = int(self._ask_p[0])
        mid = (best_bid + best_ask) // 2
        half = (mid * int(depth_bps)) // 10_000
        lim_lo = mid - half
        lim_hi = mid + half

        ask_usd = 0
        i = 0
        while i < self._ask_n:
            ap = int(self._ask_p[i])
            if ap > lim_hi:
                break
            aq = int(self._ask_q[i])
            ask_usd += (ap * aq) // SCALE
            i += 1

        bid_usd = 0
        j = 0
        while j < self._bid_n:
            bp = int(-self._bid_neg[j])
            if bp < lim_lo:
                break
            bq = int(self._bid_q[j])
            bid_usd += (bp * bq) // SCALE
            j += 1

        return bid_usd, ask_usd

    def calculate_msq(self, target_usd_scaled: int) -> tuple[int, int] | None:
        """None if book is invalidated or not yet anchored — do not trade."""
        if not self._consistency_ok or target_usd_scaled <= 0 or self._ask_n <= 0:
            return None
        return self._calculate_msq_unlocked(int(target_usd_scaled))

    def try_occ_msq(self, target_usd_scaled: int) -> tuple[tuple[int, int], int] | None:
        """
        OCC helper: returns `((qty, avg_px), read_epoch)` for the Signal Engine to compare
        after its own awaits (same-tick ingest is already atomic without threading locks).
        """
        if not self._consistency_ok or target_usd_scaled <= 0 or self._ask_n <= 0 or self._u == 0:
            return None
        ep = int(self._read_epoch)
        out = self._calculate_msq_unlocked(int(target_usd_scaled))
        if out is None or out == (0, 0):
            return None
        return (out, ep)

    def _calculate_msq_unlocked(self, target_usd_scaled: int) -> tuple[int, int] | None:
        remaining: int = int(target_usd_scaled)
        total_qty: int = 0
        total_notional: int = 0
        idx = 0
        while idx < self._ask_n and remaining > 0:
            p = int(self._ask_p[idx])
            q_raw = int(self._ask_q[idx])
            if q_raw <= 0:
                idx += 1
                continue

            # Hot-Path L2-LMPM Fixed-Point Pessimisation (No allocations, no float math)
            surviving_ctr_factor = 1024 - int(self._ask_ctr_fp[idx])
            lut_idx = int(self._ask_lambda_fp[idx])
            decay_factor = int(self._survival_lut[lut_idx])
            q = (q_raw * surviving_ctr_factor * decay_factor) >> 20

            if q <= 0:
                idx += 1
                continue

            level_notional = (p * q) // SCALE
            fee = (level_notional * TAKER_FEE_NUMERATOR) // TAKER_FEE_DENOMINATOR
            pay = level_notional + fee
            if pay <= remaining:
                remaining -= pay
                total_qty += q
                total_notional += level_notional
                idx += 1
                continue
            lo = 0
            hi = q
            best = 0
            while lo <= hi:
                midq = (lo + hi) // 2
                n_partial = (p * midq) // SCALE
                f_partial = (n_partial * TAKER_FEE_NUMERATOR) // TAKER_FEE_DENOMINATOR
                if n_partial + f_partial <= remaining:
                    best = midq
                    lo = midq + 1
                else:
                    hi = midq - 1
            if best <= 0:
                break
            n_take = (p * best) // SCALE
            f_take = (n_take * TAKER_FEE_NUMERATOR) // TAKER_FEE_DENOMINATOR
            total_qty += best
            total_notional += n_take
            remaining -= n_take + f_take
            break

        if total_qty <= 0:
            return None
        avg_px = (total_notional * SCALE) // total_qty
        return total_qty, avg_px


@dataclass(frozen=True, slots=True)
class _BookSlice:
    """Immutable point-in-time snapshot of a single book's MSQ + metadata."""
    symbol: str
    readiness: BookReadiness
    epoch: int
    msq: tuple[int, int] | None
    data_age_sec: float


@dataclass(frozen=True, slots=True)
class CrossAssetSnapshot:
    """
    Atomic cross-asset OCC snapshot for Protocol 2 (Leader-Follower / StatArb).

    PROBLEM (Snapshot Isolation violation):
        Signal Engine reads MSQ_A (epoch 10), does `await asyncio.sleep(0)`,
        event loop processes L2 delta → book_A.read_epoch becomes 11,
        then reads MSQ_B (epoch 15). Ratio is computed on stale A data.

    SOLUTION (Cooperative Multitasking Physics):
        In asyncio, synchronous code between awaits is ATOMIC — the event loop
        CANNOT interleave. Therefore:

        1. `CrossAssetSnapshot.take()` reads ALL books' epochs + MSQ results
           in a single synchronous pass (zero awaits) → guaranteed consistent.
        2. If the Signal Engine MUST await between `take()` and `use()`,
           call `snap.is_consistent(books)` to OCC-validate no mutation occurred.
        3. If validation fails → retry the snapshot (bounded retries).

    Usage:
        snap = CrossAssetSnapshot.take(
            books={"ETHUSDT": eth_book, "OPUSDT": op_book},
            book_states={"ETHUSDT": mux.book_state("ETHUSDT"), ...},
            target_usd_scaled=target,
            circuit_breaker_open=gate.is_circuit_open,
        )
        if not snap.all_ready:
            return  # At least one book is blind/stale/recovering

        # ... potentially await something (logger, DB) ...

        if not snap.is_consistent(books):
            # OCC violation — at least one book mutated during our await
            return  # retry or abort

        ratio = snap.slices["ETHUSDT"].msq[0] / snap.slices["OPUSDT"].msq[0]
    """

    slices: dict[str, _BookSlice]
    snapshot_mono: float

    @property
    def all_ready(self) -> bool:
        """True only if EVERY book in the snapshot is READY."""
        return all(s.readiness == BookReadiness.READY for s in self.slices.values())

    @property
    def worst_readiness(self) -> BookReadiness:
        """Returns the worst readiness state across all books (highest IntEnum value)."""
        return max(s.readiness for s in self.slices.values())

    def is_consistent(
        self,
        books: dict[str, NdOrderBookStateMachine],
    ) -> bool:
        """
        Strict OCC validation: returns False if ANY book's read_epoch changed.
        Use `is_consistent_msq` for physical-damage validation that tolerates
        deep-level HFT noise.
        """
        for sym, sl in self.slices.items():
            book = books.get(sym)
            if book is None:
                return False
            if book.read_epoch != sl.epoch:
                return False
        return True

    def is_consistent_msq(
        self,
        books: dict[str, NdOrderBookStateMachine],
        book_states: dict[str, BookState],
        target_usd_scaled: int,
        *,
        circuit_breaker_open: bool = False,
    ) -> bool:
        """
        Physical Damage OCC (institutional grade).

        Unlike `is_consistent()` which rejects on ANY epoch change, this method
        validates whether the *actual MSQ execution physics* changed:

        Fast Path: epoch unchanged → O(1) skip (most common case).
        Slow Path: epoch mutated → re-check readiness + re-calculate MSQ.
          - If readiness degraded (READY → BLIND/STALE) → reject (safety).
          - If MSQ value changed → reject (price impact shifted).
          - If MSQ unchanged (only deep L2 levels mutated) → accept (noise).

        This eliminates False Rejects on volatile markets where HFT noise mutates
        deep levels hundreds of times per second, while our MSQ only consumes
        the top 1-3 levels.
        """
        for sym, sl in self.slices.items():
            book = books.get(sym)
            if book is None:
                return False

            # Fast path: epoch unchanged → 100% consistent, skip
            if book.read_epoch == sl.epoch:
                continue

            # Slow path: epoch mutated — check if OUR execution is affected

            # 1. Readiness degraded? (READY → BLIND/STALE/RECOVERING)
            current_st = book_states.get(sym, BookState.DESYNCED)
            current_rd = book.readiness(
                current_st, circuit_breaker_open=circuit_breaker_open,
            )
            if current_rd != sl.readiness:
                return False

            # 2. MSQ physics changed? (price impact shifted)
            if current_rd == BookReadiness.READY:
                new_msq = book.calculate_msq(target_usd_scaled)
                if new_msq != sl.msq:
                    return False

        return True

    @staticmethod
    def take(
        books: dict[str, NdOrderBookStateMachine],
        book_states: dict[str, BookState],
        target_usd_scaled: int,
        *,
        circuit_breaker_open: bool = False,
    ) -> CrossAssetSnapshot:
        """
        Atomic multi-book read. MUST be called WITHOUT any intervening awaits.

        In asyncio cooperative multitasking, this entire method executes in
        a single event loop tick — no L2 delta can mutate any book mid-read.
        This is the hardware guarantee of Snapshot Isolation.
        """
        slices: dict[str, _BookSlice] = {}
        for sym, book in books.items():
            st = book_states.get(sym, BookState.DESYNCED)
            rd = book.readiness(st, circuit_breaker_open=circuit_breaker_open)
            ep = book.read_epoch
            msq: tuple[int, int] | None = None
            if rd == BookReadiness.READY:
                msq = book.calculate_msq(target_usd_scaled)
            slices[sym] = _BookSlice(
                symbol=sym,
                readiness=rd,
                epoch=ep,
                msq=msq,
                data_age_sec=book.data_age_sec,
            )
        return CrossAssetSnapshot(
            slices=slices,
            snapshot_mono=time.monotonic(),
        )

    @staticmethod
    async def take_with_retry(
        books: dict[str, NdOrderBookStateMachine],
        book_states_fn: Callable[[], dict[str, BookState]],
        target_usd_scaled: int,
        *,
        circuit_breaker_open: bool = False,
        intent_ttl_sec: float = 5.0,
        max_retries: int = 5,
    ) -> CrossAssetSnapshot:
        """
        Bounded Retry with Fail-Fast guarantee.

        Returns a PERFECT `all_ready` snapshot or raises `SnapshotIsolationError`.
        Never returns a degraded snapshot — capital doesn't know "best-effort".

        Budget mechanics:
          - Monotonic deadline = now + intent_ttl_sec
          - Each retry: micro-yield (5ms) → re-take → check all_ready
          - If budget exhausted OR max_retries hit → raise SnapshotIsolationError
          - Caller MUST catch and suppress the signal

        Why 5ms micro-jitter instead of sleep(0)?
          On a saturated event loop (capitulation, burst of L2 frames),
          sleep(0) returns immediately — 5 retries spin in <1ms without
          letting the WS multiplexer reassemble TCP window fragments.
          5ms gives the ingest pipeline real time to process queued frames.

        Args:
            books: Symbol → NdOrderBookStateMachine map.
            book_states_fn: Callable returning fresh BookState map
                (called synchronously inside take).
            target_usd_scaled: MSQ target notional.
            circuit_breaker_open: From RestResyncGate.is_circuit_open.
            intent_ttl_sec: Hard deadline (default 5.0s — operator cognitive window).
            max_retries: Maximum re-take attempts (default 5).

        Raises:
            SnapshotIsolationError: Market is in absolute chaos. Suppress signal.
        """
        import asyncio
        from src.infrastructure.gektor_l2.errors import SnapshotIsolationError

        _MICRO_JITTER_SEC = 0.005  # 5ms — enough for TCP window reassembly

        deadline = time.monotonic() + intent_ttl_sec
        last_snap: CrossAssetSnapshot | None = None

        for attempt in range(1, max_retries + 1):
            if time.monotonic() >= deadline:
                break

            states = book_states_fn()
            snap = CrossAssetSnapshot.take(
                books,
                states,
                target_usd_scaled,
                circuit_breaker_open=circuit_breaker_open,
            )

            if snap.all_ready:
                return snap

            last_snap = snap

            # Micro-jitter yield — let WS multiplexer process queued frames
            if attempt < max_retries and (deadline - time.monotonic()) > _MICRO_JITTER_SEC:
                await asyncio.sleep(_MICRO_JITTER_SEC)

        # All retries exhausted or TTL budget depleted — FAIL FAST
        worst = last_snap.worst_readiness.name if last_snap else "UNKNOWN"
        raise SnapshotIsolationError(
            attempts=max_retries,
            ttl_sec=intent_ttl_sec,
            worst_readiness=worst,
        )
