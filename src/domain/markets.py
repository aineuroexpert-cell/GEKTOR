import ctypes
import bisect
import asyncio
import math
import time
from multiprocessing import shared_memory, RLock
from typing import Dict, List, Optional, Tuple, Deque
from collections import deque
from decimal import Decimal
from loguru import logger
from src.infrastructure.shm_layout import SHMOrderBook, PRICE_SCALE, VOLUME_SCALE

# SharedOrderBookStruct removed in favor of SHMOrderBook from shm_layout.py

from src.infrastructure.hydration import FastScaledHydrator

class StateMachineOrderBook:
    """
    [GEKTOR v10.8] Scaled Integer Orderbook (int64).
    Zero Decimal. Zero Float. Прямое маппирование в SHM.
    """
    __slots__ = ['symbol', '_bids', '_asks', '_sorted_bids', '_sorted_asks', 
                 'last_update_id', 'is_synchronized', 'is_hydrating', 
                 'playback_buffer', 'priority', 'epoch', 'status',
                 'shm_name', 'lock', 'shm', 'shared_state']

    def __init__(self, symbol: str, priority: int = 2):
        self.symbol = symbol
        self._bids: Dict[int, int] = {} # Price (Scaled) -> Qty (Scaled)
        self._asks: Dict[int, int] = {} 
        
        self._sorted_bids: List[int] = [] 
        self._sorted_asks: List[int] = [] 
        
        self.last_update_id: int = 0
        self.is_synchronized: bool = False
        self.is_hydrating: bool = False
        self.priority = priority
        self.playback_buffer: Deque[dict] = deque(maxlen=2000)
        
        self.shm_name = f"gektor_l2_{symbol.lower()}"
        self.lock = RLock()
        
        struct_size = ctypes.sizeof(SHMOrderBook)
        try:
            self.shm = shared_memory.SharedMemory(create=True, name=self.shm_name, size=struct_size)
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(name=self.shm_name)
        
        self.shared_state = SHMOrderBook.from_buffer(self.shm.buf)
        self.status: str = "UNSYNCED"
        self.epoch = 0

    @property
    def is_crossed(self) -> bool:
        """
        O(1) проверка скрещенного стакана (Crossed Book Guard).
        Использует кэшированные сортированные списки цен.
        """
        # [GEKTOR v12.11] Мы используем _sorted_bids[0] и _sorted_asks[0] 
        # как наиболее производительный способ доступа к BBO (Scaled Ints).
        if not self._sorted_bids or not self._sorted_asks:
            return False
            
        return self._sorted_bids[0] >= self._sorted_asks[0]

    def _write_shm_inplace(self):
        """
        [GEKTOR v12.12] PRIVATE SHM Sync — NO epoch increment.
        MUST be called ONLY within an existing begin_write/end_write bracket.
        Zero-allocation: no dict creation, no lambda, inplace ctypes write.
        """
        _status_map_synced = 1
        _status_map_unsynced = 0
        _status_map_degraded = 2
        if self.status == "SYNCED":
            self.shared_state.status = _status_map_synced
        elif self.status == "DEGRADED":
            self.shared_state.status = _status_map_degraded
        else:
            self.shared_state.status = _status_map_unsynced

        b_count = min(50, len(self._sorted_bids))
        a_count = min(50, len(self._sorted_asks))

        for i in range(b_count):
            p = self._sorted_bids[i]
            self.shared_state.bids[i].price = p
            self.shared_state.bids[i].volume = self._bids[p]

        for i in range(a_count):
            p = self._sorted_asks[i]
            self.shared_state.asks[i].price = p
            self.shared_state.asks[i].volume = self._asks[p]

        # [SANITY CHECK] Защита от скрещенного стакана
        if b_count > 0 and a_count > 0:
            if self._sorted_bids[0] >= self._sorted_asks[0]:
                self.shared_state.status = _status_map_degraded
                logger.error(f"🛑 [ANOMALY] Crossed book for {self.symbol}: {self._sorted_bids[0]} >= {self._sorted_asks[0]}")

    def sync_to_shm(self):
        """[COMPAT] Public SHM sync with its own epoch bracket. Used by on_ws_delta_received."""
        self.begin_write()
        try:
            self._write_shm_inplace()
        finally:
            self.end_write()

    def begin_write(self):
        self.lock.acquire()
        self.shared_state.epoch += 1

    def end_write(self):
        self.shared_state.epoch += 1
        self.lock.release()

    def on_ws_delta_received(self, event: dict) -> bool:
        """[GEKTOR v12.10] Sparse Delta Routing & Overflow Protection."""
        if self.is_hydrating:
            # 1. Protection against 'Causal Fracture' (Buffer Overflow)
            # Если REST API висит долго, мы не копим пакеты до OOM, а сзываем цикл.
            if len(self.playback_buffer) >= 10000:
                logger.error(f"💀 [CAUSALITY LINK BROKEN] {self.symbol} buffer overflow (>10k). Stitching impossible.")
                self.playback_buffer.clear()
                self.is_hydrating = False 
                self.is_synchronized = False
                return False 
            
            self.playback_buffer.append(event)
            return True
        
        # 2. Проверка монотонности (Update ID)
        if event.get('u', 0) < self.last_update_id:
            return True # Дубликат или старье
        
        # Санити-чек на разрыв (аппликационный уровень)
        U_id = event.get('U', 0)
        if U_id > self.last_update_id + 5000:
             logger.warning(f"⚠️ [SEQUENCE_GAP] {self.symbol} too large gap. Triggering RED.")
             self.trigger_desync("LARGE_GAP")
             return False

        self.begin_write()
        try:
            self._apply_delta(event)
        finally:
            self.end_write()
        return True

    def trigger_desync(self, reason: str = "GAP"):
        """Сброс стейта при потере цепочки."""
        self.status = "UNSYNCED"
        self.is_synchronized = False
        self.is_hydrating = True
        self.playback_buffer.clear()
        logger.warning(f"🔄 [DESYNC] {self.symbol} reset requested. Reason: {reason}")

    async def apply_rest_snapshot(self, last_update_id: int, bids: List[Tuple[int, int]], asks: List[Tuple[int, int]]):
        """
        [GEKTOR v12.12] REST L0 Anchor (Scaled Integers).
        Гидрация начального состояния и бесшовное сшивание (Stitching).

        SEQLOCK INVARIANT: Exactly ONE begin_write/end_write bracket.
        SHM is written via _write_shm_inplace (no nested epoch increment).
        Reader is guaranteed to see either the old state or the fully new state.
        """
        self.begin_write()
        try:
            # 1. Атомарная зачистка (inplace, no reallocation)
            self._bids.clear()
            self._asks.clear()

            # 2. Ингестия Scaled Integers
            for p, q in bids:
                if q > 0: self._bids[p] = q
            for p, q in asks:
                if q > 0: self._asks[p] = q

            # 3. Перестроение индексов
            self._sorted_bids = sorted(self._bids.keys(), reverse=True)
            self._sorted_asks = sorted(self._asks.keys())

            self.last_update_id = last_update_id
            self.is_synchronized = True
            self.is_hydrating = False
            self.status = "SYNCED"

            # 4. [SEAMLESS STITCHING] Проверка непрерывности
            applied_count = 0
            while self.playback_buffer:
                ev = self.playback_buffer.popleft()

                # Пропускаем события внутри снапшота
                if ev.get('u', 0) <= self.last_update_id:
                    continue

                # Проверка: первая дельта должна накладываться без разрыва
                # First Update ID (U) должен быть <= Snapshot Last ID + 1
                if ev.get('U', 0) > self.last_update_id + 1:
                    logger.critical(f"🛑 [STITCHING GAP] {self.symbol} break: REST {self.last_update_id} vs WS First {ev.get('U')}")
                    self.trigger_desync("STITCH_GAP")
                    return

                self._apply_delta(ev)
                applied_count += 1

            # 5. [ATOMIC SHM SYNC] Inlined — no nested epoch bracket.
            # _write_shm_inplace does NOT call begin/end_write.
            self._write_shm_inplace()

            logger.success(f"✅ [STITCHED] {self.symbol} LIVE. (ID: {self.last_update_id}, {applied_count} catch-up events)")

        finally:
            self.end_write()

    def _apply_delta(self, ev: dict):
        self._update_side(self._bids, self._sorted_bids, ev.get('b', []), reverse=True)
        self._update_side(self._asks, self._sorted_asks, ev.get('a', []), reverse=False)
        self.last_update_id = ev.get('u', ev.get('U', self.last_update_id))

    def _update_side(self, book_side: Dict[int, int], sorted_prices: List[int], updates: list, reverse: bool):
        """
        [GEKTOR v12.12] Zero-lambda delta application.
        Split logic avoids per-call lambda creation in bisect key= param.
        """
        if reverse:
            self._update_side_bids(book_side, sorted_prices, updates)
        else:
            self._update_side_asks(book_side, sorted_prices, updates)

    def _update_side_bids(self, book_side: Dict[int, int], sorted_prices: List[int], updates: list):
        """Bids are sorted descending. bisect on negated prices avoids lambda."""
        for p_str, q_str in updates:
            price = FastScaledHydrator.to_scaled_int(p_str)
            qty = FastScaledHydrator.to_scaled_int(q_str)

            if qty <= 0:
                if price in book_side:
                    book_side.pop(price)
                    # Bids sorted descending: find via negated comparison
                    neg_price = -price
                    lo, hi = 0, len(sorted_prices)
                    while lo < hi:
                        mid = (lo + hi) >> 1
                        if -sorted_prices[mid] < neg_price:
                            lo = mid + 1
                        else:
                            hi = mid
                    if lo < len(sorted_prices) and sorted_prices[lo] == price:
                        sorted_prices.pop(lo)
            else:
                if price not in book_side:
                    neg_price = -price
                    lo, hi = 0, len(sorted_prices)
                    while lo < hi:
                        mid = (lo + hi) >> 1
                        if -sorted_prices[mid] < neg_price:
                            lo = mid + 1
                        else:
                            hi = mid
                    sorted_prices.insert(lo, price)
                book_side[price] = qty

    def _update_side_asks(self, book_side: Dict[int, int], sorted_prices: List[int], updates: list):
        """Asks are sorted ascending. Standard bisect, no lambda."""
        for p_str, q_str in updates:
            price = FastScaledHydrator.to_scaled_int(p_str)
            qty = FastScaledHydrator.to_scaled_int(q_str)

            if qty <= 0:
                if price in book_side:
                    book_side.pop(price)
                    idx = bisect.bisect_left(sorted_prices, price)
                    if idx < len(sorted_prices) and sorted_prices[idx] == price:
                        sorted_prices.pop(idx)
            else:
                if price not in book_side:
                    idx = bisect.bisect_left(sorted_prices, price)
                    sorted_prices.insert(idx, price)
                book_side[price] = qty

    def apply_trade_ghost_update(self, side: str, price_str: str, volume_str: str):
        self.begin_write()
        try:
            price = FastScaledHydrator.to_scaled_int(price_str)
            volume = FastScaledHydrator.to_scaled_int(volume_str)

            book_side = self._asks if side.capitalize() == 'Buy' else self._bids
            sorted_prices = self._sorted_asks if side.capitalize() == 'Buy' else self._sorted_bids
            reverse = side.capitalize() != 'Buy'
            
            if price in book_side:
                new_volume = book_side[price] - volume
                if new_volume <= 0:
                    book_side.pop(price)
                    idx = bisect.bisect_left(sorted_prices, -price if reverse else price, 
                                             key=lambda x: -x if reverse else x)
                    if idx < len(sorted_prices) and sorted_prices[idx] == price:
                        sorted_prices.pop(idx)
                else:
                    book_side[price] = new_volume
        finally:
            self.end_write()

    def get_vwap_levels(self, side: str, max_depth: int = 50) -> List[Tuple[int, int]]:
        if not self.is_synchronized: return []
        
        prices = self._sorted_asks if (side == "BUY" or side == 1) else self._sorted_bids
        book = self._asks if (side == "BUY" or side == 1) else self._bids
        return [(p, book[p]) for p in prices[:max_depth]]

    def get_snapshot(self, depth: int = 50) -> Tuple[int, str, List[Tuple[int, int]], List[Tuple[int, int]], bool]:
        """
        [GEKTOR v12.8] O(K) High-Performance Read Path.
        Возвращает детерминированный срез стакана для логгеров и Оркестратора.
        """
        # Возвращаем копии срезов из кэшированных сортированных списков
        b_p = self._sorted_bids[:depth]
        a_p = self._sorted_asks[:depth]
        
        # Генерация списков кортежей (Scaled Integers)
        best_bids = [(p, self._bids[p]) for p in b_p]
        best_asks = [(p, self._asks[p]) for p in a_p]
        
        # is_dirty сигнализирует о том, что идет процесс записи в SHM (нечетная эпоха)
        is_dirty = (self.shared_state.epoch % 2 != 0)
        
        return self.shared_state.epoch, self.status, best_bids, best_asks, is_dirty

    def close(self):
        """[SHM GRACEFUL SHUTDOWN]"""
        if hasattr(self, 'shm') and self.shm:
            try:
                self.shm.close()
            except Exception:
                pass
            try:
                self.shm.unlink()
                logger.info(f"🗑️ [SHM] Segment '{self.shm_name}' unlinked.")
            except FileNotFoundError:
                pass
            except Exception as e:
                logger.warning(f"⚠️ [SHM] Unlink error for {self.shm_name}: {e}")


