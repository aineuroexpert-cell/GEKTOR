import ctypes
import numpy as np
from typing import Tuple

# [GEKTOR v6.0] БЕЗУПРЕЧНАЯ ПРЕДЕЛЬНАЯ ТОЧНОСТЬ (Scaled Integers)
# Мы используем масштаб 10^8 для цен и объемов (как в Bybit V5 API)
PRICE_SCALE = 100_000_000
VOLUME_SCALE = 100_000_000

class SHMLevel(ctypes.Structure):
    """
    Уровень стакана без потери точности.
    Никаких c_double. Только жесткие 64-битные целые.
    """
    _fields_ = [
        ("price", ctypes.c_int64),
        ("volume", ctypes.c_int64)
    ]

class SHMOrderBook(ctypes.Structure):
    """
    Lock-free стакан с Seqlock (epoch).
    Размер: Depth 50 (Bids + Asks).
    """
    _fields_ = [
        ("epoch", ctypes.c_uint64),      # Нечетное = запись, Четное = готово
        ("update_id", ctypes.c_uint64),  # UpdateID биржи
        ("exch_ts", ctypes.c_uint64),    # Exchange Timestamp
        ("bids", SHMLevel * 50),
        ("asks", SHMLevel * 50)
    ]

def get_shm_view_np(shm_book: SHMOrderBook) -> Tuple[np.ndarray, np.ndarray]:
    """
    [ZERO-ALLOCATION] Возвращает numpy-представление уровней без копирования.
    Указатели смотрят напрямую в общую память.
    """
    # Маппинг bids. Используем stride для прямого доступа.
    bids_view = np.frombuffer(shm_book.bids, dtype=[('price', 'i8'), ('volume', 'i8')], count=50)
    asks_view = np.frombuffer(shm_book.asks, dtype=[('price', 'i8'), ('volume', 'i8')], count=50)
    
    return bids_view, asks_view

def read_lockfree_seqlock(shm_book: SHMOrderBook, dest_bids: np.ndarray, dest_asks: np.ndarray) -> bool:
    """
    [CRITICAL PATH] Чтение данных с гарантией консистентности через Seqlock.
    Копирование выполняется во внешние буферы dest_* (inplace).
    """
    for _ in range(10): # Ограниченный ретрай
        seq1 = shm_book.epoch
        if seq1 & 1:
            continue # В текущий момент идет запись
            
        # [MEMORY BARRIER] Синхронное копирование блока памяти
        # Numpy.copyto эффективно делает memcpy
        bids, asks = get_shm_view_np(shm_book)
        np.copyto(dest_bids, bids)
        np.copyto(dest_asks, asks)
        
        seq2 = shm_book.epoch
        if seq1 == seq2:
            return True # Данные прочитаны без коллизий
            
    return False # Высокая волатильность: коллизия записи
