"""Shared parsing for Bybit L2 rows (REST + WS)."""

from __future__ import annotations

from typing import Any
import numpy as np
from src.infrastructure.gektor_l2.scaling import parse_scaled_int

# [ZERO-ALLOCATION] Статические буферы (Double-Buffering) для Bids и Asks.
# Размер 4096 уровней гарантированно вмещает любой снимок стакана Bybit Linear (глубина 50/200/500).
_BIDS_STATIC_BUFFER = np.zeros((4096, 2), dtype=np.int64)
_ASKS_STATIC_BUFFER = np.zeros((4096, 2), dtype=np.int64)

def parse_bids(raw_rows: Any) -> np.ndarray:
    return _parse_levels(raw_rows, _BIDS_STATIC_BUFFER)

def parse_asks(raw_rows: Any) -> np.ndarray:
    return _parse_levels(raw_rows, _ASKS_STATIC_BUFFER)

def _parse_levels(raw_rows: Any, buffer: np.ndarray) -> np.ndarray:
    if not isinstance(raw_rows, list):
        return np.empty((0, 2), dtype=np.int64)
    
    idx = 0
    max_len = buffer.shape[0]
    
    # Прямой маппинг в C-массив без динамической аллокации
    for row in raw_rows:
        if not isinstance(row, list) or len(row) < 2:
            continue
        if idx >= max_len:
            break  # Защита от переполнения (Ring Buffer limit)
            
        buffer[idx, 0] = parse_scaled_int(row[0])
        buffer[idx, 1] = parse_scaled_int(row[1])
        idx += 1
        
    return buffer[:idx]


def optional_cross_id(val: Any) -> int | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None
