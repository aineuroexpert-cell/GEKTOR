#!/usr/bin/env python3
"""
InFlightWatchdog — Zero-Allocation Transit Latency Shield
Извлекает метку времени из сырых байтов WebSocket ДО десериализации JSON.
Защищает Event Loop от аллокационного шторма при буферизации ОС/NAT.
"""

import time
import logging

logger = logging.getLogger("GEKTOR.LatencyShield")


class InFlightWatchdog:
    """
    Аппаратный счетчик транзитной задержки.
    Оперирует сырыми байтами до передачи в Event Sourcing ядро.
    """

    def __init__(self, calibrated_offset_ms: float, max_latency_ms: int = 150):
        # calibrated_offset_ms вычисляется на этапе HFT Time Sync Gate (при старте)
        self.offset = calibrated_offset_ms
        self.max_latency = max_latency_ms

        # Предкомпилированные байтовые маркеры для O(1) поиска в сыром payload
        self._ts_marker_v5 = b'"ts":'
        self._e_marker_v5 = b'"E":'

        # Телеметрия
        self.total_checked: int = 0
        self.total_dropped: int = 0

    def is_packet_dead(self, raw_payload: bytes) -> bool:
        """
        Извлекает время из JSON без создания dict.
        Защищает Event Loop от аллокационного шторма при буферизации ОС.
        """
        self.total_checked += 1

        # Быстрый поиск метки времени
        idx = raw_payload.find(self._ts_marker_v5)
        if idx == -1:
            idx = raw_payload.find(self._e_marker_v5)
            if idx == -1:
                return False  # Топик без метки времени (системные пинги)

        # Парсинг только 13 байт (миллисекунды UNIX) после маркера
        try:
            start_idx = idx + 5
            # Ищем конец числа (запятая или закрывающая скобка)
            end_idx = raw_payload.find(b',', start_idx)
            if end_idx == -1:
                end_idx = raw_payload.find(b'}', start_idx)

            payload_ts = int(raw_payload[start_idx:end_idx])

            # Точная математика с учетом калиброванного дрейфа
            local_time_ms = time.time() * 1000
            true_transit_latency = (local_time_ms - self.offset) - payload_ts

            if true_transit_latency > self.max_latency:
                self.total_dropped += 1
                return True

        except (ValueError, IndexError):
            pass  # Если парсинг сбился, отдаем на растерзание orjson

        return False

    @property
    def drop_ratio(self) -> float:
        """Процент отброшенных пакетов. Для телеметрии Dead Man's Switch."""
        if self.total_checked == 0:
            return 0.0
        return self.total_dropped / self.total_checked
