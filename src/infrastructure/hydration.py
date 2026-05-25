import msgspec
from typing import List, Tuple
from decimal import Decimal
from loguru import logger

# [GEKTOR v10.7] ZERO-ALLOCATION SCALED INGESTION
# Мы выжигаем Decimal и float из горячего пути ингестии.
PRICE_SCALE = 100_000_000 

class FastScaledHydrator:
    """
    [GEKTOR v10.7] Высокопроизводительный гидратор на базе msgspec.
    Преобразует JSON-строки напрямую в Scaled Integers (int64).
    """
    
    @staticmethod
    def to_scaled_int(val) -> int:
        """
        [O(1) FIXED POINT PARSER]
        Конвертация str, float или int в Scaled Integer (10^8) без потери точности.
        """
        s = str(val)
        if '.' not in s:
            return int(s) * PRICE_SCALE
        
        whole, frac = s.split('.', 1)
        # Добиваем дробную часть нулями до масштаба 10^8
        frac = frac.ljust(8, '0')[:8]
        
        # Обработка отрицательных чисел
        is_negative = s.startswith('-')
        val_scaled = abs(int(whole)) * PRICE_SCALE + int(frac)
        return -val_scaled if is_negative else val_scaled

    def process_level_fast(self, level: List[str]) -> Tuple[int, int]:
        """
        Преобразование уровня [price, size] из Bybit WS.
        """
        # В Bybit V5 данные приходят как строки: ["60000.5", "0.1"]
        return self.to_scaled_int(level[0]), self.to_scaled_int(level[1])

    @staticmethod
    def parse_levels(levels: List[List[str]]) -> List[Tuple[int, int]]:
        """Пакетная конвертация уровней в Scaled Integers."""
        return [(FastScaledHydrator.to_scaled_int(p), FastScaledHydrator.to_scaled_int(q)) for p, q in levels]

# Оптимизированный декодер структур Bybit
class BybitL2Update(msgspec.Struct):
    s: str  # symbol
    b: List[List[str]]  # bids
    a: List[List[str]]  # asks
    u: int  # updateId

decoder = msgspec.json.Decoder(msgspec.Raw)

def fast_parse_l2(data: bytes) -> Tuple[str, List[Tuple[int, int]], List[Tuple[int, int]], int]:
    """
    [ZERO-COPY ATTEMPT] Парсинг JSON напрямую в типы GEKTOR.
    """
    # msgspec.json.decode работает на C и в 2-3 раза быстрее orjson для структур
    obj = msgspec.json.decode(data, type=BybitL2Update)
    
    # Прямая конвертация в Scaled Integers
    bids = [(FastScaledHydrator.to_scaled_int(p), FastScaledHydrator.to_scaled_int(q)) for p, q in obj.b]
    asks = [(FastScaledHydrator.to_scaled_int(p), FastScaledHydrator.to_scaled_int(q)) for p, q in obj.a]
    
    return obj.s, bids, asks, obj.u
