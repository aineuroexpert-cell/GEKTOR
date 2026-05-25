import ctypes

# Прямой маппинг в память. Сборщик мусора безработный.
class L2State(ctypes.Structure):
    _fields_ = [
        ("timestamp", ctypes.c_uint64),
        # В реальной реализации используются Scaled Integers (int64) 
        # для исключения IEEE 754 Drift (Float-галлюцинаций)
        ("best_bid", ctypes.c_longlong), 
        ("best_ask", ctypes.c_longlong)
    ]

class RingBufferConflation:
    def __init__(self, capacity: int = 10000):
        self.capacity = capacity
        self.head = 0
        # Предварительная аллокация памяти. Никаких list.append.
        self.buffer = (L2State * self.capacity)()
        
    def ingest_blindly(self, timestamp: int, bid: int, ask: int) -> None:
        """
        Zero-latency запись. Никаких asyncio.gather или генерации корутин.
        O(1) мутация стейта.
        """
        idx = self.head % self.capacity
        self.buffer[idx].timestamp = timestamp
        self.buffer[idx].best_bid = bid
        self.buffer[idx].best_ask = ask
        self.head += 1
        
    def get_latest_causal_state(self) -> L2State:
        """
        Синхронное чтение стейта оракула только в момент T_0 для принятия решения.
        """
        return self.buffer[(self.head - 1) % self.capacity]
