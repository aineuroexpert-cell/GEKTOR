import ctypes

class DollarBarState(ctypes.Structure):
    """
    Абсолютно плоская C-структура для агрегации тиков в Долларовые Свечи.
    Живет в L1/L2 кэше процессора. Zero-Allocation Information Clocks.
    """
    _pack_ = 1
    _fields_ = [
        ("open", ctypes.c_double),
        ("high", ctypes.c_double),
        ("low", ctypes.c_double),
        ("close", ctypes.c_double),
        ("volume", ctypes.c_double),
        ("cumulative_dollar_value", ctypes.c_double),
        ("ticks_count", ctypes.c_uint32),
        ("threshold", ctypes.c_double), # Порог генерации новой свечи (напр. $1,000,000)
        ("is_ready", ctypes.c_uint8),    # 1, если свеча сформирована и ждет Inference
        ("last_exchange_timestamp", ctypes.c_uint64), # Метка времени последнего тика
        ("is_corrupted", ctypes.c_uint8) # 1 = Poisoned State (отбрасывается)
    ]

def ingest_tick_to_dollar_bar(tick_price: float, tick_qty: float, state: DollarBarState) -> None:
    """
    O(1) агрегация. Вызывается для каждого трейда из websocket'а.
    Никаких аллокаций памяти.
    """
    dollar_value = tick_price * tick_qty
    
    if state.ticks_count == 0:
        state.open = tick_price
        state.high = tick_price
        state.low = tick_price
        
    state.high = tick_price if tick_price > state.high else state.high
    state.low = tick_price if tick_price < state.low else state.low
    state.close = tick_price
    state.volume += tick_qty
    state.cumulative_dollar_value += dollar_value
    state.ticks_count += 1
    
    # Информационные часы "пробили" порог: сформирована свеча
    if state.cumulative_dollar_value >= state.threshold:
        if state.is_corrupted == 0:
            state.is_ready = 1
            # Inference trigger occurs here
        else:
            # Poison Pill: Отбрасываем отравленный бар и снимаем карантин
            state.is_corrupted = 0
            state.is_ready = 0
        
        # Сброс счетчиков для следующего цикла (кроме threshold)
        state.cumulative_dollar_value = 0.0
        state.ticks_count = 0
        state.volume = 0.0

def reconcile_gap(current_ws_timestamp_ms: int, state: DollarBarState) -> None:
    """
    Hydration Reconciliation (Gap Detector).
    Вызывается при холодном старте и первом WS-сообщении.
    """
    if state.last_exchange_timestamp == 0:
        return
        
    delta_ms = current_ws_timestamp_ms - state.last_exchange_timestamp
    if delta_ms > 1500:  # Разрыв > 1.5s
        # Карантин (Poison Pill)
        state.is_corrupted = 1
        # Система уходит в WARMUP_MODE, блокируя выдачу сигналов
        # пока is_corrupted = 1.

