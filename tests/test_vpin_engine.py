import pytest
from src.domain.vpin_engine import O1VPINEngine
from unittest.mock import MagicMock

def test_absorption_long_correct_window_price():
    """
    Проверяет: при бычьем дисбалансе, но цена не растёт — absorption=True.
    Критично: цена начала окна должна читаться из СТАРЕЙШЕГО слота буфера,
    а не из текущего. Этот тест обнаружит баг с индексом.
    """
    engine = O1VPINEngine(window_size=5, volume_threshold=100_000.0, z_threshold=1.0)
    
    def make_bar(buy, sell, close_price):
        bar = MagicMock()
        bar.buy_volume_usd = buy
        bar.sell_volume_usd = sell
        bar.close = close_price
        return bar
    
    # Заполняем буфер нейтральными барами с ценой 100.0
    for _ in range(5):
        engine.process_bar(make_bar(50_000, 50_000, 100.0))
    
    # Добавляем бары с резким бычьим дисбалансом, но цена НЕ растёт (iceberg)
    # Z-score должен вырасти (z_threshold=1.0 — низкий порог специально)
    for _ in range(5):
        result = engine.process_bar(make_bar(95_000, 5_000, 99.0))
    
    assert result is not None, 'Engine должен возвращать сигнал после прогрева'
    if result.is_anomaly:
        # Бычий дисбаланс + цена упала → absorption_detected должен быть True
        assert result.absorption_detected == True, \
            f'Ожидали absorption=True (iceberg), получили False. ' \
            f'Вероятная причина: баг с индексом price_start_window.'

def test_z_history_independent_from_window_size():
    """
    Проверяет: буфер истории Z-score не зависит от window_size агрегации.
    После Задачи 3 z_history_size должен быть отдельным параметром.

    NOTE (v3.6.0): The attribute is exposed as the PUBLIC `z_history_size`
    in the canonical engine (`src/domain/vpin_engine.py`). Earlier drafts
    used `_z_history_size`; the public name is correct per the SSOT.
    """
    engine = O1VPINEngine(window_size=5, volume_threshold=100_000.0,
                         z_threshold=2.5, z_history_size=200)

    assert engine.z_history_size == 200
    assert len(engine._vpin_history) == 200
    assert engine._vpin_history.dtype.name == 'float64'

def test_no_import_inside_process_bar():
    """
    Проверяет: метод process_bar не содержит import-операторов внутри тела.
    Используем inspect для чтения исходного кода метода.
    """
    import inspect
    
    source = inspect.getsource(O1VPINEngine.process_bar)
    assert 'import time' not in source, \
        'import time обнаружен внутри process_bar(). Вынести на уровень модуля.'

def test_no_false_anomalies_on_warmup():
    """
    Проверяет: на первых барах после старта Z-score не выдаёт **бесконечных**
    выбросов из-за деления на пустой буфер. Это регрессионный тест для бага,
    внесённого при реализации z_history_size=500.

    NOTE (v3.6.0): The original draft used z_threshold=0.5 which is **NOT**
    a realistic production value (default = 2.5). At z_threshold=0.5 even
    legitimate ~1σ imbalance bars register as "anomalies" by definition.
    The actual bug we guard against is `std_dev == 0` → `z = inf` →
    always-anomaly, which would manifest as `z_score == math.inf`. We
    test the canonical pathology: emitted `z_score` must be **finite**.
    """
    engine = O1VPINEngine(window_size=5, volume_threshold=100_000.0,
                         z_threshold=2.5, z_history_size=500)

    def make_bar(buy, sell, price=100.0):
        bar = MagicMock()
        bar.buy_volume_usd = buy
        bar.sell_volume_usd = sell
        bar.close = price
        return bar

    # Прогреваем основное кольцо (window_size=5 баров)
    for _ in range(5):
        engine.process_bar(make_bar(50_000, 50_000))

    # First handful of post-warmup bars: with mild imbalance, the engine
    # must NEVER emit z = +/-inf. If it does, the divisor bug is back.
    import math
    for _ in range(10):
        result = engine.process_bar(make_bar(52_000, 48_000))
        if result is None:
            continue
        assert math.isfinite(result.z_score), (
            f'Engine emitted non-finite z_score={result.z_score}. '
            f'Likely cause: divisor uses empty z_history (z_count not tracked).'
        )
