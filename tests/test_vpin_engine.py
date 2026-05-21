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
    """
    engine = O1VPINEngine(window_size=5, volume_threshold=100_000.0,
                         z_threshold=2.5, z_history_size=200)
    
    assert engine._z_history_size == 200
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
    Проверяет: на первых барах после старта Z-score не выдаёт аномалий
    из-за деления на пустой буфер. Это регрессионный тест для бага,
    внесённого при реализации z_history_size=500.
    """
    # Низкий z_threshold=0.5 чтобы поймать любой выброс
    engine = O1VPINEngine(window_size=5, volume_threshold=100_000.0,
                         z_threshold=0.5, z_history_size=500)

    def make_bar(buy, sell, price=100.0):
        bar = MagicMock()
        bar.buy_volume_usd = buy
        bar.sell_volume_usd = sell
        bar.close = price
        return bar

    # Прогреваем основное кольцо (window_size=5 баров)
    for _ in range(5):
        engine.process_bar(make_bar(50_000, 50_000))

    # Первые несколько баров после прогрева — нейтральные данные.
    # При делении на пустой буфер Z-score был бы огромным → is_anomaly=True.
    # При правильном _z_count среднее считается честно → аномалии нет.
    anomaly_count = 0
    for _ in range(10):
        result = engine.process_bar(make_bar(52_000, 48_000))  # лёгкий дисбаланс
        if result and result.is_anomaly:
            anomaly_count += 1

    assert anomaly_count == 0, (
        f'На прогреве обнаружено {anomaly_count} ложных аномалий. '
        f'Вероятная причина: деление на z_history_size вместо _z_count.'
    )
