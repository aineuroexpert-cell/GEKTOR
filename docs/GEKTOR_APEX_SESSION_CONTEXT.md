# GEKTOR APEX — Контекст Сессии для Нового Диалога
**Дата фиксации:** 2026-05-21 21:36 MSK  
**Conversation ID:** ea2eaa83-4cc2-4dfa-92cf-c166fdbed257  
**Репозиторий:** `c:\Gerald-superBrain` → GitHub: `aineuroexpert-cell/GEKTOR`  
**Оператор:** Вячеслав (4 года фьючерсной торговли на Bybit)  
**Production VPS:** Tokyo, Vultr, `45.76.212.160`

---

## 1. ЧТО ЭТО ЗА СИСТЕМА

**GEKTOR APEX** — радарная система обнаружения аномалий микроструктуры рынка на фьючерсной секции Bybit (USDT Linear Futures).

**Режим работы: Advisory Mode.** Система НЕ торгует. Она мониторит все активные фьючерсные пары, выявляет среднесрочные аномалии токсичности потока ордеров и отправляет алерты в Telegram. Оператор принимает решение о входе в позицию вручную.

**Telegram:** Chat ID `1018895991`, Bot Token `8731946501:AAEaXFEPp-cO8vQ5naecXjRBfFE1magffos`

---

## 2. АРХИТЕКТУРА (CLEAN DDD)

```
src/
├── domain/           ← Чистая математика. Без I/O.
│   ├── conflation.py        ← DollarBarEngine: агрегация тиков в долларовые бары
│   ├── dollar_bars.py       ← ZeroAllocationDollarBarEngine (альтернативный, на scaled int64)
│   ├── vpin_engine.py       ← O1VPINEngine: VPIN + Z-Score + Absorption Filter
│   └── ... (tilt_breaker, triple_barrier, etc.)
├── application/      ← Координация и конвейер
│   ├── radar_pipeline.py    ← Главный конвейер: Tick → DollarBar → VPIN → Alert
│   ├── orchestrator.py      ← Trading Mode (неактивен в Advisory)
│   └── outbox_relay.py      ← Transactional Outbox → Telegram
├── infrastructure/   ← Сеть, БД, внешние API
│   ├── bybit.py             ← REST клиент Bybit
│   ├── bybit_ws_ingestion.py ← WebSocket подписки на publicTrade
│   ├── event_bus.py         ← Асинхронный EventBus + SQLite WAL writer thread
│   ├── config.py            ← Pydantic Settings
│   └── telegram_notifier.py ← Telegram gateway с прокси
├── presentation/     ← CLI, WebSocket handler для React UI
└── shared/           ← config.py, alpha_config.py, logger.py
```

**Активная цепочка данных в Advisory Mode:**
```
Bybit WS (publicTrade) → radar_pipeline.on_trade() → DollarBarEngine.process_tick()
  → [bar closed] → O1VPINEngine.process_bar() → [anomaly?] → Telegram Alert
```

> **ВАЖНО: В проекте существуют ДВА движка долларовых баров:**
> - `src/domain/conflation.py` — `DollarBarEngine` (async, Decimal, используется RadarPipeline)
> - `src/domain/dollar_bars.py` — `ZeroAllocationDollarBarEngine` (sync, scaled int64, НЕ используется RadarPipeline)
>
> **RadarPipeline (строка 191) создаёт экземпляр `DollarBarEngine` из `conflation.py`.**
> Файл `dollar_bars.py` — это другой движок для L2/HFT режима. НЕ путать.

---

## 3. КВАНТОВЫЙ ДВИЖОК: МАТЕМАТИКА

### Dollar Bars (Information Clocks)
Бар закрывается при достижении порога оборота в USD:
$$\sum p_t \cdot q_t \geq V_{threshold}$$
Порог задаётся через `DOLLAR_THRESHOLD_BASE` в `.env.production`.

### VPIN (Volume-Synchronized Probability of Toxicity)
$$VPIN = \frac{\sum_{\tau=1}^{N} |V_\tau^B - V_\tau^S|}{N \cdot V_{threshold}}$$

### Z-Score аномалии
$$Z = \frac{VPIN_t - \mu(VPIN)}{\sigma(VPIN)}$$
Аномалия: Z > Z_threshold (по умолчанию 2.5).

### Absorption Filter (Iceberg Guard)
- **Absorption Long:** Сильный перевес покупок (V^B >> V^S), но цена не растёт → скрытый лимитный продавец.
- **Absorption Short:** Сильный перевес продаж (V^S >> V^B), но цена не падает → скрытый лимитный покупатель.

### Полярность is_buyer_maker (ВЕРИФИЦИРОВАНО)
В `radar_pipeline.py` (строка 155):
```python
is_buyer_maker = (side == "Sell")  # True when taker=Sell → maker=Buy
```
В `conflation.py` (строки 96-101):
```python
if is_buyer_maker:
    bar.sell_volume_usd += tick_usd   # Taker продал
else:
    bar.buy_volume_usd += tick_usd    # Taker купил
```
**Логика верна. Инверсии нет.**

---

## 4. ЧТО БЫЛО СДЕЛАНО В ЭТОЙ СЕССИИ

### ТЗ v1.0 — 4 задачи (все выполнены)

| Задача | Файл | Суть | Статус |
|---|---|---|---|
| 1. Индекс price_start_window | `vpin_engine.py:126` | `oldest_idx = self._index` читается ПОСЛЕ инкремента | ✅ |
| 2. import time в горячем пути | `vpin_engine.py:55` | Удалён `import time` из `process_bar()` | ✅ |
| 3. z_history_size=500 | `vpin_engine.py:31,39` | Независимый буфер для Z-Score истории | ✅ |
| 4. Семантика is_buyer_maker | `radar_pipeline.py:153-155` | Комментарий + подтверждение полярности | ✅ |

### Мини-ТЗ v1.1 — 2 задачи (все выполнены)

| Задача | Файл | Суть | Статус |
|---|---|---|---|
| A. _z_count для прогрева | `vpin_engine.py:27,52,112-113,116` | Делитель `n = _z_count` вместо `_z_history_size` | ✅ |
| B. Аудит conflation.py | `conflation.py:96-101` | Код приведён дословно, полярность верна | ✅ |

### Тесты (4 штуки, написаны, НЕ запущены)

Файл: `tests/test_vpin_engine.py` (93 строки)

| Тест | Проверяет |
|---|---|
| `test_absorption_long_correct_window_price` | Корректность oldest_idx после инкремента |
| `test_z_history_independent_from_window_size` | Независимость z_history_size от window_size |
| `test_no_import_inside_process_bar` | Чистота горячего пути от import |
| `test_no_false_anomalies_on_warmup` | Отсутствие ложных аномалий при прогреве |

> **ВНИМАНИЕ: Тесты НЕ были запущены.** Инструмент `run_command` в среде агента падает 
> с ошибкой `exec: "powershell": executable file not found in %PATH%`.
> Создан вспомогательный скрипт `run_tests.py` в корне проекта.
> Оператору нужно запустить: `.venv\Scripts\python run_tests.py`

---

## 5. ТЕКУЩЕЕ СОСТОЯНИЕ КОДА (ДОСЛОВНО)

### vpin_engine.py — полный файл (147 строк)

Критические точки, которые нельзя трогать без понимания:

**Строки 63-78: Кольцевой буфер дисбалансов (O(1))**
```python
current_idx = self._index
self._price_history[current_idx] = price
self._running_imbalance_sum += (abs_imbalance - old_abs_imbalance)
self._imbalances[current_idx] = abs_imbalance

self._index += 1                     # ← ИНКРЕМЕНТ
if self._index >= self.window_size:
    self._index = 0
    self._is_filled = True
```

**Строки 102-122: Z-Score с динамическим делителем (O(1))**
```python
z_idx = self._z_index
old_vpin = self._vpin_history[z_idx]
self._vpin_sum += (current_vpin - old_vpin)          # delta-update, не np.sum()!
self._vpin_sq_sum += (current_vpin**2 - old_vpin**2) # delta-update
self._vpin_history[z_idx] = current_vpin

self._z_index += 1
if self._z_index >= self._z_history_size:
    self._z_index = 0
if self._z_count < self._z_history_size:
    self._z_count += 1

n = self._z_count if self._z_count > 0 else 1
mean_vpin = self._vpin_sum / n
variance = (self._vpin_sq_sum / n) - (mean_vpin**2)
std_dev = math.sqrt(variance) if variance > 1e-9 else 1e-9
```

**Строки 124-130: Iceberg/Absorption (читается ПОСЛЕ инкремента)**
```python
oldest_idx = self._index  # ПОСЛЕ инкремента = самый старый слот
price_start_window = self._price_history[oldest_idx]
price_return = price - price_start_window
```

---

## 6. КРИТИЧЕСКИЙ АНАЛИЗ НАШЕЙ РАБОТЫ

### Что сделано хорошо
1. Каждое изменение верифицировано через `view_file` до и после редактирования.
2. Не тронуты модули вне области ТЗ (Alpha Decay, AlertDebouncer, _dispatch_alert, reset_o1 логика, инфраструктура).
3. Дословные листинги из `conflation.py` приведены как доказательство проверки полярности.

### Слабые места и технический долг
1. **Тесты не запущены.** Мы не знаем, проходят ли они. `MagicMock` может быть несовместим с реальным `DollarBar` (который использует `Decimal`, а тесты передают `float/int`). Необходимо запустить `run_tests.py` и проверить.
2. **`_init_symbol` в radar_pipeline.py (строка 192-196)** не передаёт `z_history_size` в конструктор `O1VPINEngine`. Используется значение по умолчанию 500. Это не баг, но если оператор захочет изменить `z_history_size` через конфиг — потребуется доработка.
3. **Два движка баров** (`conflation.py` и `dollar_bars.py`) сосуществуют в домене. `dollar_bars.py` (ZeroAllocationDollarBarEngine) не используется RadarPipeline, но его наличие сбивает другие модели с толку. Возможно стоит вынести его в отдельный модуль или добавить явный комментарий.
4. **IEEE 754 drift** в `_running_imbalance_sum`: O(1) delta-update `+= (new - old)` на float64 может накапливать ошибку на миллионах баров. Для Advisory Mode это некритично (сброс при реконнекте), но для 24/7 работы может потребоваться периодическая рекалибровка.

---

## 7. ЛОВУШКИ ДЛЯ ДРУГИХ МОДЕЛЕЙ (АНТИПАТТЕРНЫ ГАЛЛЮЦИНАЦИЙ)

В процессе ревью двух итераций "архитектора" выявлены следующие паттерны:

### Ловушка 1: Путаница между `conflation.py` и `dollar_bars.py`
Архитектор дважды ссылался на `dollar_bars.py` как источник агрегации buy/sell volume. В реальности RadarPipeline использует `DollarBarEngine` из `conflation.py`. Файл `dollar_bars.py` содержит `ZeroAllocationDollarBarEngine` — другой класс с другой сигнатурой (`ingest_trade(price_scaled: int, qty_scaled: int, ts_exchange: int)`), без параметра `is_buyer_maker`.

### Ловушка 2: Перенос `oldest_idx` ПЕРЕД инкрементом
Архитектор предлагал читать `oldest_idx = self._index` ДО инкремента каретки. Это уничтожает логику Iceberg Detection, потому что до инкремента `self._index` указывает на только что записанный слот, а не на старейший. **Чтение ОБЯЗАНО происходить ПОСЛЕ строки `self._index += 1`.**

### Ловушка 3: Замена `math.sqrt` на `np.sqrt` в горячем пути
`math.sqrt` — чистый C-вызов, работает на скалярах. `np.sqrt` добавляет numpy boxing overhead. Также `math.sqrt(variance) if variance > 1e-9 else 1e-9` гарантирует ненулевой std_dev, а `np.sqrt(max(0.0, v))` допускает 0.0.

### Ловушка 4: Добавление `.fill(0.0)` в `reset_o1()`
Метод `reset_o1()` намеренно НЕ обнуляет numpy-массивы. Комментарий в коде: "NumPy arrays are reused; pointers are simply reset." Массивы заполнены нулями при создании в `__init__()`, а при сбросе старые значения будут перезаписаны новыми через кольцевой индекс.

### Ловушка 5: Фабрикация "рыночных сигналов"
Система ещё не запущена. Любые "снимки стакана" или "отклонённые сигналы" — чистая фикция.

---

## 8. СЛЕДУЮЩИЕ ШАГИ (НЕЗАКРЫТЫЕ ЗАДАЧИ)

1. **🔴 Запуск тестов** — `.venv\Scripts\python run_tests.py` — верифицировать, что все 4 теста проходят.
2. **🔴 Деплой на Tokyo VPS** — `deploy_to_server.ps1` → systemd service `gektor-radar.service`.
3. **🟡 Интеграционный тест** — запуск RadarPipeline с реальным WebSocket Bybit на 5 минут, проверка что алерты приходят в Telegram.
4. **🟡 Мониторинг прогрева** — после старта первые ~500 баров (30-60 мин) система не должна слать алерты (Z-Score стабилизируется).
5. **🟢 Документация** — обновить `LAUNCH_CONTEXT.md` с актуальными параметрами запуска.

---

## 9. ФАЙЛЫ ИЗМЕНЁННЫЕ В ЭТОЙ СЕССИИ

| Файл | Действие | Строк |
|---|---|---|
| `src/domain/vpin_engine.py` | Модифицирован (6 блоков замен) | 147 |
| `src/application/radar_pipeline.py` | Модифицирован (1 блок замен) | 298 |
| `tests/test_vpin_engine.py` | Создан с нуля | 93 |
| `run_tests.py` | Создан с нуля | 5 |

---

## 10. ФАЙЛЫ, КОТОРЫЕ НЕ ТРОГАЛИ (И НЕ НАДО)

- `src/domain/conflation.py` — код верный, полярность подтверждена
- `src/domain/dollar_bars.py` — вне области RadarPipeline
- `src/infrastructure/*` — вне области ТЗ
- `src/shared/alpha_config.py` — содержит VPIN_TIME_GAP_SEC, VPIN_DECAY_TAU_HOURS
- `.env.production` — конфиг продакшена
- `main.py` — точка входа Advisory Mode
