# GEKTOR APEX — Полный аудит репозитория (v3.6.2)

> **Дата:** 2026-05-24
> **Версия кода:** ветка `devin/1779390912-apex-radar-hardening`, коммит `42e685a`
> **Метод:** AST-трассировка `import`-графа от `main.py` + `grep -rn` каждого утверждения каждого `.md` файла.
> **Цель:** установить, какие компоненты реально существуют в работающем коде, какие — галлюцинации в документации, и какие изменения нужны для активного трейдинга 500 монет (а не «чёрного лебедя»).
>
> Этот отчёт — не маркетинг. Он жёсткий и проверяемый. Каждое утверждение можно перепроверить теми же `grep`-запросами, что приведены ниже.

---

## 1. Структура репозитория: что реально работает

### 1.1. Полная инвентаризация `src/`

| Метрика | Значение |
|---|---|
| Всего `.py` файлов в `src/` | **105** |
| Достижимы из `main.py` (импорт-граф, включая `__init__.py` пакетов) | **23** |
| **Орфаны** (нигде не используются основным контуром) | **82** |

Метод: AST-парсер построил BFS импорт-граф от `main.py`, включая ансекторов-пакетов через их `__init__.py`. Скрипт: `/tmp/trace_imports.py`.

### 1.2. Радар-контур (23 файла)

```
main.py
├── src/application/
│   ├── __init__.py
│   ├── formatters.py            ← Telegram message formatter
│   ├── microstructure.py        ← (используется через formatters)
│   ├── outbox_alert_sink.py     ← OutboxAlertSink: alert → DB outbox
│   ├── outbox_relay.py          ← TelegramRelayWorker: DB outbox → Telegram
│   ├── radar_pipeline.py        ← КАНОНИЧЕСКИЙ pipeline (tick → bar → VPIN → alert)
│   └── watchdog.py              ← PartialBlindnessWatchdog (silence detector)
├── src/domain/
│   ├── __init__.py
│   ├── conflation.py            ← DollarBarEngine (dollar bars)
│   ├── exit_protocol.py         ← (используется через events)
│   ├── vpin_engine.py           ← O(1) VPIN ring buffer
│   └── entities/
│       ├── __init__.py
│       └── events.py            ← ExecutionEvent, ConflatedEvent
├── src/infrastructure/
│   ├── __init__.py
│   ├── bybit.py                 ← BybitRestClient (symbol discovery + 24h tickers)
│   ├── bybit_ws_ingestion.py    ← BybitWSIngestion (WS reader, polarity)
│   ├── config.py                ← Settings (pydantic-settings)
│   ├── telegram_notifier.py     ← TelegramRadarNotifier (with retries)
│   └── database/
│       ├── __init__.py
│       └── connection.py        ← DatabaseManager (SQLAlchemy dispatch)
└── src/shared/
    ├── alpha_config.py          ← AlphaConfig (loads thresholds from JSON/env)
    └── resilience.py            ← LoopMonitor, MemoryShield (used by bybit.py)
```

**Это весь GEKTOR APEX, который реально работает.** Остальные 82 файла — мертвы.

### 1.3. Орфаны (82 файла, не достижимы из `main.py`)

Перечень — в Приложении A. Эти файлы остались от трёх предыдущих архитектурных эпох:

- **Эпоха «Trading Bot»** — `alpha_decay`, `dead_mans_switch`, `friction_guard`, `triple_barrier`, `gravitational_anchor`, `intent_capsule`, `intent_ledger`, `schrodinger_ledger`, `tilt_breaker`, `sentry_brain`, `sentinel_watchdog`, `supervisor`, `vanguard`, `defender`, `state_healer`, `operator_gate`, `quarantine`, `runtime_guardian`. Это код для торгующего бота. **GEKTOR APEX по определению Advisory-only (см. SSOT §2)**, поэтому весь этот стек не нужен.

- **Эпоха «v12.0 NerveCenter / TradeSweeper»** — `gektor_l2/` (20+ файлов: `seqlock_orderbook`, `nd_orderbook`, `ws_multiplexer`, `book_state`, `bybit_orderbook_rest`, `reconnect_throttle`, `resync_gate`, `wire_parse`, `universe_manager`, `scaling`, `constants`, `errors`, `protocols`, `conflation`). Это L2 orderbook stack. **GEKTOR APEX работает только с публичными trade-тиками `publicTrade`**, никакой L2 не реализован.

- **Эпоха «Ambitious side-streams»** — `event_bus`, `feature_store`, `flight_recorder`, `hydration`, `information_clocks`, `ipc`, `latency_shield`, `macro_risk`, `monitoring`, `network_tuning`, `oob_defender`, `rest_layer`, `shadow_ledger`, `shm_layout`, `spillover_writer`, `sqlite_outbox`, `state_healer`, `telegram_gateway`, `telemetry`, `time_sync`, `vault`, `voip`, `watchdog`, `zero_alloc_parser`. Все недостижимы.

- **Дубликаты конфигов** — `src/shared/config.py`, `src/shared/logger.py`, `src/shared/monitoring.py`, `src/shared/error_handler.py`, `src/shared/gpu_monitor.py`. Не используются.

- **Дубликаты домена** — `src/domain/dollar_bar.py` (≠ `conflation.py`), `src/domain/dollar_bars.py`, `src/domain/cortex.py`, `src/domain/macro_regime.py`, `src/domain/markets.py`, `src/domain/math_core.py`, `src/domain/quant_radar.py`, `src/domain/scoring.py`, `src/domain/alpha_model.py`, `src/domain/state_snapshoter.py`. Альтернативные версии того же, что уже есть в радар-контуре.

**Решение по орфанам (отложено в отдельный PR — «cleanup»):** массовое удаление без понимания не делаем. Каждый файл проверяется отдельно, тесты, импортирующие легаси, помечаются `pytest.skip` с причиной. Сейчас фокус — НЕ удаление мёртвого кода (это уже не блокирует деплой), а **повышение чувствительности активного радара**.

---

## 2. Документация vs реальность: галлюцинации

### 2.1. README.md — критические галлюцинации (TOP половина файла)

| Утверждение в README | Реальность в коде | Доказательство |
|---|---|---|
| `TradeSweeper (L3 Aggregation)` | **Не существует.** | `grep -rn TradeSweeper src/` → 0 совпадений |
| `Identifies Aggressor Signatures (Icebergs, Impulse, Distribution)` | Есть только `absorption_detected` в `vpin_engine.py:235-245` (бинарный флаг, не «signature») | Файла или класса с таким API нет |
| `Pessimistic Sweep Fill (v12.0)` | **Не существует.** | `grep -rn PessimisticSweep src/` → 0 |
| `Dynamic Universe Shaker` | **Не существует.** | `grep -rn UniverseShaker src/` → 0 (есть `universe_manager.py` в `gektor_l2/`, но это орфан) |
| `Background autonomous loop for monitoring pool rebalancing (15m interval)` | **Не существует.** Universe фиксируется один раз при старте (`fetch_active_symbols()`, `main.py:123`) и не переоценивается. | `grep -rn "15m\|15.*interval\|rebalance" src/` → ноль для радар-контура |
| `Filters: Turnover > $50M, Spread < 15bps` | **Не реализованы.** Универс — все `USDT-Linear` контракты без фильтрации. | `bybit.py::fetch_active_symbols()` возвращает весь список |
| `Macro-Radar Engine: Alpha-Neutral Discovery (beta-neutralizing CVD against BTC)` | **Не существует.** VPIN считается изолированно по каждому символу без BTC-привязки. | `grep -rn "beta\|alpha.neutral\|cvd" src/domain/vpin_engine.py` → 0 |
| `Stealth CUSUM Detector (TWAP/VWAP accumulation drift)` | **Не существует.** | `grep -rn "CUSUM\|StealthCUSUM\|TWAP\|VWAP" src/` → 0 (есть упоминания в `secrets/alpha_weights.json` ключе `exit_cusum_reversal_sigma`, но эта ветка кода не работает) |
| `Spatial Basis Audit (Perp vs Spot)` | **Не существует.** | `grep -rn "SpatialBasis\|perp.*spot\|basis" src/` → 0 |
| `Aegis (Decay Sentinel)` | **Не существует.** | `grep -rn Aegis src/` → 0 |
| `Persistent monitoring of Signal Expectancy (WR & R-Multiple) in Redis` | **Не существует.** WR/R-Multiple вообще не считаются. Redis в радар-контуре опционально (только `ReliableIngestionBuffer`, который никем не вызывается). | См. ниже §2.5 |
| `Auto-Halt Protocol: Instantly disables advisory signals on mathematical Alpha decay` | **Не существует.** Радар не имеет понятия «alpha decay». | `grep -rn "AutoHalt\|alpha_decay\|auto.halt" src/application/ src/infrastructure/ src/domain/` → попадания только в орфан `alpha_decay.py` |
| `Self-Excitation Filter (suppresses feedback loops)` | **Не существует.** | `grep -rn "SelfExcitation\|excitation" src/` → 0 |
| `Zero-GIL Math: ProcessPoolExecutor for Z-Score, MAD, Variance Ratio` | **НЕ ВЫПОЛНЯЕТСЯ В РАДАР-КОНТУРЕ.** Упоминание `ProcessPoolExecutor` есть только в docstring `src/domain/math_core.py:80` — но этот файл орфан, не импортируется. Реальный VPIN-движок (`vpin_engine.py::process_bar`) считает Z-Score и std_dev **в основном event loop**. | См. §2.5 |
| `NerveCenter (Redis-Bus): Ultra-low latency asynchronous synchronization across distributed shards` | **Не существует.** | `grep -rn NerveCenter src/` → 0 |
| `StateReconciler: Automatic REST-based gap filling on WebSocket disconnections` | **Полу-существует, но не как описано.** Есть `src/application/reconnect_reconciler.py` (орфан, не импортируется в `main.py`) — это просто WS-reconnect, никаких REST gap-fill там нет. | `grep -rn "REST.*gap\|gap.*fill" src/` → 0 |
| `Aegis-Halt: Manual or automatic global kill-switch` | **Не существует.** | `grep -rn "AegisHalt\|kill.switch\|kill_switch" src/` → 0 |

**Вердикт по верхней половине README:** ~90% — выдумка. Это маркетинговый текст эпохи v12.0, написанный до того, как фактический радар был построен в v3.6.0. Документ не обновили после реализации.

### 2.2. README.md — нижняя половина (GETTING STARTED) — точна

Блоки `Requirements`, `Deployment`, `Monitoring`, `Tests`, `Hardened in v3.6.0` — соответствуют коду. Это часть, которую я (Devin) переписал в коммите `d696579`. Без галлюцинаций.

### 2.3. SINGLE_SOURCE_OF_TRUTH.md — мелкие расхождения

| Утверждение в SSOT | Реальность | Действие |
|---|---|---|
| §5: `Redis 5.0.1 для буфера ингестии` | Redis в радар-контуре **никем не используется**. `ReliableIngestionBuffer` существует, но не вызывается из `main.py`. | Уточнить: «Redis — опционален и не используется в Advisory радаре v3.6.x». |
| §6: `DOLLAR_THRESHOLD_BASE=100000` | В коде (`config.py:65`) default = `1_000_000`, на Tokyo VPS в production .env стоит `1000000`. Между SSOT и кодом несогласованность в 10×. | Привести SSOT в соответствие с кодом ИЛИ ввести адаптивный per-symbol порог (см. §4). |
| §3 Архитектура | Описание точное, схема pipeline соответствует `radar_pipeline.py`. | Изменений не требуется. |
| §4 Инварианты I1–I5 | Все 5 инвариантов реально защищены тестами в `tests/regression/test_vpin_invariants.py`. | Без изменений. |
| §7 «68+ passed, 8 skipped» | Сейчас `tests/regression/ + tests/test_vpin_engine.py` дают **54 passed** (свежий прогон 2026-05-24 18:55 UTC, см. §3). Остальное — старые user-тесты, многие импортируют легаси. | Уточнить в SSOT: «54+ passed в радар-контуре, остальные тесты — legacy». |

### 2.4. CLAUDE.md (v2.0 STRICT) — устаревшие утверждения

| Утверждение в CLAUDE.md | Реальность | Действие |
|---|---|---|
| `FFD (Fractional Differentiation): Preserve memory while achieving stationarity` | **Не существует.** | Удалить пункт. |
| `Purged K-Fold CV: Eliminate data leakage and autocorrelation` | Файл `src/domain/entities/purged_cv.py` — **орфан**, не используется. | Удалить пункт. |
| `Embargoing: A mandatory 1% temporal gap between training and testing` | **Не существует.** | Удалить пункт. |
| `PostgreSQL/TimescaleDB (Signals) + Redis (Volatile telemetry)` | По умолчанию SQLite, Redis опционален. | Привести в соответствие. |
| `[🚨 ABORT MISSION] alerts if microstructural premise breaks` | **Не существует.** | Удалить пункт. |
| `ProcessPoolExecutor` для тяжёлой математики | Не реализовано в радар-контуре. VPIN — O(1) per bar (numpy ring buffer), event loop не блокируется. | Уточнить: «требование к будущим тяжёлым модулям, не к VPIN-движку». |
| `Always read GEKTOR_MANIFESTO.md` | Файла не существует. | Заменить на ссылку на SSOT. |

CLAUDE.md следует **существенно ужать** до короткого pointer-а на SSOT, либо привести в полное соответствие. Сейчас он противоречит и SSOT, и коду.

### 2.5. AGENTS.md / .cursorrules — точны

Эти два файла дисциплинируют ИИ-агентов. Все правила в них соответствуют реальности (Advisory-only, без `FOR UPDATE SKIP LOCKED`, без `except Exception: pass`, инварианты I1–I5). Никаких галлюцинаций. **Изменений не требуется.**

### 2.6. Анти-паттерны, реально присутствующие в коде

Несмотря на то, что AGENTS.md прямо запрещает `try: ... except Exception: pass`, в коде есть **3 живых нарушения**:

| Файл | Строки | Нарушение | Серьёзность |
|---|---|---|---|
| `main.py` | 204-207 | `try: await asyncio.timeout(3.0, self.tg._queue.join()); except Exception: pass` | КРИТИЧНО — `asyncio.timeout()` в Python 3.11+ это **async context manager**, не функция-обёртка. Этот вызов сразу падает в `TypeError`, а `except Exception: pass` это маскирует. Очередь алертов на shutdown **не дожидается отправки** — алерты теряются. |
| `main.py` | 237-241 | то же самое в `hot_reload` | КРИТИЧНО — то же. |
| `config.py` | 138-147 | `try: ctypes.c_ubyte.from_address(...); except Exception: pass` в `wipe_sensitive()` | СРЕДНЯЯ — функция «зануляет» строку в памяти через ctypes по найденному offset. При любой ошибке поиска offset, очистка тихо пропускается. Также сам подход к стиранию иммутабельных Python-строк через ctypes ненадёжен (CPython может кэшировать short strings). |

**Все три требуют исправления** в текущем PR.

---

## 3. Тесты: фактическое состояние

Свежий прогон (2026-05-24 18:55 UTC, ветка `devin/1779390912-apex-radar-hardening`):

```bash
.venv/bin/python -m pytest tests/regression tests/test_vpin_engine.py -q
# → 54 passed, 39 warnings in 4.57s
```

Полный набор тестов делится на:
- **Радар-контур (54 теста, все зелёные):** `tests/regression/*` + `tests/test_vpin_engine.py`. Защищают инварианты I1–I5, polarity, watchdog, outbox SQL-портабельность, pipeline integration, settings aliases (новые регрессы из деплоя 2026-05-24).
- **Legacy (примерно 35-40 тестов, многие сломаны):** `tests/test_intent_capsule.py`, `tests/test_schrodinger_ledger.py`, `tests/test_tilt_breaker.py`, `tests/test_sniper_*.py`, `tests/unit/test_state_healer.py`, `tests/unit/test_microstructure.py`, `tests/unit/test_gektor_l2_engine.py`, `tests/chaos/test_flatline.py`. Импортируют орфан-модули.

**Действие:** в SSOT прямо записать, что «54 теста — это сторожа радара v3.6.x; остальное legacy и не показатель здоровья системы».

---

## 4. Радар-настройка: «чёрный лебедь» vs «активный трейдинг»

### 4.1. Сейчас (production на Tokyo VPS)

| Параметр | Значение | Источник |
|---|---|---|
| `DOLLAR_THRESHOLD_BASE` | $1 000 000 / бар | `/opt/gektor/.env` |
| `VPIN window_size` | 50 баров | `vpin_engine.py:87` (default) |
| `VPIN z_history_size` | 500 баров | `vpin_engine.py:90` (default) |
| `z_threshold` | 2.5σ | `main.py:64` (когда `alpha.VPIN_ANOMALY_Z == 0`) |
| `RADAR_COOLDOWN_SEC` | 300 секунд | `/opt/gektor/.env` |
| Universe | весь Bybit USDT-Linear (~569 контрактов) | `bybit.py::fetch_active_symbols` |

### 4.2. Сколько времени до первого алерта

При фиксированном пороге $1M/бар время заполнения первого бара = $1M / (turnover_24h / 86400).
Warmup VPIN-движка = 50 баров (НЕ 500 — это `z_history_size`, заполняется ленива через `_z_count`, см. invariant I3).

| Символ | 24h turnover (оценка) | Бар $1M собирается за | Warmup 50 баров |
|---|---|---|---|
| BTCUSDT | $50 000 000 000 / день | ~1.7 сек | ~85 сек |
| ETHUSDT | $20 000 000 000 / день | ~4 сек | ~3.3 мин |
| SOLUSDT | $3 000 000 000 / день | ~30 сек | ~25 мин |
| MEMEUSDT mid-cap | $300 000 000 / день | ~5 мин | ~4 часа |
| NILUSDT tail | $10 000 000 / день | ~2.5 часа | **~5 дней** |

Из 569 контрактов: топ-10 «отогреваются» за минуты, топ-100 — за часы, оставшиеся 469 — никогда (или раз в неделю).

**Это и есть «чёрный лебедь конфигурация».** Не из-за `z_threshold=2.5σ` (нормальная сигма), а из-за **фиксированного $1M-порога**, который для tail-альтов означает **отсутствие алертов вообще**.

### 4.3. Что нужно для активного трейдинга

User cite: «торгуем все», «настрой радар чтоб видеть ликвидрность и анамалии», «активно торгую уже 4 года».

**Цель:** при 500 активно торгуемых монетах ожидать 5-15 алертов в день (`Active` тир) или 30-50 (`Scanner` тир), а не 0-3.

### 4.4. Решение (см. §5)

Три независимых улучшения, каждое решает свой аспект:

1. **Адаптивный per-symbol $-порог** → решает warmup-проблему для tail-альтов.
2. **Сенсорные тиры (`SENSITIVITY=conservative|active|scanner`)** → даёт оператору один-кликовый выбор чувствительности.
3. **Liquidity-detectors без warmup'а (Sweep, Large Print, OFI Pulse)** → дают мгновенные сигналы, не дожидаясь Z-score статистики.

---

## 5. Реализация — изменения в этом PR

### 5.1. Документация

- [ ] **Перепишу README.md** — оставлю только то, что реально работает в радар-контуре. Никакого TradeSweeper / Aegis / NerveCenter / Stealth CUSUM / StateReconciler / ProcessPoolExecutor.
- [ ] **Обновлю SSOT.md** — уточню §5 (Redis не используется), §6 (DOLLAR_THRESHOLD coherence), §7 (54 passed).
- [ ] **Сокращу CLAUDE.md** до короткого pointer-а на SSOT.

### 5.2. Bug fixes

- [ ] **`main.py:204-207, 237-241`** — заменить сломанный `asyncio.timeout(t, coro)` на корректный `asyncio.wait_for(coro, timeout=t)` или `async with asyncio.timeout(t)`. Убрать `except Exception: pass`.
- [ ] **`config.py:138-147`** — убрать `try: except Exception: pass` в `wipe_sensitive()`, заменить на конкретные `except (TypeError, ValueError, OSError)` с логированием.

### 5.3. Новые компоненты

#### 5.3.1. `SENSITIVITY` тир (минимальное изменение, .env-driven)

В `.env`:
```
SENSITIVITY=active                       # conservative | active | scanner
```

В `config.py` добавляется один enum-поле. В `main.py` — отображение в `z_threshold` и `window_size`:

| Тир | `z_threshold` | `window_size` | `RADAR_COOLDOWN_SEC` | Ожидаемая частота алертов |
|---|---|---|---|---|
| `conservative` | 2.5σ | 50 | 600 (10 мин) | 1-3 в день |
| **`active` (default)** | **2.0σ** | **50** | **300 (5 мин)** | **5-15 в день** |
| `scanner` | 1.7σ | 30 | 120 (2 мин) | 30-50 в день |

#### 5.3.2. Адаптивный per-symbol $-порог

Новый модуль `src/infrastructure/adaptive_threshold.py`:

```
AdaptiveDollarThresholdProvider
├── __init__(rest_client, target_bars_per_day=200, min_usd=20_000, max_usd=5_000_000)
├── async refresh()      ← fetches 24h turnover for ALL symbols
└── threshold_for(symbol) → Decimal
```

Логика: `threshold = clamp(turnover_24h / target_bars_per_day, min_usd, max_usd)`.

Для **NILUSDT** ($10M/день, 200 баров/день) → $50k/бар → warmup 50 баров занимает ~2 часа вместо 5 дней.
Для **BTC** ($50B/день, 200 баров/день) → $250M/бар, обрежется max_usd до $5M → warmup ~85 сек.

`RadarPipeline` принимает callable `threshold_provider: Callable[[str], Decimal]` и спрашивает per-symbol порог при создании `DollarBarEngine`. Refresh — раз в час фоновой задачей.

#### 5.3.3. Liquidity-detectors без warmup'а

Новый модуль `src/domain/liquidity_detectors.py`:

```python
class SweepDetector:
    """Fires when N consecutive aggressor trades (same side) accumulate > $threshold within W seconds.
    Pure tick-level state, no warmup. O(1) per tick."""

class LargePrintDetector:
    """Fires when a single trade size > pct_of_24h_turnover. Instant."""

class OFIPulseDetector:
    """Fires when 1-minute order flow imbalance > k × rolling 1-hour median.
    Uses a small bounded deque; warmup = 1 hour, but median fills incrementally."""
```

Все три работают на сырых тиках, не зависят от VPIN warmup'а. Эмитят отдельный тип алерта (`LiquidityAlert`) с `kind="SWEEP" | "LARGE_PRINT" | "OFI_PULSE"`. Используют тот же Outbox → Telegram path.

#### 5.3.4. Wiring

`main.py` создаёт:
- `AdaptiveDollarThresholdProvider(bybit_rest, ...)` + фон-задача refresh
- `LiquidityDetectorBank` (контейнер из трёх детекторов)
- Передаёт в `RadarPipeline(threshold_provider=..., liquidity_detectors=...)`
- `RadarPipeline.on_trade` после VPIN-обработки прогоняет тик через `liquidity_detectors.process_tick()`

### 5.4. Тесты

- `tests/regression/test_liquidity_detectors.py` — unit-тесты Sweep / LargePrint / OFIPulse
- `tests/regression/test_adaptive_threshold.py` — тест clamping + per-symbol logic
- Обновить `test_radar_pipeline.py` для нового сигнатуры (с default-providers — обратно совместимо)

Ожидаемый итог: **54 → ~64 passed** в радар-контуре.

### 5.5. Деплой на Tokyo

После мержа:
```bash
ssh root@45.76.212.160 'cd /opt/gektor && git pull origin devin/1779390912-apex-radar-hardening'
ssh root@45.76.212.160 'echo "SENSITIVITY=active" >> /opt/gektor/.env'
ssh root@45.76.212.160 'systemctl restart gektor.service'
```

Observability: в течение 2 часов наблюдаем `[RADAR METRICS] ticks=... bars=... signals=... alerts=...`. Ожидаемая частота alerts > 0 в течение первого часа (sweep/large_print мгновенно срабатывают).

---

## 6. Не входит в этот PR (отложено)

- **Массовое удаление 82 орфанов.** Каждый файл нужно проверять на скрытые зависимости. Запланировано как отдельный `cleanup(repo)` PR с детальным логом, что и почему удалено.
- **BTC/ETH macro-regime контекст для alpha-neutral CVD.** Это требует второго потока (Spot vs Perp basis), сейчас не реализован. Это NEXT-step после стабилизации radar-контура.
- **ProcessPoolExecutor для VPIN-математики.** На 500 символов × 50 баров × O(1) = тривиально, event loop не блокируется. Внедрять PPE сейчас — преждевременная оптимизация. Запланировано на случай, когда (а) добавим тяжёлый алгоритм (FFD, CUSUM с rolling window), (б) увидим p99 latency `process_bar()` > 1ms.
- **L2 orderbook integration** (`gektor_l2/` стек). Огромный объём кода требует ревью + тестов + интеграции с реальным L2 endpoint. Это шаг к «настоящему institutional grade», но не блокирует current радар.

---

## Приложение A: полный список 82 орфан-файлов

```
src/application/alpha_decay.py
src/application/dead_mans_switch.py
src/application/defender.py
src/application/operator_gate.py
src/application/quarantine.py
src/application/reconnect_reconciler.py
src/application/runtime_guardian.py
src/application/sentinel.py
src/application/sentinel_watchdog.py
src/application/sentry_brain.py
src/application/state_healer.py
src/application/supervisor.py
src/application/vanguard.py
src/domain/alpha_model.py
src/domain/cortex.py
src/domain/dollar_bar.py
src/domain/dollar_bars.py
src/domain/entities/agent_output.py
src/domain/entities/fill_simulator.py
src/domain/entities/purged_cv.py
src/domain/friction_guard.py
src/domain/gravitational_anchor.py
src/domain/intent_capsule.py
src/domain/intent_ledger.py
src/domain/macro_regime.py
src/domain/markets.py
src/domain/math_core.py
src/domain/quant_radar.py
src/domain/schrodinger_ledger.py
src/domain/scoring.py
src/domain/shadow_ledger.py
src/domain/state_snapshoter.py
src/domain/tilt_breaker.py
src/domain/triple_barrier.py
src/infrastructure/conflation.py
src/infrastructure/database/backup.py
src/infrastructure/database/session_db.py
src/infrastructure/event_bus.py
src/infrastructure/feature_store.py
src/infrastructure/flight_recorder.py
src/infrastructure/gektor_l2/__init__.py
src/infrastructure/gektor_l2/book_state.py
src/infrastructure/gektor_l2/bybit_orderbook_rest.py
src/infrastructure/gektor_l2/conflation.py
src/infrastructure/gektor_l2/constants.py
src/infrastructure/gektor_l2/errors.py
src/infrastructure/gektor_l2/nd_orderbook.py
src/infrastructure/gektor_l2/protocols.py
src/infrastructure/gektor_l2/reconnect_throttle.py
src/infrastructure/gektor_l2/resync_gate.py
src/infrastructure/gektor_l2/scaling.py
src/infrastructure/gektor_l2/universe_manager.py
src/infrastructure/gektor_l2/wire_parse.py
src/infrastructure/gektor_l2/ws_multiplexer.py
src/infrastructure/hydration.py
src/infrastructure/information_clocks.py
src/infrastructure/ipc.py
src/infrastructure/latency_shield.py
src/infrastructure/macro_risk.py
src/infrastructure/monitoring.py
src/infrastructure/network_tuning.py
src/infrastructure/oob_defender.py
src/infrastructure/rest_layer.py
src/infrastructure/seqlock_orderbook.py
src/infrastructure/shadow_ledger.py
src/infrastructure/shm_layout.py
src/infrastructure/spillover_writer.py
src/infrastructure/sqlite_outbox.py
src/infrastructure/state_healer.py
src/infrastructure/telegram_gateway.py
src/infrastructure/telemetry.py
src/infrastructure/time_sync.py
src/infrastructure/vault.py
src/infrastructure/voip.py
src/infrastructure/watchdog.py
src/infrastructure/zero_alloc_parser.py
src/preflight_check.py
src/shared/config.py
src/shared/error_handler.py
src/shared/gpu_monitor.py
src/shared/logger.py
src/shared/monitoring.py
```

— **Конец отчёта.**
