# GEKTOR APEX — SINGLE SOURCE OF TRUTH (v3.6.2 APEX-RADAR)

> **Это единственный действующий манифест системы.** Все остальные документы (старые README v12.0, CLAUDE.md v2.0, CORE_MANIFESTO, LAUNCH_CONTEXT и любые более ранние спецификации) **АРХИВНЫЕ** — их можно читать как историю, но при противоречиях с этим документом приоритет имеет он.
>
> Детальный аудит кода вс. документации см. [`AUDIT_REPORT.md`](./AUDIT_REPORT.md).
>
> v3.6.2 добавляет: адаптивный per-symbol $-порог, SENSITIVITY-тиры, liquidity-detectors (Sweep / LargePrint / OFI Pulse). VPIN-ядро и инварианты I1–I5 не изменились.
>
> Любая ИИ-модель (архитектор, кодер, ревьюер), работающая в этом репозитории, **ОБЯЗАНА** прочитать этот файл первым. Если в коде или другом документе вы видите утверждение, противоречащее этому SSOT — это **галлюцинация** или **устаревший контур**; не следуйте ему.

---

## 1. Что такое GEKTOR APEX

**GEKTOR APEX — это аналитический радар институционального уровня, работающий ИСКЛЮЧИТЕЛЬНО в Advisory Mode.**

- Радар ищет **среднесрочные аномалии** (горизонт 4 часа — 2 недели) в потоке сделок Bybit USDT-Linear Futures.
- Радар **НЕ торгует, не отправляет ордера, не имеет интеграции с REST trade API**.
- Решения о входе/выходе из позиции принимает **только человек-оператор**, получив алерт в Telegram.
- Все алерты — справочные. Они не являются финансовой рекомендацией.

## 2. Жёсткие запреты (anti-patterns)

1. **HFT/Скальпинг запрещён.** Никаких 1m/1s баров. Никакого микро-OFI для входов на под-минутном таймфрейме.
2. **Автоисполнение запрещено.** Никаких `ExecutionEngine`, `OrderManager`, прямых API брокера.
3. **Тихие фейлы запрещены.** Никакого `except Exception: pass`. Все ошибки логируются и обрабатываются.
4. **Блокировки event loop запрещены.** CPU-bound математика выносится в `ProcessPoolExecutor` (для тяжёлых вычислений — не для горячего пути радара).
5. **Изоляция символов запрещена.** Любой символ анализируется с обязательной привязкой к BTC/ETH macro-контексту (когда такая привязка будет введена в этой ветке развития — на v3.6.0 этого нет, но запрет действует на будущие изменения).

## 3. Архитектура радара (v3.6.2 APEX-RADAR)

```
Bybit WS  →  BybitWSIngestion (parse + polarity)
                  ↓ process_tick(symbol, price, size, is_buyer_maker, exchange_ts)
            RadarPipeline (per-symbol routing, metrics)
                  ├─ DollarBarEngine.process_tick → close → on_bar_closed(bar)
                  │        ↓
                  │   O1VPINEngine.process_bar(bar)  # O(1) ring buffer, вармап 50 баров
                  │        ↓ if is_anomaly:
                  │
                  ├─ SweepDetector.process_tick      # мгновенно, без вармапа
                  ├─ LargePrintDetector.process_tick # мгновенно
                  └─ OFIPulseDetector.process_tick   # ~1 мин на медиану
                          ↓ любой детектор выдал alert
                  PerSymbolRateLimiter.allow(symbol)
                          ↓ if allowed:
                  OutboxAlertSink → INSERT INTO outbox_events (status=PENDING)
                          ↓
                  TelegramRelayWorker (separate task)
                          ↓ claim PENDING → notify → mark_delivered
                  TelegramRadarNotifier → оператор в Telegram
```

Параллельный контур: `PartialBlindnessWatchdog` опрашивает `RadarPipeline.metrics()` каждые 10 секунд и эскалирует через тот же Outbox, если за 60 секунд не было ни одного тика.

Адаптивный per-symbol $-порог: `AdaptiveDollarThresholdProvider` каждый час тянет 24h turnover всех символов через Bybit REST `/v5/market/tickers` и выдаёт порог = clamp(turnover_24h / target_bars_per_day, min_usd, max_usd). Это решает проблему tail-альтов, у которых при фикс-пороге $1M вармап занимал бы дни.

## 4. Канонические инварианты (НЕ нарушать)

| ID | Инвариант | Тест-сторож |
|----|-----------|--------------|
| **I1** | `oldest_idx` (baseline-цена для absorption) читается ПОСЛЕ инкремента кольцевого индекса. | `tests/regression/test_vpin_invariants.py::test_I1_*` |
| **I2** | `z_history_size` — независимый буфер (по умолчанию 500), `>= window_size`. | `test_I2_*` |
| **I3** | Делитель Z-Score — `_z_count` (реально заполненные слоты), НЕ `window_size`. На первом эмите z=0, не аномалия. | `test_I3_*` |
| **I4** | Time-decay применяется СНАЧАЛА к numpy-массивам, ПОТОМ скалярные суммы пересобираются из массивов. Инвариант `sum == np.sum(array)` сохраняется. | `test_I4_*` + property `test_P3_*` |
| **I5** | Полярность: `is_buyer_maker == True` означает, что **taker продал** → `sell_volume_usd += tick_usd`. Bybit `S == "Sell"` → `is_buyer_maker = True`. | `test_I5_*` + `test_pipeline_polarity_*` + `test_ingestor_polarity_*` |
| **I-noDrift** | Скалярные суммы пересобираются O(N) каждые 10 000 баров → защита от IEEE-754 drift. | `test_periodic_rebuild_*` + property `test_P3_*` |
| **I-noFill** | `reset_o1()` сбрасывает ring index, но НЕ зануляет numpy-массивы (zero-allocation). | `test_reset_o1_no_zero_fill` |

**Любая модификация этих инвариантов требует:**
1. Обновления соответствующего теста-сторожа.
2. Обновления этого SSOT.
3. Документации в PR-описании.

## 5. Технологический стек (жёстко зафиксирован)

| Слой | Технология | Версия |
|------|-----------|--------|
| Язык | Python | 3.11+ (strict typing) |
| Async | asyncio | stdlib |
| Сетевой парсинг | orjson | ^3.9.10 (Rust-биндинги) |
| HTTP/WS | aiohttp (+speedups) | 3.9.1 |
| Числа | numpy | 1.26.2 (O(1) ring buffers) |
| ORM | SQLAlchemy + async | 2.0.23 |
| БД (default) | SQLite (WAL) | через aiosqlite |
| БД (prod, опционально) | PostgreSQL | через asyncpg |
| Логирование | loguru | 0.7.2 (structured) |
| Конфиг | pydantic-settings | 2.1.0 |
| Тесты | pytest, pytest-asyncio, hypothesis | см. pyproject.toml |

**Redis в радар-контуре v3.6.x НЕ используется.** Старые версии этого документа упоминали Redis как буфер ингестии — это была аспирация. `ReliableIngestionBuffer` существует в репо, но из `main.py` не вызывается. Радар работает полностью в in-process pipeline + SQLite outbox.

## 6. Конфигурация (`.env`)

Минимальный набор + новые v3.6.2 переменные (полный список — `.env.example`):

```
BOT_TOKEN=<выдаёт @BotFather; ХРАНИТЬ ТОЛЬКО В .env.production НА VPS>
CHAT_ID=<Telegram chat ID куда слать алерты>
ASYNC_DATABASE_URL=sqlite+aiosqlite:///gektor.db

# Чувствительность радара (v3.6.2)
SENSITIVITY=active                       # conservative | active | scanner

# Dollar-bar порог
DOLLAR_THRESHOLD_BASE=1000000            # fallback для символов, в которых нет 24h turnover
ADAPTIVE_THRESHOLD_ENABLE=true           # per-symbol адаптация по Bybit /v5/market/tickers
ADAPTIVE_TARGET_BARS_PER_DAY=200         # целевая частота баров в день на символ
ADAPTIVE_MIN_USD=20000                   # минимум per-symbol порога
ADAPTIVE_MAX_USD=5000000                 # максимум

# Cooldown / watchdog
RADAR_COOLDOWN_SEC=300                   # Per-symbol cooldown между алертами
WATCHDOG_SILENCE_SEC=60                  # Триггер PARTIAL_BLINDNESS
WATCHDOG_POLL_SEC=10
```

### Сенсорные тиры (v3.6.2)

| Tier | `z_threshold` | `vpin_window` | `RADAR_COOLDOWN_SEC` | Ожидаемая частота |
|---|---|---|---|---|
| `conservative` | 2.5σ | 50 | 600 | 1-3 алерта в день (swing) |
| **`active` (default)** | **2.0σ** | **50** | **300** | **5-15 алертов в день (intraday)** |
| `scanner` | 1.7σ | 30 | 120 | 30-50 алертов в день (скаут) |

**ТОКЕН TELEGRAM НЕ КОММИТИТЬ В РЕПОЗИТОРИЙ.** Если он засветился в чате — отзовите его через @BotFather и выпустите новый.

## 7. Тестовая защита от регресса

Все инварианты защищены автоматически:

| Папка | Назначение |
|-------|-----------|
| `tests/unit/` | Юнит-тесты модулей (math_core, conflation, фасады) |
| `tests/regression/test_vpin_invariants.py` | I1-I5, time-decay, IEEE-drift, hot-path clean |
| `tests/regression/test_vpin_properties.py` | Hypothesis property-based: P1-P5 |
| `tests/regression/test_radar_pipeline.py` | Pipeline + polarity + rate limit + isolation + error resilience |
| `tests/regression/test_outbox_sqlite.py` | Outbox SQL portable claim pattern (SQLite + PostgreSQL) |
| `tests/regression/test_ingestor_integration.py` | End-to-end синтетический Bybit-тейп → алерт |
| `tests/regression/test_watchdog.py` | PARTIAL BLINDNESS state machine |
| `tests/test_vpin_engine.py` | User-authored sanity tests (4) — absorption baseline, z_history independence, no inline import, finite z_score after warmup |

Запуск:

```bash
.venv/bin/python -m pytest tests/regression tests/test_vpin_engine.py -q
# Ожидаемый результат v3.6.2: 64+ passed (из них 54 — радар-контур v3.6.0+v3.6.1, ~10 новых — liquidity_detectors + adaptive_threshold).
```

Остальные тесты в репо (`tests/test_intent_capsule.py`, `tests/test_sniper_*.py`, `tests/test_schrodinger_ledger.py`, `tests/unit/test_state_healer.py`, и др.) — **legacy**, импортируют орфан-модули. Не входят в контур здоровья радара v3.6.x. См. AUDIT_REPORT.md.

## 8. Out-of-scope подсистемы (помечены skip с явной причиной)

| Тестовый файл | Причина skip |
|---------------|-------------|
| `tests/unit/test_state_healer.py` | L6 StateHealer — Trading Mode, не Advisory radar |
| `tests/unit/test_microstructure.py` | L2 MicrostructureDefender — API диверговал, требует отдельной стабилизации |
| `tests/unit/test_gektor_l2_engine.py` | L2 parse_levels — pre-existing broken import |
| `tests/chaos/test_flatline.py` | Зависимость от Redis в тесте |
| `tests/test_architect_v2.py` | Зависимость от lancedb (RAG-стек) |
| `tests/test_chroma_direct.py` | Зависимость от chromadb (RAG-стек) |
| `tests/test_skill_retrieval.py` | Зависимость от Redis + LLM-стек |

Эти контуры существуют в репо для других режимов (Trading Mode, RAG-помощник, L2 микроструктура). В v3.6.0 APEX-RADAR они не используются и не блокируют производственную работу радара.

## 9. Антигаллюцинационный протокол для ИИ-моделей

Перед тем, как утверждать, что какой-то модуль/класс/функция существует:

1. **Откройте файл и процитируйте строку.** Если файла нет — это галлюцинация.
2. **Проверьте `git ls-files | grep <name>`.** Если не нашли — галлюцинация.
3. **Проверьте импорты.** Если модуль импортируется только в тестах, помеченных skip — это не работающий код.
4. **Никогда не утверждайте, что "тесты проходят" без явного `pytest` запуска.**

Подробнее: `AGENTS.md` в корне репозитория.

## 10. История версий

| Версия | Дата | Изменения |
|--------|------|-----------|
| 3.6.2 APEX-RADAR | 2026-05-24 | Полный аудит репо (AUDIT_REPORT.md). Адаптивный per-symbol $-порог. SENSITIVITY-тиры. Liquidity-detectors (Sweep / LargePrint / OFI Pulse). Честная перепись README. |
| 3.6.1 | 2026-05-24 | Tokyo deploy fixes: env aliases, optional Bybit keys, SQLite-aware pool. |
| 3.6.0 APEX-RADAR | 2025-05-21 | VPIN hardening (I1-I5), RadarPipeline, Outbox SQL portability, watchdog, регрессионные + property тесты, SSOT |
| 3.5.0 | прошлая итерация | Спецификация RadarPipeline (код не запушен) |
| ... | ... | Архивная история — см. README_*.md / *_MANIFESTO.md |
