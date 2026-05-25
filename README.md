# GEKTOR APEX — Liquidity Radar (Advisory Mode)

[![status](https://img.shields.io/badge/status-advisory--only-blue)]()
[![radar](https://img.shields.io/badge/radar-VPIN%20%2B%20liquidity-green)]()
[![tests](https://img.shields.io/badge/tests-54%20passing-success)]()

> **GEKTOR APEX слушает поток сделок Bybit USDT-Linear, ищет аномалии ликвидности и шлёт алерты оператору в Telegram.**
>
> Это **Advisory-only** радар. Он не торгует, не отправляет ордера, не имеет интеграции с REST trade API.
> Решение о входе/выходе принимает только оператор-человек. Все алерты справочные, не являются финансовой рекомендацией.

---

## Что радар видит

Три независимых детектора, каждый со своим «инфоклоком» и временем реакции:

| Детектор | Что ловит | Warmup | Файл |
|---|---|---|---|
| **VPIN (O(1) ring buffer)** | Аномальный имбаланс buy/sell за окно 50 dollar-баров (Z-Score ≥ порог) | 50 баров на символ | `src/domain/vpin_engine.py` |
| **Sweep** | 5+ агрессорных тейкер-сделок одного знака за ≤ 30 сек, суммой > порог | Нет (мгновенно) | `src/domain/liquidity_detectors.py` |
| **Large Print** | Один трейд > N% от 24h оборота символа | Нет (мгновенно) | `src/domain/liquidity_detectors.py` |
| **OFI Pulse** | 1-минутный OFI > k × медиана за 1 час | ~1 мин (медиана набирается инкрементально) | `src/domain/liquidity_detectors.py` |

Все алерты идут одним каналом: in-process pipeline → SQLite outbox → `TelegramRelayWorker` → Telegram-чат оператора.

## Архитектурный контур

```
Bybit WS publicTrade
   │
   ▼  process_tick(symbol, side, price, size, ts)
BybitWSIngestion (parse + polarity)
   │
   ▼
RadarPipeline (per-symbol routing)
   ├── DollarBarEngine.process_tick → close → on_bar_closed(bar)
   │       └── O1VPINEngine.process_bar(bar) ─┐
   │                                          │ is_anomaly?
   ├── SweepDetector.process_tick     ────────┤
   ├── LargePrintDetector.process_tick ───────┤
   └── OFIPulseDetector.process_tick   ───────┤
                                              ▼
                                     PerSymbolRateLimiter
                                              ▼
                                     OutboxAlertSink → outbox_events row
                                              ▼
                                     TelegramRelayWorker (separate task)
                                              ▼
                                     TelegramRadarNotifier → @bot → operator
```

Параллельный страж: `PartialBlindnessWatchdog` каждые 10 сек проверяет, что тики продолжают приходить. Если 60 сек тишины — отдельный алерт через тот же Outbox.

## Технологический стек

| Слой | Технология |
|---|---|
| Язык | Python 3.11+ (strict typing) |
| Async | `asyncio` stdlib |
| JSON парсинг | `orjson` (Rust bindings) |
| HTTP/WS | `aiohttp` 3.9+ |
| Математика | `numpy` 1.26+ (O(1) ring buffers, без np.sqrt на скалярах) |
| ORM | SQLAlchemy 2.0 async |
| БД (default) | SQLite (WAL) через aiosqlite |
| БД (опционально) | PostgreSQL через asyncpg |
| Логирование | `loguru` + structured |
| Конфиг | `pydantic-settings` 2.x |
| Тесты | `pytest`, `pytest-asyncio`, `hypothesis` |

**Redis в радар-контуре v3.6.x не используется.** Класс `ReliableIngestionBuffer` существует, но из `main.py` не вызывается. Будет либо удалён, либо подключён в отдельной итерации.

## Конфигурация (.env)

Все параметры — из `.env` (см. полный пример `.env.example`):

| Переменная | Значение по умолчанию | Назначение |
|---|---|---|
| `ASYNC_DATABASE_URL` | `sqlite+aiosqlite:///gektor.db` | URL базы (SQLite или PostgreSQL) |
| `BOT_TOKEN` / `TELEGRAM_BOT_TOKEN` / `TG_BOT_TOKEN` | _required_ | Telegram bot token (любой из алиасов работает) |
| `CHAT_ID` | _required_ | Telegram chat ID |
| `DOLLAR_THRESHOLD_BASE` | `1000000` | Base размер dollar-бара. Может быть переопределён адаптивным per-symbol провайдером. |
| `ADAPTIVE_THRESHOLD_ENABLE` | `true` | Включить per-symbol адаптацию ($-порог от 24h turnover каждого символа) |
| `ADAPTIVE_TARGET_BARS_PER_DAY` | `200` | Целевое число баров/день для адаптивной формулы (BTC→max_usd, NIL→min_usd) |
| `ADAPTIVE_MIN_USD` | `20000` | Нижняя граница per-symbol порога |
| `ADAPTIVE_MAX_USD` | `5000000` | Верхняя граница per-symbol порога |
| `SENSITIVITY` | `active` | Тир чувствительности: `conservative` / `active` / `scanner` |
| `RADAR_COOLDOWN_SEC` | `300` | Per-symbol cooldown между алертами одного символа |
| `WATCHDOG_SILENCE_SEC` | `60` | После N секунд без тиков — алерт «PARTIAL BLINDNESS» |
| `PROXY_URL` / `TG_PROXY_URL` | _empty_ | SOCKS5/HTTP прокси (опционально, для регионов с блокировками) |
| `BYBIT_API_KEY` / `BYBIT_API_SECRET` | _empty_ | **Необязательны.** Радар использует только публичный WS, REST symbol discovery не подписывается. |

### Сенсорные тиры

| Тир | `z_threshold` | `vpin_window` | Cooldown | Ожидаемая частота |
|---|---|---|---|---|
| `conservative` | 2.5σ | 50 | 600 сек | 1-3 алерта в день (swing trader) |
| **`active` (default)** | **2.0σ** | **50** | **300 сек** | **5-15 алертов в день (intraday)** |
| `scanner` | 1.7σ | 30 | 120 сек | 30-50 алертов в день (скаут, больше шума) |

Tier меняется одной строкой в `.env` + `systemctl restart gektor.service`. Lock-in нет.

## Запуск

### Локально

```bash
make install
cp .env.example .env       # отредактируй BOT_TOKEN, CHAT_ID
make run-local
```

### На Tokyo VPS

```bash
ssh root@45.76.212.160
cd /opt/gektor
git pull origin devin/1779390912-apex-radar-hardening   # или main после мержа
systemctl restart gektor.service
journalctl -u gektor.service -f
```

В логе должна появиться строка вида:
```
[RADAR METRICS] ticks=N bars=N signals=N alerts=N symbols=N
```
И через минуту — первое `[RadarPipeline] ALERT ... z=...` (или раньше — sweep/large_print работают мгновенно).

## Тесты

```bash
make test          # full suite
make test-radar    # regression suite only (быстро, ~5 сек)
make test-vpin     # VPIN invariants + Hypothesis property tests
make test-pipeline # ingestor → radar → outbox end-to-end
```

Baseline радар-контура: **54 passed** (см. `tests/regression/`). Тесты сторожат:
- инварианты VPIN I1–I5 (см. `SINGLE_SOURCE_OF_TRUTH.md` §4)
- polarity (Bybit `S=="Sell"` ⇔ `is_buyer_maker=True` ⇔ `sell_volume_usd +=`)
- per-symbol cooldown
- per-symbol isolation (BTC сигнал не аффектит ETH стейт)
- SQLite-portable outbox SQL (никакого `FOR UPDATE SKIP LOCKED`)
- Watchdog state machine
- Settings aliases (регресс-сторожа после деплоя 2026-05-24)

Кроме регрессионных, в репо есть legacy-тесты (`tests/test_intent_capsule.py`, `tests/test_sniper_*.py` и т.д.), импортирующие модули за пределами радар-контура. Они помечены `skip` либо явно сломаны — это **не** показатель здоровья радара.

## Источники истины

| Документ | Назначение |
|---|---|
| [`SINGLE_SOURCE_OF_TRUTH.md`](./SINGLE_SOURCE_OF_TRUTH.md) | Канонический манифест архитектуры. Все противоречия — в его пользу. |
| [`AGENTS.md`](./AGENTS.md) | Правила для ИИ-агентов, работающих с репо. **Обязательно к прочтению перед любым PR.** |
| [`.cursorrules`](./.cursorrules) | Mirror AGENTS.md для Cursor IDE. |
| [`AUDIT_REPORT.md`](./AUDIT_REPORT.md) | Полный аудит репо (что реально работает, что галлюцинации, какие компоненты планируются). |

## Что NOT в коде (явно)

Чтобы не было путаницы — следующие имена встречаются в старых внутренних документах, но **в работающем коде их нет** (отделено от радара или вовсе галлюцинации эпохи v12.0):

- `TradeSweeper`, `Aegis`, `NerveCenter`, `StateReconciler`, `Stealth CUSUM`, `Spatial Basis Audit`, `Universe Shaker`, `Auto-Halt`, `Self-Excitation Filter`, `ProcessPoolExecutor` (для VPIN-математики), `FFD`, `Purged K-Fold CV`, `Embargoing`, `ABORT MISSION` алерты.

Если в каком-то PR/issue/документе ты видишь ссылку на одно из этих имён — это либо плановая фича, либо устаревшая документация. **Текущий контур работает БЕЗ них.**

## История версий

| Версия | Изменения |
|---|---|
| **v3.6.2** (2026-05-24) | Полный аудит репо. Liquidity-detectors (Sweep / LargePrint / OFI-Pulse). Adaptive per-symbol $-threshold. SENSITIVITY tier selector. Honest README+SSOT rewrite. |
| v3.6.1 (2026-05-24) | Tokyo deploy fixes: env aliases, optional Bybit keys, SQLite-aware pool. |
| v3.6.0 APEX-RADAR (2025-05-21) | VPIN hardening (I1–I5), RadarPipeline, watchdog, outbox SQL portability, регресс-тесты, SSOT манифест. |

---

_Status: Advisory-only liquidity radar. Operator decides every trade._
