# GEKTOR APEX — Краткий манифест (для Claude / Cursor / любой ИИ-модели)

> Этот файл — **короткий pointer**. Полная архитектура и правила:
> - [`SINGLE_SOURCE_OF_TRUTH.md`](./SINGLE_SOURCE_OF_TRUTH.md) — каноническая архитектура (приоритет над всем остальным).
> - [`AGENTS.md`](./AGENTS.md) — правила для ИИ-агентов.
> - [`AUDIT_REPORT.md`](./AUDIT_REPORT.md) — что реально работает, что галлюцинации, что планируется.
> - [`README.md`](./README.md) — пользовательский обзор.
>
> При любом противоречии между этим файлом и SSOT — приоритет у SSOT.

## Идентичность

**GEKTOR APEX — это Advisory-only радар.** Он слушает Bybit, ищет аномалии ликвидности, шлёт алерты в Telegram. **Он НЕ торгует.**

| Параметр | Значение |
|---|---|
| Режим | Advisory (оператор-человек принимает решение) |
| Горизонт | Intraday → swing (минуты до недель), не HFT |
| Биржа | Bybit USDT-Linear Futures (публичный `publicTrade` WS) |
| Детекторы | VPIN (Z-score), Sweep, Large Print, OFI Pulse |
| Тиры чувствительности | `conservative` / `active` (default) / `scanner` |

## Жёсткие запреты

1. **Никакого автоисполнения.** Ни `ExecutionEngine`, ни `OrderManager`, ни прямых вызовов trade-REST.
2. **Никакого HFT.** Не вводить 1m/1s бары, не строить под-минутные стратегии.
3. **Никаких тихих фейлов.** `try: ... except Exception: pass` — запрещено. Все ошибки логируются с контекстом.
4. **Не блокировать event loop.** CPU-bound тяжёлая математика — в `ProcessPoolExecutor`. (Текущий VPIN O(1) на бар, в PPE не выносится — это будущая опция для тяжёлых модулей вроде FFD/CUSUM, если они появятся.)
5. **Не модифицировать `src/domain/vpin_engine.py` без обновления `tests/regression/test_vpin_invariants.py` в том же PR.** Инварианты I1–I5 защищаются автоматически.

## Перед каждым PR

1. Прочитать SSOT (если ещё не читал).
2. Проверить через `grep -rn` / `git ls-files`, что упоминаемые в коде классы/файлы реально существуют. Нет ссылок на TradeSweeper / Aegis / NerveCenter / Stealth CUSUM — этого в коде НЕТ.
3. Прогнать `.venv/bin/python -m pytest tests/regression tests/test_vpin_engine.py -q`. Базлайн v3.6.2: **83 passed** (радар-контур). Падения = сломали инвариант, фиксим в том же PR.
4. Никаких секретов в коммите. `.env` в `.gitignore`. Bot token живёт только в `.env.production` на VPS.

## Стек (короткой строкой)

Python 3.11+ · asyncio · aiohttp · orjson · numpy · SQLAlchemy + aiosqlite (default) / asyncpg (опц.) · loguru · pydantic-settings · pytest+hypothesis.

**Redis в радар-контуре v3.6.x не используется.** Старые упоминания «NerveCenter Redis-Bus» — артефакт документации эпохи v12.0.

## Тех-долг и orphan-код

В `src/` ~82 файла, не достижимых из `main.py` (см. AUDIT_REPORT.md §1.3 и Приложение A). Это legacy от Trading-Bot эпохи и аспирационная v12.0 архитектура. Будут удалены отдельным `cleanup(repo)` PR. Не использовать orphan-модули в новых фичах.

---

_v3.6.2 — 2026-05-24. Update history: до v3.6.0 этот файл содержал «v2.0 STRICT manifesto» с упоминаниями FFD/PurgedKFold/Embargoing/NerveCenter/ProcessPoolExecutor для VPIN — этих компонентов в коде НИКОГДА не было. Удалено для предотвращения галлюцинаций._
