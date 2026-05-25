# Rules for AI agents working on GEKTOR APEX

> **READ `SINGLE_SOURCE_OF_TRUTH.md` BEFORE ANY OTHER FILE.**
>
> Any conflict between this file and `SINGLE_SOURCE_OF_TRUTH.md` is
> resolved in favour of the SSOT.

## Mode of operation

GEKTOR APEX is an **Advisory-Mode** radar. The agent must NEVER:

- Add any code path that places orders, manages positions, or talks to a
  trade execution REST API.
- Write 1-minute or sub-minute logic ("scalping", "HFT").
- Add a `try: ... except Exception: pass` block. All errors must be
  logged with context.
- Block the asyncio event loop with CPU-bound math; offload to
  `ProcessPoolExecutor` if heavy.
- Touch the Telegram bot token in committed code. The token lives only
  in `.env.production` on the VPS.

## Anti-hallucination rules

Before claiming any file, class, function, or behaviour exists:

1. **Open the actual file and quote a real line.** If the file doesn't
   exist in `git ls-files`, you are hallucinating.
2. **Never invent line numbers or "previous architect's commits".** If
   you cannot find it with `grep -rn`, it isn't there.
3. **Never claim tests pass without running pytest.** Run them and
   paste the output.
4. **If a previous summary or another agent claims something exists,
   verify it.** Agents lie under load. Files written "in a prior
   session" may have never been committed.

## Invariants you must NOT break silently

See `SINGLE_SOURCE_OF_TRUTH.md` §4 for the canonical list (I1–I5,
I-noDrift, I-noFill). Each invariant has a test sentinel under
`tests/regression/`. If your change requires breaking one:

1. Update the test FIRST (TDD-style) with a justification comment.
2. Document the change in the PR description with rationale.
3. Update `SINGLE_SOURCE_OF_TRUTH.md` in the same PR.

Otherwise: **do not modify the invariant.**

## When in doubt

- **VPIN math**: `src/domain/vpin_engine.py` is the authoritative
  implementation. Do not write a second one.
- **Polarity**: `is_buyer_maker == True` ⇔ taker sold ⇔ sell_volume_usd
  increments. Bybit wire format: `trade["S"] == "Sell"` maps to
  `is_buyer_maker = True`.
- **Database**: default is SQLite (`sqlite+aiosqlite:///gektor.db`).
  Do not use PostgreSQL-only SQL (`FOR UPDATE SKIP LOCKED`,
  `ON CONFLICT DO UPDATE` if it relies on PG operators, etc.).
  See `src/application/outbox_relay.py::fetch_pending` for the
  portable claim pattern.
- **Configuration**: read from `src/infrastructure/config.py` via
  `pydantic-settings`. Do not hard-code paths, tokens, URLs.

## Testing protocol

Before declaring work done:

```bash
.venv/bin/python -m pytest tests/ -q
```

Expected baseline (v3.6.2 radar contour): `.venv/bin/python -m pytest tests/regression tests/test_vpin_engine.py -q` → **83 passed, 39 warnings**. Wider `tests/` suite includes legacy modules out of the radar scope (the 8 skipped/failing are
documented out-of-scope subsystems — see SSOT §8).

If your change adds new behaviour, write at least one regression test
under `tests/regression/`. Property-based tests with Hypothesis are
strongly preferred for math/algorithmic changes.

## Commit hygiene

- Branch naming: `devin/<unix-ts>-<short-slug>` or `feat/<scope>`.
- Never push directly to `main`. Always go through PR.
- Never commit `.env` or files containing the Telegram token. CI greps
  for `BOT_TOKEN=` and rejects.
- Pre-commit hooks (when enabled) run ruff + black + mypy + pytest.
  Do not bypass with `--no-verify`.
