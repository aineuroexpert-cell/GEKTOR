"""Regression sentinels for deploy-time config bugs (v3.6.1).

These exist because the Tokyo VPS deploy on 2026-05-24 burned ~20
minutes hitting three avoidable config bugs in a row:

  1. The operator set ``TELEGRAM_BOT_TOKEN=...`` in ``.env`` but the
     Pydantic field aliased only ``gerald_bot_token``, so the value
     was silently ignored and ``TelegramRadarNotifier`` crashed with
     ``Telegram Bot Token is empty.``
  2. ``BYBIT_API_KEY`` / ``BYBIT_API_SECRET`` were marked required even
     though the Advisory radar uses only the public ``publicTrade`` WS
     endpoint, forcing the operator to put real signed keys on disk
     (and into the chat log) to start the radar.
  3. The DB engine factory always passed ``pool_size`` / ``max_overflow``
     on Linux, which breaks SQLite/aiosqlite (NullPool) with a
     ``TypeError: Invalid argument(s) ... sent to create_engine``.

If any of these regressions returns, this file is the smoke alarm.
"""
from __future__ import annotations

import importlib

import pytest


def _reload_settings(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]):
    """Reload ``src.infrastructure.config`` under a controlled env."""
    # Wipe every alias we accept so the test is hermetic.
    for var in (
        "BOT_TOKEN",
        "TELEGRAM_BOT_TOKEN",
        "TG_BOT_TOKEN",
        "GERALD_BOT_TOKEN",
        "CHAT_ID",
        "TELEGRAM_CHAT_ID",
        "TG_CHAT_ID",
        "BYBIT_API_KEY",
        "BYBIT_API_SECRET",
        "ASYNC_DATABASE_URL",
    ):
        monkeypatch.delenv(var, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    # Prevent picking up the repo-root .env during the reload.
    monkeypatch.chdir("/tmp")

    import src.infrastructure.config as cfg
    importlib.reload(cfg)
    return cfg.Settings()


@pytest.mark.parametrize(
    "alias",
    ["BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TG_BOT_TOKEN", "GERALD_BOT_TOKEN"],
)
def test_bot_token_accepts_all_documented_aliases(monkeypatch, alias):
    """Operators must be able to set the token under any of the names
    the .env.example claims are supported. Regression for the Tokyo
    deploy failure where TELEGRAM_BOT_TOKEN was silently ignored."""
    token = "1234567890:" + "A" * 35
    settings = _reload_settings(
        monkeypatch,
        {
            alias: token,
            "ASYNC_DATABASE_URL": "sqlite+aiosqlite:///:memory:",
        },
    )
    assert settings.bot_token == token, (
        f"Setting {alias} should populate settings.bot_token; got empty"
    )


@pytest.mark.parametrize(
    "alias", ["CHAT_ID", "TELEGRAM_CHAT_ID", "TG_CHAT_ID"]
)
def test_chat_id_accepts_all_documented_aliases(monkeypatch, alias):
    settings = _reload_settings(
        monkeypatch,
        {
            alias: "123456789",
            "ASYNC_DATABASE_URL": "sqlite+aiosqlite:///:memory:",
        },
    )
    assert settings.chat_id == "123456789"


def test_bybit_credentials_are_optional_for_advisory_mode(monkeypatch):
    """Advisory radar uses only the public publicTrade WS endpoint and
    must boot without any Bybit credentials. Regression for the
    'Field required' validation error on first deploy."""
    settings = _reload_settings(
        monkeypatch,
        {"ASYNC_DATABASE_URL": "sqlite+aiosqlite:///:memory:"},
    )
    assert settings.BYBIT_API_KEY == ""
    assert settings.BYBIT_API_SECRET == ""


def test_bybit_credentials_still_format_validated_when_set(monkeypatch):
    """If the operator DOES set Bybit keys, the regex validators must
    still fire — we don't want to silently accept garbage."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc:
        _reload_settings(
            monkeypatch,
            {
                "ASYNC_DATABASE_URL": "sqlite+aiosqlite:///:memory:",
                "BYBIT_API_KEY": "too_short",
                "BYBIT_API_SECRET": "a" * 36,
            },
        )
    assert "Bybit API Key" in str(exc.value)


def test_async_database_url_is_still_required(monkeypatch):
    """The DB URL remains the single hard requirement."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _reload_settings(monkeypatch, {})


def test_sqlite_url_does_not_pass_postgres_only_kwargs(monkeypatch):
    """The DB engine factory must dispatch by URL scheme so SQLite
    deployments don't crash with 'Invalid argument(s) pool_size,...'.

    We import DatabaseManager and just verify it constructs without
    raising under a SQLite URL — the actual TypeError from the old
    code would surface at __init__ time."""
    pytest.importorskip(
        "redis", reason="redis is a runtime dep of DatabaseManager"
    )
    _reload_settings(
        monkeypatch,
        {"ASYNC_DATABASE_URL": "sqlite+aiosqlite:///:memory:"},
    )
    # Force-reload the connection module so it picks up the new settings.
    import src.infrastructure.database.connection as conn
    importlib.reload(conn)
    mgr = conn.DatabaseManager()
    assert mgr.engine is not None
    # SQLAlchemy picks NullPool (file DB) or StaticPool (:memory:) for
    # aiosqlite. The key invariant is that we are NOT using a QueuePool
    # — that's the only pool flavour that would accept pool_size /
    # max_overflow, and accepting those would mean we silently passed
    # PG-only kwargs into SQLite.
    from sqlalchemy.pool import QueuePool
    assert not isinstance(mgr.engine.sync_engine.pool, QueuePool)
