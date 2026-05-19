# src/application/formatters.py
"""
[GEKTOR APEX v5.25] Telegram Message Formatter — Presenter Layer.

Strict event_type → template routing with full HTML sanitization.
All dynamic values pass through html.escape() to prevent Telegram API
HTTP 400 errors caused by unclosed tags in error messages / stack traces.

SECURITY: Never trust raw payload strings. Always escape before embedding in HTML.
"""
from __future__ import annotations

import html
import time
from datetime import datetime, timezone
from typing import Any


class TelegramMessageFormatter:
    """Formats raw signal payloads into Telegram-safe HTML messages (RU locale)."""

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _esc(value: Any, default: str = "Н/Д") -> str:
        """HTML-escape any dynamic value.  None → default."""
        if value is None:
            return html.escape(default)
        return html.escape(str(value))

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _ts_utc(payload: dict[str, Any]) -> str:
        """Extract timestamp and format as HH:MM:SS UTC."""
        raw = payload.get("timestamp")
        if raw is None:
            return datetime.now(timezone.utc).strftime("%H:%M:%S")
        try:
            ts = float(raw)
            # Auto-detect millis vs seconds
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")
        except (TypeError, ValueError):
            return "??:??:??"

    # ── Router ───────────────────────────────────────────────────────────

    def format(self, payload: dict[str, Any]) -> str:
        """Master dispatcher. Routes payload → handler by event_type."""
        event_type = str(payload.get("event_type", "")).upper()

        router: dict[str, Any] = {
            "STARTUP":   self._fmt_startup,
            "HEARTBEAT": self._fmt_heartbeat,
            "APPROVED":  self._fmt_approved,
            "REJECTED":  self._fmt_rejected,
            "ERROR":     self._fmt_error,
            "CRITICAL":  self._fmt_critical,
            "ABORT":     self._fmt_abort,
            "RAW_TEXT":  self._fmt_raw_text,
        }

        handler = router.get(event_type)
        if handler is not None:
            return handler(payload)

        # Legacy fallback: treat unknown types as APPROVED
        if payload.get("abort_mission"):
            return self._fmt_abort(payload)
        return self._fmt_approved(payload)

    # ── Templates ────────────────────────────────────────────────────────

    def _fmt_startup(self, payload: dict[str, Any]) -> str:
        ts = self._ts_utc(payload)
        version = self._esc(payload.get("version", "v13.7.1"))
        return (
            "🚀 <b>[GEKTOR] Система инициализирована</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 Радар активен. Версия: <code>{version}</code>\n"
            f"⏰ <code>{ts} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>СТАТУС: БОЕВОЙ РЕЖИМ</b>"
        )

    def _fmt_heartbeat(self, payload: dict[str, Any]) -> str:
        snapshots = int(self._safe_float(payload.get("snapshot_count", 0)))
        ts = self._ts_utc(payload)
        return (
            "💓 <b>[PULSE] Контрольный сигнал</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Обработано <b>{snapshots}</b> снепшотов стакана.\n"
            "🧠 Утечек памяти нет.\n"
            f"⏰ <code>{ts} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>СТАТУС: ШТАТНАЯ РАБОТА</b>"
        )

    def _fmt_approved(self, payload: dict[str, Any]) -> str:
        symbol = self._esc(payload.get("symbol", "UNKNOWN"))
        direction_raw = str(payload.get("direction", payload.get("side", "LONG"))).upper()
        direction = "🟢 LONG (Покупка)" if direction_raw in {"LONG", "BUY", "BUY_IMPULSE"} else "🔴 SHORT (Продажа)"

        ofi_pct = self._safe_float(payload.get("ofi_pct", payload.get("ofi", 0.0)))
        spread_bps = self._safe_float(payload.get("spread_bps", 0.0))
        kde_distance = self._safe_float(payload.get("kde_distance_pct", 0.0))
        top_volume = self._safe_float(payload.get("top_volume", payload.get("top_level_volume", 0.0)))
        price = self._safe_float(payload.get("price", 0.0))
        ts = self._ts_utc(payload)

        return (
            "🚨 <b>GEKTOR: БОЕВОЙ СИГНАЛ</b> 🚨\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Тикер:</b> <code>#{symbol}</code>\n"
            f"📈 <b>Направление:</b> {direction}\n"
            f"💵 <b>Цена:</b> <code>${price:,.2f}</code>\n"
            f"📊 <b>Перекос стакана (OFI):</b> <code>{ofi_pct:.2f}%</code>\n"
            f"📏 <b>Спред:</b> <code>{spread_bps:.2f} bps</code>\n"
            f"🧲 <b>Дистанция до уровня:</b> <code>{kde_distance:.3f}%</code>\n"
            f"⚖️ <b>Объем в топе:</b> <code>{top_volume:.2f}</code>\n"
            f"⏰ <code>{ts} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ <i>Сигнал передан на исполнение.</i>"
        )

    def _fmt_rejected(self, payload: dict[str, Any]) -> str:
        symbol = self._esc(payload.get("symbol", "UNKNOWN"))
        reason = self._esc(payload.get("reason", "Неизвестная причина"))
        fact = self._esc(payload.get("fact", payload.get("current_value", "Н/Д")))
        limit = self._esc(payload.get("limit", payload.get("required_value", "Н/Д")))
        ts = self._ts_utc(payload)

        return (
            f"⚠️ <b>ОТБРАКОВКА СИГНАЛА:</b> <code>#{symbol}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>Причина:</b> {reason}\n"
            f"📊 <b>Факт:</b> <code>{fact}</code>\n"
            f"📏 <b>Лимит:</b> <code>{limit}</code>\n"
            f"⏰ <code>{ts} UTC</code>"
        )

    def _fmt_error(self, payload: dict[str, Any]) -> str:
        module = self._esc(payload.get("module", payload.get("source", "UNKNOWN")))
        message = self._esc(payload.get("message", payload.get("error", "Нет деталей")))
        symbol = self._esc(payload.get("symbol", "GLOBAL"))
        ts = self._ts_utc(payload)

        return (
            "🔴 <b>[ОШИБКА] Сбой модуля</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🧩 <b>Модуль:</b> <code>{module}</code>\n"
            f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
            f"📝 <b>Детали:</b> <code>{message}</code>\n"
            f"⏰ <code>{ts} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️ <i>Требуется внимание оператора.</i>"
        )

    def _fmt_critical(self, payload: dict[str, Any]) -> str:
        module = self._esc(payload.get("module", payload.get("source", "UNKNOWN")))
        message = self._esc(payload.get("message", payload.get("error", "СИСТЕМНЫЙ СБОЙ")))
        symbol = self._esc(payload.get("symbol", "GLOBAL"))
        ts = self._ts_utc(payload)

        return (
            "🚨🚨🚨 <b>[CRITICAL] АВАРИЙНАЯ СИТУАЦИЯ</b> 🚨🚨🚨\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🧩 <b>Модуль:</b> <code>{module}</code>\n"
            f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
            f"💀 <b>Детали:</b> <code>{message}</code>\n"
            f"⏰ <code>{ts} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🛑 <b>ТРЕБУЕТСЯ НЕМЕДЛЕННОЕ ВМЕШАТЕЛЬСТВО!</b>"
        )

    def _fmt_abort(self, payload: dict[str, Any]) -> str:
        symbol = self._esc(payload.get("symbol", "UNKNOWN"))
        price = self._safe_float(payload.get("price", 0.0))
        ts = self._ts_utc(payload)

        reason_raw = payload.get("abort_reason", payload.get("exit_reason", "Нарушение структуры"))
        reason_map = {
            "VPIN_DECAY": "Затухание токсичности / Потеря интереса",
            "STOP_LOSS": "Срабатывание ценового стопа",
            "VOLATILITY_EXPANSION": "Взрывной рост волатильности (Риск)",
            "TIME_DECAY": "Истечение времени актуальности",
            "WALL_COLLAPSE": "Разрушение стены ликвидности",
            "CUSUM_REVERSAL": "CUSUM разворот — смена импульса",
        }
        friendly_reason = reason_map.get(str(reason_raw), str(reason_raw))
        friendly_reason = html.escape(friendly_reason)

        vpin_raw = payload.get("vpin")
        vpin_str = f"{float(vpin_raw):.4f}" if vpin_raw is not None else "Н/Д"

        return (
            "🛡️ <b>[ЗАЩИТА КАПИТАЛА: СБРОС]</b>\n"
            f"Пресечение риска по активу <b>{symbol}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
            f"📉 <b>Тип:</b> Снятие торговой гипотезы\n"
            f"🧬 <b>Причина:</b> <i>{friendly_reason}</i>\n"
            f"📊 <b>VPIN:</b> <code>{vpin_str}</code>\n"
            f"💵 <b>Цена:</b> <code>${price:,.2f}</code>\n"
            f"⏰ <code>{ts} UTC</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🛑 <b>СТАТУС: ВЫХОД / OFF-MARKET</b>"
        )

    def _fmt_raw_text(self, payload: dict[str, Any]) -> str:
        """Fallback for pre-formatted or plain-text payloads."""
        text = payload.get("text", payload.get("fact", ""))
        return self._esc(text)
