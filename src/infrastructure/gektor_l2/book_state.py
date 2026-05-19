"""Per-symbol synchronization state for WS ↔ REST recovery (no threads on book)."""

from __future__ import annotations

from enum import IntEnum


class BookState(IntEnum):
    """Gate for inbound public WS frames (Kleppmann-style dirty / recover)."""

    SYNCED = 1
    """Normal path: apply WS snapshot/delta synchronously in the event loop."""

    DESYNCED = 2
    """Sequence gap or missing anchor: drop WS; schedule REST resync once."""

    RECOVERING = 3
    """REST in flight: drop all WS for this symbol until snapshot is applied."""


class BookReadiness(IntEnum):
    """
    Signal Engine disambiguation layer (Kleppmann Connection Epoch + Temporal Decay).

    Answers TWO questions:
      1. "Is the book silent because the market is empty, or because our radar is blind?"
      2. "Even if the radar SAYS it's alive, is the data fresh enough to trade on?"

    Usage in Signal Engine:
        readiness = book.readiness(book_state, circuit_breaker_open=cb_open)
        if readiness in (BookReadiness.BLIND_NETWORK, BookReadiness.BLIND_STALE):
            # DO NOT TRADE — radar failure or stale data
        elif readiness == BookReadiness.EMPTY_BUT_VALID:
            # Genuinely thin market — alpha logic decides
        elif readiness == BookReadiness.READY:
            # Normal path — data is fresh and consistent
    """

    READY = 1
    """Book is anchored, consistent, receiving live WS data, AND data age < max_age.
    Safe to trade."""

    EMPTY_BUT_VALID = 2
    """Book is anchored, consistent, and fresh, but has zero depth on one or both sides.
    This is a REAL market condition (e.g. illiquid altcoin), not a data gap."""

    RECOVERING = 3
    """REST resync in-flight or sequence gap detected. Data is stale.
    Signal Engine MUST suppress any signal — this is NOT zero resistance."""

    BLIND_NETWORK = 4
    """Circuit Breaker is open OR connection epoch invalidated the book (DESYNCED).
    The radar cannot see due to network/infrastructure failure."""

    BLIND_STALE = 5
    """TCP is alive, ping/pong works, BookState is SYNCED, book is consistent —
    BUT no L2 update received within max_age_sec. Exchange-side Kafka bridge
    may be frozen for this specific symbol. Data is TOXIC — do not trade.
    'Absence of events is also an event.' (Kleppmann)"""
