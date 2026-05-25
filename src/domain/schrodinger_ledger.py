# src/domain/schrodinger_ledger.py
"""
[GEKTOR v15.2] Schrödinger's Ledger — Deterministic State Reconciliation.

Solves the Schrödinger's Order problem: after a network partition,
the system cannot know whether a dispatched order was:
  A) Never received by the matching engine (safe to re-send)
  B) Received and filled (position exists, capital at risk)
  C) Received but rejected by exchange (safe, no position)

Architecture: CONSERVATIVE SHADOW POSITION.

The ledger maintains TWO position views:
  1. Confirmed Position: Updated ONLY by WS fill confirmations
  2. Shadow Position: Updated IMMEDIATELY on dispatch (Outbox Pattern)

The invariant: shadow_qty >= confirmed_qty (always pessimistic).

During a partition, the Shadow Position assumes the WORST CASE:
  - All dispatched orders are assumed FILLED at worst_price
  - Risk calculations use shadow_qty, not confirmed_qty
  - New strikes are blocked if shadow exposure exceeds limits

On reconnect, the Reconciler queries the exchange REST API
and collapses the superposition:
  - Orders actually filled → confirmed_qty catches up to shadow
  - Orders never received → shadow_qty rolls back
  - Orders partially filled → both adjust to reality

ZERO I/O. ZERO infrastructure. Pure Domain Logic.

References:
  - Kleppmann: "Designing Data-Intensive Applications" — Fencing Tokens
  - López de Prado: Post-trade toxicity analysis
  - Linux Kernel: seqlock for generation-based state sync
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum, auto
import ctypes
import zlib
import mmap
import os

LEDGER_SIZE: int = 1024 * 1024  # 1MB Memory Map

class OrderRecord(ctypes.Structure):
    """
    Zero-Copy representation of an order in shared memory.
    Protects against Torn Writes during hard power loss.
    """
    _pack_ = 1  # Disable alignment for exact sizing
    _fields_ = [
        ("order_id", ctypes.c_uint64),
        ("status", ctypes.c_uint8),  # 0: PENDING, 1: FILLED, 2: INDETERMINATE
        ("scaled_volume", ctypes.c_uint64),
        ("crc32", ctypes.c_uint32)   # Cryptographic seal
    ]

    def compute_crc(self) -> int:
        # Compute CRC32 of payload, excluding the last 4 bytes (the crc32 field itself)
        payload = bytes(self)[:-4]
        return zlib.crc32(payload) & 0xFFFFFFFF

    def commit(self) -> None:
        # Strict write: payload first, then checksum
        self.crc32 = self.compute_crc()

    def is_valid(self) -> bool:
        return self.crc32 == self.compute_crc()


# ═══════════════════════════════════════════════════════════════════
# VALUE OBJECTS
# ═══════════════════════════════════════════════════════════════════

class OrderFate(Enum):
    """Terminal state of a Schrödinger order after reconciliation."""
    NEVER_RECEIVED = auto()    # Exchange never saw it (safe to retry)
    FILLED = auto()            # Exchange filled it (position exists)
    PARTIALLY_FILLED = auto()  # Partial fill (partial position)
    CANCELLED = auto()         # Exchange cancelled it (no position)
    REJECTED = auto()          # Exchange rejected it (no position)
    INDETERMINATE = auto()     # Still cannot determine (hold lock)


@dataclass(frozen=True, slots=True)
class PurgatoryOrder:
    """An order in superposition — dispatched but unconfirmed."""
    cl_ord_id: str
    symbol: str
    side: str               # BUY or SELL
    qty: float              # Requested quantity
    worst_price: float      # IOC limit price (worst case)
    dispatched_mono: float  # time.monotonic() when API call returned
    pre_flight_price: float # Price at signal emission
    attempt_count: int = 0  # Reconciliation attempts
    fate: OrderFate = OrderFate.INDETERMINATE


@dataclass(frozen=True, slots=True)
class ShadowPosition:
    """Conservative position estimate — Value Object."""
    symbol: str
    confirmed_qty: float    # Known-good from WS fills
    shadow_qty: float       # Pessimistic (confirmed + all purgatory)
    confirmed_avg_px: float # Weighted avg from confirmed fills
    shadow_avg_px: float    # Worst-case avg price
    purgatory_count: int    # Number of orders in superposition
    exposure_usd: float     # shadow_qty * shadow_avg_px


@dataclass(frozen=True, slots=True)
class ReconciliationVerdict:
    """Result of collapsing a single order's superposition."""
    cl_ord_id: str
    fate: OrderFate
    filled_qty: float       # Actual fill (0 if never received)
    filled_avg_px: float    # Actual fill price
    message: str = ""


# ═══════════════════════════════════════════════════════════════════
# SCHRÖDINGER'S LEDGER — The Domain Service
# ═══════════════════════════════════════════════════════════════════

class SchrodingerLedger:
    """
    Conservative position tracker with purgatory management.

    Invariants:
      1. shadow_qty >= confirmed_qty (ALWAYS pessimistic)
      2. No new strikes while purgatory_count > 0 for a symbol
      3. Shadow position resets ONLY on deterministic reconciliation
      4. All mutations are monotonic (generation counter)

    Thread-safety: Single-writer (asyncio event loop). No locks.
    """
    __slots__ = (
        '_confirmed', '_purgatory', '_generation',
        '_max_exposure_usd', '_reconciliation_log',
    )

    def __init__(self, max_exposure_usd: float = 10_000.0) -> None:
        # symbol → {qty, avg_px, cost_basis}
        self._confirmed: dict[str, dict] = {}
        # cl_ord_id → PurgatoryOrder
        self._purgatory: dict[str, PurgatoryOrder] = {}
        self._generation: int = 0
        self._max_exposure_usd = max_exposure_usd
        # Bounded reconciliation audit trail
        self._reconciliation_log: list[ReconciliationVerdict] = []

    # ──────────────────────────────────────────────────
    # DISPATCH: Record order entering purgatory
    # ──────────────────────────────────────────────────

    def record_dispatch(
        self,
        cl_ord_id: str,
        symbol: str,
        side: str,
        qty: float,
        worst_price: float,
        pre_flight_price: float,
        mono_now: Optional[float] = None,
    ) -> None:
        """
        Called AFTER FlightRecorder.log_intent, BEFORE the API call.

        This is the Outbox Pattern: we record the intent BEFORE
        the network call. If the call fails, the order stays in
        purgatory until reconciliation confirms it was never received.
        """
        if mono_now is None:
            mono_now = time.monotonic()

        order = PurgatoryOrder(
            cl_ord_id=cl_ord_id,
            symbol=symbol,
            side=side,
            qty=qty,
            worst_price=worst_price,
            dispatched_mono=mono_now,
            pre_flight_price=pre_flight_price,
        )

        self._purgatory[cl_ord_id] = order
        self._generation += 1

    # ──────────────────────────────────────────────────
    # CONFIRMATION: WS fill callback (happy path)
    # ──────────────────────────────────────────────────

    def confirm_fill(
        self,
        cl_ord_id: str,
        filled_qty: float,
        avg_price: float,
    ) -> ReconciliationVerdict:
        """
        Called when WS reports a fill for a known cl_ord_id.

        Moves the order from purgatory to confirmed position.
        This is the happy path — no reconciliation needed.
        """
        order = self._purgatory.pop(cl_ord_id, None)
        if order is None:
            # Fill for an order we don't track — log but don't crash
            return ReconciliationVerdict(
                cl_ord_id=cl_ord_id,
                fate=OrderFate.FILLED,
                filled_qty=filled_qty,
                filled_avg_px=avg_price,
                message="FILL_FOR_UNKNOWN_ORDER",
            )

        # Update confirmed position
        self._merge_confirmed(order.symbol, order.side, filled_qty, avg_price)
        self._generation += 1

        verdict = ReconciliationVerdict(
            cl_ord_id=cl_ord_id,
            fate=OrderFate.FILLED,
            filled_qty=filled_qty,
            filled_avg_px=avg_price,
        )
        self._audit(verdict)
        return verdict

    def confirm_rejection(self, cl_ord_id: str, reason: str = "") -> ReconciliationVerdict:
        """
        Called when WS reports an order rejection or cancellation.
        Removes from purgatory without updating position.
        """
        order = self._purgatory.pop(cl_ord_id, None)
        fate = OrderFate.REJECTED if "reject" in reason.lower() else OrderFate.CANCELLED

        self._generation += 1

        verdict = ReconciliationVerdict(
            cl_ord_id=cl_ord_id,
            fate=fate,
            filled_qty=0.0,
            filled_avg_px=0.0,
            message=reason,
        )
        self._audit(verdict)
        return verdict

    # ──────────────────────────────────────────────────
    # RECONCILIATION: Collapse superposition after reconnect
    # ──────────────────────────────────────────────────

    def reconcile_order(
        self,
        cl_ord_id: str,
        exchange_status: str,
        filled_qty: float,
        avg_price: float,
    ) -> ReconciliationVerdict:
        """
        Called during reconnect reconciliation.

        The Reconciler queries the exchange REST API for each
        purgatory order's cl_ord_id, then calls this method
        with the exchange's response.

        Args:
            cl_ord_id: The order's client ID.
            exchange_status: Exchange order status string.
            filled_qty: Actual filled quantity from exchange.
            avg_price: Actual average fill price.

        Returns:
            ReconciliationVerdict with the order's terminal fate.
        """
        order = self._purgatory.get(cl_ord_id)
        if order is None:
            return ReconciliationVerdict(
                cl_ord_id=cl_ord_id,
                fate=OrderFate.INDETERMINATE,
                filled_qty=0.0,
                filled_avg_px=0.0,
                message="ORDER_NOT_IN_PURGATORY",
            )

        # Normalize exchange status to OrderFate
        status_upper = exchange_status.upper()
        fate: OrderFate

        if status_upper in ("FILLED",):
            fate = OrderFate.FILLED
        elif status_upper in ("PARTIALLYFILLED", "PARTIALLY_FILLED"):
            fate = OrderFate.PARTIALLY_FILLED
        elif status_upper in ("CANCELLED", "CANCELED", "DEACTIVATED"):
            fate = OrderFate.CANCELLED
        elif status_upper in ("REJECTED",):
            fate = OrderFate.REJECTED
        elif status_upper in ("NEW", "ACTIVE", "UNTRIGGERED"):
            # Still alive on exchange — this shouldn't happen for IOC
            # but handle gracefully
            fate = OrderFate.INDETERMINATE
        else:
            fate = OrderFate.INDETERMINATE

        # Collapse the wavefunction
        if fate in (OrderFate.FILLED, OrderFate.PARTIALLY_FILLED):
            self._purgatory.pop(cl_ord_id, None)
            if filled_qty > 0:
                self._merge_confirmed(
                    order.symbol, order.side, filled_qty, avg_price,
                )
        elif fate in (OrderFate.CANCELLED, OrderFate.REJECTED, OrderFate.NEVER_RECEIVED):
            self._purgatory.pop(cl_ord_id, None)
        else:
            # INDETERMINATE: increment attempt counter, keep in purgatory
            self._purgatory[cl_ord_id] = PurgatoryOrder(
                cl_ord_id=order.cl_ord_id,
                symbol=order.symbol,
                side=order.side,
                qty=order.qty,
                worst_price=order.worst_price,
                dispatched_mono=order.dispatched_mono,
                pre_flight_price=order.pre_flight_price,
                attempt_count=order.attempt_count + 1,
                fate=OrderFate.INDETERMINATE,
            )

        self._generation += 1

        verdict = ReconciliationVerdict(
            cl_ord_id=cl_ord_id,
            fate=fate,
            filled_qty=filled_qty,
            filled_avg_px=avg_price,
            message=f"exchange_status={exchange_status}",
        )
        self._audit(verdict)
        return verdict

    def mark_never_received(self, cl_ord_id: str) -> ReconciliationVerdict:
        """
        Called when exchange REST confirms "order not found" AND
        position size hasn't changed. The order never reached
        the matching engine — safe to roll back shadow.
        """
        self._purgatory.pop(cl_ord_id, None)
        self._generation += 1

        verdict = ReconciliationVerdict(
            cl_ord_id=cl_ord_id,
            fate=OrderFate.NEVER_RECEIVED,
            filled_qty=0.0,
            filled_avg_px=0.0,
            message="ORDER_NEVER_REACHED_EXCHANGE",
        )
        self._audit(verdict)
        return verdict

    # ──────────────────────────────────────────────────
    # QUERIES: Position views
    # ──────────────────────────────────────────────────

    def get_shadow_position(self, symbol: str) -> ShadowPosition:
        """
        Returns the CONSERVATIVE position estimate.

        Shadow = confirmed + all purgatory orders assumed filled at worst_price.
        This is the number used for risk calculations during a partition.
        """
        confirmed = self._confirmed.get(symbol, {})
        conf_qty = confirmed.get("qty", 0.0)
        conf_avg = confirmed.get("avg_px", 0.0)

        # Sum all purgatory orders for this symbol
        purg_qty = 0.0
        purg_cost = 0.0
        purg_count = 0

        for order in self._purgatory.values():
            if order.symbol == symbol:
                purg_qty += order.qty
                purg_cost += order.qty * order.worst_price
                purg_count += 1

        shadow_qty = conf_qty + purg_qty

        if shadow_qty > 0:
            shadow_avg = (
                (conf_qty * conf_avg + purg_cost) / shadow_qty
            )
        else:
            shadow_avg = conf_avg

        return ShadowPosition(
            symbol=symbol,
            confirmed_qty=conf_qty,
            shadow_qty=shadow_qty,
            confirmed_avg_px=conf_avg,
            shadow_avg_px=round(shadow_avg, 8),
            purgatory_count=purg_count,
            exposure_usd=round(shadow_qty * shadow_avg, 2),
        )

    def is_strike_allowed(self, symbol: str) -> bool:
        """
        O(1) gate. Returns False if:
          1. Any purgatory orders exist for this symbol
          2. Shadow exposure exceeds max limit
        """
        # Rule 1: No new strikes while orders are in superposition
        for order in self._purgatory.values():
            if order.symbol == symbol:
                return False

        # Rule 2: Total shadow exposure within limits
        total_exposure = 0.0
        for sym_data in self._confirmed.values():
            total_exposure += abs(sym_data.get("qty", 0.0) * sym_data.get("avg_px", 0.0))
        # Add all purgatory exposure
        for order in self._purgatory.values():
            total_exposure += abs(order.qty * order.worst_price)

        return total_exposure < self._max_exposure_usd

    def get_purgatory_orders(self, symbol: Optional[str] = None) -> list[PurgatoryOrder]:
        """Returns all orders in purgatory, optionally filtered by symbol."""
        if symbol is None:
            return list(self._purgatory.values())
        return [o for o in self._purgatory.values() if o.symbol == symbol]

    def get_purgatory_age_sec(self, cl_ord_id: str, mono_now: Optional[float] = None) -> float:
        """How long an order has been in purgatory."""
        if mono_now is None:
            mono_now = time.monotonic()
        order = self._purgatory.get(cl_ord_id)
        if order is None:
            return 0.0
        return mono_now - order.dispatched_mono

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def total_purgatory(self) -> int:
        return len(self._purgatory)

    # ──────────────────────────────────────────────────
    # PRIVATE HELPERS
    # ──────────────────────────────────────────────────

    def _merge_confirmed(
        self, symbol: str, side: str, qty: float, avg_px: float,
    ) -> None:
        """
        Merge a fill into the confirmed position using VWAP.

        BUY increases position, SELL decreases.
        """
        if symbol not in self._confirmed:
            self._confirmed[symbol] = {"qty": 0.0, "avg_px": 0.0}

        pos = self._confirmed[symbol]

        if side.upper() == "BUY":
            # Add to position
            new_qty = pos["qty"] + qty
            if new_qty > 0:
                pos["avg_px"] = (
                    (pos["qty"] * pos["avg_px"] + qty * avg_px) / new_qty
                )
            pos["qty"] = new_qty
        else:
            # Reduce position
            pos["qty"] = max(0.0, pos["qty"] - qty)
            # avg_px stays the same on reduction (PnL calculated externally)

    def _audit(self, verdict: ReconciliationVerdict) -> None:
        """Bounded audit trail (last 1000 verdicts)."""
        self._reconciliation_log.append(verdict)
        if len(self._reconciliation_log) > 1000:
            self._reconciliation_log = self._reconciliation_log[-500:]

    @property
    def audit_trail(self) -> list[ReconciliationVerdict]:
        return list(self._reconciliation_log)
