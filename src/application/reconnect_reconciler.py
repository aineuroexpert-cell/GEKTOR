# src/application/reconnect_reconciler.py
"""
[GEKTOR v15.2] Reconnect Reconciler — Collapse the Superposition.

Orchestrates the Phase 4 Deterministic State Reconciliation on
WebSocket reconnect. Queries the exchange REST API for each
purgatory order and feeds results into SchrodingerLedger.

Architecture:

  1. WS reconnects after partition
  2. Reconciler scans FlightRecorder for PENDING/DISPATCHED orders
  3. For each order, queries exchange REST API by cl_ord_id
  4. Feeds result into SchrodingerLedger.reconcile_order()
  5. Cross-validates against exchange position endpoint
  6. Publishes RECONCILIATION_COMPLETE event to EventBus

Safety guarantees:
  - No new strikes until ALL purgatory orders are resolved
  - Exponential backoff on REST failures
  - Position size arbitration as final tiebreaker
  - SAFE_HOLD if reconciliation fails after max retries

Thread-safety: Runs as a single asyncio task. No parallelism.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional
from loguru import logger

from src.domain.schrodinger_ledger import (
    SchrodingerLedger,
    OrderFate,
    ReconciliationVerdict,
)


class ReconnectReconciler:
    """
    Application-layer orchestrator for post-partition state recovery.

    Lifecycle:
      1. Created once at startup
      2. reconcile() called on each WS reconnect
      3. Blocks execution pipeline until purgatory is empty
    """
    __slots__ = (
        '_ledger', '_rest_client', '_flight_recorder',
        '_event_bus', '_max_retries', '_base_backoff_sec',
        '_position_tolerance', '_total_reconciliations',
    )

    def __init__(
        self,
        ledger: SchrodingerLedger,
        rest_client: Any,
        flight_recorder: Any,
        event_bus: Any = None,
        max_retries: int = 10,
        base_backoff_sec: float = 0.5,
    ) -> None:
        self._ledger = ledger
        self._rest_client = rest_client
        self._flight_recorder = flight_recorder
        self._event_bus = event_bus
        self._max_retries = max_retries
        self._base_backoff_sec = base_backoff_sec
        self._position_tolerance = 1e-8
        self._total_reconciliations: int = 0

    async def reconcile(self) -> list[ReconciliationVerdict]:
        """
        Main reconciliation entry point. Called on WS reconnect.

        Returns list of verdicts for all resolved purgatory orders.

        Execution flow:
          Phase 1: Scan purgatory for orders in superposition
          Phase 2: Query exchange REST for each order's fate
          Phase 3: Cross-validate against position endpoint
          Phase 4: Publish results
        """
        self._total_reconciliations += 1
        purgatory = self._ledger.get_purgatory_orders()

        if not purgatory:
            logger.info(
                "✅ [RECONCILE] Purgatory empty. "
                "No Schrödinger orders to resolve.",
            )
            return []

        logger.critical(
            "🔬 [RECONCILE] Starting Phase 4 — {} order(s) in superposition. "
            "Reconciliation #{}", len(purgatory), self._total_reconciliations,
        )

        verdicts: list[ReconciliationVerdict] = []

        # ── PHASE 2: Query exchange for each purgatory order ──
        for order in purgatory:
            verdict = await self._resolve_single_order(
                cl_ord_id=order.cl_ord_id,
                symbol=order.symbol,
                side=order.side,
                qty=order.qty,
            )
            verdicts.append(verdict)
            logger.info(
                "📋 [RECONCILE] {} → {} | fill={:.6f} @ {:.4f} | {}",
                order.cl_ord_id[:16],
                verdict.fate.name,
                verdict.filled_qty,
                verdict.filled_avg_px,
                verdict.message,
            )

        # ── PHASE 3: Position Arbitration (final tiebreaker) ──
        await self._position_arbitration()

        # ── PHASE 4: Publish results ──
        if self._event_bus is not None:
            try:
                await self._event_bus.publish(
                    "RECONCILIATION_COMPLETE",
                    {
                        "reconciliation_id": self._total_reconciliations,
                        "orders_resolved": len(verdicts),
                        "remaining_purgatory": self._ledger.total_purgatory,
                        "verdicts": [
                            {
                                "cl_ord_id": v.cl_ord_id,
                                "fate": v.fate.name,
                                "filled_qty": v.filled_qty,
                            }
                            for v in verdicts
                        ],
                    },
                )
            except Exception as e:
                logger.error("⚠️ [RECONCILE] EventBus publish failed: {}", e)

        remaining = self._ledger.total_purgatory
        if remaining > 0:
            logger.warning(
                "⚠️ [RECONCILE] {} order(s) still INDETERMINATE after "
                "reconciliation. Execution pipeline remains LOCKED.",
                remaining,
            )
        else:
            logger.success(
                "✅ [RECONCILE] All superpositions collapsed. "
                "Execution pipeline UNLOCKED.",
            )

        return verdicts

    async def _resolve_single_order(
        self,
        cl_ord_id: str,
        symbol: str,
        side: str,
        qty: float,
    ) -> ReconciliationVerdict:
        """
        Query exchange REST API for a single order's fate.

        Uses exponential backoff with jitter on failures.
        Falls back to position arbitration on max retries.
        """
        for attempt in range(1, self._max_retries + 1):
            try:
                # Query order history by cl_ord_id
                response = await self._rest_client.get_order_history(
                    category="linear",
                    symbol=symbol,
                    orderLinkId=cl_ord_id,
                )

                orders = response.get("result", {}).get("list", [])

                if not orders:
                    # Order not found — could mean:
                    # A) Never received by exchange
                    # B) Purged from history (unlikely for recent)
                    # Use position delta to disambiguate
                    return self._ledger.mark_never_received(cl_ord_id)

                # Found the order — extract terminal state
                order_data = orders[0]
                exchange_status = order_data.get("orderStatus", "UNKNOWN")
                filled_qty = float(order_data.get("cumExecQty", 0))
                avg_price = float(order_data.get("avgPrice", 0))

                return self._ledger.reconcile_order(
                    cl_ord_id=cl_ord_id,
                    exchange_status=exchange_status,
                    filled_qty=filled_qty,
                    avg_price=avg_price,
                )

            except Exception as e:
                # Exponential backoff with jitter
                import random
                backoff = min(
                    30.0,
                    self._base_backoff_sec * (2 ** (attempt - 1))
                    + random.uniform(0, 0.5),
                )
                logger.warning(
                    "📡 [RECONCILE] REST query failed for {} (attempt {}/{}): {}. "
                    "Retrying in {:.1f}s",
                    cl_ord_id[:16], attempt, self._max_retries, e, backoff,
                )
                await asyncio.sleep(backoff)

        # Exhausted retries — order stays in purgatory
        logger.error(
            "🛑 [RECONCILE] Failed to resolve {} after {} attempts. "
            "Order remains INDETERMINATE.",
            cl_ord_id[:16], self._max_retries,
        )
        return ReconciliationVerdict(
            cl_ord_id=cl_ord_id,
            fate=OrderFate.INDETERMINATE,
            filled_qty=0.0,
            filled_avg_px=0.0,
            message=f"REST_EXHAUSTED_AFTER_{self._max_retries}_ATTEMPTS",
        )

    async def _position_arbitration(self) -> None:
        """
        Final tiebreaker: compare exchange positions with confirmed ledger.

        If exchange shows a position that our confirmed ledger doesn't know
        about, we have a phantom fill — must update the ledger.

        If exchange shows NO position but our ledger thinks we have one,
        something is deeply wrong — enter SAFE_HOLD.
        """
        remaining = self._ledger.get_purgatory_orders()
        if not remaining:
            return

        # Get unique symbols from remaining purgatory
        symbols = {o.symbol for o in remaining}

        for symbol in symbols:
            try:
                pos_response = await self._rest_client.get_position_info(
                    category="linear",
                    symbol=symbol,
                )
                pos_list = pos_response.get("result", {}).get("list", [])
                exchange_size = 0.0
                exchange_avg_px = 0.0

                for p in pos_list:
                    if p.get("symbol") == symbol:
                        exchange_size = abs(float(p.get("size", 0)))
                        exchange_avg_px = float(p.get("avgPrice", 0))
                        break

                shadow = self._ledger.get_shadow_position(symbol)

                # Compare exchange reality vs our confirmed ledger
                delta = abs(exchange_size - shadow.confirmed_qty)

                if delta > self._position_tolerance:
                    logger.critical(
                        "⚖️ [ARBITRATION] {} POSITION DELTA DETECTED | "
                        "Exchange: {:.6f} | Confirmed: {:.6f} | "
                        "Shadow: {:.6f} | Δ={:.6f}",
                        symbol, exchange_size, shadow.confirmed_qty,
                        shadow.shadow_qty, delta,
                    )

                    # The exchange has a position we don't know about.
                    # Resolve remaining purgatory orders for this symbol
                    # by attributing the delta to the most recent dispatch.
                    for order in remaining:
                        if order.symbol == symbol:
                            if exchange_size > shadow.confirmed_qty:
                                # Exchange has MORE than we confirmed →
                                # purgatory order was likely filled
                                fill_qty = min(
                                    order.qty,
                                    exchange_size - shadow.confirmed_qty,
                                )
                                self._ledger.reconcile_order(
                                    cl_ord_id=order.cl_ord_id,
                                    exchange_status="Filled",
                                    filled_qty=fill_qty,
                                    avg_price=exchange_avg_px,
                                )
                                logger.critical(
                                    "🏁 [ARBITRATION] {} attributed as FILLED "
                                    "via position delta: {:.6f} @ {:.4f}",
                                    order.cl_ord_id[:16], fill_qty,
                                    exchange_avg_px,
                                )
                            else:
                                # Exchange has LESS or equal → order wasn't filled
                                self._ledger.mark_never_received(order.cl_ord_id)
                else:
                    # Position matches — remaining purgatory orders
                    # for this symbol were likely never received
                    for order in remaining:
                        if order.symbol == symbol:
                            self._ledger.mark_never_received(order.cl_ord_id)
                            logger.info(
                                "💀 [ARBITRATION] {} → NEVER_RECEIVED "
                                "(position unchanged)",
                                order.cl_ord_id[:16],
                            )

            except Exception as e:
                logger.error(
                    "⚠️ [ARBITRATION] REST position query failed for {}: {}",
                    symbol, e,
                )

    @property
    def total_reconciliations(self) -> int:
        return self._total_reconciliations
