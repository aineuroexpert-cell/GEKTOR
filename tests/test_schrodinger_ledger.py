# tests/test_schrodinger_ledger.py
"""
[GEKTOR v15.2] Schrödinger's Ledger Domain Tests.

Tests the full lifecycle:
  Dispatch → Purgatory → Fill/Reject/Reconcile → Shadow Position
"""
import sys
sys.path.insert(0, '.')

from src.domain.schrodinger_ledger import (
    SchrodingerLedger,
    OrderFate,
    ShadowPosition,
)


def run_tests():
    passed = 0
    failed = 0

    def check(name, condition):
        nonlocal passed, failed
        if condition:
            print(f'  OK: {name}')
            passed += 1
        else:
            print(f'  FAIL: {name}')
            failed += 1

    mono = 1000.0

    # ═══════════════════════════════════════════════════
    # Initial state
    # ═══════════════════════════════════════════════════
    print('=== Initial State ===')
    ledger = SchrodingerLedger(max_exposure_usd=10_000.0)
    check('empty_purgatory', ledger.total_purgatory == 0)
    check('strike_allowed_initially', ledger.is_strike_allowed("BTCUSDT"))

    shadow = ledger.get_shadow_position("BTCUSDT")
    check('shadow_zero_qty', shadow.shadow_qty == 0.0)
    check('shadow_zero_confirmed', shadow.confirmed_qty == 0.0)

    # ═══════════════════════════════════════════════════
    # Dispatch → Purgatory
    # ═══════════════════════════════════════════════════
    print('=== Dispatch to Purgatory ===')
    ledger.record_dispatch(
        cl_ord_id="GKT_BTC_001",
        symbol="BTCUSDT",
        side="BUY",
        qty=0.1,
        worst_price=65000.0,
        pre_flight_price=64950.0,
        mono_now=mono,
    )
    check('purgatory_count_1', ledger.total_purgatory == 1)
    check('strike_blocked', not ledger.is_strike_allowed("BTCUSDT"))
    check('other_symbol_allowed', ledger.is_strike_allowed("ETHUSDT"))

    shadow = ledger.get_shadow_position("BTCUSDT")
    check('shadow_qty_includes_purgatory', shadow.shadow_qty == 0.1)
    check('shadow_confirmed_still_zero', shadow.confirmed_qty == 0.0)
    check('shadow_purgatory_count', shadow.purgatory_count == 1)
    check('shadow_exposure',
          abs(shadow.exposure_usd - 0.1 * 65000.0) < 1.0)

    purg_age = ledger.get_purgatory_age_sec("GKT_BTC_001", mono_now=mono + 5)
    check('purgatory_age', abs(purg_age - 5.0) < 0.01)

    # ═══════════════════════════════════════════════════
    # Happy path: WS Fill confirmation
    # ═══════════════════════════════════════════════════
    print('=== Happy Path: WS Fill ===')
    verdict = ledger.confirm_fill("GKT_BTC_001", filled_qty=0.1, avg_price=64980.0)
    check('fill_fate', verdict.fate == OrderFate.FILLED)
    check('fill_qty', verdict.filled_qty == 0.1)
    check('purgatory_empty', ledger.total_purgatory == 0)
    check('strike_allowed_again', ledger.is_strike_allowed("BTCUSDT"))

    shadow = ledger.get_shadow_position("BTCUSDT")
    check('confirmed_qty_updated', shadow.confirmed_qty == 0.1)
    check('shadow_equals_confirmed', shadow.shadow_qty == 0.1)
    check('confirmed_avg_px', abs(shadow.confirmed_avg_px - 64980.0) < 0.01)

    # ═══════════════════════════════════════════════════
    # Rejection path
    # ═══════════════════════════════════════════════════
    print('=== Rejection Path ===')
    ledger.record_dispatch(
        "GKT_BTC_002", "BTCUSDT", "BUY", 0.05, 66000.0, 65900.0,
        mono_now=mono + 10,
    )
    check('purgatory_after_dispatch', ledger.total_purgatory == 1)

    verdict2 = ledger.confirm_rejection("GKT_BTC_002", reason="rejected by exchange")
    check('rejection_fate', verdict2.fate == OrderFate.REJECTED)
    check('purgatory_after_reject', ledger.total_purgatory == 0)

    # Confirmed position unchanged from previous fill
    shadow = ledger.get_shadow_position("BTCUSDT")
    check('confirmed_unchanged_after_reject', shadow.confirmed_qty == 0.1)

    # ═══════════════════════════════════════════════════
    # Schrödinger scenario: Reconciliation
    # ═══════════════════════════════════════════════════
    print('=== Schrödinger Reconciliation ===')
    # Dispatch order, then simulate network partition (no WS response)
    ledger2 = SchrodingerLedger(max_exposure_usd=50_000.0)
    ledger2.record_dispatch(
        "GKT_SOL_001", "SOLUSDT", "BUY", 10.0, 140.0, 141.0,
        mono_now=mono,
    )
    ledger2.record_dispatch(
        "GKT_SOL_002", "SOLUSDT", "BUY", 5.0, 139.5, 140.0,
        mono_now=mono + 0.5,
    )
    check('two_in_purgatory', ledger2.total_purgatory == 2)

    shadow2 = ledger2.get_shadow_position("SOLUSDT")
    check('shadow_sum', shadow2.shadow_qty == 15.0)
    check('shadow_pessimistic_exposure',
          shadow2.exposure_usd > 0)

    # Reconcile: first order was FILLED
    v1 = ledger2.reconcile_order(
        "GKT_SOL_001", exchange_status="Filled",
        filled_qty=10.0, avg_price=139.8,
    )
    check('recon_filled', v1.fate == OrderFate.FILLED)

    # Reconcile: second order was NEVER RECEIVED
    v2 = ledger2.mark_never_received("GKT_SOL_002")
    check('recon_never_received', v2.fate == OrderFate.NEVER_RECEIVED)

    check('purgatory_cleared', ledger2.total_purgatory == 0)

    shadow2_after = ledger2.get_shadow_position("SOLUSDT")
    check('confirmed_matches_filled', shadow2_after.confirmed_qty == 10.0)
    check('shadow_matches_confirmed', shadow2_after.shadow_qty == 10.0)
    check('avg_px_from_fill',
          abs(shadow2_after.confirmed_avg_px - 139.8) < 0.01)

    # ═══════════════════════════════════════════════════
    # Partial fill reconciliation
    # ═══════════════════════════════════════════════════
    print('=== Partial Fill ===')
    ledger3 = SchrodingerLedger()
    ledger3.record_dispatch(
        "GKT_ETH_001", "ETHUSDT", "BUY", 5.0, 3200.0, 3190.0,
        mono_now=mono,
    )
    v3 = ledger3.reconcile_order(
        "GKT_ETH_001", exchange_status="PartiallyFilled",
        filled_qty=2.5, avg_price=3195.0,
    )
    check('partial_fill_fate', v3.fate == OrderFate.PARTIALLY_FILLED)
    check('partial_fill_qty', v3.filled_qty == 2.5)
    check('partial_purgatory_clear', ledger3.total_purgatory == 0)

    shadow3 = ledger3.get_shadow_position("ETHUSDT")
    check('partial_confirmed_qty', shadow3.confirmed_qty == 2.5)

    # ═══════════════════════════════════════════════════
    # INDETERMINATE: stays in purgatory
    # ═══════════════════════════════════════════════════
    print('=== Indeterminate ===')
    ledger4 = SchrodingerLedger()
    ledger4.record_dispatch(
        "GKT_BTC_003", "BTCUSDT", "BUY", 0.01, 67000.0, 66900.0,
        mono_now=mono,
    )
    v4 = ledger4.reconcile_order(
        "GKT_BTC_003", exchange_status="New",
        filled_qty=0.0, avg_price=0.0,
    )
    check('indeterminate_fate', v4.fate == OrderFate.INDETERMINATE)
    check('still_in_purgatory', ledger4.total_purgatory == 1)

    # Second attempt
    purg = ledger4.get_purgatory_orders("BTCUSDT")[0]
    check('attempt_incremented', purg.attempt_count == 1)

    # ═══════════════════════════════════════════════════
    # SELL reduces position
    # ═══════════════════════════════════════════════════
    print('=== Position Reduction ===')
    ledger5 = SchrodingerLedger()
    # Build a BUY position
    ledger5.record_dispatch("GKT_A1", "BTCUSDT", "BUY", 1.0, 60000.0, 59900.0, mono_now=mono)
    ledger5.confirm_fill("GKT_A1", 1.0, 60000.0)
    shadow5 = ledger5.get_shadow_position("BTCUSDT")
    check('buy_position', shadow5.confirmed_qty == 1.0)

    # Sell half
    ledger5.record_dispatch("GKT_A2", "BTCUSDT", "SELL", 0.5, 61000.0, 60900.0, mono_now=mono+1)
    ledger5.confirm_fill("GKT_A2", 0.5, 61000.0)
    shadow5b = ledger5.get_shadow_position("BTCUSDT")
    check('reduced_position', shadow5b.confirmed_qty == 0.5)
    check('avg_px_preserved', abs(shadow5b.confirmed_avg_px - 60000.0) < 0.01)

    # ═══════════════════════════════════════════════════
    # Exposure limit
    # ═══════════════════════════════════════════════════
    print('=== Exposure Limit ===')
    ledger6 = SchrodingerLedger(max_exposure_usd=5000.0)
    ledger6.record_dispatch("GKT_B1", "SOLUSDT", "BUY", 100.0, 100.0, 99.0, mono_now=mono)
    ledger6.confirm_fill("GKT_B1", 100.0, 100.0)
    # exposure = 100 * 100 = 10_000 > 5_000 limit
    check('exposure_exceeded', not ledger6.is_strike_allowed("ETHUSDT"))

    # ═══════════════════════════════════════════════════
    # Audit trail
    # ═══════════════════════════════════════════════════
    print('=== Audit Trail ===')
    check('audit_has_entries', len(ledger.audit_trail) > 0)
    check('generation_positive', ledger.generation > 0)

    # ═══════════════════════════════════════════════════
    # Fill for unknown order (defensive)
    # ═══════════════════════════════════════════════════
    print('=== Unknown Order Fill ===')
    ledger7 = SchrodingerLedger()
    v7 = ledger7.confirm_fill("UNKNOWN_ORDER", 1.0, 50000.0)
    check('unknown_fill_accepted', v7.fate == OrderFate.FILLED)
    check('unknown_fill_message', v7.message == "FILL_FOR_UNKNOWN_ORDER")

    print(f'\n=== RESULTS: {passed} passed, {failed} failed ===')
    return failed


if __name__ == "__main__":
    sys.exit(run_tests())
