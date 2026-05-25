# tests/test_intent_capsule.py
"""
[GEKTOR v15.1] Intent Capsule Tests.

Validates the full cryptographic defense cascade:
  - HMAC forgery detection
  - Nonce replay rejection
  - TTL enforcement
  - Tilt generation mismatch detection
  - Tilt lock hard gate
  - Session rotation invalidation
"""
import sys
sys.path.insert(0, '.')

from src.domain.intent_capsule import CapsuleForge, CapsuleValidator, IntentCapsule
from src.domain.tilt_breaker import CognitiveSentinel, TiltState


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
    # CapsuleForge Tests
    # ═══════════════════════════════════════════════════
    print('=== CapsuleForge ===')

    forge = CapsuleForge()
    capsule = forge.issue("sig_001", "BTCUSDT", "BUY", 42, mono_now=mono)
    check('capsule_created', capsule.signal_id == "sig_001")
    check('capsule_symbol', capsule.symbol == "BTCUSDT")
    check('capsule_side', capsule.side == "BUY")
    check('capsule_tilt_gen', capsule.tilt_generation == 42)
    check('capsule_has_nonce', len(capsule.nonce) == 32)  # 16 bytes → 32 hex chars
    check('capsule_has_signature', len(capsule.signature) == 64)  # SHA256 → 64 hex chars

    # Verify signature
    check('signature_valid', forge.verify_signature(capsule))

    # Tampered capsule (different signal_id)
    tampered = IntentCapsule(
        signal_id="FORGED",
        symbol=capsule.symbol,
        side=capsule.side,
        tilt_generation=capsule.tilt_generation,
        nonce=capsule.nonce,
        issued_at_mono=capsule.issued_at_mono,
        signature=capsule.signature,
    )
    check('tampered_rejected', not forge.verify_signature(tampered))

    # Different forge (different session key)
    other_forge = CapsuleForge()
    check('wrong_key_rejected', not other_forge.verify_signature(capsule))

    # Key rotation
    old_session = forge.session_id
    forge.rotate_key()
    check('session_rotated', forge.session_id != old_session)
    check('old_capsule_invalid_after_rotation', not forge.verify_signature(capsule))

    # ═══════════════════════════════════════════════════
    # CapsuleValidator Tests
    # ═══════════════════════════════════════════════════
    print('=== CapsuleValidator ===')

    sentinel = CognitiveSentinel()
    # Build baseline
    for i in range(10):
        sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + i)

    forge2 = CapsuleForge()
    validator = CapsuleValidator(forge2, sentinel, ttl_sec=1.2)

    tilt_gen = sentinel.get_generation()
    cap = forge2.issue("sig_002", "ETHUSDT", "SELL", tilt_gen, mono_now=mono + 10)

    # Valid capsule
    v = validator.validate(cap, tilt_gen, mono_now=mono + 10.5)
    check('valid_capsule_accepted', v.allowed)
    check('valid_signal_id', v.signal_id == "sig_002")

    # Replay: same nonce again
    v2 = validator.validate(cap, tilt_gen, mono_now=mono + 10.6)
    check('replay_rejected', not v2.allowed)
    check('replay_reason', 'NONCE_REPLAY' in v2.rejection_reason)

    # Expired capsule (TTL exceeded)
    cap_old = forge2.issue("sig_003", "SOLUSDT", "BUY", tilt_gen, mono_now=mono + 10)
    v3 = validator.validate(cap_old, tilt_gen, mono_now=mono + 12)  # 2s > 1.2s TTL
    check('ttl_expired_rejected', not v3.allowed)
    check('ttl_reason', 'TTL_EXPIRED' in v3.rejection_reason)

    # Clock manipulation (negative age)
    cap_future = forge2.issue("sig_004", "BTCUSDT", "BUY", tilt_gen, mono_now=mono + 20)
    v4 = validator.validate(cap_future, tilt_gen, mono_now=mono + 19)  # Time went backwards
    check('clock_manipulation_rejected', not v4.allowed)
    check('clock_reason', 'CLOCK_MANIPULATION' in v4.rejection_reason)

    # Tilt generation mismatch
    cap_stale_gen = forge2.issue("sig_005", "BTCUSDT", "BUY", tilt_gen, mono_now=mono + 15)
    # Ingest heartbeat to bump generation
    sentinel.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + 15.1)
    new_gen = sentinel.get_generation()
    check('gen_bumped', new_gen != tilt_gen)
    v5 = validator.validate(cap_stale_gen, new_gen, mono_now=mono + 15.5)
    check('tilt_gen_mismatch_rejected', not v5.allowed)
    check('gen_reason', 'TILT_GEN_MISMATCH' in v5.rejection_reason)

    # Tilt LOCKED gate
    sentinel2 = CognitiveSentinel()
    for i in range(10):
        sentinel2.ingest_heartbeat(500.0, 0, 0, 0, mono_now=mono + i)
    # Trigger lock
    sentinel2.ingest_heartbeat(1500.0, 10, 5, 0, mono_now=mono + 11)
    check('sentinel2_locked', not sentinel2.is_execution_allowed())

    forge3 = CapsuleForge()
    validator3 = CapsuleValidator(forge3, sentinel2, ttl_sec=5.0)
    locked_gen = sentinel2.get_generation()
    cap_locked = forge3.issue("sig_006", "BTCUSDT", "BUY", locked_gen, mono_now=mono + 11.1)
    v6 = validator3.validate(cap_locked, locked_gen, mono_now=mono + 11.2)
    check('tilt_locked_rejected', not v6.allowed)
    check('tilt_locked_reason', 'TILT_LOCKED' in v6.rejection_reason)

    # HMAC forgery
    forge4 = CapsuleForge()
    validator4 = CapsuleValidator(forge4, CognitiveSentinel(), ttl_sec=5.0)
    forged_cap = IntentCapsule(
        signal_id="sig_007",
        symbol="BTCUSDT",
        side="BUY",
        tilt_generation=0,
        nonce="deadbeefcafebabe" * 2,
        issued_at_mono=mono,
        signature="0" * 64,
    )
    v7 = validator4.validate(forged_cap, 0, mono_now=mono + 0.1)
    check('hmac_forged_rejected', not v7.allowed)
    check('hmac_reason', 'HMAC_INVALID' in v7.rejection_reason)

    # ═══════════════════════════════════════════════════
    # Telemetry
    # ═══════════════════════════════════════════════════
    print('=== Telemetry ===')
    check('rejections_counted', validator.total_rejections > 0)
    stats = validator.rejection_stats
    check('stats_has_replay', stats.get('NONCE_REPLAY', 0) > 0)
    check('stats_has_ttl', stats.get('TTL_EXPIRED', 0) > 0)

    print(f'\n=== RESULTS: {passed} passed, {failed} failed ===')
    return failed


if __name__ == "__main__":
    sys.exit(run_tests())
