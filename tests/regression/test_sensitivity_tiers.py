"""[GEKTOR v3.6.2] Regression for sensitivity tier mapping.

Pins down:
  * Each named tier resolves to a specific (z_threshold, vpin_window, cooldown).
  * Unknown tiers fall back to "active" without raising.
  * Empty / None / case-mixed input is normalized.
  * Mutating the returned dict does not affect the canonical table.
"""
from __future__ import annotations

import os

# Settings() is instantiated at module load (config.py:309) and requires
# ASYNC_DATABASE_URL. Inject a hermetic in-memory SQLite URL before the
# config module is imported; this matches the pattern in
# test_settings_aliases.py and avoids leaking the operator's .env.
os.environ.setdefault("ASYNC_DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from src.infrastructure.config import SENSITIVITY_TIERS, resolve_sensitivity  # noqa: E402


def test_active_is_default_tier() -> None:
    params = resolve_sensitivity("active")
    assert params["z_threshold"] == 2.0
    assert params["vpin_window"] == 50
    assert params["cooldown_sec"] == 300.0


def test_conservative_tier() -> None:
    params = resolve_sensitivity("conservative")
    assert params["z_threshold"] == 2.5
    assert params["cooldown_sec"] == 600.0


def test_scanner_tier() -> None:
    params = resolve_sensitivity("scanner")
    assert params["z_threshold"] == 1.7
    assert params["vpin_window"] == 30
    assert params["cooldown_sec"] == 120.0


def test_unknown_tier_falls_back_to_active() -> None:
    params = resolve_sensitivity("nonsense")
    assert params["z_threshold"] == 2.0
    assert params["vpin_window"] == 50


def test_case_and_whitespace_normalisation() -> None:
    assert resolve_sensitivity("  ACTIVE  ")["z_threshold"] == 2.0
    assert resolve_sensitivity("Scanner")["z_threshold"] == 1.7
    assert resolve_sensitivity("")["z_threshold"] == 2.0


def test_returned_dict_is_a_copy() -> None:
    params = resolve_sensitivity("active")
    params["z_threshold"] = 999.0
    # Canonical table must NOT have been mutated.
    assert SENSITIVITY_TIERS["active"]["z_threshold"] == 2.0
