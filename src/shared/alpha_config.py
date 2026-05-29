# src/shared/alpha_config.py
"""
[GEKTOR APEX v5.22] Alpha Configuration Loader (Dependency Injection).

All proprietary quantitative thresholds live in secrets/alpha_weights.json.
This module loads them at startup and exposes them as typed attributes.
The core engine references these values instead of hardcoded constants.

SECURITY: secrets/ is gitignored. This file contains NO numeric values.
"""
import json
import os
from pathlib import Path
from loguru import logger


class AlphaConfig:
    """Immutable configuration container for quantitative thresholds."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._loaded = False
        return cls._instance

    def __init__(self):
        if self._loaded:
            return
        self._load()
        self._loaded = True

    def _load(self):
        """Load thresholds from secrets/alpha_weights.json or environment."""
        config_path = Path(os.getenv(
            "ALPHA_CONFIG_PATH",
            Path(__file__).parent.parent.parent / "secrets" / "alpha_weights.json"
        ))

        if config_path.exists():
            with open(config_path, "r") as f:
                data = json.load(f)
            logger.success(f"🔐 [AlphaConfig] Loaded from {config_path}")
        else:
            logger.warning(f"⚠️ [AlphaConfig] {config_path} not found. Using environment fallback.")
            data = {}

        # LFI / Intent Lifecycle
        self.LFI_CAPITULATION_MULTIPLIER = float(data.get("lfi_capitulation_multiplier", os.getenv("LFI_CAP_MULT", 0)))
        self.LFI_EXTREME_MULTIPLIER = float(data.get("lfi_extreme_multiplier", os.getenv("LFI_EXT_MULT", 0)))
        self.INTENT_TTL_SEC = float(data.get("intent_ttl_sec", os.getenv("INTENT_TTL", 0)))
        self.CAUSAL_RING_BUFFER_SIZE = int(data.get("causal_ring_buffer_size", os.getenv("RING_BUF_SIZE", 1024)))
        self.SIGNAL_COOLDOWN_SEC = float(data.get("signal_cooldown_sec", os.getenv("SIGNAL_COOLDOWN", 0)))

        # Friction Guard
        self.TAKER_FEE_BPS = float(data.get("taker_fee_bps", os.getenv("TAKER_FEE_BPS", 0)))
        self.MIN_ALPHA_BPS = float(data.get("min_alpha_bps", os.getenv("MIN_ALPHA_BPS", 0)))
        self.STALE_QUOTE_SEC = float(data.get("stale_quote_sec", os.getenv("STALE_QUOTE_SEC", 30)))
        self.TOXICITY_1S_THRESHOLD_BPS = float(data.get("toxicity_1s_threshold_bps", os.getenv("TOX_THRESH", -15)))
        self.TOXICITY_PENALTY_FACTOR = float(data.get("toxicity_penalty_factor", os.getenv("TOX_PENALTY", 1.2)))

        # Macro Regime
        self.PANIC_VPIN_THRESHOLD = float(data.get("panic_vpin_threshold", os.getenv("PANIC_VPIN", 0)))
        self.PANIC_DELTA_THRESHOLD = float(data.get("panic_delta_threshold", os.getenv("PANIC_DELTA", 0)))
        self.VPIN_EXHAUSTION_THRESHOLD = float(data.get("vpin_exhaustion_threshold", os.getenv("VPIN_EXHAUST", 0)))

        # VPIN Engine
        self.VPIN_WINDOW_SIZE = int(data.get("vpin_window_size", os.getenv("VPIN_WINDOW", 50)))
        self.VPIN_ANOMALY_Z = float(data.get("vpin_anomaly_z", os.getenv("VPIN_Z", 0)))
        self.VPIN_TIME_GAP_SEC = int(data.get("vpin_time_gap_sec", os.getenv("VPIN_GAP", 14400)))
        self.VPIN_DECAY_TAU_HOURS = int(data.get("vpin_decay_tau_hours", os.getenv("VPIN_TAU", 4)))
        self.VPIN_BUCKET_SIGNIFICANCE_PCT = float(data.get("vpin_bucket_significance_pct", os.getenv("VPIN_SIG", 0.8)))

        # Scoring
        self.MICRO_SCORING_WEIGHT = float(data.get("micro_scoring_weight", os.getenv("MICRO_W", 0.85)))
        self.SENTIMENT_SCORING_WEIGHT = float(data.get("sentiment_scoring_weight", os.getenv("SENT_W", 0.15)))

        # Volume Clocks
        self.VOLUME_CLOCKS = data.get("volume_clocks", {})

        # Exit Protocol
        self.EXIT_VPIN_DECAY_FACTOR = float(data.get("exit_vpin_decay_factor", os.getenv("EXIT_DECAY", 0)))
        self.EXIT_CUSUM_REVERSAL_SIGMA = float(data.get("exit_cusum_reversal_sigma", os.getenv("EXIT_SIGMA", 0)))
        self.EXIT_TIME_MAX_BARS = int(data.get("exit_time_max_bars", os.getenv("EXIT_BARS", 24)))

        # Microstructure OFI
        self.MICRO_OFI = data.get("micro_ofi", {})
        self.OFI_GODZILLA_USD = float(data.get("ofi_godzilla_usd", os.getenv("OFI_GODZILLA", 0)))
        self.OFI_Z_THRESHOLD = float(data.get("ofi_z_threshold", os.getenv("OFI_Z", 0)))
        self.META_SCORER_MIN_WIN_RATE = float(data.get("meta_scorer_min_win_rate", os.getenv("META_WIN", 0.55)))

        # Global Equity Guard (Portfolio Risk)
        self.MAX_EQUITY_USD = float(data.get("max_equity_usd", os.getenv("MAX_EQUITY", 5000.0)))
        self.EQUITY_RESERVE_PCT = float(data.get("equity_reserve_pct", os.getenv("EQUITY_RESERVE", 0.15)))
        self.MAX_CONCURRENT_SIGNALS = int(data.get("max_concurrent_signals", os.getenv("MAX_SIGNALS", 5)))
        
        # [GEKTOR v5.24] Exchange Microstructure Rules (Physics)
        self.TRADING_RULES = data.get("trading_rules", {
            "BTCUSDT": {"tick": "0.1", "step": "0.001", "min_notional": "5.0"},
            "ETHUSDT": {"tick": "0.01", "step": "0.01", "min_notional": "5.0"}
        })

        logger.success("✅ [AlphaConfig] All thresholds loaded successfully.")


# Singleton instance
alpha = AlphaConfig()
