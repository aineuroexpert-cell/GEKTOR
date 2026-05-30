# Taiwan proxy configured (socks5://)
from typing import Any

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.shared.alpha_config import alpha


class Settings(BaseSettings):
    # Telegram
    # v3.6.1 deploy fix: accept several common env var names so operators
    # don't have to remember the internal alias. All of these resolve to
    # the same field: BOT_TOKEN, TELEGRAM_BOT_TOKEN, TG_BOT_TOKEN,
    # GERALD_BOT_TOKEN (legacy).
    TG_BOT_TOKEN: str = Field(
        default="",
        validation_alias=AliasChoices(
            "bot_token",
            "telegram_bot_token",
            "tg_bot_token",
            "gerald_bot_token",
        ),
    )
    TG_CHAT_ID: str = Field(
        default="",
        validation_alias=AliasChoices(
            "chat_id",
            "telegram_chat_id",
            "tg_chat_id",
        ),
    )
    TELEGRAM_API_BASE_URL: str = Field(
        default="https://api.telegram.org",
        validation_alias=AliasChoices(
            "telegram_api_base_url",
            "tg_api_base_url",
            "telegram_api_url",
        ),
    )

    # Infrastructure
    REDIS_HOST: str = Field(default="localhost", alias="redis_host")
    REDIS_PORT: int = Field(default=6379, alias="redis_port")
    REDIS_PASSWORD: str | None = Field(default=None, alias="redis_password")

    # Database (critical, fail-fast)
    ASYNC_DATABASE_URL: str = Field(..., alias="async_database_url")

    # Proxy
    PROXY_URL: str | None = Field(default=None, alias="proxy_url")
    TG_PROXY_URL: str | None = Field(default=None, alias="tg_proxy_url")
    TELEGRAM_PROXY: str | None = Field(default=None, alias="telegram_proxy")
    USE_PROXY_FOR_BYBIT: bool = Field(default=False, alias="use_proxy_for_bybit")

    # Bybit credentials.
    # v3.6.1 deploy fix: optional by default — the Advisory radar uses
    # ONLY the public publicTrade WS endpoint, which does not require
    # authentication. If the operator sets these (e.g. for a future REST
    # client used for symbol discovery), they MUST satisfy the regex
    # validators below. Empty values are accepted and skip validation.
    BYBIT_API_KEY: str = Field(default="", alias="bybit_api_key")
    BYBIT_API_SECRET: str = Field(default="", alias="bybit_api_secret")
    BYBIT_ACCOUNT_TYPE: str = "UNIFIED"

    # [GEKTOR v5.22] Math calibration via AlphaConfig
    VPIN_WINDOW: int = alpha.VPIN_WINDOW_SIZE
    VPIN_THRESHOLD: float = alpha.VPIN_EXHAUSTION_THRESHOLD
    WARMUP_VOLUME: float = 5_000_000.0

    # [GEKTOR v3.0.0] Advisory Radar — Dollar Bar threshold
    DOLLAR_THRESHOLD_BASE: float = Field(
        default=1_000_000.0,
        alias="dollar_threshold_base",
        description="Dollar volume threshold per bar (USD). Lower = more bars/alerts."
    )

    # [GEKTOR v3.6.2] Sensitivity tier selector. Maps to z_threshold,
    # VPIN window size, and per-symbol cooldown. See README §Sensitivity.
    # One of: "conservative" | "active" | "scanner".
    SENSITIVITY: str = Field(
        default="active",
        alias="sensitivity",
        description="Sensitivity tier: conservative|active|scanner.",
    )

    # [GEKTOR v3.6.2] Adaptive per-symbol dollar threshold (sizes the
    # bar by 24h turnover so tail-cap altcoins don't take days to warm
    # up). When false, every symbol uses DOLLAR_THRESHOLD_BASE.
    ADAPTIVE_THRESHOLD_ENABLE: bool = Field(
        default=True,
        alias="adaptive_threshold_enable",
    )
    ADAPTIVE_TARGET_BARS_PER_DAY: int = Field(
        default=200,
        alias="adaptive_target_bars_per_day",
        description="Target bars/day per symbol (drives clamp of bar $-size).",
    )
    ADAPTIVE_MIN_USD: float = Field(
        default=20_000.0,
        alias="adaptive_min_usd",
    )
    ADAPTIVE_MAX_USD: float = Field(
        default=5_000_000.0,
        alias="adaptive_max_usd",
    )
    ADAPTIVE_REFRESH_SEC: float = Field(
        default=3600.0,
        alias="adaptive_refresh_sec",
        description="How often to re-pull 24h turnover from Bybit REST.",
    )

    # [GEKTOR v3.6.2] Liquidity detectors (instant fire, no warmup).
    LIQUIDITY_DETECTORS_ENABLE: bool = Field(
        default=True,
        alias="liquidity_detectors_enable",
    )
    # Sweep: N+ same-side aggressor trades summing to > $threshold within W sec.
    SWEEP_MIN_TRADES: int = Field(default=5, alias="sweep_min_trades")
    SWEEP_WINDOW_SEC: float = Field(default=30.0, alias="sweep_window_sec")
    SWEEP_MIN_NOTIONAL_USD: float = Field(default=100_000.0, alias="sweep_min_notional_usd")
    # Large print: single trade > pct of 24h turnover.
    LARGE_PRINT_PCT_THRESHOLD: float = Field(
        default=0.005, alias="large_print_pct_threshold",
        description="0.005 = 0.5% of 24h turnover."
    )
    LARGE_PRINT_MIN_NOTIONAL_USD: float = Field(
        default=25_000.0, alias="large_print_min_notional_usd"
    )
    # OFI Pulse: 1-min OFI > k * rolling 1-hour median.
    OFI_PULSE_K: float = Field(default=3.0, alias="ofi_pulse_k")
    OFI_PULSE_BUCKET_SEC: float = Field(default=60.0, alias="ofi_pulse_bucket_sec")
    OFI_PULSE_HISTORY_BUCKETS: int = Field(default=60, alias="ofi_pulse_history_buckets")
    OFI_PULSE_MIN_NOTIONAL_USD: float = Field(
        default=50_000.0, alias="ofi_pulse_min_notional_usd"
    )

    # [GEKTOR v3.6.4] Noise Reduction Filters
    # 1. Market Macro-Context Filter (P9)
    MACRO_FILTER_ENABLE: bool = Field(default=True, alias="macro_filter_enable")
    MACRO_BTC_VOLATILITY_LIMIT: float = Field(default=0.01, alias="macro_btc_volatility_limit")
    MACRO_ETH_VOLATILITY_LIMIT: float = Field(default=0.015, alias="macro_eth_volatility_limit")
    MACRO_WINDOW_SIZE: int = Field(default=10, alias="macro_window_size")

    # 2. CVD Divergence Detector (P8)
    CVD_FILTER_ENABLE: bool = Field(default=True, alias="cvd_filter_enable")
    CVD_MIN_RATIO: float = Field(default=0.15, alias="cvd_min_ratio")

    # 3. Volatility-Adaptive Z-Score (P10)
    ADAPTIVE_Z_ENABLE: bool = Field(default=True, alias="adaptive_z_enable")
    ADAPTIVE_Z_VOLATILITY_BASE: float = Field(default=0.01, alias="adaptive_z_volatility_base")
    ADAPTIVE_Z_SENSITIVITY: float = Field(default=0.5, alias="adaptive_z_sensitivity")
    ADAPTIVE_Z_MIN_MULT: float = Field(default=0.8, alias="adaptive_z_min_mult")
    ADAPTIVE_Z_MAX_MULT: float = Field(default=2.0, alias="adaptive_z_max_mult")

    # [GEKTOR v5.22] Adaptive Volume Clocks
    VOLUME_BUCKETS: dict[str, float] = alpha.VOLUME_CLOCKS if alpha.VOLUME_CLOCKS else {"DEFAULT": 1_000_000.0}

    # [GEKTOR v5.22] Exit Protocol
    EXIT_VPIN_DECAY_FACTOR: float = alpha.EXIT_VPIN_DECAY_FACTOR
    EXIT_CUSUM_REVERSAL_SIGMA: float = alpha.EXIT_CUSUM_REVERSAL_SIGMA
    EXIT_TIME_MAX_BARS: int = alpha.EXIT_TIME_MAX_BARS

    # [GEKTOR v5.24] Resilience Anchors
    SNAPSHOT_TIMEOUT_SEC: float = 15.0
    CAUSAL_TIMEOUT_MS: int = 500

    # [GEKTOR v5.22] Microstructure Calibration
    MICRO_OFI_CONFIG: dict[str, Any] = alpha.MICRO_OFI if alpha.MICRO_OFI else {}

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    @model_validator(mode="after")
    def _validate_critical_runtime_vars(self) -> "Settings":
        # v3.6.1: only the DB URL is strictly required. Bybit keys are
        # optional (Advisory radar uses public WS), but if provided they
        # must satisfy the format regex.
        if not self.ASYNC_DATABASE_URL.strip():
            raise ValueError(
                "Critical runtime configuration is missing or empty: async_database_url. "
                "Set required variables in environment or .env before startup."
            )

        import re
        if self.TG_BOT_TOKEN:
            token = self.TG_BOT_TOKEN.strip()
            if not re.match(r"^\d+:[A-Za-z0-9_-]{35,45}$", token):
                raise ValueError("Invalid Telegram Bot Token signature format.")

        # Bybit API Key/Secret regex validation (only if provided).
        if self.BYBIT_API_KEY.strip():
            key = self.BYBIT_API_KEY.strip()
            if not re.match(r"^[A-Za-z0-9]{18,24}$", key):
                raise ValueError(
                    f"Invalid Bybit API Key format (expected 18-24 alphanumeric chars, got {len(key)})."
                )
        if self.BYBIT_API_SECRET.strip():
            secret = self.BYBIT_API_SECRET.strip()
            if not re.match(r"^[A-Za-z0-9]{36,50}$", secret):
                raise ValueError(
                    f"Invalid Bybit API Secret format (expected 36-50 alphanumeric chars, got {len(secret)})."
                )

        return self

    def wipe_sensitive(self) -> None:
        """
        Wipes sensitive data from memory to reduce leak surface.

        NOTE: Best-effort only. Python strings are immutable and may be
        interned (small strings, ASCII identifiers) — ctypes overwriting
        may corrupt the interpreter or simply do nothing depending on
        platform & CPython version. We mostly rely on dropping the
        references and clearing env vars; the ctypes step is a defence-
        in-depth attempt for the rare case it actually works.

        v3.6.2: silent `except Exception: pass` removed (violates
        AGENTS.md). Concrete OS/CTypes errors are caught and logged.
        """
        import ctypes
        import logging
        import os

        log = logging.getLogger("GEKTOR_RADAR")

        def zero_string(s: str) -> None:
            if not isinstance(s, str) or not s:
                return
            try:
                char_code = ord(s[0])
                offset = None
                for i in range(16, 128):
                    val = ctypes.c_ubyte.from_address(id(s) + i).value
                    if val == char_code:
                        offset = i
                        break
                if offset is not None:
                    for i in range(len(s)):
                        ctypes.c_ubyte.from_address(id(s) + offset + i).value = 0
            except (ValueError, OSError, ctypes.ArgumentError) as exc:
                # ValueError: ord() of empty / ctypes layout mismatch.
                # OSError: address inaccessible.
                # ctypes.ArgumentError: address arithmetic issue.
                log.debug(f"[CONFIG] zero_string best-effort wipe skipped: {exc!r}")

        # Wipe individual Settings attributes
        for field in ["BYBIT_API_KEY", "BYBIT_API_SECRET", "TG_BOT_TOKEN"]:
            val = getattr(self, field, None)
            if val and isinstance(val, str):
                zero_string(val)
                setattr(self, field, "")

        # Purge environment variables. Cover every accepted alias for
        # the Telegram token / chat id (v3.6.1 added BOT_TOKEN / TELEGRAM_*
        # variants — wipe them all, not just the legacy names).
        for env_var in (
            "BYBIT_API_KEY",
            "BYBIT_API_SECRET",
            "BOT_TOKEN",
            "TELEGRAM_BOT_TOKEN",
            "TG_BOT_TOKEN",
            "GERALD_BOT_TOKEN",
            "CHAT_ID",
            "TELEGRAM_CHAT_ID",
            "TG_CHAT_ID",
        ):
            val = os.environ.pop(env_var, None)
            if val:
                zero_string(val)

    @property
    def bot_token(self) -> str:
        return self.TG_BOT_TOKEN.strip() if self.TG_BOT_TOKEN else ""

    @property
    def chat_id(self) -> str:
        return self.TG_CHAT_ID.strip() if self.TG_CHAT_ID else ""

    @property
    def VPIN_BUCKET_VOLUME(self) -> float:
        return self.VOLUME_BUCKETS.get("DEFAULT", 1_000_000.0)


# ----------------------------------------------------------------------
# [GEKTOR v3.6.2] Sensitivity tier mapping
# ----------------------------------------------------------------------

# Maps the three named tiers to concrete radar parameters. Kept as a
# module-level constant so it is testable and editable without touching
# Settings logic. Tier mismatches fall back to "active" (default).
SENSITIVITY_TIERS: dict[str, dict[str, float | int]] = {
    "conservative": {
        "z_threshold": 2.5,
        "vpin_window": 50,
        "cooldown_sec": 600.0,
    },
    "active": {
        "z_threshold": 2.0,
        "vpin_window": 50,
        "cooldown_sec": 300.0,
    },
    "scanner": {
        "z_threshold": 1.7,
        "vpin_window": 30,
        "cooldown_sec": 120.0,
    },
}


def resolve_sensitivity(tier: str) -> dict[str, float | int]:
    """Return the radar parameters for a given sensitivity tier.

    Unknown tiers fall back to "active" with a warning. The returned
    dict is a fresh copy — mutating it does not affect the table.
    """
    import logging

    log = logging.getLogger("GEKTOR_RADAR")
    key = (tier or "").strip().lower()
    params = SENSITIVITY_TIERS.get(key)
    if params is None:
        log.warning(
            f"[CONFIG] Unknown SENSITIVITY tier {tier!r}; "
            f"falling back to 'active' (z=2.0, window=50, cooldown=300s)."
        )
        params = SENSITIVITY_TIERS["active"]
    return dict(params)


settings = Settings()
