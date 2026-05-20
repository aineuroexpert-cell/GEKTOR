# Taiwan proxy configured (socks5://)
from typing import Any

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.shared.alpha_config import alpha


class Settings(BaseSettings):
    # Telegram
    TG_BOT_TOKEN: str = Field(default="", alias="gerald_bot_token")
    TG_CHAT_ID: str = Field(default="", alias="telegram_chat_id")

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

    # Bybit credentials (critical, fail-fast)
    BYBIT_API_KEY: str = Field(..., alias="bybit_api_key")
    BYBIT_API_SECRET: str = Field(..., alias="bybit_api_secret")
    BYBIT_ACCOUNT_TYPE: str = "UNIFIED"

    # [GEKTOR v5.22] Math calibration via AlphaConfig
    VPIN_WINDOW: int = alpha.VPIN_WINDOW_SIZE
    VPIN_THRESHOLD: float = alpha.VPIN_EXHAUSTION_THRESHOLD
    WARMUP_VOLUME: float = 5_000_000.0

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
        missing: list[str] = []

        if not self.ASYNC_DATABASE_URL.strip():
            missing.append("async_database_url")
        if not self.BYBIT_API_KEY.strip():
            missing.append("bybit_api_key")
        if not self.BYBIT_API_SECRET.strip():
            missing.append("bybit_api_secret")

        if missing:
            required = ", ".join(missing)
            raise ValueError(
                f"Critical runtime configuration is missing or empty: {required}. "
                "Set required variables in environment or .env before startup."
            )

        import re
        if self.TG_BOT_TOKEN:
            token = self.TG_BOT_TOKEN.strip()
            if not re.match(r"^\d+:[A-Za-z0-9_-]{35,45}$", token):
                raise ValueError("Invalid Telegram Bot Token signature format.")

        return self

    def wipe_sensitive(self) -> None:
        """
        [GEKTOR v3.0.0] Wipes sensitive data from memory to prevent leaks.
        """
        import ctypes
        import os

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
            except Exception:
                pass

        # Wipe individual Settings attributes
        for field in ["BYBIT_API_KEY", "BYBIT_API_SECRET", "TG_BOT_TOKEN"]:
            val = getattr(self, field, None)
            if val and isinstance(val, str):
                zero_string(val)
                setattr(self, field, "")

        # Purge environment variables
        for env_var in ["BYBIT_API_KEY", "BYBIT_API_SECRET", "GERALD_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
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


settings = Settings()
