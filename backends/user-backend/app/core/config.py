# app/core/config.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

log = logging.getLogger("cosmicforge.config")


def _parse_list(v: Any) -> List[str]:
    """
    Accepts:
      - list: ["BTCUSDT","ETHUSDT"]
      - csv:  "BTCUSDT,ETHUSDT"
      - json: '["BTCUSDT","ETHUSDT"]'
    Returns uppercase, trimmed symbols.
    """
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().upper() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            arr = json.loads(s)
            return [str(x).strip().upper() for x in arr if str(x).strip()]
        except Exception:
            # fall back to csv parse
            pass
    return [p.strip().upper() for p in s.split(",") if p.strip()]


def _parse_kv_int(v: Any) -> Dict[str, int]:
    """
    Accepts:
      - dict: {"BTCUSDT": 10}
      - csv:  "BTCUSDT:10,ETHUSDT:10"
      - json: '{"BTCUSDT":10,"ETHUSDT":10}'
    """
    if v is None:
        return {}
    if isinstance(v, dict):
        out: Dict[str, int] = {}
        for k, val in v.items():
            ks = str(k).strip().upper()
            if not ks:
                continue
            try:
                out[ks] = int(val)
            except Exception:
                continue
        return out

    s = str(v).strip()
    if not s:
        return {}

    if s.startswith("{"):
        try:
            raw = json.loads(s)
            if isinstance(raw, dict):
                return _parse_kv_int(raw)
        except Exception:
            pass

    out: Dict[str, int] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        k, val = part.split(":", 1)
        k = k.strip().upper()
        val = val.strip()
        if not k:
            continue
        try:
            out[k] = int(val)
        except Exception:
            continue
    return out


def _parse_kv_float(v: Any) -> Dict[str, float]:
    """
    Accepts:
      - dict: {"BTCUSDT": 100}
      - csv:  "BTCUSDT:100,ETHUSDT:50"
      - json: '{"BTCUSDT":100,"ETHUSDT":50}'
    """
    if v is None:
        return {}
    if isinstance(v, dict):
        out: Dict[str, float] = {}
        for k, val in v.items():
            ks = str(k).strip().upper()
            if not ks:
                continue
            try:
                out[ks] = float(val)
            except Exception:
                continue
        return out

    s = str(v).strip()
    if not s:
        return {}

    if s.startswith("{"):
        try:
            raw = json.loads(s)
            if isinstance(raw, dict):
                return _parse_kv_float(raw)
        except Exception:
            pass

    out: Dict[str, float] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        k, val = part.split(":", 1)
        k = k.strip().upper()
        val = val.strip()
        if not k:
            continue
        try:
            out[k] = float(val)
        except Exception:
            continue
    return out


class Settings(BaseSettings):
    """Runtime configuration loaded from .env / environment variables."""

    # IMPORTANT:
    # enable_decoding=False prevents pydantic-settings from auto-json-decoding List/Dict fields.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
        enable_decoding=False,
    )

    # --- Database ---
    DATABASE_URL: str = "sqlite:///../shared/shared_lib/persistence/cosmicforge.db"

    # --- Exchange / API ---
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""

    # If you use demo/testnet, set BINANCE_ENV=testnet (recommended)
    BINANCE_ENV: str = "mainnet"  # mainnet/testnet
    BINANCE_FAPI_BASE_URL: str = "https://fapi.binance.com"
    BINANCE_RECV_WINDOW: int = 5000

    # --- Bybit Exchange ---
    BYBIT_API_KEY: str = ""
    BYBIT_API_SECRET: str = ""
    BYBIT_ENV: str = "mainnet"  # mainnet | testnet
    BYBIT_BASE_URL: str = ""    # Optional override, defaults based on env
    BYBIT_RECV_WINDOW: int = 5000
    BYBIT_TIMEOUT_SECONDS: int = 10
    BYBIT_ACCOUNT_TYPE: str = "UNIFIED"  # UNIFIED | CONTRACT
    BYBIT_CATEGORY_DEFAULT: str = "linear" # linear (USDT perps)

    # --- BingX Exchange (Future) ---
    BINGX_API_KEY: str = ""
    BINGX_API_SECRET: str = ""
    BINGX_ENV: str = "mainnet" # mainnet | testnet (or demo)
    BINGX_BASE_URL: str = ""
    BINGX_TIMEOUT_SECONDS: int = 10

    # --- Security / Auth ---
    SECRET_KEY: str = "changeme_in_production_secret_key"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15  # Short-lived for security
    REFRESH_TOKEN_EXPIRE_DAYS: int = 14   # 2 weeks
    CREDENTIAL_KEY: str = "changeme_in_production_credential_key_32bytes!!!!"  # Must be 32 bytes for Fernet if used directly, or generated.

    # --- Email Configuration ---
    SMTP_HOST: str = ""                    # e.g., "smtp.gmail.com"
    SMTP_PORT: int = 587                   # 587 for TLS, 465 for SSL
    SMTP_USER: str = ""                    # Your email address
    SMTP_PASSWORD: str = ""                # App password (not your main password)
    SMTP_FROM_EMAIL: str = ""              # Sender email address
    SMTP_FROM_NAME: str = "CosmicForge"    # Sender display name
    EMAIL_ENABLED: bool = False            # Set to True when credentials are configured

    # --- Billing / Stripe ---
    STRIPE_SECRET_KEY: str = ""
    STRIPE_WEBHOOK_SECRET: str = ""

    # --- Symbols / universe ---
    TRADE_SYMBOLS: List[str] = Field(default_factory=list)
    LIVE_SYMBOLS: List[str] = Field(default_factory=list)
    DEFAULT_INTERVAL: str = "1m"
    
    # ✅ Main runner settings
    RUN_INTERVAL_SECONDS: int = 60
    MAX_SYMBOLS: int = 100
    RUN_MAX_SYMBOLS: int = 100
    
    # Debug / Testing
    FORCE_SIGNAL: str | None = None  # set "BUY" or "SELL" to force trades
    
    # --- Execution ---
    EXECUTION_MODE: str = "paper"  # paper/live
    MAX_LIVE_TRADES_PER_CYCLE: int = 1

    # --- Strategy controls ---
    TRADE_MODE: str = "flip"
    COOLDOWN_SECONDS: int = 120
    MAX_ADDS_PER_POSITION: int = 2

    STOP_LOSS_PCT: float = 1.0
    TAKE_PROFIT_PCT: float = 2.0
    SL_COOLDOWN_SECONDS: int = 600

    # Strategy selection (broker-agnostic)
    STRATEGY_NAME: str = Field(default="robust_ensemble")
    STRATEGY_PARAMS_JSON: str = Field(default="")

    # Attribution defaults (future-proof for multi-broker)
    BROKER_ID: str = Field(default="binance_futures")
    ACCOUNT_ID: str = Field(default="default")
    ASSET_CLASS: str = Field(default="CRYPTO")

    # --- Leverage ---
    DEFAULT_LEVERAGE: int = 5
    MIN_LEVERAGE: int = 5
    SYMBOL_LEVERAGE_MAP: Dict[str, int] = Field(default_factory=dict)

    # --- Sizing ---
    TRADE_USDT_PER_ORDER: float = 100.0
    SYMBOL_USDT_MAP: Dict[str, float] = Field(default_factory=dict)
    MIN_NOTIONAL_USDT: float = 100.0

    # --- Risk ---
    DAILY_MAX_LOSS_USDT: float = 100.0
    KILL_SWITCH_CLOSE_POSITIONS: bool = True
    MAX_TRADES_DAILY: int = 20
    MAX_OPEN_POSITIONS: int = 3
    MAX_WEEKLY_DRAWDOWN_PCT: float = 0.0  # 0.0 = disabled
    MAX_MONTHLY_DRAWDOWN_PCT: float = 0.0  # 0.0 = disabled
    ACCOUNT_RISK_PCT: float = 1.0  # Risk 1% of equity per trade

    # --- Firebase / Push Notifications ---
    FIREBASE_SERVER_KEY: str = ""  # Legacy FCM (optional, deprecated)
    FIREBASE_SERVICE_ACCOUNT_PATH: str = ""  # Firebase Admin SDK service account JSON path
    TELEGRAM_BOT_TOKEN: str = ""  # Telegram bot token for notifications
    TELEGRAM_BOT_USERNAME: str = ""  # Telegram bot username

    # --- ML Admin / Monitoring ---
    # Uses the same env keys as bot-backend so admin reads the existing source of truth.
    ML_ENABLED: bool = False
    ML_SHADOW_MODE: bool = False
    ML_MODEL_PATH: str = ""
    ML_ENCODERS_PATH: str = ""
    ML_METADATA_PATH: str = ""
    ML_SCORE_THRESHOLD: float = 0.30
    ML_HARD_BLOCK_FLOOR: float = 0.0
    ML_LOG_DIR: str = "models/logs"

    @field_validator("TRADE_SYMBOLS", mode="before")
    @classmethod
    def parse_trade_symbols(cls, v: Any) -> List[str]:
        return _parse_list(v)

    @field_validator("LIVE_SYMBOLS", mode="before")
    @classmethod
    def parse_live_symbols(cls, v: Any) -> List[str]:
        return _parse_list(v)

    @field_validator("SYMBOL_LEVERAGE_MAP", mode="before")
    @classmethod
    def parse_leverage_map(cls, v: Any) -> Dict[str, int]:
        return _parse_kv_int(v)

    @field_validator("SYMBOL_USDT_MAP", mode="before")
    @classmethod
    def parse_usdt_map(cls, v: Any) -> Dict[str, float]:
        return _parse_kv_float(v)

    def model_post_init(self, __context: Any) -> None:
        # Normalize env
        self.BINANCE_ENV = (self.BINANCE_ENV or "mainnet").lower().strip()
        self.BYBIT_ENV = (self.BYBIT_ENV or "mainnet").lower().strip()
        self.BINGX_ENV = (self.BINGX_ENV or "mainnet").lower().strip()
        self.EXECUTION_MODE = (self.EXECUTION_MODE or "paper").lower().strip()
        self.BROKER_ID = (self.BROKER_ID or "binance_futures").lower().strip()

        # Default LIVE_SYMBOLS to TRADE_SYMBOLS if empty
        if not self.LIVE_SYMBOLS:
            self.LIVE_SYMBOLS = list(self.TRADE_SYMBOLS)

        # Keep base URL consistent with BINANCE_ENV unless user explicitly overrides
        if self.BINANCE_ENV == "testnet":
            # Binance Futures Testnet URL
            if self.BINANCE_FAPI_BASE_URL.strip() == "https://fapi.binance.com":
                self.BINANCE_FAPI_BASE_URL = "https://testnet.binancefuture.com"
        
        # Bybit Base URL Defaults
        if not self.BYBIT_BASE_URL:
            if self.BYBIT_ENV == "testnet":
                self.BYBIT_BASE_URL = "https://api-testnet.bybit.com"
            else:
                self.BYBIT_BASE_URL = "https://api.bybit.com"

        # BingX Base URL Defaults
        if not self.BINGX_BASE_URL:
            if self.BINGX_ENV in ("testnet", "demo"):
                self.BINGX_BASE_URL = "https://open-api-vsw.bingx.com" # VSW is primarily for demo/test logic often
            else:
                self.BINGX_BASE_URL = "https://open-api.bingx.com"


        # ✅ ADDED: derive runner compat settings from existing config (fixes main.py usage)
        # DEFAULT_INTERVAL examples: "1m", "5m", "15m", "1h", "30s"
        try:
            s = (self.DEFAULT_INTERVAL or "").strip().lower()
            if len(s) >= 2:
                unit = s[-1]
                value = int(s[:-1])
                if unit == "s":
                    self.RUN_INTERVAL_SECONDS = value
                elif unit == "m":
                    self.RUN_INTERVAL_SECONDS = value * 60
                elif unit == "h":
                    self.RUN_INTERVAL_SECONDS = value * 60
                else:
                    # fallback
                    self.RUN_INTERVAL_SECONDS = 60
            else:
                self.RUN_INTERVAL_SECONDS = 60
        except Exception:
            self.RUN_INTERVAL_SECONDS = 60

        # Keep runner max symbols aligned with existing MAX_SYMBOLS
        self.RUN_MAX_SYMBOLS = int(self.MAX_SYMBOLS)

    def validate_runtime(self) -> List[str]:
        """
        Fail-fast validation. Returns warnings (non-fatal).
        Raises ValueError for fatal misconfiguration.
        """
        errors: List[str] = []
        warnings: List[str] = []

        if self.EXECUTION_MODE not in {"paper", "live"}:
            errors.append("EXECUTION_MODE must be 'paper' or 'live'.")

        # Broker-specific checks
        if "binance" in self.BROKER_ID:
            if self.BINANCE_ENV not in {"mainnet", "testnet"}:
                errors.append("BINANCE_ENV must be 'mainnet' or 'testnet'.")
            if self.EXECUTION_MODE == "live" and not self.BINANCE_API_KEY:
                 errors.append("BINANCE_API_KEY required for live execution with Binance.")
            if self.EXECUTION_MODE == "live" and not self.BINANCE_API_SECRET:
                 errors.append("BINANCE_API_SECRET required for live execution with Binance.")

        if "bybit" in self.BROKER_ID:
            if self.BYBIT_ENV not in {"mainnet", "testnet"}:
                errors.append("BYBIT_ENV must be 'mainnet' or 'testnet'.")
            if self.EXECUTION_MODE == "live" and not self.BYBIT_API_KEY:
                errors.append("BYBIT_API_KEY required for live execution with Bybit.")
            if self.EXECUTION_MODE == "live" and not self.BYBIT_API_SECRET:
                errors.append("BYBIT_API_SECRET required for live execution with Bybit.")

        if "bingx" in self.BROKER_ID:
             if self.BINGX_ENV not in {"mainnet", "testnet", "demo"}:
                errors.append("BINGX_ENV must be 'mainnet', 'testnet' or 'demo'.")
             if self.EXECUTION_MODE == "live" and not self.BINGX_API_KEY:
                errors.append("BINGX_API_KEY required for live execution with BingX.")

        # Symbols sanity
        if not self.TRADE_SYMBOLS:
            warnings.append("TRADE_SYMBOLS is empty. Bot will have nothing to trade.")

        if self.LIVE_SYMBOLS:
            missing = set(self.LIVE_SYMBOLS) - set(self.TRADE_SYMBOLS)
            if missing:
                errors.append(
                    f"LIVE_SYMBOLS contains symbols not in TRADE_SYMBOLS: {sorted(missing)}"
                )

        # Limits sanity
        if self.MAX_SYMBOLS <= 0:
            errors.append("MAX_SYMBOLS must be > 0.")

        if self.TRADE_USDT_PER_ORDER <= 0 and not self.SYMBOL_USDT_MAP:
            errors.append(
                "TRADE_USDT_PER_ORDER must be > 0 (or provide SYMBOL_USDT_MAP)."
            )

        # Notional sanity (warning because exchanges may vary)
        if self.TRADE_USDT_PER_ORDER > 0 and self.MIN_NOTIONAL_USDT > 0:
            if (
                self.TRADE_USDT_PER_ORDER < self.MIN_NOTIONAL_USDT
                and not self.SYMBOL_USDT_MAP
            ):
                warnings.append(
                    f"TRADE_USDT_PER_ORDER ({self.TRADE_USDT_PER_ORDER}) is below MIN_NOTIONAL_USDT "
                    f"({self.MIN_NOTIONAL_USDT}). Orders may be rejected (Binance min is usually 20)."
                )
            
            if self.TRADE_USDT_PER_ORDER < 20.0:
                 warnings.append(
                    f"TRADE_USDT_PER_ORDER ({self.TRADE_USDT_PER_ORDER}) is likely too small. "
                    "Binance Futures minimum is typically 20 USDT for many pairs."
                )

        # Risk sanity
        if self.DAILY_MAX_LOSS_USDT < 0:
            errors.append("DAILY_MAX_LOSS_USDT must be >= 0.")

        if self.STOP_LOSS_PCT <= 0:
            errors.append("STOP_LOSS_PCT must be > 0.")
        if self.TAKE_PROFIT_PCT <= 0:
            errors.append("TAKE_PROFIT_PCT must be > 0.")

        # Leverage sanity
        if self.DEFAULT_LEVERAGE < 1:
            errors.append("DEFAULT_LEVERAGE must be >= 1.")
        if self.MIN_LEVERAGE < 1:
            errors.append("MIN_LEVERAGE must be >= 1.")
        if self.MIN_LEVERAGE > self.DEFAULT_LEVERAGE:
            warnings.append(
                "MIN_LEVERAGE is greater than DEFAULT_LEVERAGE; check if this is intended."
            )

        # Safety: mismatch guard
        if "binance" in self.BROKER_ID and (
            self.BINANCE_FAPI_BASE_URL.strip() == "https://fapi.binance.com"
            and self.BINANCE_ENV != "mainnet"
        ):
            errors.append(
                "BINANCE_ENV mismatch: base URL is mainnet but BINANCE_ENV is not 'mainnet'."
            )
        
        # Bybit Safety
        if "bybit" in self.BROKER_ID and (
             self.BYBIT_BASE_URL.strip() == "https://api.bybit.com"
             and self.BYBIT_ENV != "mainnet"
        ):
            errors.append("BYBIT_ENV mismatch: base URL is mainnet but BYBIT_ENV is not 'mainnet'.")

        # Safety warning for real money
        if self.EXECUTION_MODE == "live":
             if "binance" in self.BROKER_ID and self.BINANCE_ENV == "mainnet":
                warnings.append(
                    "EXECUTION_MODE=live with BINANCE_ENV=mainnet will trade REAL money. "
                    "Confirm your broker settings."
                )
             if "bybit" in self.BROKER_ID and self.BYBIT_ENV == "mainnet":
                 warnings.append(
                    "EXECUTION_MODE=live with BYBIT_ENV=mainnet will trade REAL money on Bybit."
                )

        if errors:
            msg = "Config validation failed:\n" + "\n".join([f"- {e}" for e in errors])
            raise ValueError(msg)

        return warnings


# Pydantic v2 + postponed annotations safety
Settings.model_rebuild()
settings = Settings()
