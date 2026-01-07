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

    # --- Exchange / API ---
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""

    # If you use demo/testnet, set BINANCE_ENV=testnet (recommended)
    BINANCE_ENV: str = "mainnet"  # mainnet/testnet
    BINANCE_FAPI_BASE_URL: str = "https://fapi.binance.com"
    BINANCE_RECV_WINDOW: int = 5000

    # --- Symbols / universe ---
    TRADE_SYMBOLS: List[str] = Field(default_factory=list)
    LIVE_SYMBOLS: List[str] = Field(default_factory=list)
    DEFAULT_INTERVAL: str = "1m"
    MAX_SYMBOLS: int = 10

    # ✅ ADDED (to fix AttributeError in main.py)
    RUN_INTERVAL_SECONDS: int = 60
    RUN_MAX_SYMBOLS: int = 10

    # --- Execution ---
    EXECUTION_MODE: str = "paper"  # paper/live
    MAX_LIVE_TRADES_PER_CYCLE: int = 1

    # --- Strategy controls ---
    TRADE_MODE: str = "flip"
    COOLDOWN_SECONDS: int = 60
    MAX_ADDS_PER_POSITION: int = 2

    STOP_LOSS_PCT: float = 1.0
    TAKE_PROFIT_PCT: float = 2.0
    SL_COOLDOWN_SECONDS: int = 60

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
        self.EXECUTION_MODE = (self.EXECUTION_MODE or "paper").lower().strip()

        # Default LIVE_SYMBOLS to TRADE_SYMBOLS if empty
        if not self.LIVE_SYMBOLS:
            self.LIVE_SYMBOLS = list(self.TRADE_SYMBOLS)

        # Keep base URL consistent with BINANCE_ENV unless user explicitly overrides
        if self.BINANCE_ENV == "testnet":
            # Binance Futures Testnet URL
            if self.BINANCE_FAPI_BASE_URL.strip() == "https://fapi.binance.com":
                self.BINANCE_FAPI_BASE_URL = "https://testnet.binancefuture.com"

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

        if self.BINANCE_ENV not in {"mainnet", "testnet"}:
            errors.append("BINANCE_ENV must be 'mainnet' or 'testnet'.")

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
                    f"({self.MIN_NOTIONAL_USDT}). Orders may be rejected."
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
        if (
            self.BINANCE_FAPI_BASE_URL.strip() == "https://fapi.binance.com"
            and self.BINANCE_ENV != "mainnet"
        ):
            errors.append(
                "BINANCE_ENV mismatch: base URL is mainnet but BINANCE_ENV is not 'mainnet'."
            )

        # Safety warning for real money
        if self.EXECUTION_MODE == "live" and self.BINANCE_ENV == "mainnet":
            warnings.append(
                "EXECUTION_MODE=live with BINANCE_ENV=mainnet will trade REAL money. "
                "If you meant demo/testnet, set BINANCE_ENV=testnet (recommended)."
            )

        if errors:
            msg = "Config validation failed:\n" + "\n".join([f"- {e}" for e in errors])
            raise ValueError(msg)

        return warnings


# Pydantic v2 + postponed annotations safety
Settings.model_rebuild()
settings = Settings()
