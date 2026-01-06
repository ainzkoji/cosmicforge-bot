# app/core/config.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


"""""
def execute_signal(self, symbol: str, exec_signal: str, trade_usdt: float):
    # ✅ Global Kill-Switch: block ALL opens/adds at execution layer
    if getattr(self, "daily", None) and self.daily.kill:
        if exec_signal in ("OPEN_LONG", "OPEN_SHORT", "ADD_LONG", "ADD_SHORT"):
            return {
                "ok": False,
                "action": "BLOCKED_KILL_SWITCH",
                "symbol": symbol,
                "exec_signal": exec_signal,
            }
""" ""


def _parse_list(v: Any) -> List[str]:
    """
    Accepts:
      - list: ["BTCUSDT","ETHUSDT"]
      - csv str: "BTCUSDT,ETHUSDT"
      - json str: '["BTCUSDT","ETHUSDT"]'
    Returns a de-duplicated, uppercased list preserving order.
    """
    if v is None:
        return []

    if isinstance(v, list):
        items = v
    else:
        s = str(v).strip()
        if not s:
            return []
        # If it's JSON-ish, try JSON first (because enable_decoding=False)
        if s.startswith("["):
            try:
                items = json.loads(s)
            except Exception:
                # fall back to csv
                items = s.split(",")
        else:
            items = s.split(",")

    out: List[str] = []
    seen = set()
    for x in items:
        x = str(x).strip().upper()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _parse_kv_int(v: Any) -> Dict[str, int]:
    """
    Accepts:
      - dict: {"BTCUSDT": 5}
      - csv:  "BTCUSDT:5,ETHUSDT:10"
      - json: '{"BTCUSDT":5,"ETHUSDT":10}'
    """
    if v is None:
        return {}
    if isinstance(v, dict):
        return {str(k).strip().upper(): int(val) for k, val in v.items()}

    s = str(v).strip()
    if not s:
        return {}

    if s.startswith("{"):
        try:
            d = json.loads(s)
            return {str(k).strip().upper(): int(val) for k, val in d.items()}
        except Exception:
            pass

    out: Dict[str, int] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        k, val = part.split(":", 1)
        out[k.strip().upper()] = int(val.strip())
    return out


def _parse_kv_float(v: Any) -> Dict[str, float]:
    """
    Accepts:
      - dict: {"XRPUSDT": 100}
      - csv:  "XRPUSDT:100,ADAUSDT:100"
      - json: '{"XRPUSDT":100,"ADAUSDT":100}'
    """
    if v is None:
        return {}
    if isinstance(v, dict):
        return {str(k).strip().upper(): float(val) for k, val in v.items()}

    s = str(v).strip()
    if not s:
        return {}

    if s.startswith("{"):
        try:
            d = json.loads(s)
            return {str(k).strip().upper(): float(val) for k, val in d.items()}
        except Exception:
            pass

    out: Dict[str, float] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        k, val = part.split(":", 1)
        out[k.strip().upper()] = float(val.strip())
    return out


class Settings(BaseSettings):
    # ✅ Key fix:
    # enable_decoding=False prevents pydantic-settings from json.loads() on List/Dict env values.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",  # ✅ prevents BINANCE_API_KEY etc from crashing if not defined
        enable_decoding=False,  # ✅ IMPORTANT: stops auto json.loads for TRADE_SYMBOLS/LIVE_SYMBOLS
    )

    # --- Exchange / API ---
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""
    BINANCE_FAPI_BASE_URL: str = "https://fapi.binance.com"
    BINANCE_RECV_WINDOW: int = 5000

    # --- Symbols / universe ---
    TRADE_SYMBOLS: List[str] = Field(default_factory=list)
    LIVE_SYMBOLS: List[str] = Field(default_factory=list)
    DEFAULT_INTERVAL: str = "1m"
    MAX_SYMBOLS: int = 10

    # --- Execution ---
    EXECUTION_MODE: str = "paper"  # paper/live
    MAX_LIVE_TRADES_PER_CYCLE: int = 1

    # --- Leverage ---
    DEFAULT_LEVERAGE: int = 5
    MIN_LEVERAGE: int = 5
    SYMBOL_LEVERAGE_MAP: Dict[str, int] = Field(default_factory=dict)

    # --- Sizing ---
    TRADE_USDT_PER_ORDER: float = 100.0
    SYMBOL_USDT_MAP: Dict[str, float] = Field(default_factory=dict)
    MIN_NOTIONAL_USDT: float = 100.0

    # --- Risk ---
    DAILY_MAX_LOSS_USDT: float = 100
    KILL_SWITCH_CLOSE_POSITIONS: bool = True

    # --- Strategy controls ---
    TRADE_MODE: str = "flip"
    COOLDOWN_SECONDS: int = 60
    MAX_ADDS_PER_POSITION: int = 2
    STOP_LOSS_PCT: float = 0.7
    TAKE_PROFIT_PCT: float = 1.2

    RUN_INTERVAL_SECONDS: int = 60
    RUN_MAX_SYMBOLS: int = 10

    # After a STOP LOSS, block new entries on that symbol for this many minutes
    SL_COOLDOWN_SECONDS: int = int(os.getenv("SL_COOLDOWN_SECONDS", "3600"))

    # After cooldown expires, require N consecutive identical signals before re-entry
    REENTRY_CONFIRMATION_COUNT: int = 2

    # ✅ Parse env strings into correct types
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
        # default LIVE_SYMBOLS to TRADE_SYMBOLS if empty
        if not self.LIVE_SYMBOLS:
            self.LIVE_SYMBOLS = list(self.TRADE_SYMBOLS)

        self.EXECUTION_MODE = (self.EXECUTION_MODE or "paper").lower()

        # if self.FORCE_SIGNAL is not None:
        #    self.FORCE_SIGNAL = self.FORCE_SIGNAL.strip().lower() or None

        # ✅ instance-safe defaulting
        if not self.RUN_MAX_SYMBOLS:
            self.RUN_MAX_SYMBOLS = self.MAX_SYMBOLS


settings = Settings()
