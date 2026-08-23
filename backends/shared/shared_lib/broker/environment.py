"""
Single authoritative source for broker environment definitions and base URL mapping.

Rules:
  1. BrokerEnvironment is the ONLY enum allowed in the system for broker environments.
  2. All base URL decisions must go through resolve_base_url().
  3. No other module may hard-code "https://demo-fapi.binance.com" or similar.
  4. To add a new broker: add entries to BASE_URLS only.
"""
from __future__ import annotations
from enum import Enum
from typing import Dict, Tuple


class BrokerEnvironment(str, Enum):
    LIVE = "live"
    DEMO = "demo"


# ── Canonical base URL table ─────────────────────────────────────────────────
# Key: (broker_id_lowercase, BrokerEnvironment)
# Value: base URL (no trailing slash)
#
# Adding a new broker:  add two entries here and nowhere else.
_BASE_URLS: Dict[Tuple[str, BrokerEnvironment], str] = {
    ("binance",  BrokerEnvironment.LIVE): "https://fapi.binance.com",
    ("binance",  BrokerEnvironment.DEMO): "https://demo-fapi.binance.com",
    ("bybit",    BrokerEnvironment.LIVE): "https://api.bybit.com",
    ("bybit",    BrokerEnvironment.DEMO): "https://api-testnet.bybit.com",
    ("bingx",    BrokerEnvironment.LIVE): "https://open-api.bingx.com",
    ("bingx",    BrokerEnvironment.DEMO): "https://open-api-vst.bingx.com",
    ("oanda",    BrokerEnvironment.LIVE): "https://api-fxtrade.oanda.com",
    ("oanda",    BrokerEnvironment.DEMO): "https://api-fxpractice.oanda.com",
}

# Aliases accepted on input → normalized BrokerEnvironment
_NORMALIZE_MAP: Dict[str, BrokerEnvironment] = {
    "live":         BrokerEnvironment.LIVE,
    "mainnet":      BrokerEnvironment.LIVE,
    "production":   BrokerEnvironment.LIVE,
    "demo":         BrokerEnvironment.DEMO,
    "testnet":      BrokerEnvironment.DEMO,
    "sandbox":      BrokerEnvironment.DEMO,
    "paper":        BrokerEnvironment.DEMO,
    "test":         BrokerEnvironment.DEMO,
}


def normalize_environment(raw: str) -> BrokerEnvironment:
    """
    Normalize any input string to a BrokerEnvironment enum value.

    Raises ValueError if the string is not recognized so callers can
    surface it as broker_env_mismatch rather than silently defaulting.
    """
    if isinstance(raw, BrokerEnvironment):
        return raw
    normalized = _NORMALIZE_MAP.get(str(raw).strip().lower())
    if normalized is None:
        known = sorted(_NORMALIZE_MAP.keys())
        raise ValueError(
            f"Unrecognized broker environment {raw!r}. Accepted values: {known}"
        )
    return normalized


def resolve_base_url(broker_id: str, environment: BrokerEnvironment) -> str:
    """
    Return the canonical base URL for a (broker_id, environment) pair.

    Raises ValueError for unknown broker/environment combinations so the
    caller can surface broker_env_mismatch rather than silently using None.
    """
    key = (broker_id.strip().lower(), environment)
    url = _BASE_URLS.get(key)
    if url is None:
        raise ValueError(
            f"No base URL configured for broker={broker_id!r}, "
            f"environment={environment!r}. "
            f"Registered combinations: {sorted(str(k) for k in _BASE_URLS)}"
        )
    return url


def environments_match(a: str, b: str) -> bool:
    """Return True if two raw environment strings resolve to the same enum."""
    try:
        return normalize_environment(a) == normalize_environment(b)
    except ValueError:
        return False
