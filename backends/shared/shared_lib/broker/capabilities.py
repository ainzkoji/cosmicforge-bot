"""Declared broker capability profiles and the execution-readiness gate.

A capability is a fact about what THIS codebase's adapter for a broker has
implemented and proven -- never an assumption about the venue made from its
name. Each entry carries an explicit state:

* ``SUPPORTED``             implemented and validated against the venue
* ``UNVALIDATED``           implemented, not yet proven against the venue
* ``UNSUPPORTED``           not implemented by the platform adapter
* ``ACCOUNT_RESTRICTED``    implemented, but this account/key cannot use it
* ``VENUE_API_UNAVAILABLE`` the venue does not expose it through its API

The declared profile is the ceiling. ``for_account()`` narrows it using the
permission evidence persisted for the credential version (a key without
TRADE cannot trade, a key without INTERNAL_TRANSFER cannot transfer); it
never widens it.

Withdrawals are not a capability of this platform, for any broker, ever:
``WITHDRAWALS_SUPPORTED_BY_PLATFORM`` is a constant ``False`` and there is
no capability key for it.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Dict, Iterable, Mapping, Optional, Tuple

from shared_lib.broker.environment import BrokerEnvironment, normalize_environment

#: The platform never moves funds out of a broker account.
WITHDRAWALS_SUPPORTED_BY_PLATFORM = False

REASON_EXECUTION_CAPABILITY_INCOMPLETE = "BROKER_EXECUTION_CAPABILITY_INCOMPLETE"
REASON_EXECUTION_UNVALIDATED_LIVE = "BROKER_EXECUTION_UNVALIDATED_FOR_LIVE"
REASON_UNKNOWN_BROKER = "BROKER_UNKNOWN"
REASON_NOT_IMPLEMENTED = "PLATFORM_ADAPTER_NOT_IMPLEMENTED"
REASON_NOT_VALIDATED = "PLATFORM_ADAPTER_NOT_VALIDATED"
REASON_VENUE_API = "VENUE_API_NOT_SUPPORTED"
REASON_PERMISSION_MISSING = "API_KEY_PERMISSION_MISSING"
REASON_WITHDRAW_PERMISSION_PRESENT = "API_KEY_WITHDRAW_PERMISSION_PRESENT"
REASON_PRODUCT_FROM_DISCOVERY = "PRODUCT_AVAILABILITY_RESOLVED_BY_DISCOVERY"


class CapabilityState(str, Enum):
    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    UNVALIDATED = "UNVALIDATED"
    ACCOUNT_RESTRICTED = "ACCOUNT_RESTRICTED"
    VENUE_API_UNAVAILABLE = "VENUE_API_UNAVAILABLE"


class Capability(str, Enum):
    MARKET_DATA = "market_data"
    INSTRUMENT_DISCOVERY = "instrument_discovery"
    SPOT_TRADING = "spot_trading"
    CRYPTO_PERPETUALS = "crypto_perpetuals"
    FX_PERPETUALS = "fx_perpetuals"
    TRADFI = "tradfi"
    ACCOUNT_BALANCE = "account_balance"
    POSITIONS = "positions"
    ORDERS = "orders"
    PROTECTION_ORDERS = "protection_orders"
    ORDER_LOOKUP = "order_lookup"
    FILLS = "fills"
    PERMISSION_INSPECTION = "permission_inspection"
    INTERNAL_TRANSFER = "internal_transfer"
    TRANSFER_HISTORY = "transfer_history"
    SUBACCOUNTS = "subaccounts"
    DEMO_ENVIRONMENT = "demo_environment"


#: What the executor needs from a broker before a bot may trade on it.
EXECUTION_REQUIRED: Tuple[Capability, ...] = (
    Capability.MARKET_DATA,
    Capability.INSTRUMENT_DISCOVERY,
    Capability.ACCOUNT_BALANCE,
    Capability.POSITIONS,
    Capability.ORDERS,
    Capability.ORDER_LOOKUP,
    Capability.FILLS,
    Capability.PROTECTION_ORDERS,
)

#: Capabilities an API-key permission can narrow (capability -> permission needed).
_PERMISSION_GATED = {
    Capability.ACCOUNT_BALANCE: "READ_ACCOUNT",
    Capability.POSITIONS: "READ_POSITIONS",
    Capability.ORDER_LOOKUP: "READ_ORDERS",
    Capability.FILLS: "READ_ORDERS",
    Capability.ORDERS: "TRADE",
    Capability.PROTECTION_ORDERS: "TRADE",
    Capability.INTERNAL_TRANSFER: "INTERNAL_TRANSFER",
    Capability.TRANSFER_HISTORY: "READ_ACCOUNT",
}

_USABLE_ON_LIVE = {CapabilityState.SUPPORTED}
_USABLE_ON_DEMO = {CapabilityState.SUPPORTED, CapabilityState.UNVALIDATED}


@dataclass(frozen=True)
class CapabilityEntry:
    state: CapabilityState
    reason_code: Optional[str] = None
    detail: str = ""

    def to_dict(self) -> dict:
        return {"state": self.state.value, "reason_code": self.reason_code, "detail": self.detail}


def _e(state: CapabilityState, reason: Optional[str] = None, detail: str = "") -> CapabilityEntry:
    return CapabilityEntry(state, reason, detail)


S = CapabilityState.SUPPORTED
U = CapabilityState.UNSUPPORTED
V = CapabilityState.UNVALIDATED
X = CapabilityState.VENUE_API_UNAVAILABLE


@dataclass(frozen=True)
class ExecutionReadiness:
    permitted: bool
    reason_code: Optional[str]
    missing: Tuple[str, ...] = ()
    detail: str = ""

    def to_dict(self) -> dict:
        return {"permitted": self.permitted, "reason_code": self.reason_code,
                "missing": list(self.missing), "detail": self.detail}


@dataclass(frozen=True)
class BrokerCapabilityProfile:
    broker: str
    entries: Mapping[Capability, CapabilityEntry]
    source: str = "DECLARED"  # DECLARED | ACCOUNT_NARROWED
    version: str = "broker-capabilities-v1"

    @property
    def withdrawals_supported_by_platform(self) -> bool:
        return WITHDRAWALS_SUPPORTED_BY_PLATFORM

    def entry(self, cap: Capability) -> CapabilityEntry:
        return self.entries.get(cap) or _e(U, REASON_NOT_IMPLEMENTED, "no declaration")

    def state(self, cap: Capability) -> CapabilityState:
        return self.entry(cap).state

    def usable(self, cap: Capability, environment) -> bool:
        env = normalize_environment(environment)
        allowed = _USABLE_ON_LIVE if env == BrokerEnvironment.LIVE else _USABLE_ON_DEMO
        return self.state(cap) in allowed

    def execution_readiness(self, environment, product: Capability = Capability.CRYPTO_PERPETUALS
                            ) -> ExecutionReadiness:
        """Can a bot place orders for ``product`` on this broker in ``environment``?

        LIVE requires every EXECUTION_REQUIRED capability (and the product)
        to be SUPPORTED. DEMO/TESTNET also accept UNVALIDATED: demo is where
        an unvalidated adapter earns its validation, with no user capital.
        """
        env = normalize_environment(environment)
        needed = tuple(EXECUTION_REQUIRED) + (product,)
        missing = tuple(c.value for c in needed if not self.usable(c, env))
        if not missing:
            return ExecutionReadiness(True, None)
        unvalidated_only = all(self.state(Capability(m)) == V for m in missing)
        if env == BrokerEnvironment.LIVE and unvalidated_only:
            return ExecutionReadiness(False, REASON_EXECUTION_UNVALIDATED_LIVE, missing,
                                      f"{self.broker}: adapter not validated for live execution")
        return ExecutionReadiness(False, REASON_EXECUTION_CAPABILITY_INCOMPLETE, missing,
                                  f"{self.broker}: missing {', '.join(missing)}")

    def for_account(self, permissions: Optional[Mapping[str, object]]) -> "BrokerCapabilityProfile":
        """Narrow (never widen) by the key's persisted permission evidence.

        ``permissions`` maps permission name -> True/False/None (None =
        unknown). Unknown evidence does not narrow trading capabilities (the
        executor still fails closed on the first real denial) but DOES
        narrow INTERNAL_TRANSFER: moving money requires positive evidence.
        """
        if not permissions:
            permissions = {}
        out: Dict[Capability, CapabilityEntry] = dict(self.entries)
        for cap, perm in _PERMISSION_GATED.items():
            cur = self.entry(cap)
            if cur.state not in (S, V):
                continue
            granted = permissions.get(perm)
            if granted is False or (cap == Capability.INTERNAL_TRANSFER and granted is not True):
                out[cap] = _e(CapabilityState.ACCOUNT_RESTRICTED, REASON_PERMISSION_MISSING,
                              f"API key lacks {perm}" if granted is False else f"{perm} not evidenced")
        return replace(self, entries=out, source="ACCOUNT_NARROWED")

    def to_dict(self) -> dict:
        return {
            "broker": self.broker,
            "source": self.source,
            "version": self.version,
            "withdrawals_supported_by_platform": WITHDRAWALS_SUPPORTED_BY_PLATFORM,
            "capabilities": {c.value: self.entry(c).to_dict() for c in Capability},
        }


def _profile(broker: str, entries: Mapping[Capability, CapabilityEntry]) -> BrokerCapabilityProfile:
    return BrokerCapabilityProfile(broker=broker, entries=dict(entries))


# ── Declared profiles ────────────────────────────────────────────────────────
# Binance USD-M futures is the validated production path (demo-fapi verified;
# see trading_intelligence/venue/registry.py). Bybit / BingX clients do not
# yet satisfy the executor contract (no order lookup / fills / protection /
# instrument discovery) -> UNSUPPORTED, so bots on them refuse to start.

_BINANCE = _profile("binance", {
    Capability.MARKET_DATA: _e(S),
    Capability.INSTRUMENT_DISCOVERY: _e(S),
    Capability.SPOT_TRADING: _e(U, REASON_NOT_IMPLEMENTED, "USD-M futures only"),
    Capability.CRYPTO_PERPETUALS: _e(S),
    Capability.FX_PERPETUALS: _e(V, REASON_PRODUCT_FROM_DISCOVERY,
                                 "no FX product assumed; eligibility comes from instrument discovery"),
    Capability.TRADFI: _e(V, REASON_PRODUCT_FROM_DISCOVERY,
                          "TradFi perpetuals only if discovered in exchangeInfo"),
    Capability.ACCOUNT_BALANCE: _e(S),
    Capability.POSITIONS: _e(S),
    Capability.ORDERS: _e(S),
    Capability.PROTECTION_ORDERS: _e(S),
    Capability.ORDER_LOOKUP: _e(S),
    Capability.FILLS: _e(S),
    Capability.PERMISSION_INSPECTION: _e(V, REASON_NOT_VALIDATED, "GET /sapi/v1/account/apiRestrictions"),
    Capability.INTERNAL_TRANSFER: _e(V, REASON_NOT_VALIDATED, "POST /sapi/v1/asset/transfer (universal transfer)"),
    Capability.TRANSFER_HISTORY: _e(V, REASON_NOT_VALIDATED, "GET /sapi/v1/asset/transfer"),
    Capability.SUBACCOUNTS: _e(U, REASON_NOT_IMPLEMENTED),
    Capability.DEMO_ENVIRONMENT: _e(S),
})

_BYBIT = _profile("bybit", {
    Capability.MARKET_DATA: _e(V, REASON_NOT_VALIDATED),
    Capability.INSTRUMENT_DISCOVERY: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.SPOT_TRADING: _e(U, REASON_NOT_IMPLEMENTED),
    Capability.CRYPTO_PERPETUALS: _e(V, REASON_NOT_VALIDATED, "linear USDT/USDC perpetuals"),
    Capability.FX_PERPETUALS: _e(V, REASON_PRODUCT_FROM_DISCOVERY,
                                 "FX perpetuals only where V5 instruments-info lists them"),
    Capability.TRADFI: _e(X, REASON_VENUE_API,
                          "Bybit TradFi/MT5 is a separate platform not reachable through V5"),
    Capability.ACCOUNT_BALANCE: _e(V, REASON_NOT_VALIDATED),
    Capability.POSITIONS: _e(V, REASON_NOT_VALIDATED),
    Capability.ORDERS: _e(V, REASON_NOT_VALIDATED),
    Capability.PROTECTION_ORDERS: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.ORDER_LOOKUP: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.FILLS: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.PERMISSION_INSPECTION: _e(V, REASON_NOT_VALIDATED, "GET /v5/user/query-api"),
    Capability.INTERNAL_TRANSFER: _e(V, REASON_NOT_VALIDATED, "POST /v5/asset/transfer/inter-transfer"),
    Capability.TRANSFER_HISTORY: _e(V, REASON_NOT_VALIDATED, "GET /v5/asset/transfer/query-inter-transfer-list"),
    Capability.SUBACCOUNTS: _e(U, REASON_NOT_IMPLEMENTED),
    Capability.DEMO_ENVIRONMENT: _e(S),
})

_BINGX = _profile("bingx", {
    Capability.MARKET_DATA: _e(V, REASON_NOT_VALIDATED),
    Capability.INSTRUMENT_DISCOVERY: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.SPOT_TRADING: _e(U, REASON_NOT_IMPLEMENTED),
    Capability.CRYPTO_PERPETUALS: _e(V, REASON_NOT_VALIDATED, "USDT-M perpetual swap"),
    Capability.FX_PERPETUALS: _e(X, REASON_VENUE_API,
                                 "no official API execution path for BingX TradFi/FX is implemented"),
    Capability.TRADFI: _e(X, REASON_VENUE_API,
                          "BingX TradFi is not exposed through the supported swap API"),
    Capability.ACCOUNT_BALANCE: _e(V, REASON_NOT_VALIDATED),
    Capability.POSITIONS: _e(V, REASON_NOT_VALIDATED),
    Capability.ORDERS: _e(V, REASON_NOT_VALIDATED),
    Capability.PROTECTION_ORDERS: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.ORDER_LOOKUP: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.FILLS: _e(U, REASON_NOT_IMPLEMENTED, "executor contract method missing"),
    Capability.PERMISSION_INSPECTION: _e(U, REASON_NOT_IMPLEMENTED,
                                         "no verified BingX API-key permission endpoint"),
    Capability.INTERNAL_TRANSFER: _e(V, REASON_NOT_VALIDATED, "asset transfer endpoint"),
    Capability.TRANSFER_HISTORY: _e(V, REASON_NOT_VALIDATED),
    Capability.SUBACCOUNTS: _e(U, REASON_NOT_IMPLEMENTED),
    Capability.DEMO_ENVIRONMENT: _e(S, detail="VST demo host"),
})

DECLARED_PROFILES: Mapping[str, BrokerCapabilityProfile] = {
    "binance": _BINANCE,
    "bybit": _BYBIT,
    "bingx": _BINGX,
}


def _unknown(broker: str) -> BrokerCapabilityProfile:
    return BrokerCapabilityProfile(
        broker=broker,
        entries={c: _e(U, REASON_UNKNOWN_BROKER, "no platform adapter") for c in Capability},
    )


def declared_profile(broker: str) -> BrokerCapabilityProfile:
    """The declared profile, or an all-UNSUPPORTED one for an unknown broker."""
    key = str(broker or "").strip().lower()
    return DECLARED_PROFILES.get(key) or _unknown(key or "unknown")


def execution_readiness(broker: str, environment, *, permissions: Optional[Mapping[str, object]] = None,
                        product: Capability = Capability.CRYPTO_PERPETUALS) -> ExecutionReadiness:
    try:
        env = normalize_environment(environment)
    except ValueError:
        return ExecutionReadiness(False, REASON_EXECUTION_CAPABILITY_INCOMPLETE, (),
                                  f"unrecognized environment {environment!r}")
    if permissions is not None and permissions.get("WITHDRAW") is True:
        # A withdrawal-capable key is never used for automated trading.
        return ExecutionReadiness(False, REASON_WITHDRAW_PERMISSION_PRESENT, ("WITHDRAW",),
                                  "API key has withdrawal permission; replace it with a trade-only key")
    profile = declared_profile(broker)
    if permissions is not None:
        profile = profile.for_account(permissions)
    return profile.execution_readiness(env, product)


def supported_brokers(cap: Capability, environment) -> Iterable[str]:
    return tuple(b for b, p in DECLARED_PROFILES.items() if p.usable(cap, environment))


__all__ = [
    "WITHDRAWALS_SUPPORTED_BY_PLATFORM",
    "REASON_EXECUTION_CAPABILITY_INCOMPLETE",
    "REASON_EXECUTION_UNVALIDATED_LIVE",
    "REASON_VENUE_API",
    "REASON_PERMISSION_MISSING",
    "REASON_WITHDRAW_PERMISSION_PRESENT",
    "CapabilityState",
    "Capability",
    "CapabilityEntry",
    "BrokerCapabilityProfile",
    "ExecutionReadiness",
    "EXECUTION_REQUIRED",
    "DECLARED_PROFILES",
    "declared_profile",
    "execution_readiness",
    "supported_brokers",
]
