"""Broker-account market capability engine (AUTO_ACTIVE_IF_ELIGIBLE per account).

For ONE connected broker account (never a global cache: every input is this
account's own permission evidence, discovered instruments, health and
transfer state) derive, per capability and per market family:

* what the venue lists            -> ``markets_available``
* what this account's API may trade -> ``markets_api_tradable``
* what CATI may trade             -> ``markets_cati_eligible``

Capabilities: CRYPTO_MARKET_DATA, CRYPTO_EXECUTION, FX_MARKET_DATA,
FX_EXECUTION, TRADFI_EXECUTION, INTERNAL_TRANSFER, UNIFIED_COLLATERAL,
FUNDING_DATA, ORDERBOOK_DATA, POSITION_MODE, MARGIN_MODE.

Each is ``ActivationDecision``-shaped (ACTIVE / BLOCKED / UNSUPPORTED plus
the first unmet reason) with a UI ``status`` from the fixed vocabulary
(AVAILABLE, ACTIVE, BLOCKED, UNSUPPORTED, DATA_NOT_READY,
CERTIFICATION_NOT_READY, ACCOUNT_NOT_ELIGIBLE, PERMISSION_MISSING,
VENUE_API_NOT_SUPPORTED, MARKET_CLOSED, RISK_BLOCKED, TRANSFER_PENDING,
RECONCILIATION_REQUIRED). Withdrawal permission is never required: a key that
HAS it is refused (``API_KEY_WITHDRAW_PERMISSION_PRESENT``).

Nothing is hard-coded per symbol: market counts come from discovery, the
product capability from the broker's declared profile narrowed by the key's
permission evidence. When the declared profile or discovery changes, the
state changes with it -- no switch to remember.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .model import ActivationDecision, ActivationState, Prerequisite, decide

FAMILIES = ("CRYPTO", "FX", "COMMODITIES", "STOCK", "INDEX")
TRADFI = ("COMMODITIES", "STOCK", "INDEX")
UI_STATUSES = ("AVAILABLE", "ACTIVE", "BLOCKED", "UNSUPPORTED", "DATA_NOT_READY", "CERTIFICATION_NOT_READY",
               "ACCOUNT_NOT_ELIGIBLE", "PERMISSION_MISSING", "VENUE_API_NOT_SUPPORTED", "MARKET_CLOSED",
               "RISK_BLOCKED", "TRANSFER_PENDING", "RECONCILIATION_REQUIRED")

_UI_BY_REASON = (
    ("WITHDRAW", "ACCOUNT_NOT_ELIGIBLE"),          # a withdraw-capable key is refused, not "missing" a permission
    ("API_KEY_PERMISSION_MISSING", "PERMISSION_MISSING"),
    ("PERMISSION", "PERMISSION_MISSING"),
    ("VENUE_API", "VENUE_API_NOT_SUPPORTED"),
    ("NO_DISCOVERED", "DATA_NOT_READY"),
    ("DISCOVERY", "DATA_NOT_READY"),
    ("CERTIF", "CERTIFICATION_NOT_READY"),
    ("GOVERNANCE", "CERTIFICATION_NOT_READY"),
    ("MARKET_CLOSED", "MARKET_CLOSED"),
    ("QUARANTIN", "RISK_BLOCKED"),
    ("CIRCUIT", "RISK_BLOCKED"),
    ("RISK", "RISK_BLOCKED"),
    ("TRANSFER_PENDING", "TRANSFER_PENDING"),
    ("RECONCILIATION", "RECONCILIATION_REQUIRED"),
    ("UNVALIDATED_FOR_LIVE", "ACCOUNT_NOT_ELIGIBLE"),
    ("ACCOUNT", "ACCOUNT_NOT_ELIGIBLE"),
)


def ui_status(decision: ActivationDecision, *, evidence_only: bool = False) -> str:
    if decision.state == ActivationState.ACTIVE:
        return "AVAILABLE" if evidence_only else "ACTIVE"
    if decision.state == ActivationState.UNSUPPORTED:
        return "VENUE_API_NOT_SUPPORTED" if "VENUE_API" in (decision.reason or "") else "UNSUPPORTED"
    r = decision.reason or ""
    for needle, status in _UI_BY_REASON:
        if needle in r:
            return status
    return "BLOCKED"


def _client_class(broker: str):
    b = str(broker or "").lower()
    try:
        if b == "binance":
            from app.exchange.binance.client import BinanceFuturesClient as C
        elif b == "bybit":
            from app.exchange.bybit.client import BybitClient as C
        elif b == "bingx":
            from app.exchange.bingx.client import BingXClient as C
        else:
            return None
        return C
    except Exception:
        return None


def _implements(broker: str, method: str) -> bool:
    cls = _client_class(broker)
    return bool(cls is not None and callable(getattr(cls, method, None)))


def _product_decision(name: str, *, broker: str, environment: str, permissions: Optional[Mapping[str, Any]],
                      product_cap: Any, instruments: Sequence[Any], health: Mapping[str, Any]) -> ActivationDecision:
    from shared_lib.broker.capabilities import CapabilityState, declared_profile, execution_readiness

    profile = declared_profile(broker)
    entry = profile.entry(product_cap)
    if entry.state in (CapabilityState.UNSUPPORTED, CapabilityState.VENUE_API_UNAVAILABLE):
        return decide(name, [Prerequisite("product_capability", False, entry.reason_code or "PRODUCT_UNSUPPORTED",
                                          detail=entry.detail, structural=True)], scope=broker)
    ready = execution_readiness(broker, environment, permissions=permissions, product=product_cap)
    tradable = [i for i in instruments if i.api_tradable]
    prereqs = [
        Prerequisite("instruments_discovered", bool(instruments), "NO_DISCOVERED_INSTRUMENTS"),
        Prerequisite("instrument_api_tradable", bool(tradable), "NO_API_TRADABLE_INSTRUMENT_NOW",
                     detail=f"{len(tradable)}/{len(instruments)}"),
        Prerequisite("execution_readiness", ready.permitted, ready.reason_code or "NOT_PERMITTED",
                     detail=",".join(ready.missing)),
    ]
    # account health facts gate execution when the caller could read them (a supplied None = unknown = blocks)
    for key, pname, reason, invert in (("quarantined", "account_not_quarantined", "ACCOUNT_QUARANTINED_RISK", True),
                                       ("circuit_breaker_active", "circuit_breaker_clear", "CIRCUIT_BREAKER_ACTIVE",
                                        True),
                                       ("reconciliation_healthy", "reconciliation_healthy", "RECONCILIATION_REQUIRED",
                                        False)):
        if key in health:
            v = health[key]
            prereqs.append(Prerequisite(pname, None if v is None else ((not v) if invert else bool(v)), reason))
    return decide(name, prereqs, scope=broker, authority="PLATFORM_EXECUTION",
                  detail={"declared_state": entry.state.value})


def _cati_eligible(family: str, broker: str, environment: str, db: Any, broker_account_id: Optional[str],
                   certified_scopes: Iterable[str]) -> ActivationDecision:
    from app.activation.cati import active_execution

    venue = {"binance": "binance_usdm", "bybit": "bybit_linear", "bingx": "bingx_swap"}.get(broker, broker)
    act = active_execution(db, environment=environment, venue=venue, broker_account_id=broker_account_id)
    scope_ok = any(s.startswith(f"{family}/{broker.upper()}/") for s in certified_scopes)
    return decide(f"CATI_{family}_EXECUTION", [
        Prerequisite("certified_scope", scope_ok, "CERTIFICATION_NOT_COMPLETE", detail=f"{family}/{broker.upper()}"),
        Prerequisite("cati_active_execution", act.active, act.reason or "CATI_EXECUTION_BLOCKED"),
    ], scope=f"{broker}:{environment}", authority=act.authority)


def account_market_status(*, broker: str, environment: str, permissions: Optional[Mapping[str, Any]],
                          instruments: Optional[Sequence[Any]], account_mode: Optional[str] = None,
                          health: Optional[Mapping[str, Any]] = None, transfers_in_flight: Optional[int] = None,
                          db: Any = None, broker_account_id: Optional[str] = None,
                          certified_scopes: Iterable[str] = ()) -> Dict[str, Any]:
    """Pure over its inputs (the caller reads THIS account's rows)."""
    from shared_lib.broker.capabilities import (WITHDRAWALS_SUPPORTED_BY_PLATFORM, Capability, CapabilityState,
                                                declared_profile)
    from shared_lib.broker.wallets import TopologyMode, WalletPurpose, topology_for

    broker = str(broker or "").lower()
    health = dict(health or {})
    ins = list(instruments or [])
    by_family: Dict[str, List[Any]] = {f: [i for i in ins if i.asset_class == f] for f in FAMILIES}
    caps: Dict[str, Dict[str, Any]] = {}

    def put(d: ActivationDecision, *, evidence_only: bool = False) -> None:
        caps[d.capability] = {**d.to_dict(), "status": ui_status(d, evidence_only=evidence_only)}

    discovery_ok = declared_profile(broker).state(Capability.INSTRUMENT_DISCOVERY) in (
        CapabilityState.SUPPORTED, CapabilityState.UNVALIDATED)
    for fam, cap_name in (("CRYPTO", "CRYPTO_MARKET_DATA"), ("FX", "FX_MARKET_DATA")):
        put(decide(cap_name, [
            Prerequisite("discovery_implemented", discovery_ok, "PLATFORM_ADAPTER_NOT_IMPLEMENTED", structural=True),
            Prerequisite("instruments_discovered", bool(by_family[fam]) if instruments is not None else None,
                         "NO_DISCOVERED_INSTRUMENTS"),
        ], scope=broker), evidence_only=True)
    put(_product_decision("CRYPTO_EXECUTION", broker=broker, environment=environment, permissions=permissions,
                          product_cap=Capability.CRYPTO_PERPETUALS, instruments=by_family["CRYPTO"], health=health))
    put(_product_decision("FX_EXECUTION", broker=broker, environment=environment, permissions=permissions,
                          product_cap=Capability.FX_PERPETUALS, instruments=by_family["FX"], health=health))
    put(_product_decision("TRADFI_EXECUTION", broker=broker, environment=environment, permissions=permissions,
                          product_cap=Capability.TRADFI,
                          instruments=[i for f in TRADFI for i in by_family[f]], health=health))

    # internal transfer: declared + positive permission evidence + no unresolved transfer
    prof = declared_profile(broker).for_account(permissions)
    tentry = prof.entry(Capability.INTERNAL_TRANSFER)
    t_usable = prof.usable(Capability.INTERNAL_TRANSFER, environment)
    wallet_host = None
    try:
        from shared_lib.broker.environment import normalize_environment, resolve_wallet_base_url

        wallet_host = resolve_wallet_base_url(broker, normalize_environment(environment))
    except Exception:
        wallet_host = None
    put(decide("INTERNAL_TRANSFER", [
        Prerequisite("declared", tentry.state not in (CapabilityState.UNSUPPORTED, CapabilityState.VENUE_API_UNAVAILABLE),
                     tentry.reason_code or "UNSUPPORTED", structural=True),
        # no verified wallet/asset API host for this environment (e.g. Binance demo): fail closed
        Prerequisite("wallet_api_host_verified", wallet_host is not None, "VENUE_API_NOT_SUPPORTED_IN_ENVIRONMENT"),
        Prerequisite("usable_for_account", t_usable, tentry.reason_code or "CAPABILITY_NOT_USABLE",
                     detail=tentry.detail),
        Prerequisite("no_unresolved_transfer", None if transfers_in_flight is None else transfers_in_flight == 0,
                     "TRANSFER_PENDING_RECONCILIATION_REQUIRED"),
    ], scope=broker, authority="USER_SETTINGS"))
    topo = topology_for(broker, account_mode)
    shared = bool(topo and topo.mode_between(WalletPurpose.UNIFIED, WalletPurpose.DERIVATIVES)
                  == TopologyMode.SHARED_COLLATERAL)
    put(decide("UNIFIED_COLLATERAL", [
        Prerequisite("topology_known", topo is not None, "TOPOLOGY_UNKNOWN", structural=topo is None),
        Prerequisite("shared_collateral", shared, "ACCOUNT_MODE_NOT_UNIFIED", detail=getattr(topo, "account_mode", "")),
    ], scope=broker), evidence_only=True)
    for cap_name, method in (("FUNDING_DATA", "get_funding"), ("ORDERBOOK_DATA", "get_orderbook")):
        put(decide(cap_name, [Prerequisite("implemented", _implements(broker, method),
                                           "PLATFORM_ADAPTER_NOT_IMPLEMENTED", structural=True)],
                   scope=broker), evidence_only=True)
    for cap_name in ("POSITION_MODE", "MARGIN_MODE"):
        put(decide(cap_name, [Prerequisite("implemented", False, "PLATFORM_ADAPTER_NOT_IMPLEMENTED",
                                           detail="the venue account default is used; the platform does not "
                                                  "switch position/margin mode", structural=True)], scope=broker))

    fam_out: Dict[str, Dict[str, Any]] = {}
    from app.exchange.instruments import execution_eligibility

    for fam in FAMILIES:
        rows = by_family[fam]
        api_ok = [i for i in rows if execution_eligibility(i, broker=broker, environment=environment,
                                                             permissions=permissions)[0]]
        cati = _cati_eligible(fam, broker, environment, db, broker_account_id, certified_scopes)
        fam_out[fam] = {"markets_available": len(rows), "markets_api_tradable": len(api_ok),
                        "markets_cati_eligible": len(api_ok) if cati.active else 0,
                        "cati": {**cati.to_dict(), "status": ui_status(cati)}}
    withdraw_present = bool(permissions and permissions.get("WITHDRAW") is True)
    return {"broker": broker, "environment": str(environment).upper(), "account_mode": getattr(topo, "account_mode", None),
            "withdrawal_permission_required": False,
            "withdrawals_supported_by_platform": WITHDRAWALS_SUPPORTED_BY_PLATFORM,
            "withdraw_permission_present": withdraw_present,
            "capabilities": caps, "markets": fam_out}


__all__ = ["FAMILIES", "UI_STATUSES", "account_market_status", "ui_status"]
