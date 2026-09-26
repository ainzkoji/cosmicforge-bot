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


#: Finer, stable reason classes for "why is this market blocked for THIS account" (Section 8.9). The UI
#: ``status`` vocabulary above is unchanged; ``reason_class`` separates what that vocabulary folds together
#: (an unvalidated adapter on LIVE vs an ineligible account; governance vs certification; missing vs
#: unverifiable permission evidence).
REASON_CLASSES = ("ACTIVE", "DATA_NOT_READY", "VENUE_API_NOT_SUPPORTED", "ADAPTER_NOT_VALIDATED",
                  "PERMISSION_MISSING", "PERMISSION_EVIDENCE_REQUIRED", "ACCOUNT_NOT_ELIGIBLE",
                  "CERTIFICATION_NOT_READY", "GOVERNANCE_NOT_READY", "RISK_BLOCKED", "TRANSFER_PENDING",
                  "RECONCILIATION_REQUIRED", "ACCOUNT_TOPOLOGY_UNKNOWN", "NOT_LISTED", "UNSUPPORTED")

_CLASS_BY_REASON = (
    ("WITHDRAW", "ACCOUNT_NOT_ELIGIBLE"),
    ("PERMISSION_EVIDENCE_REQUIRED", "PERMISSION_EVIDENCE_REQUIRED"),
    ("NOT_EVIDENCED", "PERMISSION_EVIDENCE_REQUIRED"),
    ("API_KEY_PERMISSION_MISSING", "PERMISSION_MISSING"),
    ("UNVALIDATED_FOR_LIVE", "ADAPTER_NOT_VALIDATED"),
    ("ADAPTER_NOT_VALIDATED", "ADAPTER_NOT_VALIDATED"),
    ("VENUE_API", "VENUE_API_NOT_SUPPORTED"),
    ("INSTRUMENT_NOT_API_TRADABLE", "VENUE_API_NOT_SUPPORTED"),
    ("CLASSIFICATION_NOT_VENUE_EVIDENCED", "VENUE_API_NOT_SUPPORTED"),
    ("CONTRACT_TYPE_NOT_SUPPORTED", "UNSUPPORTED"),
    ("ASSET_CLASS_NOT_SUPPORTED", "UNSUPPORTED"),
    ("TOPOLOGY_UNKNOWN", "ACCOUNT_TOPOLOGY_UNKNOWN"),
    ("NOT_LISTED", "NOT_LISTED"),
    ("NO_DISCOVERED", "DATA_NOT_READY"),
    ("DISCOVERY", "DATA_NOT_READY"),
    ("GOVERNANCE", "GOVERNANCE_NOT_READY"),
    ("KILL_SWITCH", "GOVERNANCE_NOT_READY"),
    ("RUNTIME_AUTHORITY", "GOVERNANCE_NOT_READY"),
    ("M7_SCOPE", "GOVERNANCE_NOT_READY"),
    ("ECONOMIC_ADAPTER", "CERTIFICATION_NOT_READY"),
    ("CERTIF", "CERTIFICATION_NOT_READY"),
    ("QUARANTIN", "RISK_BLOCKED"),
    ("CIRCUIT", "RISK_BLOCKED"),
    ("TRANSFER_PENDING", "TRANSFER_PENDING"),
    ("RECONCILIATION", "RECONCILIATION_REQUIRED"),
    ("NOT_IMPLEMENTED", "UNSUPPORTED"),
    ("UNKNOWN_BROKER", "UNSUPPORTED"),
    ("BROKER_UNKNOWN", "UNSUPPORTED"),
)


def reason_class(reason: Optional[str], *, active: bool = False) -> str:
    if active:
        return "ACTIVE"
    r = str(reason or "")
    for needle, cls in _CLASS_BY_REASON:
        if needle in r:
            return cls
    return "ACCOUNT_NOT_ELIGIBLE"


def _ui_for_reason(reason: Optional[str]) -> str:
    r = reason or ""
    for needle, status in _UI_BY_REASON:
        if needle in r:
            return status
    return "BLOCKED"


def ui_status(decision: ActivationDecision, *, evidence_only: bool = False) -> str:
    if decision.state == ActivationState.ACTIVE:
        return "AVAILABLE" if evidence_only else "ACTIVE"
    if decision.state == ActivationState.UNSUPPORTED:
        return "VENUE_API_NOT_SUPPORTED" if "VENUE_API" in (decision.reason or "") else "UNSUPPORTED"
    return _ui_for_reason(decision.reason)


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
                      product_cap: Any, instruments: Sequence[Any], health: Mapping[str, Any],
                      discovery_fresh: Optional[bool] = True) -> ActivationDecision:
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
        # a catalog older than the refresh window may miss delistings / metadata changes: never assumed current
        Prerequisite("discovery_current", discovery_fresh, "DISCOVERY_STALE"),
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
                          certified_scopes: Iterable[str] = (), discovery_fresh: Optional[bool] = True
                          ) -> Dict[str, Any]:
    """Pure over its inputs (the caller reads THIS account's rows).

    ``account_mode`` is the mode READ from the broker for this account (None = not read): a broker whose
    wallet structure depends on it is ACCOUNT_TOPOLOGY_UNKNOWN until then -- never assumed unified.
    ``markets[<family>].execution`` answers "can THIS account automate this family now, and if not, why"
    with a stable ``reason_class``; ``blocked_reasons`` counts the per-instrument reasons."""
    from shared_lib.broker.capabilities import (WITHDRAWALS_SUPPORTED_BY_PLATFORM, Capability, CapabilityState,
                                                declared_profile)
    from shared_lib.broker.wallets import (ACCOUNT_TOPOLOGY_UNKNOWN, TopologyMode, WalletPurpose, topology_for,
                                           topology_for_account)

    broker = str(broker or "").lower()
    health = dict(health or {})
    ins = list(instruments or [])
    by_family: Dict[str, List[Any]] = {f: [i for i in ins if i.asset_class == f] for f in FAMILIES}
    caps: Dict[str, Dict[str, Any]] = {}

    def put(d: ActivationDecision, *, evidence_only: bool = False) -> None:
        caps[d.capability] = {**d.to_dict(), "status": ui_status(d, evidence_only=evidence_only),
                              "reason_class": reason_class(d.reason, active=d.active)}

    discovery_ok = declared_profile(broker).state(Capability.INSTRUMENT_DISCOVERY) in (
        CapabilityState.SUPPORTED, CapabilityState.UNVALIDATED)
    for fam, cap_name in (("CRYPTO", "CRYPTO_MARKET_DATA"), ("FX", "FX_MARKET_DATA")):
        put(decide(cap_name, [
            Prerequisite("discovery_implemented", discovery_ok, "PLATFORM_ADAPTER_NOT_IMPLEMENTED", structural=True),
            Prerequisite("instruments_discovered", bool(by_family[fam]) if instruments is not None else None,
                         "NO_DISCOVERED_INSTRUMENTS"),
        ], scope=broker), evidence_only=True)
    put(_product_decision("CRYPTO_EXECUTION", broker=broker, environment=environment, permissions=permissions,
                          product_cap=Capability.CRYPTO_PERPETUALS, instruments=by_family["CRYPTO"], health=health,
                          discovery_fresh=discovery_fresh))
    put(_product_decision("FX_EXECUTION", broker=broker, environment=environment, permissions=permissions,
                          product_cap=Capability.FX_PERPETUALS, instruments=by_family["FX"], health=health,
                          discovery_fresh=discovery_fresh))
    put(_product_decision("TRADFI_EXECUTION", broker=broker, environment=environment, permissions=permissions,
                          product_cap=Capability.TRADFI,
                          instruments=[i for f in TRADFI for i in by_family[f]], health=health,
                          discovery_fresh=discovery_fresh))

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
        # unprovable (not denied) transfer permission, e.g. BingX: evidence is required, never assumed
        Prerequisite("usable_for_account", t_usable,
                     "PERMISSION_EVIDENCE_REQUIRED" if (tentry.state == CapabilityState.ACCOUNT_RESTRICTED
                                                        and (permissions or {}).get("INTERNAL_TRANSFER") is None)
                     else (tentry.reason_code or "CAPABILITY_NOT_USABLE"), detail=tentry.detail),
        Prerequisite("no_unresolved_transfer", None if transfers_in_flight is None else transfers_in_flight == 0,
                     "TRANSFER_PENDING_RECONCILIATION_REQUIRED"),
        # wallets/routes of a mode-dependent account are unknown until its mode was read: no route assumed
        Prerequisite("topology_known", topology_for_account(broker, account_mode) is not None
                     or topology_for(broker) is None, ACCOUNT_TOPOLOGY_UNKNOWN),
    ], scope=broker, authority="USER_SETTINGS"))
    topo = topology_for_account(broker, account_mode)
    declared_topo = topology_for(broker) is not None
    shared = bool(topo and topo.mode_between(WalletPurpose.UNIFIED, WalletPurpose.DERIVATIVES)
                  == TopologyMode.SHARED_COLLATERAL)
    put(decide("UNIFIED_COLLATERAL", [
        Prerequisite("topology_declared", declared_topo, "TOPOLOGY_UNSUPPORTED", structural=True),
        Prerequisite("topology_known", topo is not None, ACCOUNT_TOPOLOGY_UNKNOWN,
                     detail="account mode not read from the broker"),
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

    product_cap = {"CRYPTO": Capability.CRYPTO_PERPETUALS, "FX": Capability.FX_PERPETUALS}
    for fam in FAMILIES:
        rows = by_family[fam]
        api_ok, blocked = [], {}
        for i in rows:
            ok, why = execution_eligibility(i, broker=broker, environment=environment, permissions=permissions)
            if ok:
                api_ok.append(i)
            else:
                key = why[-1] if why else "NOT_ELIGIBLE"
                blocked[key] = blocked.get(key, 0) + 1
        family_exec = _product_decision(f"{fam}_EXECUTION", broker=broker, environment=environment,
                                        permissions=permissions, product_cap=product_cap.get(fam, Capability.TRADFI),
                                        instruments=rows, health=health, discovery_fresh=discovery_fresh)
        if instruments is None:
            reason: Optional[str] = "DISCOVERY_NOT_SYNCED"
        elif not rows:
            reason = "NOT_LISTED_BY_VENUE"
        elif family_exec.state == ActivationState.UNSUPPORTED:
            reason = family_exec.reason
        elif not api_ok:
            # the dominant per-instrument reason (e.g. every FX contract API-closed, or LIVE unvalidated)
            reason = sorted(blocked.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        elif not family_exec.active:
            reason = family_exec.reason
        else:
            reason = None
        cati = _cati_eligible(fam, broker, environment, db, broker_account_id, certified_scopes)
        fam_out[fam] = {"markets_available": len(rows), "markets_api_tradable": len(api_ok),
                        "markets_cati_eligible": len(api_ok) if (cati.active and reason is None) else 0,
                        "execution": {"status": "ACTIVE" if reason is None else _ui_for_reason(reason),
                                      "reason": reason, "reason_class": reason_class(reason, active=reason is None)},
                        "blocked_reasons": dict(sorted(blocked.items())),
                        "cati": {**cati.to_dict(), "status": ui_status(cati),
                                 "reason_class": reason_class(cati.reason, active=cati.active)}}
    withdraw_present = bool(permissions and permissions.get("WITHDRAW") is True)
    return {"broker": broker, "environment": str(environment).upper(), "account_mode": getattr(topo, "account_mode", None),
            "withdrawal_permission_required": False,
            "withdrawals_supported_by_platform": WITHDRAWALS_SUPPORTED_BY_PLATFORM,
            "withdraw_permission_present": withdraw_present,
            "topology_class": (topo.topology_class.value if topo is not None
                               else ("UNKNOWN" if declared_topo else "UNSUPPORTED")),
            "permission_health": permission_health(permissions),
            "capabilities": caps, "markets": fam_out}


#: permission -> what needs it (least privilege; WITHDRAW is never needed)
_PERMISSION_USE = (("READ_ACCOUNT", "REQUIRED"), ("READ_POSITIONS", "REQUIRED"), ("READ_ORDERS", "REQUIRED"),
                   ("TRADE", "REQUIRED_FOR_TRADING"), ("INTERNAL_TRANSFER", "REQUIRED_ONLY_FOR_PHYSICAL_TRANSFER"))


def permission_health(permissions: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Abstract permission health: VERIFIED / MISSING / UNVERIFIED per permission -- never raw key metadata.

    UNVERIFIED (the venue cannot be asked, e.g. BingX) lets trading proceed under the broker's own
    enforcement, but a physical transfer needs VERIFIED INTERNAL_TRANSFER (PERMISSION_EVIDENCE_REQUIRED)."""
    p = dict(permissions or {})
    out: Dict[str, Any] = {}
    for perm, use in _PERMISSION_USE:
        v = p.get(perm)
        out[perm] = {"state": "VERIFIED" if v is True else ("MISSING" if v is False else "UNVERIFIED"), "use": use}
    w = p.get("WITHDRAW")
    out["WITHDRAW"] = {"state": "PRESENT_KEY_REFUSED" if w is True else ("ABSENT" if w is False else "UNVERIFIED"),
                       "use": "NEVER_REQUIRED"}
    return out


def instrument_capabilities(ins: Any, *, broker: str, environment: str, permissions: Optional[Mapping[str, Any]],
                            certified_instruments: Optional[Iterable[str]] = None,
                            cati_decision: Optional[ActivationDecision] = None,
                            delisted: bool = False) -> Dict[str, Any]:
    """Every capability DIMENSION of one discovered instrument for THIS account (Section 7.6) and the
    lifecycle stage it has reached (7.9). Dimensions are independent: market existence never implies API
    execution, which never implies account eligibility, certification or governance authority.

    ``certified_instruments``: canonical ids / venue symbols of a certified universe (None = none ->
    CERTIFICATION_READY is NO). A newly listed instrument is in no certified universe, so it can never
    reach CATI execution before certification AND governance."""
    from shared_lib.broker.capabilities import Capability, CapabilityState, declared_profile

    from app.exchange.instruments import VENUE_EVIDENCED_SOURCES, execution_eligibility
    from app.trading_intelligence.venue.registry import ADAPTER_STATUS_REGISTRY

    b = str(broker or "").lower()
    env = str(environment or "").upper()
    declared = declared_profile(b)
    narrowed = declared.for_account(permissions)
    dims: Dict[str, Dict[str, Any]] = {}

    def dim(name: str, ok: Optional[bool], reason: Optional[str]) -> None:
        dims[name] = {"state": "YES" if ok is True else ("NO" if ok is False else "UNKNOWN"),
                      "reason": None if ok is True else (reason or f"{name}_UNKNOWN")}

    def usable(cap: Any) -> bool:
        try:
            return narrowed.usable(cap, env)
        except Exception:
            return False

    fam_cap = {"CRYPTO": Capability.CRYPTO_PERPETUALS, "FX": Capability.FX_PERPETUALS}.get(ins.asset_class,
                                                                                         Capability.TRADFI)
    product_entry = declared.entry(fam_cap)
    api_closed = str((ins.venue_metadata or {}).get("apiStateOpen", "")).lower() == "false"
    product_hard = product_entry.state in (CapabilityState.UNSUPPORTED, CapabilityState.VENUE_API_UNAVAILABLE)
    evidenced = ins.asset_class == "CRYPTO" or ins.classification_source in VENUE_EVIDENCED_SOURCES
    dim("MARKET_EXISTS", not delisted, "DELISTED")
    dim("MARKET_DATA_SUPPORTED", usable(Capability.MARKET_DATA) and not delisted,
        "DELISTED" if delisted else "MARKET_DATA_NOT_SUPPORTED_BY_ADAPTER")
    if delisted:
        api_reason = "DELISTED"
    elif api_closed or product_hard:
        api_reason = "VENUE_API_NOT_SUPPORTED"
    elif not evidenced:
        api_reason = "CLASSIFICATION_NOT_VENUE_EVIDENCED"
    else:
        api_reason = "INSTRUMENT_NOT_API_TRADABLE"
    dim("API_EXECUTION_SUPPORTED",
        bool(ins.api_tradable) and not (delisted or api_closed or product_hard) and evidenced, api_reason)
    ok, why = execution_eligibility(ins, broker=b, environment=env, permissions=permissions)
    dim("ACCOUNT_ELIGIBLE", ok and not delisted, "DELISTED" if delisted else (why[-1] if why else None))
    for name, cap in (("ORDER_SUBMISSION_SUPPORTED", Capability.ORDERS),
                      ("ORDER_LOOKUP_SUPPORTED", Capability.ORDER_LOOKUP),
                      ("POSITION_SUPPORTED", Capability.POSITIONS), ("FILL_SUPPORTED", Capability.FILLS),
                      ("PROTECTION_SUPPORTED", Capability.PROTECTION_ORDERS),
                      ("TRANSFER_SUPPORTED", Capability.INTERNAL_TRANSFER)):
        e = narrowed.entry(cap)
        dim(name, usable(cap), e.reason_code or f"{e.state.value}_ON_{env}")
    venue = {"binance": "binance_usdm", "bybit": "bybit_linear", "bingx": "bingx_swap"}.get(b, b)
    econ = ADAPTER_STATUS_REGISTRY.get((venue, "REAL" if env in ("LIVE", "REAL") else env))
    dim("ECONOMICS_AVAILABLE", econ is not None, "CATI_ECONOMIC_ADAPTER_NOT_VALIDATED_FOR_VENUE")
    certified = bool(certified_instruments is not None
                     and {ins.canonical_symbol, ins.venue_symbol} & set(certified_instruments))
    dim("CERTIFICATION_READY", certified, "INSTRUMENT_NOT_IN_CERTIFIED_UNIVERSE")
    dim("GOVERNANCE_AUTHORISED", None if cati_decision is None else cati_decision.active,
        cati_decision.reason if cati_decision is not None else "GOVERNANCE_STATE_UNKNOWN")

    ladder = (("DISCOVERED", "MARKET_EXISTS"), ("RESEARCH_ONLY", "MARKET_DATA_SUPPORTED"),
              ("DATA_READY", "API_EXECUTION_SUPPORTED"), ("CERTIFICATION_READY", "CERTIFICATION_READY"),
              ("ACCOUNT_ELIGIBLE", "ACCOUNT_ELIGIBLE"), ("GOVERNANCE_AUTHORISED", "GOVERNANCE_AUTHORISED"))
    stage, blocker = ("DELISTED", "MARKET_EXISTS") if delisted else ("DISCOVERED", None)
    if not delisted:
        for st, d in ladder:
            if dims[d]["state"] != "YES":
                blocker = d
                break
            stage = st
        else:
            stage = "CATI_EXECUTABLE"
    return {"venue": venue, "environment": env, "venue_symbol": ins.venue_symbol,
            "canonical_symbol": ins.canonical_symbol, "asset_class": ins.asset_class,
            "product_type": ins.product_type, "dimensions": dims, "lifecycle_stage": stage,
            "next_blocker": None if blocker is None else {
                "dimension": blocker, "reason": dims[blocker]["reason"],
                "reason_class": reason_class(dims[blocker]["reason"])}}


__all__ = ["FAMILIES", "REASON_CLASSES", "UI_STATUSES", "account_market_status", "instrument_capabilities",
           "permission_health", "reason_class", "ui_status"]
