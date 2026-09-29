"""The runtime ORDER-AUTHORITY router (Section 25 runtime switch): for one broker-account scope, exactly one
engine -- or none -- may place NEW entries.

    owner = CATI   the persisted phase grants CATI this scope (M6: demo scopes; M7: demo + promoted live
                   scopes; M8+: every scope), the CATI kill switch is off and CATI is healthy
    owner = V2     CATI does not own the scope and the phase still gives V2 order authority there
                   (M0-M5 everywhere; M6 live; M7 unpromoted live)
    owner = NONE   everything else -- including CATI-owned scopes whose kill switch is on or whose CATI
                   runtime is unhealthy, and scopes whose user has not enabled Auto Trading

There is NO fallback: a scope CATI owns never reverts to V2 because CATI stopped, failed, or was killed; it
halts new entries until governance records an explicit rollback transition (``PromotionGovernance``,
audited). ``phases.PhaseSpec.v2_fallback_allowed`` is migration metadata, never a runtime path.

Only NEW entries are routed. Exits, protection and reconciliation of positions that already exist are
broker-authoritative and continue regardless of the owner. The router reads only the durable governance
record (phase, M7 scopes, kill switch), so a restart resolves the same owner; no environment variable,
flag, deploy, or UI toggle can grant authority here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .phases import PHASE_BY_ID, phase_index
from .promotion import NO_CAPITAL_ENVIRONMENTS, PromotionGovernance

CATI, V2, NONE = "CATI", "V2", "NONE"
ROUTER_VERSION = "runtime-order-authority-v1"


@dataclass(frozen=True)
class OrderAuthority:
    owner: str                  # CATI | V2 | NONE
    reason: str
    phase: Optional[str]
    environment: str
    broker_account_id: Optional[str]
    venue: Optional[str]

    def allows(self, engine: str) -> bool:
        return self.owner == engine

    def to_dict(self) -> Dict[str, Any]:
        return {"owner": self.owner, "reason": self.reason, "phase": self.phase, "environment": self.environment,
                "broker_account_id": self.broker_account_id, "venue": self.venue, "version": ROUTER_VERSION}


def _env(environment: Optional[str]) -> str:
    env = str(environment or "").strip().upper()
    return "DEMO" if env in ("DEMO", "TESTNET", "PAPER", "PRACTICE") else ("LIVE" if env in ("LIVE", "REAL") else env)


def canonical_venue(venue: Optional[str]) -> Optional[str]:
    """An account's venue as governance scopes record it: broker type or catalog key -> e.g. ``BINANCE_USDM``."""
    if not venue:
        return None
    from app.exchange.catalog_refresh import VENUE_KEY

    v = str(venue).strip().lower()
    return VENUE_KEY.get(v, v).upper()


def cati_scope_granted(gov: PromotionGovernance, phase: str, *, environment: str, broker_account_id: Optional[str],
                       venue: Optional[str]) -> bool:
    """Does the phase give CATI order authority for this scope (before kill switch / health)?"""
    i = phase_index(phase)
    if i < phase_index("M6"):
        return False
    if environment in NO_CAPITAL_ENVIRONMENTS:
        return True
    if phase == "M6" or environment != "LIVE":
        return False
    if phase == "M7":
        v = canonical_venue(venue)
        return bool(broker_account_id and v and any(
            gov.scope_granted(broker_account_id=broker_account_id, venue=v, environment=e) for e in ("REAL", "LIVE")))
    return True  # M8+: sole alpha


def _v2_may_trade(phase: str, gov: PromotionGovernance, *, environment: str, broker_account_id: Optional[str],
                  venue: Optional[str]) -> bool:
    v2 = PHASE_BY_ID[phase].v2_authority
    if v2 == "ACTIVE":
        return True
    if v2 == "BENCHMARK_ON_DEMO":
        return environment not in NO_CAPITAL_ENVIRONMENTS
    if v2 == "BENCHMARK_ON_PROMOTED_SCOPES":
        return environment not in NO_CAPITAL_ENVIRONMENTS and not cati_scope_granted(
            gov, phase, environment=environment, broker_account_id=broker_account_id, venue=venue)
    return False


def resolve_order_authority(db: Any, *, broker_account_id: Optional[str], venue: Optional[str],
                            environment: Optional[str], auto_trading_enabled: bool = True,
                            cati_healthy: bool = True) -> OrderAuthority:
    """The single owner of NEW entries for one account scope. Never raises: unreadable governance -> NONE."""
    env = _env(environment)
    base = dict(environment=env, broker_account_id=broker_account_id, venue=venue)
    try:
        gov = PromotionGovernance(db)
        phase = gov.current_phase()
    except Exception:
        return OrderAuthority(NONE, "GOVERNANCE_UNAVAILABLE", None, **base)
    if phase not in PHASE_BY_ID:
        return OrderAuthority(NONE, f"GOVERNANCE_PHASE_UNKNOWN:{phase}", phase, **base)
    if not auto_trading_enabled:
        return OrderAuthority(NONE, "USER_AUTO_TRADING_OFF", phase, **base)
    if env not in ("DEMO", "LIVE"):
        # below M6 CATI owns nothing and V2 holds authority on every scope, so the scope is irrelevant;
        # from M6 on an unresolved environment can be neither routed nor assumed
        if PHASE_BY_ID[phase].v2_authority == "ACTIVE":
            return OrderAuthority(V2, f"{phase}_V2_ALL_SCOPES_AUTHORITY", phase, **base)
        return OrderAuthority(NONE, f"ENVIRONMENT_UNKNOWN:{environment}", phase, **base)
    try:
        if cati_scope_granted(gov, phase, environment=env, broker_account_id=broker_account_id, venue=venue):
            if gov.kill_switch_on(scope=broker_account_id):
                return OrderAuthority(NONE, "CATI_NEW_ENTRY_KILL_SWITCH_NO_V2_FALLBACK", phase, **base)
            if not cati_healthy:
                return OrderAuthority(NONE, "CATI_UNHEALTHY_NEW_ENTRIES_HALTED_NO_V2_FALLBACK", phase, **base)
            return OrderAuthority(CATI, f"{phase}_CATI_{env}_AUTHORITY", phase, **base)
        if _v2_may_trade(phase, gov, environment=env, broker_account_id=broker_account_id, venue=venue):
            return OrderAuthority(V2, f"{phase}_V2_{env}_AUTHORITY", phase, **base)
    except Exception:
        return OrderAuthority(NONE, "GOVERNANCE_UNAVAILABLE", phase, **base)
    if phase_index(phase) < phase_index("M6"):
        return OrderAuthority(NONE, f"GOVERNANCE_PHASE_{phase}_NO_ORDER_AUTHORITY", phase, **base)
    if phase == "M6":
        return OrderAuthority(NONE, "M6_IS_DEMO_ONLY", phase, **base)
    return OrderAuthority(NONE, f"{phase}_SCOPE_NOT_PROMOTED", phase, **base)


__all__ = ["CATI", "V2", "NONE", "OrderAuthority", "ROUTER_VERSION", "canonical_venue", "cati_scope_granted",
           "resolve_order_authority"]
