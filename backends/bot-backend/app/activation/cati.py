"""CATI capability activation: AUTO_ACTIVE_IF_ELIGIBLE for every CATI feature.

The legacy ``CATI_*_ENABLED`` flags are OPERATOR OVERRIDES now (only an
explicit off is honoured, see ``activation.model``). What each capability
needs:

``CATI_CYCLE_SHADOW`` (evidence only, exception-proof)
    the implementation. The outcome library is reported in ``detail``: without
    it forecasts fail closed (OUTCOME_LIBRARY_UNAVAILABLE) while MarketState,
    regime and setup evidence is still recorded.
``CATI_CAPITAL_ROUTING_SHADOW`` (evidence only; nothing is ever submitted)
    the cycle shadow it runs inside.
``CATI_GLOBAL_MARKET_STATE`` (evidence / context only)
    the cycle shadow (it is computed from the cycle's MarketStates).
``CATI_ACTIVE_EXECUTION`` (authority)
    Section 25 governance phase >= M6, the new-entry kill switch off, the
    environment the phase allows (M6: demo only; M7: an explicitly granted
    scope; M8+: promoted), a validated CATI economic adapter for the venue,
    AND the runtime authority switch (V2 -> benchmark, CATI -> sole alpha on
    the phase's environments). That switch is NOT implemented in the runner
    yet, so runtime CATI execution is BLOCKED with
    ``RUNTIME_AUTHORITY_SWITCH_NOT_IMPLEMENTED`` even after M6 -- the honest
    state, listed as an engineering blocker, never faked ACTIVE.
``CATI_EXIT_INTENT_ROUTING`` (authority)
    CATI_ACTIVE_EXECUTION.
``CATI_ML_AUTHORITY:<ROLE>`` (authority)
    a PROMOTED model for the role AND governance authorizing CATI; otherwise
    the deterministic CATI estimator is used (never V2).
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Mapping, Optional

from .model import ActivationDecision, OperatorOverride, Prerequisite, decide, operator_override

CYCLE_SHADOW = "CATI_CYCLE_SHADOW"
CAPITAL_ROUTING_SHADOW = "CATI_CAPITAL_ROUTING_SHADOW"
GLOBAL_MARKET_STATE = "CATI_GLOBAL_MARKET_STATE"
ACTIVE_EXECUTION = "CATI_ACTIVE_EXECUTION"
EXIT_INTENT_ROUTING = "CATI_EXIT_INTENT_ROUTING"
ML_AUTHORITY = "CATI_ML_AUTHORITY"

#: legacy flag -> capability (operator override only)
OVERRIDE_FLAGS: Mapping[str, str] = {
    CYCLE_SHADOW: "CATI_CYCLE_SHADOW_ENABLED",
    CAPITAL_ROUTING_SHADOW: "CATI_CAPITAL_ROUTING_SHADOW_ENABLED",
    GLOBAL_MARKET_STATE: "CATI_GLOBAL_MARKET_STATE_ENABLED",
    ACTIVE_EXECUTION: "CATI_ACTIVE_EXECUTION_ENABLED",
    EXIT_INTENT_ROUTING: "CATI_EXIT_INTENT_ROUTING_ENABLED",
    ML_AUTHORITY: "CATI_ML_ENABLED",
}

#: The runner does not yet switch alpha authority (V2 -> benchmark, CATI ->
#: sole alpha) when a phase is entered: ``GovernanceAuthority.v2_may_place_orders``
#: has no runtime caller and the runner never calls the execution boundary.
#: Flip only together with that runtime change and its tests.
RUNTIME_AUTHORITY_SWITCH_IMPLEMENTED = False

_DEMO_ENVS = ("DEMO", "TESTNET", "PAPER")


def _override(cap: str, environ: Optional[Mapping[str, str]]) -> OperatorOverride:
    return operator_override(OVERRIDE_FLAGS.get(cap), environ)


def library_prerequisite(environ: Optional[Mapping[str, str]] = None) -> Prerequisite:
    """Cheap (no load): configured path exists and, in RUNTIME mode, its identity hash is pinned.
    The controller still validates the artifact itself and fails closed."""
    env = environ if environ is not None else os.environ
    from app.trading_intelligence.config import (ENV_LIBRARY_EXPECTED_HASH, ENV_LIBRARY_MODE, ENV_LIBRARY_PATH,
                                                 LIBRARY_EXPECTED_HASH_REQUIRED, LIBRARY_NOT_CONFIGURED)

    path = (env.get(ENV_LIBRARY_PATH) or "").strip()
    mode = (env.get(ENV_LIBRARY_MODE) or "RUNTIME").strip().upper()
    if not path:
        return Prerequisite("outcome_library", False, LIBRARY_NOT_CONFIGURED)
    if not os.path.exists(path):
        return Prerequisite("outcome_library", False, "OUTCOME_LIBRARY_FILE_MISSING")
    if mode == "RUNTIME" and not (env.get(ENV_LIBRARY_EXPECTED_HASH) or "").strip():
        return Prerequisite("outcome_library", False, LIBRARY_EXPECTED_HASH_REQUIRED)
    return Prerequisite("outcome_library", True, "", detail=f"mode={mode}")


def cycle_shadow(environ: Optional[Mapping[str, str]] = None) -> ActivationDecision:
    """Evidence only and exception-proof: its sole prerequisite is the implementation. The outcome
    library is reported, not required -- without it the controller still records MarketState /
    regime / setup evidence and every forecast fails closed as OUTCOME_LIBRARY_UNAVAILABLE."""
    lib = library_prerequisite(environ)
    return decide(CYCLE_SHADOW, [Prerequisite("implementation", True, "")],
                  override=_override(CYCLE_SHADOW, environ), authority="EVIDENCE_ONLY",
                  detail={"forecasting": "AVAILABLE" if lib.satisfied else f"UNAVAILABLE:{lib.reason}"})


def capital_routing_shadow(environ: Optional[Mapping[str, str]] = None) -> ActivationDecision:
    parent = cycle_shadow(environ)
    return decide(CAPITAL_ROUTING_SHADOW,
                  [Prerequisite("cycle_shadow_active", parent.active, f"CYCLE_SHADOW_{parent.reason}")],
                  override=_override(CAPITAL_ROUTING_SHADOW, environ), authority="EVIDENCE_ONLY")


def global_market_state(environ: Optional[Mapping[str, str]] = None) -> ActivationDecision:
    parent = cycle_shadow(environ)
    return decide(GLOBAL_MARKET_STATE,
                  [Prerequisite("cycle_shadow_active", parent.active, f"CYCLE_SHADOW_{parent.reason}")],
                  override=_override(GLOBAL_MARKET_STATE, environ), authority="EVIDENCE_ONLY")


def _phase(db: Any) -> Optional[str]:
    if db is None:
        return None
    try:
        from app.trading_intelligence.governance.promotion import PromotionGovernance

        return PromotionGovernance(db).current_phase()
    except Exception:
        return None  # governance tables unreadable -> unknown -> blocked


def active_execution(db: Any, *, environment: str = "DEMO", venue: str = "binance_usdm",
                     broker_account_id: Optional[str] = None,
                     environ: Optional[Mapping[str, str]] = None) -> ActivationDecision:
    from app.trading_intelligence.governance.phases import phase_index
    from app.trading_intelligence.governance.promotion import PromotionGovernance
    from app.trading_intelligence.venue.registry import ADAPTER_STATUS_REGISTRY

    env = str(environment or "").upper()
    phase = _phase(db)
    idx = phase_index(phase) if phase else -1
    prereqs: List[Prerequisite] = [
        Prerequisite("governance_phase", None if phase is None else idx >= phase_index("M6"),
                     f"GOVERNANCE_PHASE_{phase}_NO_CATI_AUTHORITY", detail=f"phase={phase}"),
    ]
    if phase == "M6" or idx < phase_index("M6"):
        prereqs.append(Prerequisite("environment_permitted", env in _DEMO_ENVS,
                                    "LIVE_REQUIRES_M7_SCOPE" if env not in _DEMO_ENVS else ""))
    elif phase == "M7" and env not in _DEMO_ENVS:
        granted = None
        if db is not None and broker_account_id:
            try:
                granted = PromotionGovernance(db).scope_granted(broker_account_id=broker_account_id, venue=venue,
                                                                 environment=env)
            except Exception:
                granted = None
        prereqs.append(Prerequisite("m7_scope_granted", granted, "M7_SCOPE_NOT_PROMOTED"))
    kill = None
    if db is not None:
        try:
            kill = PromotionGovernance(db).kill_switch_on(scope=broker_account_id)
        except Exception:
            kill = None
    prereqs.append(Prerequisite("kill_switch_off", None if kill is None else not kill, "CATI_NEW_ENTRY_KILL_SWITCH"))
    adapter_env = "REAL" if env in ("LIVE", "REAL") else env
    status = ADAPTER_STATUS_REGISTRY.get((str(venue).lower(), adapter_env))
    prereqs.append(Prerequisite("economic_adapter_validated", status is not None,
                                "CATI_ECONOMIC_ADAPTER_NOT_VALIDATED_FOR_VENUE", detail=str(status)))
    prereqs.append(Prerequisite("runtime_authority_switch", RUNTIME_AUTHORITY_SWITCH_IMPLEMENTED,
                                "RUNTIME_AUTHORITY_SWITCH_NOT_IMPLEMENTED"))
    authority = "DEMO" if env in _DEMO_ENVS else ("SCOPED_LIVE" if phase == "M7" else "LIVE")
    return decide(ACTIVE_EXECUTION, prereqs, scope=f"{venue}:{env}:{broker_account_id or '*'}",
                  override=_override(ACTIVE_EXECUTION, environ), authority=authority, detail={"phase": phase})


def exit_intent_routing(db: Any, *, environment: str = "DEMO", venue: str = "binance_usdm",
                        broker_account_id: Optional[str] = None,
                        environ: Optional[Mapping[str, str]] = None) -> ActivationDecision:
    parent = active_execution(db, environment=environment, venue=venue, broker_account_id=broker_account_id,
                              environ=environ)
    return decide(EXIT_INTENT_ROUTING, [Prerequisite("active_execution", parent.active,
                                                     f"ACTIVE_EXECUTION_{parent.reason}")],
                  scope=parent.scope, override=_override(EXIT_INTENT_ROUTING, environ), authority=parent.authority)


def ml_authority(role: str, *, registry: Any, db: Any = None,
                 environ: Optional[Mapping[str, str]] = None) -> ActivationDecision:
    from app.trading_intelligence.governance.phases import phase_index
    from app.trading_intelligence.ml.promotion import MIN_ML_GOVERNANCE_PHASE

    promoted = None
    if registry is not None:
        try:
            promoted = bool(registry.promoted(role))
        except Exception:
            promoted = None
    phase = _phase(db)
    gov = None if phase is None else phase_index(phase) >= phase_index(MIN_ML_GOVERNANCE_PHASE)
    return decide(f"{ML_AUTHORITY}:{role}", [
        Prerequisite("model_promoted", promoted, "NO_PROMOTED_MODEL"),
        Prerequisite("governance_phase", gov, f"GOVERNANCE_PHASE_{phase}_BELOW_{MIN_ML_GOVERNANCE_PHASE}"),
    ], override=_override(ML_AUTHORITY, environ), authority="ESTIMATOR",
        detail={"fallback": "DETERMINISTIC_CATI_ESTIMATOR_NEVER_V2", "phase": phase})


def cati_status(db: Any = None, *, registry: Any = None, environ: Optional[Mapping[str, str]] = None
                ) -> Dict[str, Dict[str, Any]]:
    """Every CATI capability's derived state (for the ops/status API)."""
    from app.trading_intelligence.ml.contracts import ModelRole

    # the ACTIVE_EXECUTION scopes share a capability name: key scoped decisions by scope
    out = {}
    for d in (cycle_shadow(environ), capital_routing_shadow(environ), global_market_state(environ),
              active_execution(db, environment="DEMO", environ=environ),
              active_execution(db, environment="LIVE", environ=environ),
              exit_intent_routing(db, environment="DEMO", environ=environ)):
        out[f"{d.capability}@{d.scope}" if d.scope != "GLOBAL" else d.capability] = d.to_dict()
    for role in ModelRole:
        d = ml_authority(role.value, registry=registry, db=db, environ=environ)
        out[d.capability] = d.to_dict()
    return out


__all__ = ["ACTIVE_EXECUTION", "CAPITAL_ROUTING_SHADOW", "CYCLE_SHADOW", "EXIT_INTENT_ROUTING", "GLOBAL_MARKET_STATE",
           "ML_AUTHORITY", "OVERRIDE_FLAGS", "RUNTIME_AUTHORITY_SWITCH_IMPLEMENTED", "active_execution",
           "capital_routing_shadow", "cati_status", "cycle_shadow", "exit_intent_routing", "global_market_state",
           "library_prerequisite", "ml_authority"]
