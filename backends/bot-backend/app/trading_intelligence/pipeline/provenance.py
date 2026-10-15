"""Immutable decision-source verification at the final execution boundary (Section H, Step 3.3).

Before this module the boundary asked one question about the source of a plan: "is it the frozen residual
family?". It now asks the registry, for any family, and the answer has two independent halves that must both
hold before an exchange is touched:

1. **Family authorization** (``family_authorization``): the family is defined in code, registered, its code
   still has the registered rule hash, it is not disabled or killed, the research register says its
   hypothesis PASSED and that certification was not revoked, and its governance stage carries order authority
   for the plan's mode and environment. A registered family is not thereby executable.
2. **Decision verification** (``decision_verification``): the plan points at an immutable decision record;
   the record exists, its content still hashes to the stored hash and to the hash the plan carries, it was
   made for this account, this user and this environment by this family under this rule hash, it is neither
   stale nor dated in the future, and what the plan asks for is what the decision said.
3. **Venue certification** (``venue_certification``, Step 4.2): the exchange adapter of the plan's venue
   holds a complete, unexpired certification for the plan's environment in the certification register. This
   is about the infrastructure, not the strategy: an adapter certification says nothing about a family, and
   a certified family may not trade through an adapter whose certification is missing, failed, revoked or
   older than its validity.

Every refusal has a stable code (``REFUSALS``) and happens before any reservation is consumed and before any
adapter method is called. The same gate runs on the first pass, again immediately before the submit, and in
the recovery of a pending submission, so a family whose certification is revoked between the decision and the
order is stopped at the last moment.

The operator-scoped infrastructure harness (``DEMO_CERTIFICATION``) is not a family and is not authorized
here. It keeps its own narrow permit in ``execution.demo_boundary_certification``; an order it places says
nothing about any strategy.

There is no override in this module: no environment variable, setting or argument turns a refusal into a pass.
"""
from __future__ import annotations

import json
import time
from importlib import import_module
from typing import Any, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from shared_lib.persistence.cati_platform_schema import DECISION_TABLE

from . import contracts as C
from .family_registry import CERTIFIED, FAILED, NOT_CERTIFIED, REVOKED, FamilyRegistry, RegistryEntry

FAMILY_UNKNOWN = "FAMILY_UNKNOWN"
FAMILY_NOT_REGISTERED = "FAMILY_NOT_REGISTERED"
FAMILY_FAILED_RESEARCH = "FAMILY_FAILED_RESEARCH"
FAMILY_NOT_CERTIFIED = "FAMILY_NOT_CERTIFIED"
FAMILY_CERTIFICATION_REVOKED = "FAMILY_CERTIFICATION_REVOKED"
FAMILY_EVIDENCE_MISSING = "FAMILY_EVIDENCE_MISSING"
FAMILY_RULE_HASH_MISMATCH = "FAMILY_RULE_HASH_MISMATCH"
FAMILY_DISABLED = "FAMILY_DISABLED"
FAMILY_KILL_SWITCH = "FAMILY_KILL_SWITCH"
FAMILY_STAGE_NO_ORDER_AUTHORITY = "FAMILY_STAGE_NO_ORDER_AUTHORITY"
FAMILY_MODE_NOT_EXECUTABLE = "FAMILY_MODE_NOT_EXECUTABLE"
FAMILY_ENVIRONMENT_NOT_AUTHORIZED = "FAMILY_ENVIRONMENT_NOT_AUTHORIZED"
FAMILY_MARKET_NOT_SUPPORTED = "FAMILY_MARKET_NOT_SUPPORTED"
FAMILY_GATE_UNAVAILABLE = "FAMILY_GATE_UNAVAILABLE"
VENUE_ADAPTER_NOT_CERTIFIED = "VENUE_ADAPTER_NOT_CERTIFIED"
VENUE_NOT_SUPPORTED_BY_PLATFORM = "VENUE_NOT_SUPPORTED_BY_PLATFORM"
DECISION_EVIDENCE_MISSING = "DECISION_EVIDENCE_MISSING"
DECISION_TAMPERED = "DECISION_TAMPERED"
DECISION_STALE = "DECISION_STALE"
DECISION_FUTURE_DATED = "DECISION_FUTURE_DATED"
DECISION_ACCOUNT_MISMATCH = "DECISION_ACCOUNT_MISMATCH"
DECISION_PLAN_MISMATCH = "DECISION_PLAN_MISMATCH"
REFUSALS: Tuple[str, ...] = (
    FAMILY_UNKNOWN, FAMILY_NOT_REGISTERED, FAMILY_FAILED_RESEARCH, FAMILY_NOT_CERTIFIED, FAMILY_CERTIFICATION_REVOKED,
    FAMILY_EVIDENCE_MISSING, FAMILY_RULE_HASH_MISMATCH, FAMILY_DISABLED, FAMILY_KILL_SWITCH,
    FAMILY_STAGE_NO_ORDER_AUTHORITY, FAMILY_MODE_NOT_EXECUTABLE, FAMILY_ENVIRONMENT_NOT_AUTHORIZED,
    FAMILY_MARKET_NOT_SUPPORTED, FAMILY_GATE_UNAVAILABLE, DECISION_EVIDENCE_MISSING, DECISION_TAMPERED,
    DECISION_STALE, DECISION_FUTURE_DATED, DECISION_ACCOUNT_MISMATCH, DECISION_PLAN_MISMATCH,
    VENUE_ADAPTER_NOT_CERTIFIED, VENUE_NOT_SUPPORTED_BY_PLATFORM)

_CERTIFICATION_REFUSAL = {FAILED: FAMILY_FAILED_RESEARCH, NOT_CERTIFIED: FAMILY_NOT_CERTIFIED,
                          REVOKED: FAMILY_CERTIFICATION_REVOKED}
_HASHED = ("family_id", "family_version", "rule_hash", "mandate_id", "account_id", "user_id", "environment", "mode",
           "decision_time", "data_cutoff", "expires_at", "cycle_id")


# ----------------------------------------------------------------------------------------- decision records
class DecisionConflict(RuntimeError):
    """A second, different decision was offered for a cycle and an account that already have one."""


class DecisionStore:
    """Immutable family decisions (``cati_family_decisions``, append-only by trigger)."""

    def __init__(self, db: Any) -> None:
        self._db = db

    @staticmethod
    def content_hash(fields: Mapping[str, Any], document: Mapping[str, Any]) -> str:
        return stable_hash({**{k: fields.get(k) for k in _HASHED}, "document": json.loads(json.dumps(document, sort_keys=True))})

    @staticmethod
    def decision_id(cycle_id: str, account_id: str) -> str:
        return short_id("fdec", {"cycle": cycle_id, "account": account_id})

    def record(self, *, now_ms: Optional[int] = None, **fields: Any) -> Dict[str, Any]:
        """Store the decision of one cycle for one account, once (see ``record_on``)."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self.record_on(conn, now_ms=now_ms, **fields)

    @classmethod
    def record_on(cls, conn: Any, *, cycle_id: str, family_id: str, family_version: str, rule_hash: str,
                  mandate_id: Optional[str], account_id: str, user_id: Optional[str], environment: str, mode: str,
                  decision_time: int, data_cutoff: str, expires_at: int, document: Mapping[str, Any],
                  now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Inside the caller's transaction. The same decision offered again (a replayed cycle) returns the
        stored record; a different one for the same cycle and account is a conflict and nothing is written."""
        fields = dict(cycle_id=cycle_id, family_id=family_id, family_version=family_version, rule_hash=rule_hash,
                      mandate_id=mandate_id, account_id=account_id, user_id=user_id, environment=environment, mode=mode,
                      decision_time=int(decision_time), data_cutoff=str(data_cutoff), expires_at=int(expires_at))
        digest = cls.content_hash(fields, document)
        decision_id = cls.decision_id(cycle_id, account_id)
        row = conn.execute(f"SELECT decision_hash FROM {DECISION_TABLE} WHERE decision_id=?", (decision_id,)).fetchone()
        if row is not None:
            if row[0] != digest:
                raise DecisionConflict(f"{decision_id}: a different decision is already recorded for this cycle")
            return {"decision_id": decision_id, "decision_hash": digest, "replayed": True}
        conn.execute(
            f"INSERT INTO {DECISION_TABLE} (decision_id, cycle_id, family_id, family_version, rule_hash, mandate_id, "
            "account_id, user_id, environment, mode, decision_time, data_cutoff, expires_at, document, "
            "decision_hash, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (decision_id, cycle_id, family_id, family_version, rule_hash, mandate_id, account_id, user_id,
             environment, mode, fields["decision_time"], fields["data_cutoff"], fields["expires_at"],
             json.dumps(document, sort_keys=True), digest, int(now_ms if now_ms is not None else time.time() * 1000)))
        return {"decision_id": decision_id, "decision_hash": digest, "replayed": False}

    def get(self, decision_id: str) -> Optional[Dict[str, Any]]:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {DECISION_TABLE} WHERE decision_id=?", (decision_id,)).fetchone()
        if row is None:
            return None
        out = dict(row)
        out["document"] = json.loads(out["document"])
        return out

    def intact(self, row: Mapping[str, Any]) -> bool:
        return self.content_hash(row, row["document"]) == row["decision_hash"]


# ------------------------------------------------------------------------------------- decision verifiers
def verify_family_decision(db: Any, entry: RegistryEntry, plan: Any, now_ms: int) -> Optional[str]:
    """The default verifier: the plan against its record in ``cati_family_decisions``."""
    p = C.provenance_of(plan)
    decision_id = p.get("decision_id")
    store = DecisionStore(db)
    row = store.get(decision_id) if decision_id else None
    if row is None:
        return DECISION_EVIDENCE_MISSING
    if not store.intact(row) or p.get("decision_hash") != row["decision_hash"]:
        return DECISION_TAMPERED
    d = entry.definition
    if p.get("rule_hash") != d.rule_hash or p.get("family_version") != d.family_version             or (row["family_id"], row["rule_hash"], row["family_version"]) != (d.family_id, d.rule_hash, d.family_version):
        return FAMILY_RULE_HASH_MISMATCH        # the plan, the record and the code must name the same rules
    if row["account_id"] != p.get("broker_account_id") or (row["user_id"] or None) != (p.get("user_id") or None):
        return DECISION_ACCOUNT_MISMATCH
    if row["environment"] != p.get("environment"):
        return FAMILY_ENVIRONMENT_NOT_AUTHORIZED
    if row["mode"] != p.get("mode"):
        return FAMILY_MODE_NOT_EXECUTABLE         # a decision recorded for shadow never becomes an order
    if int(row["decision_time"]) != int(p.get("decision_time_ms") or -1):
        return DECISION_TAMPERED
    if int(row["decision_time"]) > int(now_ms) + C.FUTURE_TOLERANCE_MS:
        return DECISION_FUTURE_DATED
    if int(now_ms) >= int(row["expires_at"]):
        return DECISION_STALE
    return _plan_matches_document(plan, row["document"])


def _plan_matches_document(plan: Any, document: Mapping[str, Any]) -> Optional[str]:
    """What the plan asks for must be what the decision said: nothing added, nothing enlarged, no stop moved."""
    recorded = {i["symbol"]: i for i in document.get("intents") or []}
    if isinstance(getattr(plan, "provenance", None), C.DecisionProvenance):
        for intent in plan.intents:
            if C.to_plain(intent) != recorded.get(intent.symbol):
                return DECISION_PLAN_MISMATCH
        return None
    symbol = getattr(getattr(plan, "instrument_key", None), "venue_symbol", None)
    intent = recorded.get(symbol)
    if intent is None or intent.get("kind") != C.ENTRY or intent.get("direction") != getattr(plan, "side", None) \
            or float(intent.get("stop_price")) != float(getattr(plan, "structural_invalidation_price", float("nan"))):
        return DECISION_PLAN_MISMATCH
    return None


def verify_residual_decision(db: Any, entry: RegistryEntry, plan: Any, now_ms: int) -> Optional[str]:
    """The residual family keeps its decisions in ``cati_residual_decisions`` (Step 1). These are the checks
    the boundary made before the registry existed, unchanged, now reached only after family authorization."""
    from app.trading_intelligence.integration.production_execution import eligibility, latest_decision

    if dict(getattr(plan, "versions", ()) or ()).get("residual_registry") != entry.definition.rule_hash:
        return FAMILY_RULE_HASH_MISMATCH
    row = latest_decision(db)
    if row is None or row["decision_id"] != plan.source_candidate_id:
        return "CURRENT_CATI_DECISION_CHANGED"
    why = eligibility(row, now_ms)
    if why:
        return why
    if row["side"] != plan.side or row["selected_symbol"] != plan.instrument_key.venue_symbol \
            or float(row["stop"]) != plan.structural_invalidation_price \
            or not plan.target_zones or float(row["target"]) != plan.target_zones[0].price_high:
        return "FROZEN_RESIDUAL_PLAN_GEOMETRY_MISMATCH"
    return None


#: family definition ``implementation`` -> verifier of its decision source. A family not listed uses the
#: default record store; nothing here names a family.
_VERIFIERS: Mapping[str, str] = {
    "app.trading_intelligence.integration.residual_prospective":
        "app.trading_intelligence.pipeline.provenance:verify_residual_decision",
}


def _verifier_for(entry: RegistryEntry):
    target = _VERIFIERS.get(entry.definition.implementation or "")
    if not target:
        return verify_family_decision
    module, name = target.split(":")
    return getattr(import_module(module), name)


def platform_venue(venue: Any) -> Optional[str]:
    """A plan's venue name (``BINANCE_USDM``, ``binance``, ``bybit_linear`` ...) as the platform adapter contract
    names it, or None when the platform has no adapter for it."""
    from app.exchange.platform_adapter.contract import VENUES

    name = str(venue or "").strip().lower()
    return next((v for v in VENUES if name == v or name.startswith(v + "_")), None)


# ------------------------------------------------------------------------------------------------ the gate
class FamilyExecutionGate:
    """Asked by ``CATIExecutionBoundary`` before any order. ``authorize`` never raises: a failure to decide
    is a refusal with ``FAMILY_GATE_UNAVAILABLE``."""

    def __init__(self, db: Any, *, registry: Optional[FamilyRegistry] = None,
                 venue_certified: Optional[Any] = None) -> None:
        """``venue_certified``: ``(venue, environment, now_ms) -> (bool, reason)``. Absent (every production
        caller), the adapter certification register of the database is asked."""
        self._db = db
        self._registry = registry
        self._venue_certified = venue_certified

    @property
    def registry(self) -> FamilyRegistry:
        return self._registry if self._registry is not None else FamilyRegistry(self._db)

    def family_authorization(self, plan: Any) -> Tuple[Optional[RegistryEntry], Optional[str]]:
        """``(entry, None)`` when the family may send this plan's kind of order, else ``(entry or None, code)``."""
        p = C.provenance_of(plan)
        family_id = p.get("family_id")
        entry = self.registry.entry(family_id) if family_id else None
        if entry is None:
            return None, FAMILY_UNKNOWN
        if not entry.registered:
            return entry, FAMILY_NOT_REGISTERED
        if "FAMILY_RULE_HASH_MISMATCH" in entry.blockers:
            return entry, FAMILY_RULE_HASH_MISMATCH
        if entry.disabled:
            return entry, FAMILY_DISABLED
        if entry.kill_switch_state == "ON":
            return entry, FAMILY_KILL_SWITCH
        if entry.certification_status != CERTIFIED:
            return entry, _CERTIFICATION_REFUSAL.get(entry.certification_status, FAMILY_EVIDENCE_MISSING)
        mode, environment = p.get("mode"), str(p.get("environment") or "").upper()
        if mode not in C.EXECUTING_MODES:
            return entry, FAMILY_MODE_NOT_EXECUTABLE
        if mode != environment:
            return entry, FAMILY_ENVIRONMENT_NOT_AUTHORIZED
        if mode not in entry.allowed_modes:
            return entry, f"{FAMILY_STAGE_NO_ORDER_AUTHORITY}:{entry.governance_stage}"
        from app.trading_intelligence.governance.runtime_authority import canonical_venue

        if canonical_venue(p.get("venue")) not in {canonical_venue(v) for v, _ in entry.definition.supported_markets}:
            return entry, FAMILY_MARKET_NOT_SUPPORTED   # a plan without a venue is not on a supported one
        return entry, None

    def decision_verification(self, entry: RegistryEntry, plan: Any, now_ms: int) -> Optional[str]:
        return _verifier_for(entry)(self._db, entry, plan, int(now_ms))

    def venue_certification(self, plan: Any, now_ms: int) -> Optional[str]:
        """None when the adapter of the plan's venue is certified for the plan's environment today."""
        p = C.provenance_of(plan)
        venue = platform_venue(p.get("venue"))
        if venue is None:
            return VENUE_NOT_SUPPORTED_BY_PLATFORM
        environment = str(p.get("environment") or "").upper()
        if self._venue_certified is not None:
            certified, reason = self._venue_certified(venue, environment, int(now_ms))
        else:
            from app.exchange.platform_adapter.certification import adapter_certified

            certified, reason = adapter_certified(self._db, venue, environment, int(now_ms))
        return None if certified else f"{VENUE_ADAPTER_NOT_CERTIFIED}:{reason}"

    def authorize(self, plan: Any, *, now_ms: int) -> Tuple[bool, str]:
        try:
            entry, why = self.family_authorization(plan)
            if why is None:
                why = self.decision_verification(entry, plan, now_ms)
            if why is None:
                why = self.venue_certification(plan, now_ms)
        except Exception as exc:  # noqa: BLE001 -- an undecidable authorization is a refusal
            return False, f"{FAMILY_GATE_UNAVAILABLE}:{type(exc).__name__}"
        return (True, "FAMILY_AUTHORIZED") if why is None else (False, why)


class StaticFamilyGate:
    """Test and tooling double with a fixed verdict, for exercising the execution tail with fakes. The boundary
    refuses to use anything but the real gate in production, so this can never authorize a real order."""

    def __init__(self, allowed: bool = True, reason: str = "STATIC_FAMILY_GATE") -> None:
        self.allowed, self.reason = allowed, reason

    def authorize(self, plan: Any, *, now_ms: int) -> Tuple[bool, str]:
        return self.allowed, self.reason


__all__ = ["REFUSALS", "DecisionStore", "DecisionConflict", "FamilyExecutionGate", "StaticFamilyGate",
           "verify_family_decision", "verify_residual_decision", "FAMILY_UNKNOWN", "FAMILY_NOT_REGISTERED", "FAMILY_FAILED_RESEARCH",
           "FAMILY_NOT_CERTIFIED", "FAMILY_CERTIFICATION_REVOKED", "FAMILY_EVIDENCE_MISSING",
           "FAMILY_RULE_HASH_MISMATCH", "FAMILY_DISABLED", "FAMILY_KILL_SWITCH", "FAMILY_STAGE_NO_ORDER_AUTHORITY",
           "FAMILY_MODE_NOT_EXECUTABLE", "FAMILY_ENVIRONMENT_NOT_AUTHORIZED", "FAMILY_MARKET_NOT_SUPPORTED",
           "FAMILY_GATE_UNAVAILABLE", "DECISION_EVIDENCE_MISSING", "DECISION_TAMPERED", "DECISION_STALE",
           "DECISION_FUTURE_DATED", "DECISION_ACCOUNT_MISMATCH", "DECISION_PLAN_MISMATCH",
           "VENUE_ADAPTER_NOT_CERTIFIED", "VENUE_NOT_SUPPORTED_BY_PLATFORM", "platform_venue"]
