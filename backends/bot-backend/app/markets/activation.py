"""Staged market activation per (market, family): PAPER -> DEMO -> LIVE, and REVOKED (Section H, Step 7.4).

An append-only, hash-chained ledger (``market_activation_events``). The stage of a (market, family) pair is
the fold of its records; nothing else moves it, and nothing moves it automatically: every transition is asked
for by an authenticated owner / administrator (``OperatorAuthorization`` of ``pipeline.family_governance``,
the same object and the same semantics) and is then checked here against:

* **entry criteria** -- the market is known; it is a traditional market (crypto markets are governed by the
  family governance log, M0 ... M6, and are refused here so there are never two authorities); the family
  declares the market (``pipeline.family_markets``); the evidence the policy requires for the stage is
  referenced;
* **DEMO** additionally needs the research register to say the family is CERTIFIED for that market (read from
  the existing register through ``FamilyRegistry.research_certification`` at the moment of asking; no family
  is certified for any traditional market, so DEMO is refused today), a VERIFIED calendar and an execution
  adapter for the venue (none exists for a traditional venue);
* **durations** -- the minimum time in PAPER before DEMO, and in DEMO before LIVE, are values of an
  ``ActivationPolicy``. Their default is ``None`` (no owner decision exists), and a transition that depends on
  an unset duration is ``BLOCKED_PENDING_POLICY``, never approved by default;
* **LIVE** is refused in this build: ``TRADFI_LIVE_NOT_PERMITTED_IN_THIS_BUILD``.

Every decision -- approved, refused, blocked -- is recorded with its blockers, the operator and the evidence
references. A revocation is always possible (the safe direction is never blocked) and returns the pair to
REVOKED; from there only PAPER may be asked for again.

Risk restrictions per stage cannot exceed the platform caps (0.40 % of equity per trade, entry stop at most
15 %); account eligibility per stage is by broker environment (PAPER: no account at all; DEMO: demo accounts of
the market's venue; LIVE: none in this build).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.pipeline import family_markets
from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
from shared_lib.persistence.markets_schema import ACTIVATION_EVENT_TABLE

from .calendar import VERIFIED
from .registry import MarketRegistry, MarketRegistryError, default_registry

LEDGER_VERSION = "1.0.0"
GENESIS = "0" * 64

NONE, PAPER, DEMO, LIVE, REVOKED = "NONE", "PAPER", "DEMO", "LIVE", "REVOKED"
STAGES = (NONE, PAPER, DEMO, LIVE, REVOKED)
#: the only forward moves; anything else is a skip or not a transition
NEXT_STAGE: Mapping[str, str] = {NONE: PAPER, REVOKED: PAPER, PAPER: DEMO, DEMO: LIVE}

TRANSITION_APPROVED = "TRANSITION_APPROVED"
TRANSITION_REFUSED = "TRANSITION_REFUSED"
TRANSITION_BLOCKED_PENDING_POLICY = "TRANSITION_BLOCKED_PENDING_POLICY"
ACTIVATION_REVOKED = "ACTIVATION_REVOKED"
EVENT_TYPES = (TRANSITION_APPROVED, TRANSITION_REFUSED, TRANSITION_BLOCKED_PENDING_POLICY, ACTIVATION_REVOKED)

APPROVED, REFUSED, BLOCKED_PENDING_POLICY = "APPROVED", "REFUSED", "BLOCKED_PENDING_POLICY"

# blocker codes
TRADFI_LIVE_NOT_PERMITTED_IN_THIS_BUILD = "TRADFI_LIVE_NOT_PERMITTED_IN_THIS_BUILD"
MARKET_UNKNOWN = "MARKET_UNKNOWN"
MARKET_GOVERNED_BY_FAMILY_GOVERNANCE = "MARKET_GOVERNED_BY_FAMILY_GOVERNANCE"
FAMILY_DOES_NOT_DECLARE_MARKET = "FAMILY_DOES_NOT_DECLARE_MARKET"
STAGE_SKIPPED = "STAGE_SKIPPED"
CALENDAR_UNVERIFIED = "CALENDAR_UNVERIFIED"
NO_EXECUTION_ADAPTER = "NO_EXECUTION_ADAPTER"
REASON_REQUIRED = "REASON_REQUIRED"

PLATFORM_MAX_RISK_PER_TRADE_PCT = Decimal("0.40")
PLATFORM_MAX_ENTRY_STOP_PCT = Decimal("15")


class ActivationError(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


@dataclass(frozen=True)
class RiskRestriction:
    max_risk_per_trade_pct: Decimal = PLATFORM_MAX_RISK_PER_TRADE_PCT
    max_entry_stop_pct: Decimal = PLATFORM_MAX_ENTRY_STOP_PCT

    def __post_init__(self) -> None:
        if not (Decimal("0") < self.max_risk_per_trade_pct <= PLATFORM_MAX_RISK_PER_TRADE_PCT):
            raise ActivationError("RISK_ABOVE_PLATFORM_CAP", f"risk per trade is at most {PLATFORM_MAX_RISK_PER_TRADE_PCT} %")
        if not (Decimal("0") < self.max_entry_stop_pct <= PLATFORM_MAX_ENTRY_STOP_PCT):
            raise ActivationError("RISK_ABOVE_PLATFORM_CAP", f"entry stop is at most {PLATFORM_MAX_ENTRY_STOP_PCT} %")

    def to_plain(self) -> Dict[str, str]:
        return {"max_risk_per_trade_pct": str(self.max_risk_per_trade_pct),
                "max_entry_stop_pct": str(self.max_entry_stop_pct)}


def _default_evidence() -> Mapping[str, Tuple[str, ...]]:
    return {PAPER: ("research_specification",), DEMO: ("paper_record", "research_certification"),
            LIVE: ("demo_record",)}


def _default_exit_criteria() -> Mapping[str, Tuple[str, ...]]:
    common = ("owner revocation", "family certification revoked or failed", "calendar or contract data invalidated",
              "family kill switch")
    return {PAPER: common, DEMO: common + ("account brake: drawdown stop",), LIVE: common + ("account brake: drawdown stop",)}


def _default_risk() -> Mapping[str, RiskRestriction]:
    return {PAPER: RiskRestriction(), DEMO: RiskRestriction(), LIVE: RiskRestriction()}


def _default_eligibility() -> Mapping[str, Tuple[str, ...]]:
    return {PAPER: (), DEMO: ("DEMO",), LIVE: ()}


@dataclass(frozen=True)
class ActivationPolicy:
    """What the owner has decided about staged activation. Every duration defaults to ``None``: no decision."""
    policy_id: str = "UNSET"
    source: str = "No owner decision recorded; durations are unset (research/trend_v2_tradfi/SPEC.md D16)."
    paper_min_days: Optional[int] = None
    demo_min_days: Optional[int] = None
    required_evidence: Mapping[str, Tuple[str, ...]] = field(default_factory=_default_evidence)
    exit_criteria: Mapping[str, Tuple[str, ...]] = field(default_factory=_default_exit_criteria)
    risk_restrictions: Mapping[str, RiskRestriction] = field(default_factory=_default_risk)
    account_environments: Mapping[str, Tuple[str, ...]] = field(default_factory=_default_eligibility)

    def __post_init__(self) -> None:
        for name in ("paper_min_days", "demo_min_days"):
            v = getattr(self, name)
            if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v < 1):
                raise ActivationError("INVALID_POLICY", f"{name} is a positive number of days or None")
        if self.account_environments.get(LIVE):
            raise ActivationError("INVALID_POLICY", "no account is eligible for LIVE in this build")

    def duration_for(self, to_stage: str) -> Tuple[str, Optional[int]]:
        """(policy field, days) the move into ``to_stage`` requires; (\"\", 0) when none."""
        if to_stage == DEMO:
            return "paper_min_days", self.paper_min_days
        if to_stage == LIVE:
            return "demo_min_days", self.demo_min_days
        return "", 0

    def to_plain(self) -> Dict[str, Any]:
        return {"policy_id": self.policy_id, "source": self.source, "paper_min_days": self.paper_min_days,
                "demo_min_days": self.demo_min_days,
                "required_evidence": {k: list(v) for k, v in self.required_evidence.items()},
                "exit_criteria": {k: list(v) for k, v in self.exit_criteria.items()},
                "risk_restrictions": {k: v.to_plain() for k, v in self.risk_restrictions.items()},
                "account_environments": {k: list(v) for k, v in self.account_environments.items()}}


@dataclass(frozen=True)
class ActivationState:
    market_id: str
    family_id: str
    stage: str
    entered_at: Optional[int]
    last_event_seq: int
    decisions: int


def _require_operator(operator: Any) -> OperatorAuthorization:
    if not isinstance(operator, OperatorAuthorization):
        raise ActivationError("OPERATOR_AUTHORIZATION_REQUIRED",
                              "an activation is decided by an authenticated owner / administrator")
    return operator


def default_certification_lookup(db: Any) -> Callable[[str, str], Dict[str, Any]]:
    """The research register, read now through the family registry. A family is certified only for the
    markets it declares; an unknown family is UNKNOWN."""
    def lookup(family_id: str, market_id: str) -> Dict[str, Any]:
        if not family_markets.supports(family_id, market_id):
            return {"status": "NOT_CERTIFIED", "reason": "MARKET_NOT_IN_FAMILY_DECLARATION"}
        from app.trading_intelligence.pipeline.family_registry import FamilyRegistry, definitions

        definition = definitions().get(family_id)
        if definition is None:
            return {"status": "UNKNOWN", "reason": "FAMILY_NOT_DEFINED"}
        return dict(FamilyRegistry(db).research_certification(definition))
    return lookup


def default_adapter_available(venue: str) -> bool:
    from app.exchange.platform_adapter.factory import ADAPTER_VERSIONS

    return str(venue).lower() in ADAPTER_VERSIONS


class MarketActivationLedger:
    def __init__(self, db: Any, *, registry: Optional[MarketRegistry] = None, policy: Optional[ActivationPolicy] = None,
                 certification_lookup: Optional[Callable[[str, str], Mapping[str, Any]]] = None,
                 adapter_available: Optional[Callable[[str], bool]] = None,
                 supports: Optional[Callable[[str, str], bool]] = None) -> None:
        self._db = db
        self._registry = registry
        self.policy = policy or ActivationPolicy()
        self._certification = certification_lookup or default_certification_lookup(db)
        self._adapter = adapter_available or default_adapter_available
        self._supports = supports or family_markets.supports

    @property
    def registry(self) -> MarketRegistry:
        return self._registry if self._registry is not None else default_registry()

    # ------------------------------------------------------------------------------------------- the log
    def _append(self, conn: Any, market_id: str, family_id: str, event_type: str, *, actor_ref: str,
                payload: Mapping[str, Any], from_stage: Optional[str], to_stage: Optional[str],
                now_ms: Optional[int]) -> Dict[str, Any]:
        if event_type not in EVENT_TYPES:
            raise ActivationError("UNKNOWN_EVENT_TYPE", event_type)
        last = conn.execute(f"SELECT seq, event_hash FROM {ACTIVATION_EVENT_TABLE} ORDER BY seq DESC LIMIT 1").fetchone()
        seq, prev = (int(last[0]) + 1, last[1]) if last else (1, GENESIS)
        ts = int(now_ms if now_ms is not None else time.time() * 1000)
        body = json.dumps(dict(payload), sort_keys=True, default=str)
        core = {"seq": seq, "market_id": market_id, "family_id": family_id, "event_type": event_type,
                "from_stage": from_stage, "to_stage": to_stage, "actor_ref": actor_ref, "recorded_at": ts,
                "payload": body, "prev_hash": prev}
        event_hash = stable_hash(core)
        event_id = short_id("mae", {"seq": seq, "hash": event_hash})
        conn.execute(
            f"INSERT INTO {ACTIVATION_EVENT_TABLE} (seq, event_id, market_id, family_id, event_type, from_stage, "
            "to_stage, actor_ref, recorded_at, payload, prev_hash, event_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (seq, event_id, market_id, family_id, event_type, from_stage, to_stage, actor_ref, ts, body, prev,
             event_hash))
        return {**core, "event_id": event_id, "event_hash": event_hash, "payload": dict(payload)}

    @staticmethod
    def _rows(conn: Any, market_id: Optional[str] = None, family_id: Optional[str] = None) -> List[Dict[str, Any]]:
        sql, args = f"SELECT * FROM {ACTIVATION_EVENT_TABLE}", []
        if market_id is not None:
            sql += " WHERE market_id=? AND family_id=?"
            args = [market_id, family_id]
        rows = [dict(r) for r in conn.execute(sql + " ORDER BY seq", args)]
        for r in rows:
            r["payload"] = json.loads(r["payload"])
        return rows

    @staticmethod
    def _fold(market_id: str, family_id: str, rows: List[Dict[str, Any]]) -> ActivationState:
        stage, entered = NONE, None
        for r in rows:
            if r["event_type"] == TRANSITION_APPROVED:
                stage, entered = r["to_stage"], r["recorded_at"]
            elif r["event_type"] == ACTIVATION_REVOKED:
                stage, entered = REVOKED, r["recorded_at"]
        return ActivationState(market_id=market_id, family_id=family_id, stage=stage, entered_at=entered,
                               last_event_seq=rows[-1]["seq"] if rows else 0, decisions=len(rows))

    def history(self, market_id: Optional[str] = None, family_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            return self._rows(conn, market_id, family_id)

    def state(self, market_id: str, family_id: str) -> ActivationState:
        with self._db.connect() as conn:
            return self._fold(market_id, family_id, self._rows(conn, market_id, family_id))

    def states(self) -> List[ActivationState]:
        with self._db.connect() as conn:
            pairs = conn.execute(f"SELECT DISTINCT market_id, family_id FROM {ACTIVATION_EVENT_TABLE} "
                                 "ORDER BY market_id, family_id").fetchall()
            return [self._fold(m, f, self._rows(conn, m, f)) for m, f in pairs]

    def highest_stage(self, market_id: str) -> str:
        """The furthest stage any family has reached on ``market_id`` (NONE when nothing was ever approved)."""
        order = {NONE: 0, REVOKED: 0, PAPER: 1, DEMO: 2, LIVE: 3}
        best = NONE
        for s in self.states():
            if s.market_id == market_id and order[s.stage] > order[best]:
                best = s.stage
        return best

    def verify_chain(self) -> Dict[str, Any]:
        prev, count = GENESIS, 0
        with self._db.connect() as conn:
            for r in conn.execute(f"SELECT * FROM {ACTIVATION_EVENT_TABLE} ORDER BY seq"):
                r = dict(r)
                count += 1
                core = {k: r[k] for k in ("seq", "market_id", "family_id", "event_type", "from_stage", "to_stage",
                                          "actor_ref", "recorded_at", "payload", "prev_hash")}
                if r["seq"] != count or r["prev_hash"] != prev or stable_hash(core) != r["event_hash"]:
                    return {"ok": False, "records": count, "broken_at_seq": r["seq"]}
                prev = r["event_hash"]
        return {"ok": True, "records": count, "head_hash": prev}

    # ------------------------------------------------------------------------------------- the criteria
    def evaluate(self, state: ActivationState, to_stage: str, *, evidence: Optional[Mapping[str, str]] = None,
                 now_ms: Optional[int] = None) -> Dict[str, Any]:
        """What stands between ``state`` and ``to_stage``: hard blockers and unset policy values."""
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        evidence = dict(evidence or {})
        blockers: List[str] = []
        pending: List[str] = []
        detail: Dict[str, Any] = {}
        if to_stage not in (PAPER, DEMO, LIVE):
            raise ActivationError("UNKNOWN_STAGE", f"{to_stage}: transitions go to PAPER, DEMO or LIVE")
        if NEXT_STAGE.get(state.stage) != to_stage:
            blockers.append(f"{STAGE_SKIPPED}:{state.stage}->{to_stage}")
        if to_stage == LIVE:
            blockers.append(TRADFI_LIVE_NOT_PERMITTED_IN_THIS_BUILD)
        try:
            market = self.registry.market(state.market_id)
        except MarketRegistryError:
            market = None
            blockers.append(MARKET_UNKNOWN)
        if market is not None:
            if not market.is_tradfi:
                blockers.append(MARKET_GOVERNED_BY_FAMILY_GOVERNANCE)
            if not self._supports(state.family_id, state.market_id):
                blockers.append(FAMILY_DOES_NOT_DECLARE_MARKET)
        for key in self.policy.required_evidence.get(to_stage, ()):
            if not str(evidence.get(key) or "").strip():
                blockers.append(f"EVIDENCE_MISSING:{key}")
        if to_stage in (DEMO, LIVE) and market is not None:
            cert = dict(self._certification(state.family_id, state.market_id) or {})
            detail["certification"] = cert
            if cert.get("status") != "CERTIFIED":
                blockers.append(f"RESEARCH_CERTIFICATION_{cert.get('status') or 'UNKNOWN'}")
            if self.registry.calendar(market.calendar_id).verification_status != VERIFIED:
                blockers.append(CALENDAR_UNVERIFIED)
            if not self._adapter(market.venue):
                blockers.append(NO_EXECUTION_ADAPTER)
        field_name, days = self.policy.duration_for(to_stage)
        if field_name:
            if days is None:
                pending.append(f"POLICY_DURATION_UNSET:{field_name}")
            elif state.entered_at is None or now - state.entered_at < days * 86_400_000:
                blockers.append(f"STAGE_DURATION_NOT_MET:{field_name}")
        status = REFUSED if blockers else (BLOCKED_PENDING_POLICY if pending else APPROVED)
        return {"status": status, "blockers": blockers, "pending_policy": pending, "detail": detail}

    # --------------------------------------------------------------------------------------- the actions
    def request_transition(self, market_id: str, family_id: str, to_stage: str, *, operator: OperatorAuthorization,
                           reason: str, evidence: Optional[Mapping[str, str]] = None,
                           now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Decide one transition and record the decision. Returns ``{status, blockers, pending_policy, event}``.
        Only an ``APPROVED`` decision moves the stage."""
        operator = _require_operator(operator)
        if not reason or not str(reason).strip():
            raise ActivationError(REASON_REQUIRED, "a transition states its reason")
        ts = int(now_ms if now_ms is not None else time.time() * 1000)
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = self._fold(market_id, family_id, self._rows(conn, market_id, family_id))
            decision = self.evaluate(state, to_stage, evidence=evidence, now_ms=ts)
            event_type = {APPROVED: TRANSITION_APPROVED, REFUSED: TRANSITION_REFUSED,
                          BLOCKED_PENDING_POLICY: TRANSITION_BLOCKED_PENDING_POLICY}[decision["status"]]
            payload = {"reason": reason, "operator": operator.operator_id, "channel": operator.channel,
                       "permission": operator.permission, "requested_stage": to_stage,
                       "evidence": dict(evidence or {}), "blockers": decision["blockers"],
                       "pending_policy": decision["pending_policy"], "policy": self.policy.to_plain(),
                       "detail": decision["detail"], "ledger_version": LEDGER_VERSION}
            if decision["status"] == APPROVED:
                payload["risk_restrictions"] = self.policy.risk_restrictions[to_stage].to_plain()
                payload["exit_criteria"] = list(self.policy.exit_criteria.get(to_stage, ()))
            event = self._append(conn, market_id, family_id, event_type, actor_ref=operator.actor_ref, payload=payload,
                                 from_stage=state.stage,
                                 to_stage=to_stage if decision["status"] == APPROVED else state.stage, now_ms=ts)
        return {**decision, "event": event}

    def revoke(self, market_id: str, family_id: str, *, operator: OperatorAuthorization, reason: str,
               now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Always possible from an active stage; needs no evidence."""
        operator = _require_operator(operator)
        if not reason or not str(reason).strip():
            raise ActivationError(REASON_REQUIRED, "a revocation states its reason")
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = self._fold(market_id, family_id, self._rows(conn, market_id, family_id))
            if state.stage in (NONE, REVOKED):
                raise ActivationError("NOTHING_TO_REVOKE", f"{market_id}/{family_id} is {state.stage}")
            return self._append(conn, market_id, family_id, ACTIVATION_REVOKED, actor_ref=operator.actor_ref,
                                payload={"reason": reason, "operator": operator.operator_id,
                                         "channel": operator.channel},
                                from_stage=state.stage, to_stage=REVOKED, now_ms=now_ms)

    # ------------------------------------------------------------------------------ what a stage allows
    def risk_restrictions(self, market_id: str, family_id: str) -> Optional[RiskRestriction]:
        stage = self.state(market_id, family_id).stage
        return self.policy.risk_restrictions.get(stage) if stage in (PAPER, DEMO, LIVE) else None

    def account_eligible(self, market_id: str, family_id: str, account: Mapping[str, Any]) -> Tuple[bool, List[str]]:
        """Whether ``account`` (``broker_id``, ``environment``) may carry this (market, family) at its stage."""
        stage = self.state(market_id, family_id).stage
        reasons: List[str] = []
        if stage not in (PAPER, DEMO, LIVE):
            return False, [f"ACTIVATION_STAGE_{stage}"]
        allowed = self.policy.account_environments.get(stage, ())
        environment = str(account.get("environment") or "").upper()
        if not allowed:
            reasons.append(f"NO_ACCOUNT_ELIGIBLE_AT_{stage}")
        elif environment not in allowed:
            reasons.append(f"ACCOUNT_ENVIRONMENT_{environment or 'UNKNOWN'}_NOT_ELIGIBLE_AT_{stage}")
        try:
            venue = self.registry.market(market_id).venue
            if str(account.get("broker_id") or "").lower() != venue:
                reasons.append("ACCOUNT_VENUE_MISMATCH")
        except MarketRegistryError:
            reasons.append(MARKET_UNKNOWN)
        return not reasons, reasons


__all__ = ["LEDGER_VERSION", "NONE", "PAPER", "DEMO", "LIVE", "REVOKED", "STAGES", "NEXT_STAGE", "EVENT_TYPES",
           "TRANSITION_APPROVED", "TRANSITION_REFUSED", "TRANSITION_BLOCKED_PENDING_POLICY", "ACTIVATION_REVOKED",
           "APPROVED", "REFUSED", "BLOCKED_PENDING_POLICY", "TRADFI_LIVE_NOT_PERMITTED_IN_THIS_BUILD", "MARKET_UNKNOWN",
           "MARKET_GOVERNED_BY_FAMILY_GOVERNANCE", "FAMILY_DOES_NOT_DECLARE_MARKET", "STAGE_SKIPPED",
           "CALENDAR_UNVERIFIED", "NO_EXECUTION_ADAPTER", "ActivationError", "RiskRestriction", "ActivationPolicy",
           "ActivationState", "MarketActivationLedger", "default_certification_lookup", "default_adapter_available"]
