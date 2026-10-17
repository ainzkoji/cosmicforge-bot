"""The go-live authorization gate (Section H, Step 6.1).

``GoLiveGate.evaluate(...)`` checks every condition for a live entry **independently** and returns all of them,
each with an explicit reason when it does not pass. Authorization requires every condition to PASS; an
exception inside a check is that check's failure (``CHECK_ERROR``), never a pass. Conditions:

 1 VENUE_ADAPTER_CERTIFIED          persisted LIVE certification of the venue adapter (certification register)
 2 STRATEGY_RESEARCH_PASSED         the family's research certification (family registry / research register)
 3 STRATEGY_DEMO_RUN_COMPLETED      a recorded PASS demo run of this family version, at least the owner's
                                    minimum duration (unset -> BLOCKED_PENDING_POLICY)
 4 FAMILY_STAGE_PERMITS_LIVE        governance stage allows LIVE (registry allowed_modes)
 5 CUSTOMER_LIVE_RISK_CONSENT       active, unrevoked live consent of the account owner, current version
 6 IDENTITY_REQUIREMENT_SATISFIED   identity policy for "start_live_trading" (identity service)
 7 ACCOUNT_CONNECTION_VALID         account exists, belongs to the user, connected, LIVE environment
 8 RISK_PROFILE_VALID               a valid risk level on the account's bot
 9 ACCOUNT_CAPITAL_RULES            a positive capital allocation within the beta budget ceiling, if enrolled
10 OPERATOR_LIVE_SWITCH_ON          the exchange's live switch, opened by two different operators
11 OPERATOR_APPROVAL_AUDITED        an unexpired, unrevoked approval for (account, family, venue) granted by an
                                    operator other than the requester, recorded in the ledger
12 LIVE_ORDER_SUBMISSION_ENABLED    the deployment's LIVE_ORDER_SUBMISSION_ENABLED
13 KILL_SWITCHES_CLEAR              platform, account and family kill switches
14 NO_OPEN_EXECUTION_UNCERTAINTY    no unresolved submission, reconciliation or protection uncertainty
15 BUILD_PERMITS_LIVE               this build refuses LIVE certification outright: always FAIL here
16 AUTHORIZATION_LEDGER_INTACT      ledger chain intact and not behind its revocation anchor
17 LIVE_BETA_ENROLLMENT_CURRENT     the account is enrolled in live beta Stage A or B and its authorization
                                    window has not expired (refused automatically after expiry)

The gate is consulted by the execution boundary before any LIVE entry; it opens nothing by itself.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

from .ledger import GoLiveLedger, fold_approvals, fold_consent, fold_demo, fold_switch, latest_policy

PASS, FAIL, BLOCKED_PENDING_POLICY, CHECK_ERROR = "PASS", "FAIL", "BLOCKED_PENDING_POLICY", "CHECK_ERROR"
GATE_VERSION = "1.0.0"
DAY_MS = 86_400_000
CONDITION_IDS = (
    "VENUE_ADAPTER_CERTIFIED", "STRATEGY_RESEARCH_PASSED", "STRATEGY_DEMO_RUN_COMPLETED", "FAMILY_STAGE_PERMITS_LIVE",
    "CUSTOMER_LIVE_RISK_CONSENT", "IDENTITY_REQUIREMENT_SATISFIED", "ACCOUNT_CONNECTION_VALID", "RISK_PROFILE_VALID",
    "ACCOUNT_CAPITAL_RULES", "OPERATOR_LIVE_SWITCH_ON", "OPERATOR_APPROVAL_AUDITED", "LIVE_ORDER_SUBMISSION_ENABLED",
    "KILL_SWITCHES_CLEAR", "NO_OPEN_EXECUTION_UNCERTAINTY", "BUILD_PERMITS_LIVE", "AUTHORIZATION_LEDGER_INTACT",
    "LIVE_BETA_ENROLLMENT_CURRENT",
)
#: This build refuses LIVE venue certification (certification.py: LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD)
#: and caps families at M6. Changing that is a separate, reviewed release -- never a setting.
BUILD_PERMITS_LIVE = False
VALID_RISK_LEVELS = ("conservative", "balanced", "aggressive")


@dataclass(frozen=True)
class Request:
    account_id: str
    user_id: str
    family_id: str
    family_version: str
    venue: str
    environment: str = "LIVE"


@dataclass
class Condition:
    id: str
    status: str
    reason: str = ""
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Decision:
    request: Request
    conditions: List[Condition]
    evaluated_at_ms: int
    gate_version: str = GATE_VERSION

    @property
    def authorized(self) -> bool:
        return bool(self.conditions) and all(c.status == PASS for c in self.conditions)

    @property
    def blocking(self) -> List[Condition]:
        return [c for c in self.conditions if c.status != PASS]

    def to_plain(self) -> Dict[str, Any]:
        return {"request": asdict(self.request), "authorized": self.authorized, "evaluated_at_ms": self.evaluated_at_ms,
                "gate_version": self.gate_version, "conditions": [asdict(c) for c in self.conditions],
                "blocking": [c.id for c in self.blocking]}


@dataclass
class Sources:
    """Everything outside the ledger the gate reads. Each callable answers one question; the production wiring
    (``production_sources``) binds them to the platform's existing authorities."""
    adapter_certified: Callable[[str, str, int], tuple]                 # (venue, env, now) -> (ok, reason)
    family_entry: Callable[[str], Optional[Mapping[str, Any]]]          # family_id -> registry entry (plain)
    identity_permits: Callable[[str, str], tuple]                       # (user_id, action) -> (ok, reason)
    account: Callable[[str], Optional[Mapping[str, Any]]]               # account_id -> broker account row
    bot_for_account: Callable[[str], Optional[Mapping[str, Any]]]       # account_id -> its single bot row
    live_submission_enabled: Callable[[], bool]
    kill_switch: Callable[[Optional[str]], bool]                        # scope (None = platform) -> ON?
    open_uncertainty: Callable[[str], List[str]]                        # account_id -> open uncertainty codes
    beta_budget_ceiling: Callable[[str], Optional[float]] = lambda account_id: None
    now_ms: Callable[[], int] = None  # type: ignore[assignment]


class GoLiveGate:
    def __init__(self, ledger: GoLiveLedger, sources: Sources) -> None:
        self.ledger = ledger
        self.sources = sources

    def evaluate(self, request: Request, *, now_ms: Optional[int] = None) -> Decision:
        if now_ms is None:
            if self.sources.now_ms is not None:
                now_ms = self.sources.now_ms()
            else:
                with self.ledger.db.transaction(write=False) as conn:
                    now_ms = conn.now_ms()
        try:
            events = self.ledger.events()
            ledger_error = None
        except Exception as exc:  # ledger unreadable: every ledger-based condition fails closed
            events, ledger_error = [], f"{type(exc).__name__}: {exc}"[:200]
        policy = latest_policy(events)
        ctx = dict(request=request, now=int(now_ms), events=events, policy=policy, ledger_error=ledger_error)
        out: List[Condition] = []
        for cid in CONDITION_IDS:
            check = getattr(self, "_" + cid.lower())
            try:
                out.append(check(**ctx))
            except Exception as exc:
                out.append(Condition(cid, CHECK_ERROR, f"{type(exc).__name__}: {exc}"[:300]))
        return Decision(request, out, int(now_ms))

    # ── checks (each independent) ─────────────────────────────────────────────────────────────────────────
    def _venue_adapter_certified(self, request, now, **_):
        ok, why = self.sources.adapter_certified(request.venue.lower(), request.environment, now)
        return Condition("VENUE_ADAPTER_CERTIFIED", PASS if ok else FAIL, "" if ok else why)

    def _strategy_research_passed(self, request, **_):
        entry = self.sources.family_entry(request.family_id)
        if entry is None:
            return Condition("STRATEGY_RESEARCH_PASSED", FAIL, "FAMILY_NOT_REGISTERED")
        status = entry.get("certification_status")
        ok = status == "CERTIFIED" and entry.get("family_version") == request.family_version
        reason = "" if ok else (f"RESEARCH_{status}" if status != "CERTIFIED" else "FAMILY_VERSION_NOT_CERTIFIED")
        return Condition("STRATEGY_RESEARCH_PASSED", PASS if ok else FAIL, reason,
                         {"certification_status": status, "registered_version": entry.get("family_version")})

    def _strategy_demo_run_completed(self, request, events, policy, ledger_error, **_):
        if ledger_error:
            return Condition("STRATEGY_DEMO_RUN_COMPLETED", FAIL, "LEDGER_UNREADABLE:" + ledger_error)
        min_days = policy.get("MIN_DEMO_RUN_DAYS")
        demo = fold_demo(events, request.family_id, request.family_version)
        passing = [r for r in demo.valid_runs() if r.payload.get("outcome") == "PASS"
                   and r.payload.get("venue") == request.venue.upper()]
        if min_days is None:
            return Condition("STRATEGY_DEMO_RUN_COMPLETED", BLOCKED_PENDING_POLICY, "MIN_DEMO_RUN_DAYS_NOT_SET",
                             {"passing_runs": len(passing)})
        long_enough = [r for r in passing
                       if (int(r.payload["ended_at_ms"]) - int(r.payload["started_at_ms"])) >= float(min_days) * DAY_MS]
        ok = bool(long_enough)
        return Condition("STRATEGY_DEMO_RUN_COMPLETED", PASS if ok else FAIL,
                         "" if ok else ("NO_PASSING_DEMO_RUN" if not passing else "DEMO_RUN_SHORTER_THAN_POLICY"),
                         {"passing_runs": len(passing), "min_days": min_days,
                          "run_refs": [r.payload.get("run_ref") for r in long_enough]})

    def _family_stage_permits_live(self, request, **_):
        entry = self.sources.family_entry(request.family_id)
        if entry is None:
            return Condition("FAMILY_STAGE_PERMITS_LIVE", FAIL, "FAMILY_NOT_REGISTERED")
        modes = list(entry.get("allowed_modes") or [])
        ok = request.environment in modes
        return Condition("FAMILY_STAGE_PERMITS_LIVE", PASS if ok else FAIL,
                         "" if ok else f"STAGE_{entry.get('governance_stage')}_DOES_NOT_PERMIT_{request.environment}",
                         {"stage": entry.get("governance_stage"), "allowed_modes": modes})

    def _customer_live_risk_consent(self, request, events, policy, ledger_error, **_):
        if ledger_error:
            return Condition("CUSTOMER_LIVE_RISK_CONSENT", FAIL, "LEDGER_UNREADABLE:" + ledger_error)
        st = fold_consent(events, request.account_id)
        required = policy.get("LIVE_CONSENT_VERSION")
        if not st.active:
            return Condition("CUSTOMER_LIVE_RISK_CONSENT", FAIL, "CONSENT_REVOKED" if st.revoked else "CONSENT_MISSING")
        if st.user_id != request.user_id:
            return Condition("CUSTOMER_LIVE_RISK_CONSENT", FAIL, "CONSENT_NOT_GIVEN_BY_ACCOUNT_OWNER")
        if required is None:
            return Condition("CUSTOMER_LIVE_RISK_CONSENT", BLOCKED_PENDING_POLICY, "LIVE_CONSENT_VERSION_NOT_SET")
        ok = st.consent_version == required
        return Condition("CUSTOMER_LIVE_RISK_CONSENT", PASS if ok else FAIL, "" if ok else "CONSENT_VERSION_OUTDATED",
                         {"consent_version": st.consent_version, "required": required})

    def _identity_requirement_satisfied(self, request, **_):
        ok, why = self.sources.identity_permits(request.user_id, "start_live_trading")
        return Condition("IDENTITY_REQUIREMENT_SATISFIED", PASS if ok else FAIL, "" if ok else why)

    def _account_connection_valid(self, request, **_):
        acct = self.sources.account(request.account_id)
        if acct is None:
            return Condition("ACCOUNT_CONNECTION_VALID", FAIL, "ACCOUNT_NOT_FOUND")
        problems = []
        if str(acct.get("user_id")) != str(request.user_id):
            problems.append("ACCOUNT_OWNERSHIP_MISMATCH")
        if str(acct.get("status", "")).lower() not in ("connected", "active"):
            problems.append(f"ACCOUNT_STATUS_{str(acct.get('status')).upper()}")
        if str(acct.get("environment", "")).upper() != request.environment:
            problems.append("ACCOUNT_ENVIRONMENT_NOT_LIVE")
        if str(acct.get("broker_id", "")).lower() != request.venue.lower():
            problems.append("ACCOUNT_VENUE_MISMATCH")
        return Condition("ACCOUNT_CONNECTION_VALID", FAIL if problems else PASS, ",".join(problems))

    def _risk_profile_valid(self, request, **_):
        bot = self.sources.bot_for_account(request.account_id)
        if bot is None:
            return Condition("RISK_PROFILE_VALID", FAIL, "NO_SINGLE_BOT_FOR_ACCOUNT")
        level = str(bot.get("risk_level") or "").lower()
        ok = level in VALID_RISK_LEVELS
        return Condition("RISK_PROFILE_VALID", PASS if ok else FAIL, "" if ok else f"RISK_LEVEL_INVALID:{level or 'none'}",
                         {"risk_level": level})

    def _account_capital_rules(self, request, events, now, **_):
        bot = self.sources.bot_for_account(request.account_id)
        if bot is None:
            return Condition("ACCOUNT_CAPITAL_RULES", FAIL, "NO_SINGLE_BOT_FOR_ACCOUNT")
        try:
            alloc = float(bot.get("capital_allocation") or 0)
        except (TypeError, ValueError):
            alloc = 0.0
        if not math.isfinite(alloc) or alloc <= 0:
            return Condition("ACCOUNT_CAPITAL_RULES", FAIL, "CAPITAL_ALLOCATION_MISSING")
        from .beta import fold_enrollment
        enrolled = fold_enrollment(events, request.account_id)
        ceiling = self.sources.beta_budget_ceiling(request.account_id)
        if enrolled.budget is not None:
            ceiling = float(enrolled.budget) if ceiling is None else min(float(ceiling), float(enrolled.budget))
        if ceiling is not None and alloc > float(ceiling):
            return Condition("ACCOUNT_CAPITAL_RULES", FAIL, "CAPITAL_ABOVE_BETA_BUDGET_CEILING",
                             {"allocation": alloc, "ceiling": ceiling})
        return Condition("ACCOUNT_CAPITAL_RULES", PASS, "", {"allocation": alloc, "ceiling": ceiling})

    def _operator_live_switch_on(self, request, events, ledger_error, **_):
        if ledger_error:
            return Condition("OPERATOR_LIVE_SWITCH_ON", FAIL, "LEDGER_UNREADABLE:" + ledger_error)
        st = fold_switch(events, request.venue.upper())
        ev = {"pending_request": st.pending_request.event_id if st.pending_request else None,
              "last_change": st.last_change.event_type if st.last_change else None}
        return Condition("OPERATOR_LIVE_SWITCH_ON", PASS if st.on else FAIL, "" if st.on else "LIVE_SWITCH_OFF", ev)

    def _operator_approval_audited(self, request, events, now, ledger_error, **_):
        if ledger_error:
            return Condition("OPERATOR_APPROVAL_AUDITED", FAIL, "LEDGER_UNREADABLE:" + ledger_error)
        matching = [a for a in fold_approvals(events).values()
                    if a.account_id == request.account_id and a.family_id == request.family_id
                    and a.venue == request.venue.upper()]
        active = [a for a in matching if a.active(now)]
        for a in active:
            if a.granted.payload.get("approved_by") == a.requested.payload.get("requested_by"):  # pragma: no cover
                return Condition("OPERATOR_APPROVAL_AUDITED", FAIL, "SELF_APPROVED")
        if active:
            a = active[-1]
            return Condition("OPERATOR_APPROVAL_AUDITED", PASS, "", {"approval_id": a.approval_id,
                                                                    "granted_event": a.granted.event_id})
        reason = "APPROVAL_MISSING"
        if any(a.revoked for a in matching):
            reason = "APPROVAL_REVOKED"
        elif any(a.granted and not a.active(now) for a in matching):
            reason = "APPROVAL_EXPIRED"
        elif any(a.granted is None and a.rejected is None for a in matching):
            reason = "APPROVAL_PENDING_SECOND_OPERATOR"
        return Condition("OPERATOR_APPROVAL_AUDITED", FAIL, reason)

    def _live_order_submission_enabled(self, **_):
        ok = bool(self.sources.live_submission_enabled())
        return Condition("LIVE_ORDER_SUBMISSION_ENABLED", PASS if ok else FAIL,
                         "" if ok else "LIVE_ORDER_SUBMISSION_DISABLED")

    def _kill_switches_clear(self, request, **_):
        on = []
        if self.sources.kill_switch(None):
            on.append("PLATFORM")
        if self.sources.kill_switch(request.account_id):
            on.append("ACCOUNT")
        entry = self.sources.family_entry(request.family_id) or {}
        if str(entry.get("kill_switch_state", "OFF")).upper() == "ON" or entry.get("disabled"):
            on.append("FAMILY")
        return Condition("KILL_SWITCHES_CLEAR", FAIL if on else PASS, ",".join(f"{s}_KILL_SWITCH_ON" for s in on))

    def _no_open_execution_uncertainty(self, request, **_):
        codes = list(self.sources.open_uncertainty(request.account_id))
        return Condition("NO_OPEN_EXECUTION_UNCERTAINTY", FAIL if codes else PASS, ",".join(codes))

    def _build_permits_live(self, **_):
        return Condition("BUILD_PERMITS_LIVE", PASS if BUILD_PERMITS_LIVE else FAIL,
                         "" if BUILD_PERMITS_LIVE else "LIVE_NOT_PERMITTED_IN_THIS_BUILD")

    def _live_beta_enrollment_current(self, request, events, now, ledger_error, **_):
        if ledger_error:
            return Condition("LIVE_BETA_ENROLLMENT_CURRENT", FAIL, "LEDGER_UNREADABLE:" + ledger_error)
        from .beta import fold_enrollment
        st = fold_enrollment(events, request.account_id)
        ok, why = st.current(now)
        if ok and st.user_id != request.user_id:
            ok, why = False, "ENROLLMENT_USER_MISMATCH"
        return Condition("LIVE_BETA_ENROLLMENT_CURRENT", PASS if ok else FAIL, why,
                         {"stage": st.stage, "authorized_until_ms": st.enrolled.payload.get("authorized_until_ms")
                          if st.enrolled else None})

    def _authorization_ledger_intact(self, ledger_error, **_):
        if ledger_error:
            return Condition("AUTHORIZATION_LEDGER_INTACT", FAIL, "LEDGER_UNREADABLE:" + ledger_error)
        ok, why = self.ledger.verify()
        return Condition("AUTHORIZATION_LEDGER_INTACT", PASS if ok else FAIL, why)


# ── production wiring ─────────────────────────────────────────────────────────────────────────────────────────
def production_sources(runtime_db: Any) -> Sources:
    """Bind the gate to the platform's existing authorities on the runtime (SQLite) database. Anything that
    cannot be read answers in the refusing direction."""

    def adapter_certified(venue, env, now):
        from app.exchange.platform_adapter.certification import adapter_certified as certified
        return certified(runtime_db, venue, env, now)

    def family_entry(family_id):
        from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
        entry = FamilyRegistry(runtime_db).entry(family_id)
        return entry.to_plain() if entry is not None else None

    def identity_permits(user_id, action):
        try:
            from shared_lib.identity import identity_permits as permits   # Step 8.3 identity service
        except ImportError:
            return False, "IDENTITY_SERVICE_UNAVAILABLE"
        result = permits(runtime_db, user_id, action)
        if isinstance(result, tuple):
            return bool(result[0]), str(result[1] if len(result) > 1 else "")
        ok = bool(getattr(result, "permitted", result))
        return ok, "" if ok else str(getattr(result, "reason", "IDENTITY_NOT_SATISFIED"))

    def account(account_id):
        with runtime_db.connect() as conn:
            row = conn.execute("SELECT id, user_id, broker_id, status, environment FROM broker_accounts WHERE id=?",
                               (account_id,)).fetchone()
        return dict(row) if row is not None else None

    def bot_for_account(account_id):
        with runtime_db.connect() as conn:
            rows = conn.execute("SELECT id, user_id, risk_level, capital_allocation FROM bot_instances "
                                "WHERE broker_account_id=? AND COALESCE(status,'') NOT IN ('archived','deleted')",
                                (account_id,)).fetchall()
        return dict(rows[0]) if len(rows) == 1 else None

    def live_submission_enabled():
        from shared_lib.core.production import order_submission_gate
        return bool(order_submission_gate("LIVE")["enabled"])

    def kill_switch(scope):
        from app.trading_intelligence.governance.promotion import PromotionGovernance
        gov = PromotionGovernance(runtime_db)
        return gov.kill_switch_on(scope=scope) if scope else gov.kill_switch_on()

    def open_uncertainty(account_id):
        from shared_lib.persistence.cati_execution_schema import INTENT_TABLE, PROTECTION_TABLE, PROTECTION_UNSETTLED
        unresolved_orders = ("SUBMITTING", "UNKNOWN", "CANCELING")   # sent; venue outcome not yet confirmed
        codes = []
        with runtime_db.connect() as conn:
            for table, column, states, code in (
                    (INTENT_TABLE, "state", unresolved_orders, "EXECUTION_INTENT_OUTCOME_UNRESOLVED"),
                    (PROTECTION_TABLE, "status", PROTECTION_UNSETTLED, "PROTECTION_UNSETTLED")):
                try:
                    n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE account_id=? AND {column} IN ("
                                     + ",".join("?" * len(states)) + ")", (account_id, *states)).fetchone()[0]
                except Exception as exc:
                    if "no such table" in str(exc):
                        continue        # not created on this database yet: nothing of that kind exists
                    codes.append(f"UNCERTAINTY_UNREADABLE:{table}")
                    continue
                if n:
                    codes.append(code)
        return codes

    return Sources(adapter_certified=adapter_certified, family_entry=family_entry, identity_permits=identity_permits,
                   account=account, bot_for_account=bot_for_account, live_submission_enabled=live_submission_enabled,
                   kill_switch=kill_switch, open_uncertainty=open_uncertainty)


def production_gate(runtime_db: Any) -> GoLiveGate:
    """The gate the execution boundary consults. Without a platform database every live entry is refused
    (the boundary turns the raised error into a refusal)."""
    import os
    from shared_lib.persistence.sql import Database
    url = os.environ.get("PLATFORM_DATABASE_URL")
    if not url:
        raise RuntimeError("PLATFORM_DATABASE_URL_NOT_CONFIGURED")
    return GoLiveGate(GoLiveLedger(Database(url, pool_size=4)), production_sources(runtime_db))


def summary(decision: Decision) -> str:
    return json.dumps(decision.to_plain(), sort_keys=True, default=str)
