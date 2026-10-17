"""Go-live console and live-beta controls (Section H, Steps 6.1-6.3).

Operator routes (``require_admin_emergency`` -- the short-lived service token minted for a verified operator):

    GET  /api/v1/admin/golive/matrix              every gate condition for (account, family, version, venue)
    GET  /api/v1/admin/golive/accounts            per-account eligibility view of every LIVE broker account
    GET  /api/v1/admin/golive/switches            per-exchange live switch state and pending requests
    GET  /api/v1/admin/golive/history             the authorization ledger and its integrity
    POST /api/v1/admin/golive/switches/{venue}/request | approve | reject | off
    POST /api/v1/admin/golive/approvals           request an account approval
    POST /api/v1/admin/golive/approvals/{id}/grant | reject | revoke
    POST /api/v1/admin/golive/demo-runs           record demo-run evidence; /demo-runs/{event_id}/invalidate
    POST /api/v1/admin/golive/policy              owner policy values (minimum demo days, consent version...)
    POST /api/v1/admin/golive/beta/stage-a        enrol the operator's own account (Stage A)
    POST /api/v1/admin/golive/beta/invitations    invite a customer account (Stage B)
    POST /api/v1/admin/golive/beta/{account_id}/revoke-invitation | disable
    GET  /api/v1/admin/golive/beta/{account_id}   status; /evidence for the export bundle
    POST /api/v1/admin/golive/beta/{account_id}/reviews/daily | weekly

Customer routes (the customer's own access token; ownership of the account is checked):

    POST /api/v1/golive/consent | /consent/revoke
    POST /api/v1/golive/beta/{account_id}/accept | /leave
    GET  /api/v1/golive/beta/{account_id}

None of these routes enables live trading: the gate decides, and in this build it refuses (BUILD_PERMITS_LIVE).
Approvals and switch openings need a second operator; closing needs one.
"""
from __future__ import annotations

import functools
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.api.emergency import get_db
from app.core.auth import get_current_user_id, require_admin_emergency
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/api/v1/admin/golive", tags=["Admin Go-Live"])
customer_router = APIRouter(prefix="/api/v1/golive", tags=["Go-Live (customer)"])
CHANNEL = "admin_api:/api/v1/admin/golive"


class Reason(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


class SwitchDecision(Reason):
    request_event_id: str = Field(min_length=8, max_length=64)


class ApprovalRequest(Reason):
    account_id: str
    family_id: str
    venue: str
    expires_at_ms: Optional[int] = None


class DemoRun(BaseModel):
    family_id: str
    family_version: str
    venue: str
    run_ref: str = Field(min_length=3)
    started_at_ms: int
    ended_at_ms: int
    outcome: str = Field(pattern="^(PASS|FAIL)$")
    evidence: Dict[str, Any] = Field(default_factory=dict)


class PolicyValue(Reason):
    key: str
    value: Any


class StageA(Reason):
    account_id: str
    authorized_until_ms: int
    budget: str
    risk_level: str = "conservative"


class Invitation(Reason):
    account_id: str
    user_id: str
    budget_ceiling: str
    expires_at_ms: int


class DailyReview(BaseModel):
    stage: str = Field(pattern="^(A|B)$")
    day: str
    shadow: List[Dict[str, Any]] = Field(default_factory=list)
    live_fills: List[Dict[str, Any]] = Field(default_factory=list)
    equity: List[float] = Field(default_factory=list)
    incidents: List[str] = Field(default_factory=list)
    limits: Dict[str, Any] = Field(default_factory=dict)


class WeeklyReview(BaseModel):
    stage: str = Field(pattern="^(A|B)$")
    week_start: str
    week_end: str
    expectations: Optional[Dict[str, Any]] = None


class Consent(BaseModel):
    account_id: str
    consent_version: str
    text_sha256: str = Field(min_length=64, max_length=64)


class AccountRef(BaseModel):
    account_id: str


class Accept(BaseModel):
    budget: str
    risk_level: str
    consent_ref: str
    authorized_until_ms: int


@functools.lru_cache(maxsize=4)
def _database(url: str):
    from shared_lib.persistence.sql import Database
    return Database(url, pool_size=4)


def _parts():
    from app.golive.authorization import GoLiveAuthority
    from app.golive.beta import BetaReviews, LiveBeta
    from app.golive.ledger import GoLiveLedger
    url = os.environ.get("PLATFORM_DATABASE_URL")
    if not url:
        raise HTTPException(status_code=503, detail="PLATFORM_DATABASE_URL_NOT_CONFIGURED")
    ledger = GoLiveLedger(_database(url))
    return ledger, GoLiveAuthority(ledger), LiveBeta(ledger), BetaReviews(ledger.db)


def _op(admin_id: str):
    from app.golive.ledger import Operator
    return Operator(operator_id=str(admin_id), channel=CHANNEL)


def _do(call):
    from app.golive.ledger import GoLiveError
    try:
        result = call()
    except GoLiveError as exc:
        return JSONResponse(status_code=409, content={"detail": str(exc), "reason": exc.code})
    return {"ok": True, "result": result}


def _gate(db: DB, ledger):
    from app.golive.gate import GoLiveGate, production_sources
    return GoLiveGate(ledger, production_sources(db))


def _own_account(db: DB, account_id: str, user_id: str) -> None:
    with db.connect() as conn:
        row = conn.execute("SELECT user_id FROM broker_accounts WHERE id=?", (account_id,)).fetchone()
    if row is None or str(row[0]) != str(user_id):
        raise HTTPException(status_code=404, detail="ACCOUNT_NOT_FOUND")


# ── operator reads ─────────────────────────────────────────────────────────────────────────────────────
@router.get("/matrix")
def matrix(account_id: str, user_id: str, family_id: str, family_version: str, venue: str,
           admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    from app.golive.gate import Request
    ledger, *_ = _parts()
    return _gate(db, ledger).evaluate(Request(account_id, user_id, family_id, family_version, venue)).to_plain()


@router.get("/accounts")
def accounts(family_id: str = "DAILY_TREND", admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    from app.golive.gate import Request
    from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
    ledger, *_ = _parts()
    entry = FamilyRegistry(db).entry(family_id)
    version = entry.definition.family_version if entry is not None else ""
    with db.connect() as conn:
        rows = conn.execute("SELECT id, user_id, broker_id FROM broker_accounts WHERE LOWER(COALESCE(environment,'live'))"
                            "='live' ORDER BY id").fetchall()
    gate = _gate(db, ledger)
    out = []
    for r in rows:
        d = gate.evaluate(Request(str(r[0]), str(r[1]), family_id, version, str(r[2])))
        out.append({"account_id": r[0], "venue": r[2], "authorized": d.authorized,
                    "blocking": [{"id": c.id, "status": c.status, "reason": c.reason} for c in d.blocking]})
    return {"family_id": family_id, "family_version": version, "accounts": out}


@router.get("/switches")
def switches(admin_id: str = Depends(require_admin_emergency)):
    from app.exchange.platform_adapter.contract import VENUES
    from app.golive.ledger import fold_switch
    ledger, *_ = _parts()
    events = ledger.events(scope_type="VENUE")
    out = []
    for v in VENUES:
        st = fold_switch(events, v.upper())
        out.append({"venue": v.upper(), "on": st.on,
                    "pending_request": {"event_id": st.pending_request.event_id,
                                        "requested_by": st.pending_request.payload.get("requested_by"),
                                        "reason": st.pending_request.payload.get("reason")} if st.pending_request else None,
                    "last_change": {"type": st.last_change.event_type, "actor": st.last_change.actor_ref,
                                    "at": st.last_change.recorded_at} if st.last_change else None})
    return {"switches": out}


@router.get("/history")
def history(limit: int = 500, admin_id: str = Depends(require_admin_emergency)):
    ledger, *_ = _parts()
    ok, why = ledger.verify()
    events = ledger.events()[-max(1, min(limit, 5000)):]
    return {"verified": ok, "reason": why, "events": [
        {"seq": e.seq, "type": e.event_type, "scope_type": e.scope_type, "scope_id": e.scope_id, "actor": e.actor_ref,
         "at": e.recorded_at, "payload": e.payload, "hash": e.hash} for e in events]}


# ── operator actions ───────────────────────────────────────────────────────────────────────────────────
@router.post("/switches/{venue}/request")
def switch_request(venue: str, body: Reason, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.request_switch_on(venue, _op(admin_id), reason=body.reason))


@router.post("/switches/{venue}/approve")
def switch_approve(venue: str, body: SwitchDecision, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.approve_switch_on(venue, body.request_event_id, _op(admin_id), reason=body.reason))


@router.post("/switches/{venue}/reject")
def switch_reject(venue: str, body: SwitchDecision, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.reject_switch_on(venue, body.request_event_id, _op(admin_id), reason=body.reason))


@router.post("/switches/{venue}/off")
def switch_off(venue: str, body: Reason, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.switch_off(venue, _op(admin_id), reason=body.reason))


@router.post("/approvals")
def approval_request(body: ApprovalRequest, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.request_approval(account_id=body.account_id, family_id=body.family_id, venue=body.venue,
                                             operator=_op(admin_id), reason=body.reason, expires_at_ms=body.expires_at_ms))


@router.post("/approvals/{approval_id}/{action}")
def approval_decide(approval_id: str, action: str, body: Reason, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    calls = {"grant": auth.grant_approval, "reject": auth.reject_approval, "revoke": auth.revoke_approval}
    if action not in calls:
        raise HTTPException(status_code=404, detail="UNKNOWN_ACTION")
    return _do(lambda: calls[action](approval_id, _op(admin_id), reason=body.reason))


@router.post("/demo-runs")
def demo_run(body: DemoRun, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.record_demo_run(operator=_op(admin_id), **body.model_dump()))


@router.post("/demo-runs/{event_id}/invalidate")
def demo_invalidate(event_id: str, family_id: str, body: Reason, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.invalidate_demo_run(family_id=family_id, run_event_id=event_id, operator=_op(admin_id),
                                                reason=body.reason))


@router.post("/policy")
def policy(body: PolicyValue, admin_id: str = Depends(require_admin_emergency)):
    _, auth, *_ = _parts()
    return _do(lambda: auth.set_policy(body.key, body.value, _op(admin_id), reason=body.reason))


@router.post("/beta/stage-a")
def stage_a(body: StageA, admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    _, _, beta, _ = _parts()
    _own_account(db, body.account_id, admin_id)   # Stage A is the operator's own account
    return _do(lambda: beta.enroll_stage_a(account_id=body.account_id, user_id=str(admin_id), operator=_op(admin_id),
                                           authorized_until_ms=body.authorized_until_ms, budget=body.budget,
                                           risk_level=body.risk_level, reason=body.reason))


@router.post("/beta/invitations")
def invite(body: Invitation, admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    _, _, beta, _ = _parts()
    _own_account(db, body.account_id, body.user_id)
    return _do(lambda: beta.invite(account_id=body.account_id, user_id=body.user_id, operator=_op(admin_id),
                                   budget_ceiling=body.budget_ceiling, expires_at_ms=body.expires_at_ms, reason=body.reason))


@router.post("/beta/{account_id}/revoke-invitation")
def revoke_invitation(account_id: str, body: Reason, admin_id: str = Depends(require_admin_emergency)):
    _, _, beta, _ = _parts()
    return _do(lambda: beta.revoke_invitation(account_id=account_id, operator=_op(admin_id), reason=body.reason))


@router.post("/beta/{account_id}/disable")
def beta_disable(account_id: str, body: Reason, admin_id: str = Depends(require_admin_emergency)):
    _, _, beta, _ = _parts()
    return _do(lambda: beta.disable(account_id=account_id, operator=_op(admin_id), reason=body.reason))


@router.get("/beta/{account_id}")
def beta_status(account_id: str, admin_id: str = Depends(require_admin_emergency)):
    _, _, beta, _ = _parts()
    return beta.status(account_id)


@router.get("/beta/{account_id}/evidence")
def beta_evidence(account_id: str, admin_id: str = Depends(require_admin_emergency)):
    from app.golive.beta import export_evidence
    ledger, _, _, reviews = _parts()
    return export_evidence(ledger, reviews, account_id)


@router.post("/beta/{account_id}/reviews/daily")
def review_daily(account_id: str, body: DailyReview, admin_id: str = Depends(require_admin_emergency)):
    _, _, _, reviews = _parts()
    return reviews.daily(account_id=account_id, reviewer_ref=_op(admin_id).ref, **body.model_dump())


@router.post("/beta/{account_id}/reviews/weekly")
def review_weekly(account_id: str, body: WeeklyReview, admin_id: str = Depends(require_admin_emergency)):
    _, _, _, reviews = _parts()
    return reviews.weekly_summary(scope_id=account_id, reviewer_ref=_op(admin_id).ref, **body.model_dump())


# ── customer ───────────────────────────────────────────────────────────────────────────────────────────
@customer_router.post("/consent")
def consent(body: Consent, user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    _own_account(db, body.account_id, user_id)
    _, auth, *_ = _parts()
    return _do(lambda: auth.give_consent(account_id=body.account_id, user_id=str(user_id),
                                         consent_version=body.consent_version, text_sha256=body.text_sha256))


@customer_router.post("/consent/revoke")
def consent_revoke(body: AccountRef, user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    _own_account(db, body.account_id, user_id)
    _, auth, *_ = _parts()
    return _do(lambda: auth.revoke_consent(account_id=body.account_id, user_id=str(user_id)))


@customer_router.post("/beta/{account_id}/accept")
def beta_accept(account_id: str, body: Accept, user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    _own_account(db, account_id, user_id)
    _, _, beta, _ = _parts()
    return _do(lambda: beta.accept(account_id=account_id, user_id=str(user_id), **body.model_dump()))


@customer_router.post("/beta/{account_id}/leave")   # leaving the beta (a path word "withdraw" is reserved for funds)
def beta_withdraw(account_id: str, user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    _own_account(db, account_id, user_id)
    _, _, beta, _ = _parts()
    return _do(lambda: beta.withdraw(account_id=account_id, user_id=str(user_id)))


@customer_router.get("/beta/{account_id}")
def beta_mine(account_id: str, user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    _own_account(db, account_id, user_id)
    _, _, beta, _ = _parts()
    return beta.status(account_id)
