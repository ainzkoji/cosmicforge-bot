"""Controlled live beta, Stages A and B (Section H, Steps 6.2 and 6.3).

**Stage A** -- the operator's own account, the conservative risk level, the smallest permitted orders, a notional
ceiling set by the owner (``STAGE_A_MAX_NOTIONAL``), an explicit authorization window, and a daily review against
the shadow comparison. The master plan asks for at least eight weeks; this module records elapsed evidence, it
does not create it.

**Stage B** -- invitation only. An operator invites a customer account with a budget ceiling (never above the
owner's ``STAGE_B_MAX_BUDGET``) and an expiry; the customer enrols with their own risk selection and budget and
may withdraw at any time; an operator may disable an enrolment at once. Weekly written summaries compare the
period with the registered backtest expectations.

Neither an invitation nor an enrolment enables anything. Enrolment is one condition the go-live gate reads
(``LIVE_BETA_ENROLLMENT_CURRENT``); every other condition -- research certification, venue certification, the
live switch, the second-operator approval, consent, kill switches -- still applies, and in this build the gate
refuses every live entry. An enrolment past its ``authorized_until_ms`` is refused automatically.
"""
from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from shared_lib.persistence.sql import Database

from .ledger import Event, GoLiveError, GoLiveLedger, Operator, _require_operator, latest_policy

STAGE_A, STAGE_B = "A", "B"


@dataclass
class Enrollment:
    account_id: str
    stage: Optional[str] = None
    user_id: Optional[str] = None
    invitation: Optional[Event] = None
    enrolled: Optional[Event] = None
    ended: Optional[Event] = None            # withdrawn, disabled or invitation revoked

    def current(self, now_ms: int) -> tuple:
        if self.enrolled is None:
            return False, "NOT_ENROLLED" if self.invitation is None else "INVITED_NOT_ENROLLED"
        if self.ended is not None:
            return False, {"BETA_WITHDRAWN": "BETA_WITHDRAWN", "BETA_DISABLED": "BETA_DISABLED"}.get(
                self.ended.event_type, "BETA_INVITATION_REVOKED")
        until = self.enrolled.payload.get("authorized_until_ms")
        if until is None or int(until) <= int(now_ms):
            return False, "BETA_AUTHORIZATION_EXPIRED"
        return True, ""

    @property
    def budget(self) -> Optional[Decimal]:
        if self.enrolled is None:
            return None
        return Decimal(str(self.enrolled.payload["budget"]))


def fold_enrollment(events: Iterable[Event], account_id: str) -> Enrollment:
    st = Enrollment(account_id)
    for e in events:
        if e.scope_type != "BETA" or e.scope_id != account_id:
            continue
        if e.event_type == "BETA_INVITED":
            st = Enrollment(account_id, STAGE_B, e.payload.get("user_id"), invitation=e)
        elif e.event_type == "BETA_ENROLLED":
            if e.payload.get("stage") == STAGE_A:
                st = Enrollment(account_id, STAGE_A, e.payload.get("user_id"), enrolled=e)
            elif st.invitation is not None and st.ended is None:
                st.enrolled = e
        elif e.event_type in ("BETA_WITHDRAWN", "BETA_DISABLED", "BETA_INVITATION_REVOKED"):
            st.ended = st.ended or e
    return st


class LiveBeta:
    def __init__(self, ledger: GoLiveLedger) -> None:
        self.ledger = ledger

    def _policy(self) -> Dict[str, Any]:
        return latest_policy(self.ledger.events(scope_type="POLICY"))

    def _now(self) -> int:
        with self.ledger.db.transaction(write=False) as conn:
            return conn.now_ms()

    # ── Stage A ──────────────────────────────────────────────────────────────────────────────────────────
    def enroll_stage_a(self, *, account_id: str, user_id: str, operator: Operator, authorized_until_ms: int,
                       budget: Any, risk_level: str, reason: str) -> None:
        op = _require_operator(operator)
        policy = self._policy()
        if user_id != op.operator_id:
            raise GoLiveError("STAGE_A_IS_THE_OPERATORS_OWN_ACCOUNT")
        if risk_level != "conservative":
            raise GoLiveError("STAGE_A_REQUIRES_CONSERVATIVE_RISK")
        ceiling = policy.get("STAGE_A_MAX_NOTIONAL")
        if ceiling is None:
            raise GoLiveError("STAGE_A_MAX_NOTIONAL_NOT_SET")
        if Decimal(str(budget)) <= 0 or Decimal(str(budget)) > Decimal(str(ceiling)):
            raise GoLiveError("STAGE_A_BUDGET_OUTSIDE_CEILING")
        if authorized_until_ms <= self._now():
            raise GoLiveError("AUTHORIZATION_WINDOW_IN_THE_PAST")
        self.ledger.append("BETA_ENROLLED", scope_type="BETA", scope_id=account_id, actor_ref=op.ref, subject_ref=user_id,
                           payload={"stage": STAGE_A, "user_id": user_id, "budget": str(budget), "risk_level": risk_level,
                                    "authorized_until_ms": int(authorized_until_ms), "reason": reason,
                                    "order_size_policy": "SMALLEST_PERMITTED"})

    # ── Stage B ──────────────────────────────────────────────────────────────────────────────────────────
    def invite(self, *, account_id: str, user_id: str, operator: Operator, budget_ceiling: Any, expires_at_ms: int,
               reason: str) -> str:
        op = _require_operator(operator)
        cap = self._policy().get("STAGE_B_MAX_BUDGET")
        if cap is None:
            raise GoLiveError("STAGE_B_MAX_BUDGET_NOT_SET")
        if Decimal(str(budget_ceiling)) <= 0 or Decimal(str(budget_ceiling)) > Decimal(str(cap)):
            raise GoLiveError("INVITATION_CEILING_ABOVE_POLICY")
        ev = self.ledger.append("BETA_INVITED", scope_type="BETA", scope_id=account_id, actor_ref=op.ref,
                                subject_ref=user_id, payload={"user_id": user_id, "budget_ceiling": str(budget_ceiling),
                                                              "expires_at_ms": int(expires_at_ms), "reason": reason})
        return ev.event_id

    def revoke_invitation(self, *, account_id: str, operator: Operator, reason: str) -> None:
        op = _require_operator(operator)
        self.ledger.append("BETA_INVITATION_REVOKED", scope_type="BETA", scope_id=account_id, actor_ref=op.ref,
                           payload={"reason": reason})

    def accept(self, *, account_id: str, user_id: str, budget: Any, risk_level: str, consent_ref: str,
               authorized_until_ms: int) -> None:
        """The customer's own enrolment. Requires a live invitation for this account and user, a budget within
        its ceiling, an explicit consent reference, and an authorization window that ends no later than the
        invitation."""
        st = fold_enrollment(self.ledger.events(scope_type="BETA", scope_id=account_id), account_id)
        inv = st.invitation
        now = self._now()
        if inv is None or st.ended is not None:
            raise GoLiveError("NO_ACTIVE_INVITATION")
        if inv.payload.get("user_id") != user_id:
            raise GoLiveError("INVITATION_FOR_ANOTHER_USER")
        if int(inv.payload["expires_at_ms"]) <= now:
            raise GoLiveError("INVITATION_EXPIRED")
        if st.enrolled is not None:
            raise GoLiveError("ALREADY_ENROLLED")
        if Decimal(str(budget)) <= 0 or Decimal(str(budget)) > Decimal(inv.payload["budget_ceiling"]):
            raise GoLiveError("BUDGET_ABOVE_INVITATION_CEILING")
        if risk_level not in ("conservative", "balanced", "aggressive"):
            raise GoLiveError("RISK_LEVEL_INVALID")
        if not consent_ref:
            raise GoLiveError("CONSENT_REFERENCE_REQUIRED")
        until = min(int(authorized_until_ms), int(inv.payload["expires_at_ms"]))
        self.ledger.append("BETA_ENROLLED", scope_type="BETA", scope_id=account_id, actor_ref=f"user:{user_id}",
                           subject_ref=user_id, payload={"stage": STAGE_B, "user_id": user_id, "budget": str(budget),
                                                         "risk_level": risk_level, "consent_ref": consent_ref,
                                                         "authorized_until_ms": until,
                                                         "invitation_event_id": inv.event_id})

    def withdraw(self, *, account_id: str, user_id: str, reason: str = "CUSTOMER_WITHDREW") -> None:
        st = fold_enrollment(self.ledger.events(scope_type="BETA", scope_id=account_id), account_id)
        if st.user_id and st.user_id != user_id:
            raise GoLiveError("ENROLLMENT_OWNER_MISMATCH")
        self.ledger.append("BETA_WITHDRAWN", scope_type="BETA", scope_id=account_id, actor_ref=f"user:{user_id}",
                           subject_ref=user_id, payload={"reason": reason})

    def disable(self, *, account_id: str, operator: Operator, reason: str) -> None:
        op = _require_operator(operator)
        self.ledger.append("BETA_DISABLED", scope_type="BETA", scope_id=account_id, actor_ref=op.ref,
                           payload={"reason": reason or "EMERGENCY_DISABLE"})

    def status(self, account_id: str, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
        st = fold_enrollment(self.ledger.events(scope_type="BETA", scope_id=account_id), account_id)
        ok, why = st.current(self._now() if now_ms is None else now_ms)
        return {"account_id": account_id, "stage": st.stage, "current": ok, "reason": why,
                "budget": str(st.budget) if st.budget is not None else None,
                "authorized_until_ms": st.enrolled.payload.get("authorized_until_ms") if st.enrolled else None}


# ── daily and weekly review ───────────────────────────────────────────────────────────────────────────────────
def compare_shadow_live(shadow: Sequence[Mapping[str, Any]], live: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Per symbol: what the shadow book intended vs what the live account executed (signed quantity)."""
    s: Dict[str, Decimal] = {}
    l: Dict[str, Decimal] = {}
    for row in shadow:
        s[row["symbol"]] = s.get(row["symbol"], Decimal(0)) + Decimal(str(row["quantity"]))
    for row in live:
        l[row["symbol"]] = l.get(row["symbol"], Decimal(0)) + Decimal(str(row["quantity"]))
    rows = []
    for sym in sorted(set(s) | set(l)):
        want, got = s.get(sym, Decimal(0)), l.get(sym, Decimal(0))
        rows.append({"symbol": sym, "shadow_quantity": str(want), "live_quantity": str(got),
                     "difference": str(got - want), "match": got == want})
    return {"symbols": rows, "all_match": all(r["match"] for r in rows)}


def slippage_report(fills: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Signed slippage in bps against each fill's decision reference price (positive = worse than reference)."""
    bps = []
    for f in fills:
        ref, px = Decimal(str(f["reference_price"])), Decimal(str(f["price"]))
        sign = 1 if f["side"] == "BUY" else -1
        bps.append(float((px - ref) / ref * Decimal(10_000) * sign))
    if not bps:
        return {"fills": 0, "mean_bps": None, "worst_bps": None, "median_bps": None}
    return {"fills": len(bps), "mean_bps": round(statistics.fmean(bps), 4), "median_bps": round(statistics.median(bps), 4),
            "worst_bps": round(max(bps), 4)}


def drawdown(equity: Sequence[Any]) -> float:
    peak, worst = None, 0.0
    for e in equity:
        v = float(e)
        peak = v if peak is None else max(peak, v)
        if peak > 0:
            worst = max(worst, (peak - v) / peak)
    return round(worst, 6)


class BetaReviews:
    def __init__(self, db: Database) -> None:
        self.db = db

    def record(self, *, kind: str, stage: str, scope_id: str, period_start: str, period_end: str, reviewer_ref: str,
               document: Mapping[str, Any], verdict: str, supersedes: Optional[str] = None) -> str:
        body = json.dumps(dict(document), sort_keys=True, default=str)
        review_id = uuid.uuid4().hex
        with self.db.transaction() as conn:
            conn.execute("INSERT INTO beta_reviews (review_id, kind, stage, scope_id, period_start, period_end, reviewer_ref, "
                         "document, document_hash, verdict, supersedes, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                         (review_id, kind, stage, scope_id, period_start, period_end, reviewer_ref, body,
                          hashlib.sha256(body.encode()).hexdigest(), verdict, supersedes, conn.now_ms()))
        return review_id

    def daily(self, *, stage: str, account_id: str, day: str, reviewer_ref: str, shadow: Sequence[Mapping[str, Any]],
              live_fills: Sequence[Mapping[str, Any]], equity: Sequence[Any], incidents: Sequence[str],
              limits: Mapping[str, Any]) -> Dict[str, Any]:
        """``limits``: the owner's expected range, e.g. {"max_drawdown": 0.02, "max_worst_slippage_bps": 25}.
        Missing limits make the verdict INCOMPLETE rather than inventing a range."""
        comparison = compare_shadow_live(shadow, live_fills)
        slip = slippage_report(live_fills)
        dd = drawdown(equity)
        verdict = "INCOMPLETE"
        if "max_drawdown" in limits and "max_worst_slippage_bps" in limits:
            inside = (dd <= float(limits["max_drawdown"]) and comparison["all_match"] and not incidents
                      and (slip["worst_bps"] is None or slip["worst_bps"] <= float(limits["max_worst_slippage_bps"])))
            verdict = "WITHIN_EXPECTATION" if inside else "OUTSIDE_EXPECTATION"
        doc = {"shadow_vs_live": comparison, "slippage": slip, "drawdown": dd, "incidents": list(incidents),
               "limits": dict(limits), "fills": len(live_fills)}
        rid = self.record(kind="DAILY", stage=stage, scope_id=account_id, period_start=day, period_end=day,
                          reviewer_ref=reviewer_ref, document=doc, verdict=verdict)
        return {"review_id": rid, "verdict": verdict, **doc}

    def weekly_summary(self, *, stage: str, scope_id: str, week_start: str, week_end: str, reviewer_ref: str,
                       expectations: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        with self.db.transaction(write=False) as conn:
            rows = conn.all("SELECT period_start, verdict, document FROM beta_reviews WHERE kind='DAILY' AND stage=? AND "
                            "scope_id=? AND period_start >= ? AND period_end <= ? ORDER BY period_start",
                            (stage, scope_id, week_start, week_end))
        days = [{"day": r[0], "verdict": r[1], **json.loads(r[2])} for r in rows]
        worst_dd = max((d["drawdown"] for d in days), default=None)
        outside = [d["day"] for d in days if d["verdict"] == "OUTSIDE_EXPECTATION"]
        incomplete = [d["day"] for d in days if d["verdict"] == "INCOMPLETE"]
        if expectations is None:
            versus = {"status": "NO_REGISTERED_EXPECTATION",
                      "note": "No certified family: there is no registered backtest expectation to compare with."}
        else:
            versus = {"status": "COMPARED", "expected_max_drawdown": expectations.get("max_drawdown"),
                      "observed_max_drawdown": worst_dd,
                      "inside": worst_dd is not None and expectations.get("max_drawdown") is not None
                      and worst_dd <= float(expectations["max_drawdown"])}
        text = (f"Stage {stage}, {scope_id}, {week_start}..{week_end}: {len(days)} daily reviews; "
                f"{len(outside)} outside expectation; {len(incomplete)} incomplete; "
                f"worst drawdown {worst_dd if worst_dd is not None else 'n/a'}; "
                f"incidents {sum(len(d['incidents']) for d in days)}. Backtest comparison: {versus['status']}.")
        verdict = ("INCOMPLETE" if (not days or incomplete) else
                   "OUTSIDE_EXPECTATION" if outside else "WITHIN_EXPECTATION")
        doc = {"summary": text, "days": [d["day"] for d in days], "outside": outside, "incomplete": incomplete,
               "versus_backtest": versus}
        rid = self.record(kind="WEEKLY", stage=stage, scope_id=scope_id, period_start=week_start, period_end=week_end,
                          reviewer_ref=reviewer_ref, document=doc, verdict=verdict)
        return {"review_id": rid, "verdict": verdict, **doc}


def export_evidence(ledger: GoLiveLedger, reviews: BetaReviews, account_id: str) -> Dict[str, Any]:
    """Everything that authorized, limited and reviewed one beta account, with hashes a reviewer can verify."""
    events = [e for e in ledger.events() if e.scope_id == account_id or e.subject_ref == account_id
              or (e.scope_type == "APPROVAL" and e.subject_ref == account_id)]
    with reviews.db.transaction(write=False) as conn:
        rows = conn.all("SELECT review_id, kind, stage, period_start, period_end, verdict, document_hash, recorded_at "
                        "FROM beta_reviews WHERE scope_id=? ORDER BY recorded_at", (account_id,))
    ok, why = ledger.verify()
    bundle = {"account_id": account_id, "ledger_verified": ok, "ledger_reason": why,
              "events": [{"seq": e.seq, "type": e.event_type, "actor": e.actor_ref, "at": e.recorded_at, "hash": e.hash}
                         for e in events],
              "reviews": [dict(zip(("review_id", "kind", "stage", "period_start", "period_end", "verdict",
                                    "document_hash", "recorded_at"), r)) for r in rows]}
    bundle["bundle_sha256"] = hashlib.sha256(json.dumps(bundle, sort_keys=True).encode()).hexdigest()
    return bundle
