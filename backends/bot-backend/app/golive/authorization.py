"""Operator and customer actions on the go-live ledger (Section H, Step 6.1).

Every method appends ledger events; none of them enables trading by itself. The go-live gate
(``app.golive.gate``) decides, from all of these and from the platform's own authorities, whether a live
entry may proceed -- and in this build it never may (``BUILD_PERMITS_LIVE``).
"""
from __future__ import annotations

import uuid
from typing import Any, Mapping, Optional

from .ledger import GoLiveError, GoLiveLedger, Operator, _require_operator, fold_approvals, fold_consent, fold_switch

#: Policy keys the owner may set (``POLICY_SET``). Unset keys leave their condition BLOCKED_PENDING_POLICY.
POLICY_KEYS = ("MIN_DEMO_RUN_DAYS", "LIVE_CONSENT_VERSION", "APPROVAL_MAX_VALIDITY_DAYS", "STAGE_A_MAX_NOTIONAL",
               "STAGE_B_MAX_BUDGET")


class GoLiveAuthority:
    def __init__(self, ledger: GoLiveLedger) -> None:
        self.ledger = ledger

    # ── exchange live switch: two operators to open, one to close ─────────────────────────────────────────
    def request_switch_on(self, venue: str, operator: Operator, *, reason: str) -> str:
        op = _require_operator(operator)
        if not reason.strip():
            raise GoLiveError("REASON_REQUIRED")
        st = fold_switch(self.ledger.events(scope_type="VENUE", scope_id=venue.upper()), venue.upper())
        if st.on:
            raise GoLiveError("SWITCH_ALREADY_ON")
        if st.pending_request is not None:
            raise GoLiveError("SWITCH_REQUEST_PENDING", st.pending_request.event_id)
        ev = self.ledger.append("SWITCH_ON_REQUESTED", scope_type="VENUE", scope_id=venue.upper(), actor_ref=op.ref,
                                payload={"reason": reason, "requested_by": op.operator_id})
        return ev.event_id

    def approve_switch_on(self, venue: str, request_event_id: str, operator: Operator, *, reason: str) -> None:
        op = _require_operator(operator)
        st = fold_switch(self.ledger.events(scope_type="VENUE", scope_id=venue.upper()), venue.upper())
        req = st.pending_request
        if req is None or req.event_id != request_event_id:
            raise GoLiveError("NO_MATCHING_SWITCH_REQUEST")
        if req.payload.get("requested_by") == op.operator_id:
            raise GoLiveError("SELF_APPROVAL_REFUSED")
        self.ledger.append("SWITCH_ON_APPROVED", scope_type="VENUE", scope_id=venue.upper(), actor_ref=op.ref,
                           payload={"request_event_id": request_event_id, "reason": reason,
                                    "approved_by": op.operator_id, "requested_by": req.payload.get("requested_by")})

    def reject_switch_on(self, venue: str, request_event_id: str, operator: Operator, *, reason: str) -> None:
        op = _require_operator(operator)
        self.ledger.append("SWITCH_ON_REJECTED", scope_type="VENUE", scope_id=venue.upper(), actor_ref=op.ref,
                           payload={"request_event_id": request_event_id, "reason": reason})

    def switch_off(self, venue: str, operator: Operator, *, reason: str) -> None:
        op = _require_operator(operator)
        self.ledger.append("SWITCH_OFF", scope_type="VENUE", scope_id=venue.upper(), actor_ref=op.ref,
                           payload={"reason": reason or "UNSPECIFIED"})

    # ── per-account approval: requester and approver must differ ──────────────────────────────────────────
    def request_approval(self, *, account_id: str, family_id: str, venue: str, operator: Operator, reason: str,
                         expires_at_ms: Optional[int] = None) -> str:
        op = _require_operator(operator)
        if not reason.strip():
            raise GoLiveError("REASON_REQUIRED")
        approval_id = uuid.uuid4().hex
        self.ledger.append("APPROVAL_REQUESTED", scope_type="APPROVAL", scope_id=approval_id, actor_ref=op.ref,
                           subject_ref=account_id,
                           payload={"account_id": account_id, "family_id": family_id, "venue": venue.upper(),
                                    "requested_by": op.operator_id, "reason": reason, "expires_at_ms": expires_at_ms})
        return approval_id

    def grant_approval(self, approval_id: str, operator: Operator, *, reason: str) -> None:
        op = _require_operator(operator)
        st = fold_approvals(self.ledger.events(scope_type="APPROVAL", scope_id=approval_id)).get(approval_id)
        if st is None or st.requested is None:
            raise GoLiveError("APPROVAL_UNKNOWN")
        if st.granted is not None or st.rejected is not None or st.revoked is not None:
            raise GoLiveError("APPROVAL_ALREADY_DECIDED")
        if st.requested.payload.get("requested_by") == op.operator_id:
            raise GoLiveError("SELF_APPROVAL_REFUSED")
        self.ledger.append("APPROVAL_GRANTED", scope_type="APPROVAL", scope_id=approval_id, actor_ref=op.ref,
                           subject_ref=st.account_id, payload={"approved_by": op.operator_id, "reason": reason})

    def reject_approval(self, approval_id: str, operator: Operator, *, reason: str) -> None:
        op = _require_operator(operator)
        self.ledger.append("APPROVAL_REJECTED", scope_type="APPROVAL", scope_id=approval_id, actor_ref=op.ref,
                           payload={"reason": reason})

    def revoke_approval(self, approval_id: str, operator: Operator, *, reason: str) -> None:
        op = _require_operator(operator)
        self.ledger.append("APPROVAL_REVOKED", scope_type="APPROVAL", scope_id=approval_id, actor_ref=op.ref,
                           payload={"reason": reason or "UNSPECIFIED"})

    # ── customer live-trading consent (revocable by the customer at any time) ─────────────────────────────
    def give_consent(self, *, account_id: str, user_id: str, consent_version: str, text_sha256: str,
                     source: str = "PORTAL") -> None:
        if not (account_id and user_id and consent_version and text_sha256):
            raise GoLiveError("CONSENT_FIELDS_REQUIRED")
        self.ledger.append("CONSENT_GIVEN", scope_type="ACCOUNT", scope_id=account_id, actor_ref=f"user:{user_id}",
                           subject_ref=user_id, payload={"user_id": user_id, "consent_version": consent_version,
                                                         "text_sha256": text_sha256, "source": source})

    def revoke_consent(self, *, account_id: str, user_id: str, reason: str = "CUSTOMER_REVOKED") -> None:
        st = fold_consent(self.ledger.events(scope_type="ACCOUNT", scope_id=account_id), account_id)
        if st.given is not None and st.user_id and st.user_id != user_id:
            raise GoLiveError("CONSENT_OWNER_MISMATCH")
        self.ledger.append("CONSENT_REVOKED", scope_type="ACCOUNT", scope_id=account_id, actor_ref=f"user:{user_id}",
                           subject_ref=user_id, payload={"reason": reason})

    # ── demo-run evidence ─────────────────────────────────────────────────────────────────────────────────
    def record_demo_run(self, *, family_id: str, family_version: str, venue: str, run_ref: str, started_at_ms: int,
                        ended_at_ms: int, outcome: str, operator: Operator, evidence: Optional[Mapping[str, Any]] = None) -> str:
        op = _require_operator(operator)
        if outcome not in ("PASS", "FAIL"):
            raise GoLiveError("DEMO_OUTCOME_INVALID")
        if ended_at_ms <= started_at_ms:
            raise GoLiveError("DEMO_PERIOD_INVALID")
        ev = self.ledger.append("DEMO_RUN_RECORDED", scope_type="FAMILY", scope_id=family_id, actor_ref=op.ref,
                                payload={"family_version": family_version, "venue": venue.upper(), "run_ref": run_ref,
                                         "started_at_ms": started_at_ms, "ended_at_ms": ended_at_ms, "outcome": outcome,
                                         "evidence": dict(evidence or {})})
        return ev.event_id

    def invalidate_demo_run(self, *, family_id: str, run_event_id: str, operator: Operator, reason: str) -> None:
        op = _require_operator(operator)
        self.ledger.append("DEMO_RUN_INVALIDATED", scope_type="FAMILY", scope_id=family_id, actor_ref=op.ref,
                           payload={"run_event_id": run_event_id, "reason": reason})

    # ── owner policy ──────────────────────────────────────────────────────────────────────────────────────
    def set_policy(self, key: str, value: Any, operator: Operator, *, reason: str) -> None:
        op = _require_operator(operator)
        if key not in POLICY_KEYS:
            raise GoLiveError("POLICY_KEY_UNKNOWN", key)
        self.ledger.append("POLICY_SET", scope_type="POLICY", scope_id=key, actor_ref=op.ref,
                           payload={"value": value, "reason": reason})
