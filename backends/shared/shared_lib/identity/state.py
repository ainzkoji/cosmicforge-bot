"""Identity verification state model on top of the existing KYC cases.

The KYC flow (``kyc_cases`` and friends, user-backend ``/kyc`` API, manual
admin review) is unchanged. This module maps a case onto one of eight
verification states, adds an optional validity period, and defines which
state changes are allowed and by whom.

``kyc_cases.status``  ->  VerificationState
---------------------------------------------------------------------------
(no case) / not_started / in_progress
                      ->  VERIFICATION_REQUIRED, or REGISTERED when no action
                          the user could take requires verification
needs_resubmission    ->  VERIFICATION_REQUIRED (a reviewer asked for more)
submitted             ->  PENDING
under_review          ->  MANUAL_REVIEW
approved              ->  VERIFIED, until its expiry; then
                          REVERIFICATION_REQUIRED (or EXPIRED when nothing
                          requires verification)
expired               ->  REVERIFICATION_REQUIRED / EXPIRED (as above)
rejected              ->  REJECTED
anything else         ->  VERIFICATION_REQUIRED (never VERIFIED)

Expiry: ``kyc_cases.expires_at`` when set, else ``approved_at`` plus
``IDENTITY_VERIFICATION_VALIDITY_DAYS`` when that is configured. Unset (the
default) means a verification does not expire unless policy sets a period.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Tuple

VALIDITY_ENV = "IDENTITY_VERIFICATION_VALIDITY_DAYS"


class VerificationState(str, Enum):
    REGISTERED = "REGISTERED"
    VERIFICATION_REQUIRED = "VERIFICATION_REQUIRED"
    PENDING = "PENDING"
    VERIFIED = "VERIFIED"
    REJECTED = "REJECTED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    EXPIRED = "EXPIRED"
    REVERIFICATION_REQUIRED = "REVERIFICATION_REQUIRED"


S = VerificationState

#: Policy-relaxed display states and the canonical state they stand for. The
#: transition table is written over canonical states only.
_CANONICAL = {S.REGISTERED: S.VERIFICATION_REQUIRED, S.EXPIRED: S.REVERIFICATION_REQUIRED}

#: (from, to) -> actor types allowed to make that change.
ALLOWED_TRANSITIONS: Dict[Tuple[VerificationState, VerificationState], FrozenSet[str]] = {
    # The user submits a complete case for review.
    (S.VERIFICATION_REQUIRED, S.PENDING): frozenset({"user"}),
    # The user starts again after the verification expired / was revoked.
    (S.REVERIFICATION_REQUIRED, S.VERIFICATION_REQUIRED): frozenset({"user"}),
    # A reviewer picks a submitted case up.
    (S.PENDING, S.MANUAL_REVIEW): frozenset({"reviewer"}),
    # Reviewer decisions.
    (S.PENDING, S.VERIFIED): frozenset({"reviewer"}),
    (S.MANUAL_REVIEW, S.VERIFIED): frozenset({"reviewer"}),
    (S.PENDING, S.REJECTED): frozenset({"reviewer"}),
    (S.MANUAL_REVIEW, S.REJECTED): frozenset({"reviewer"}),
    (S.PENDING, S.VERIFICATION_REQUIRED): frozenset({"reviewer"}),       # request resubmission
    (S.MANUAL_REVIEW, S.VERIFICATION_REQUIRED): frozenset({"reviewer"}),
    (S.REJECTED, S.VERIFICATION_REQUIRED): frozenset({"reviewer"}),      # reopen a rejected case
    # Expiry (system, once the validity period is over) or a reviewer requiring re-verification.
    (S.VERIFIED, S.REVERIFICATION_REQUIRED): frozenset({"reviewer", "system"}),
}

#: Target state -> kyc_cases.status written (per source state where it differs).
_TARGET_STATUS = {
    S.PENDING: "submitted",
    S.MANUAL_REVIEW: "under_review",
    S.VERIFIED: "approved",
    S.REJECTED: "rejected",
    S.REVERIFICATION_REQUIRED: "expired",
}

#: Transitions that need a reviewer's reason.
_REASON_REQUIRED = {
    (S.PENDING, S.REJECTED), (S.MANUAL_REVIEW, S.REJECTED), (S.PENDING, S.VERIFICATION_REQUIRED),
    (S.MANUAL_REVIEW, S.VERIFICATION_REQUIRED), (S.REJECTED, S.VERIFICATION_REQUIRED),
}


class TransitionError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------------------
# time / config
# ---------------------------------------------------------------------------

def parse_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validity_days(env: Optional[Mapping[str, str]] = None) -> Optional[int]:
    """Configured validity in days, or None (no expiry). A malformed value raises
    (callers that gate access treat that as 'cannot decide' and refuse)."""
    raw = str((env if env is not None else os.environ).get(VALIDITY_ENV, "") or "").strip()
    if not raw:
        return None
    try:
        days = int(raw)
    except ValueError:
        raise ValueError(f"{VALIDITY_ENV} must be a whole number of days, got {raw!r}") from None
    if days <= 0:
        raise ValueError(f"{VALIDITY_ENV} must be positive, got {days}")
    return days


def effective_expiry(case: Optional[Mapping[str, Any]], days: Optional[int]) -> Optional[datetime]:
    if not case:
        return None
    explicit = parse_ts(case.get("expires_at"))
    if explicit is not None:
        return explicit
    approved = parse_ts(case.get("approved_at"))
    if days and approved is not None:
        return approved + timedelta(days=int(days))
    return None


# ---------------------------------------------------------------------------
# state derivation (pure)
# ---------------------------------------------------------------------------

def derive_state(case: Optional[Mapping[str, Any]], *, required: bool, now: Optional[datetime] = None,
                 days: Optional[int] = None) -> VerificationState:
    """The verification state of a KYC case at ``now``.

    ``required``: whether any action the user can take requires verification
    (decides REGISTERED vs VERIFICATION_REQUIRED and EXPIRED vs
    REVERIFICATION_REQUIRED).
    """
    now = now or datetime.now(timezone.utc)
    status = str((case or {}).get("status") or "not_started").strip().lower()
    lapsed = S.REVERIFICATION_REQUIRED if required else S.EXPIRED
    if status in ("not_started", "in_progress"):
        return S.VERIFICATION_REQUIRED if required else S.REGISTERED
    if status == "needs_resubmission":
        return S.VERIFICATION_REQUIRED
    if status == "submitted":
        return S.PENDING
    if status == "under_review":
        return S.MANUAL_REVIEW
    if status == "rejected":
        return S.REJECTED
    if status == "expired":
        return lapsed
    if status == "approved":
        expiry = effective_expiry(case, days)
        if expiry is not None and now >= expiry:
            return lapsed
        return S.VERIFIED
    return S.VERIFICATION_REQUIRED  # unknown status: never treated as verified


def canonical(state: VerificationState) -> VerificationState:
    return _CANONICAL.get(state, state)


def source_state(case: Optional[Mapping[str, Any]], goal: VerificationState, *, now: datetime,
                 days: Optional[int]) -> VerificationState:
    """Canonical state a transition starts from (policy-strict, expiry-aware).

    Persisting an expiry that the clock already implies starts from VERIFIED:
    the stored case still says ``approved``.
    """
    status = str((case or {}).get("status") or "").lower()
    if status == "approved" and canonical(goal) == S.REVERIFICATION_REQUIRED:
        return S.VERIFIED
    return canonical(derive_state(case, required=True, now=now, days=days))


def is_allowed(source: VerificationState, target: VerificationState, actor_type: str) -> bool:
    return actor_type in ALLOWED_TRANSITIONS.get((canonical(source), canonical(target)), frozenset())


# ---------------------------------------------------------------------------
# persistence
# ---------------------------------------------------------------------------

def _fetch_case(conn, user_id: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM kyc_cases WHERE user_id = ?", (str(user_id),))
    row = cur.fetchone()
    if row is None:
        return None
    return {d[0]: row[i] for i, d in enumerate(cur.description)}


def minimum_evidence_problems(conn, case: Mapping[str, Any]) -> List[str]:
    """Database-level evidence check (records present; file contents are not read).

    The user-backend KYC API applies its stricter check (files exist and
    belong to the user) on the admin approval path; pass that as
    ``evidence_check`` to :func:`apply_transition` where it is available.
    """
    problems = []
    if not conn.execute("SELECT 1 FROM kyc_profiles WHERE kyc_case_id = ?", (case["id"],)).fetchone():
        problems.append("personal information is missing")
    if not conn.execute(
        "SELECT 1 FROM kyc_documents WHERE kyc_case_id = ? AND front_file_ref IS NOT NULL", (case["id"],)
    ).fetchone():
        problems.append("identity document upload is missing")
    if not conn.execute(
        "SELECT 1 FROM kyc_selfie_checks WHERE kyc_case_id = ? AND selfie_file_ref IS NOT NULL "
        "AND status IN ('pending_review', 'passed')", (case["id"],)
    ).fetchone():
        problems.append("selfie upload is missing")
    return problems


def _audit(conn, user_id: str, case_id: Optional[str], *, actor_type: str, actor_id: Optional[str],
           details: Dict[str, Any], now_iso: str) -> None:
    """kyc_audit_log row. Records state names, actor and reason *codes* only:
    never document contents, file references, personal data or free-text notes."""
    conn.execute(
        "INSERT INTO kyc_audit_log (id, user_id, kyc_case_id, event_type, event_data, actor_id, actor_type, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (f"kyclog_{uuid.uuid4().hex[:12]}", str(user_id), case_id, "identity_state_transition",
         json.dumps(details, sort_keys=True), actor_id or actor_type, actor_type, now_iso),
    )


def apply_transition(
    conn,
    user_id: str,
    target: VerificationState,
    *,
    actor_type: str,
    actor_id: Optional[str] = None,
    reason: Optional[str] = None,
    reason_codes: Optional[List[str]] = None,
    evidence_check: Optional[Callable[[Any, Mapping[str, Any]], List[str]]] = None,
    now: Optional[datetime] = None,
    days: Any = None,
) -> Dict[str, Any]:
    """Move the user's KYC case to ``target`` if that change is authorized.

    ``days``: validity period for a new approval / the expiry check; ``None``
    reads ``IDENTITY_VERIFICATION_VALIDITY_DAYS`` (unset = no expiry), ``0``
    forces no expiry.

    * ``actor_type`` is ``user`` (the case owner), ``reviewer`` (an admin) or
      ``system`` (scheduled expiry). Anything not in
      :data:`ALLOWED_TRANSITIONS` raises ``TransitionError(UNAUTHORIZED_TRANSITION)``.
    * VERIFIED needs an ``evidence_check`` that reports no problems -- there is
      no implicit approval path.
    * The ``system`` actor may only expire a verification whose validity has
      actually ended.
    """
    target = VerificationState(target)
    days = validity_days() if days is None else (int(days) or None)
    if actor_type not in ("user", "reviewer", "system"):
        raise TransitionError("UNKNOWN_ACTOR", f"unknown actor type {actor_type!r}")
    now = now or datetime.now(timezone.utc)
    now_iso = now.isoformat()
    case = _fetch_case(conn, user_id)
    if case is None:
        raise TransitionError("NO_CASE", "the user has no verification case")
    goal = canonical(target)
    source = source_state(case, goal, now=now, days=days)
    if not is_allowed(source, goal, actor_type):
        raise TransitionError("UNAUTHORIZED_TRANSITION",
                              f"{actor_type} may not move verification from {source.value} to {goal.value}")
    if (source, goal) in _REASON_REQUIRED and not (reason or "").strip():
        raise TransitionError("REASON_REQUIRED", "a reason is required for this decision")
    if actor_type == "system" and goal == S.REVERIFICATION_REQUIRED:
        expiry = effective_expiry(case, days)
        if expiry is None or now < expiry:
            raise TransitionError("NOT_EXPIRED", "the verification has not reached its expiry")
    if goal == S.PENDING:
        problems = minimum_evidence_problems(conn, case)
        if problems:
            raise TransitionError("EVIDENCE_INCOMPLETE", "; ".join(problems))
    if goal == S.VERIFIED:
        if evidence_check is None:
            raise TransitionError("EVIDENCE_CHECK_REQUIRED", "approval needs an evidence check")
        problems = list(evidence_check(conn, case) or [])
        if problems:
            raise TransitionError("EVIDENCE_INCOMPLETE", "; ".join(problems))

    if goal == S.VERIFICATION_REQUIRED:
        new_status = "in_progress" if source == S.REVERIFICATION_REQUIRED else "needs_resubmission"
    else:
        new_status = _TARGET_STATUS[goal]

    sets: Dict[str, Any] = {"status": new_status, "updated_at": now_iso}
    codes_json = json.dumps(reason_codes) if reason_codes else None
    if goal == S.PENDING:
        sets.update(submitted_at=now_iso, approved_at=None, rejected_at=None, expires_at=None)
    elif goal == S.VERIFIED:
        expiry = (now + timedelta(days=int(days))).isoformat() if days else None
        sets.update(approved_at=now_iso, rejected_at=None, rejection_reason=None, rejection_codes=None,
                    expires_at=expiry)
    elif goal == S.REJECTED:
        sets.update(rejected_at=now_iso, approved_at=None, rejection_reason=reason.strip(),
                    rejection_codes=codes_json)
    elif goal == S.VERIFICATION_REQUIRED and new_status == "needs_resubmission":
        sets.update(approved_at=None, rejection_reason=reason.strip(), rejection_codes=codes_json)
    elif goal == S.VERIFICATION_REQUIRED:
        sets.update(approved_at=None, expires_at=None)
    elif goal == S.REVERIFICATION_REQUIRED:
        sets.update(expires_at=case.get("expires_at") or now_iso)
    cols = sorted(sets)
    conn.execute(f"UPDATE kyc_cases SET {', '.join(f'{c} = ?' for c in cols)} WHERE id = ?",
                 (*[sets[c] for c in cols], case["id"]))
    if goal == S.VERIFIED:
        # Mirrors the admin approval path: the reviewed evidence is marked accepted.
        conn.execute("UPDATE kyc_documents SET status = 'accepted', reviewed_at = ?, updated_at = ? "
                     "WHERE kyc_case_id = ? AND front_file_ref IS NOT NULL", (now_iso, now_iso, case["id"]))
        conn.execute("UPDATE kyc_selfie_checks SET status = 'passed', updated_at = ? WHERE kyc_case_id = ?",
                     (now_iso, case["id"]))
    _audit(conn, user_id, case["id"], actor_type=actor_type, actor_id=actor_id, now_iso=now_iso, details={
        "from": source.value, "to": goal.value, "kyc_status_from": case.get("status"), "kyc_status_to": new_status,
        "reason_given": bool((reason or "").strip()), "reason_codes": list(reason_codes or []),
    })
    return {"user_id": str(user_id), "case_id": case["id"], "from": source.value, "to": goal.value,
            "kyc_status": new_status}


def expire_due_verifications(db, *, now: Optional[datetime] = None, days: Optional[int] = None) -> List[str]:
    """Persist expiry: every approved case past its validity becomes ``expired``
    (REVERIFICATION_REQUIRED). Returns the affected user ids. Idempotent.

    The existing KYC gate (``kyc_policy``) only accepts ``approved``, so after
    this sweep it refuses expired verifications too.
    """
    now = now or datetime.now(timezone.utc)
    if days is None:
        days = validity_days()
    done = []
    with db.connect() as conn:
        cur = conn.execute("SELECT * FROM kyc_cases WHERE status = 'approved'")
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            case = dict(zip(cols, row))
            expiry = effective_expiry(case, days)
            if expiry is None or now < expiry:
                continue
            apply_transition(conn, case["user_id"], S.REVERIFICATION_REQUIRED, actor_type="system",
                             actor_id="identity_expiry", now=now, days=days)
            done.append(str(case["user_id"]))
    return done


__all__ = [
    "VerificationState", "ALLOWED_TRANSITIONS", "TransitionError", "VALIDITY_ENV", "validity_days",
    "effective_expiry", "derive_state", "canonical", "source_state", "is_allowed", "minimum_evidence_problems",
    "apply_transition", "expire_due_verifications",
]
