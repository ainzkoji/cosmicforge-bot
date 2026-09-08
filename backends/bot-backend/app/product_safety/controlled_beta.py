"""Phase 10 — controlled-beta readiness state machine.

    NOT_READY
        -> READY_FOR_CONTROLLED_BETA_REVIEW    (evidence thresholds met)
        -> APPROVED_FOR_CONTROLLED_BETA        (explicit admin action only)

Approval is never automatic. Meeting the evidence bar moves a bot into
*review*, not into *approved*: a human admin must act, and that action is
recorded with who, when, against which code revision and against which policy
hash.

Nothing here lowers the existing numeric readiness thresholds. Those live in
``readiness_gate`` and are untouched; this module governs the state machine,
the per-section confirmations and approval invalidation around them.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from shared_lib.persistence.evidence_schema import ORGANIC_PROVENANCE

# ── States ──────────────────────────────────────────────────────────────────

NOT_READY = "NOT_READY"
READY_FOR_CONTROLLED_BETA_REVIEW = "READY_FOR_CONTROLLED_BETA_REVIEW"
APPROVED_FOR_CONTROLLED_BETA = "APPROVED_FOR_CONTROLLED_BETA"

VALID_STATES = (NOT_READY, READY_FOR_CONTROLLED_BETA_REVIEW, APPROVED_FOR_CONTROLLED_BETA)

#: The only transitions the machine permits. Note there is no edge from
#: NOT_READY straight to APPROVED: evidence must exist before a human may sign.
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    NOT_READY: frozenset({NOT_READY, READY_FOR_CONTROLLED_BETA_REVIEW}),
    READY_FOR_CONTROLLED_BETA_REVIEW: frozenset(
        {NOT_READY, READY_FOR_CONTROLLED_BETA_REVIEW, APPROVED_FOR_CONTROLLED_BETA}
    ),
    APPROVED_FOR_CONTROLLED_BETA: frozenset({NOT_READY, APPROVED_FOR_CONTROLLED_BETA}),
}

#: Required deployment sections. Each needs its own explicit confirmation --
#: there is no blanket "sections A-E confirmed" flag.
REQUIRED_SECTIONS = ("A", "B", "C", "D", "E")

#: Approval statuses.
APPROVED = "APPROVED"
REVOKED = "REVOKED"
INVALIDATED = "INVALIDATED"


class ReadinessTransitionError(ValueError):
    """Raised on an illegal state transition or a missing prerequisite."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def evidence_hash(snapshot: Mapping[str, Any]) -> str:
    """Deterministic fingerprint of the evidence an approval was granted on."""
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


# ── State ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ReadinessState:
    bot_instance_id: str
    state: str
    policy_hash: str | None
    evidence_hash: str | None
    updated_at: str
    updated_by: str | None
    reason: str | None


def get_state(db: Any, bot_instance_id: str) -> ReadinessState:
    """Current review state. A bot with no row is NOT_READY — fail closed."""
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM readiness_states WHERE bot_instance_id=?", (bot_instance_id,)
        ).fetchone()
    if row is None:
        return ReadinessState(bot_instance_id, NOT_READY, None, None, _now(), None, "no_state_recorded")
    return ReadinessState(
        bot_instance_id=row["bot_instance_id"],
        state=row["state"],
        policy_hash=row["policy_hash"],
        evidence_hash=row["evidence_hash"],
        updated_at=row["updated_at"],
        updated_by=row["updated_by"],
        reason=row["reason"],
    )


def set_state(
    db: Any,
    bot_instance_id: str,
    *,
    state: str,
    updated_by: str,
    reason: str,
    policy_hash: str | None = None,
    evidence_hash_value: str | None = None,
) -> ReadinessState:
    """Move a bot to ``state``, rejecting transitions the machine forbids."""
    if state not in VALID_STATES:
        raise ReadinessTransitionError(f"Unknown readiness state: {state!r}")

    current = get_state(db, bot_instance_id)
    if state not in ALLOWED_TRANSITIONS[current.state]:
        raise ReadinessTransitionError(
            f"Illegal transition {current.state} -> {state} for {bot_instance_id}. "
            f"Allowed: {sorted(ALLOWED_TRANSITIONS[current.state])}"
        )

    with db.connect() as conn:
        conn.execute(
            """INSERT INTO readiness_states
               (bot_instance_id,state,policy_hash,evidence_hash,updated_at,updated_by,reason)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(bot_instance_id) DO UPDATE SET
                 state=excluded.state, policy_hash=excluded.policy_hash,
                 evidence_hash=excluded.evidence_hash, updated_at=excluded.updated_at,
                 updated_by=excluded.updated_by, reason=excluded.reason""",
            (bot_instance_id, state, policy_hash, evidence_hash_value, _now(), updated_by, reason),
        )
    return get_state(db, bot_instance_id)


# ── Section A-E confirmations (§17) ─────────────────────────────────────────


def confirm_section(
    db: Any,
    *,
    bot_instance_id: str,
    section: str,
    confirmed_by: str,
    evidence_reference: str,
    policy_hash: str,
    code_revision: str | None = None,
    notes: str | None = None,
) -> str:
    """Record one section's confirmation against a specific policy hash.

    Confirmations are policy-scoped: a material configuration change produces a
    new policy hash, so previous confirmations no longer satisfy the gate.
    """
    if section not in REQUIRED_SECTIONS:
        raise ReadinessTransitionError(f"Unknown section {section!r}; expected one of {REQUIRED_SECTIONS}")
    if not evidence_reference or not str(evidence_reference).strip():
        raise ReadinessTransitionError(f"Section {section} requires an evidence reference")

    confirmation_id = _uid("sec")
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO readiness_section_confirmations
               (confirmation_id,bot_instance_id,section,confirmed,confirmed_by,confirmed_at,
                evidence_reference,code_revision,policy_hash,notes)
               VALUES (?,?,?,1,?,?,?,?,?,?)
               ON CONFLICT(bot_instance_id, section, policy_hash) DO UPDATE SET
                 confirmed=1, confirmed_by=excluded.confirmed_by,
                 confirmed_at=excluded.confirmed_at,
                 evidence_reference=excluded.evidence_reference,
                 code_revision=excluded.code_revision, notes=excluded.notes""",
            (confirmation_id, bot_instance_id, section, confirmed_by, _now(),
             evidence_reference, code_revision, policy_hash, notes),
        )
    return confirmation_id


def confirmed_sections(db: Any, bot_instance_id: str, policy_hash: str) -> set[str]:
    with db.connect() as conn:
        rows = conn.execute(
            """SELECT section FROM readiness_section_confirmations
               WHERE bot_instance_id=? AND policy_hash=? AND confirmed=1""",
            (bot_instance_id, policy_hash),
        ).fetchall()
    return {row["section"] for row in rows}


def missing_sections(db: Any, bot_instance_id: str, policy_hash: str) -> list[str]:
    """Sections still unconfirmed for this policy. Empty means all present."""
    have = confirmed_sections(db, bot_instance_id, policy_hash)
    return [section for section in REQUIRED_SECTIONS if section not in have]


def sections_complete(db: Any, bot_instance_id: str, policy_hash: str) -> bool:
    """Fail closed: any missing confirmation blocks the gate."""
    return not missing_sections(db, bot_instance_id, policy_hash)


# ── Approval and revocation (§14, §16) ──────────────────────────────────────


def approve_controlled_beta(
    db: Any,
    *,
    bot_instance_id: str,
    reviewer_user_id: str,
    reviewer_role: str,
    policy_hash: str,
    evidence_snapshot: Mapping[str, Any],
    code_revision: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    """Explicit admin approval. Never called automatically by the runtime.

    Refuses unless the bot is already in review and every required section is
    confirmed against this exact policy hash.
    """
    current = get_state(db, bot_instance_id)
    if current.state != READY_FOR_CONTROLLED_BETA_REVIEW:
        raise ReadinessTransitionError(
            f"{bot_instance_id} is {current.state}; approval requires "
            f"{READY_FOR_CONTROLLED_BETA_REVIEW}"
        )

    outstanding = missing_sections(db, bot_instance_id, policy_hash)
    if outstanding:
        raise ReadinessTransitionError(
            f"Sections {outstanding} are not confirmed for policy {policy_hash[:12]}"
        )

    snapshot_id = _uid("evs")
    digest = evidence_hash(evidence_snapshot)
    approval_id = _uid("apr")

    with db.connect() as conn:
        conn.execute(
            """INSERT INTO readiness_approvals
               (id,bot_instance_id,reviewer_admin_id,approved_at,source_commit_sha,
                policy_hash,evidence_snapshot_json,notes,approval_status,reviewer_role,
                evidence_snapshot_id,evidence_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (approval_id, bot_instance_id, reviewer_user_id, _now(), code_revision,
             policy_hash, json.dumps(dict(evidence_snapshot), sort_keys=True, default=str),
             notes, APPROVED, reviewer_role, snapshot_id, digest),
        )

    set_state(
        db, bot_instance_id, state=APPROVED_FOR_CONTROLLED_BETA,
        updated_by=reviewer_user_id, reason="ADMIN_APPROVED",
        policy_hash=policy_hash, evidence_hash_value=digest,
    )
    return {
        "approval_id": approval_id,
        "bot_instance_id": bot_instance_id,
        "approval_status": APPROVED,
        "policy_hash": policy_hash,
        "evidence_snapshot_id": snapshot_id,
        "evidence_hash": digest,
    }


def revoke_controlled_beta(
    db: Any, *, bot_instance_id: str, revoked_by: str, reason: str,
    current_policy_hash: str | None = None,
) -> int:
    """Explicit admin revocation. Takes effect immediately."""
    if not reason or not str(reason).strip():
        raise ReadinessTransitionError("Revocation requires a reason")
    with db.connect() as conn:
        cursor = conn.execute(
            """UPDATE readiness_approvals
               SET revoked_at=?, revoked_by=?, revocation_reason=?, approval_status=?
               WHERE bot_instance_id=? AND revoked_at IS NULL""",
            (_now(), revoked_by, reason, REVOKED, bot_instance_id),
        )
        affected = cursor.rowcount or 0
    set_state(
        db, bot_instance_id, state=NOT_READY, updated_by=revoked_by,
        reason=f"REVOKED: {reason}", policy_hash=current_policy_hash,
    )
    return affected


def invalidate_on_policy_change(
    db: Any, *, bot_instance_id: str, current_policy_hash: str,
) -> int:
    """Invalidate approvals granted against a different policy hash (§15).

    Approval is never silently carried forward across a material configuration
    change: the approval row is marked INVALIDATED and the bot drops back to
    NOT_READY, so it must be re-reviewed.
    """
    with db.connect() as conn:
        cursor = conn.execute(
            """UPDATE readiness_approvals
               SET invalidated_at=?, invalidation_reason='MATERIAL_POLICY_CHANGE',
                   approval_status=?
               WHERE bot_instance_id=? AND revoked_at IS NULL AND invalidated_at IS NULL
                 AND policy_hash <> ?""",
            (_now(), INVALIDATED, bot_instance_id, current_policy_hash),
        )
        affected = cursor.rowcount or 0

    if affected:
        current = get_state(db, bot_instance_id)
        if current.state != NOT_READY:
            set_state(
                db, bot_instance_id, state=NOT_READY, updated_by="SYSTEM",
                reason="MATERIAL_POLICY_CHANGE", policy_hash=current_policy_hash,
            )
    return affected


def active_approval(db: Any, bot_instance_id: str, policy_hash: str | None = None) -> dict | None:
    """The approval currently in force, or None. Fails closed on mismatch."""
    with db.connect() as conn:
        row = conn.execute(
            """SELECT * FROM readiness_approvals
               WHERE bot_instance_id=? AND revoked_at IS NULL AND invalidated_at IS NULL
                 AND approval_status=?
               ORDER BY approved_at DESC LIMIT 1""",
            (bot_instance_id, APPROVED),
        ).fetchone()
    if row is None:
        return None
    approval = dict(row)
    if policy_hash is not None and approval.get("policy_hash") != policy_hash:
        return None
    return approval


def is_approved_for_controlled_beta(db: Any, bot_instance_id: str, policy_hash: str) -> bool:
    """The single question the runtime should ask before controlled beta."""
    return (
        get_state(db, bot_instance_id).state == APPROVED_FOR_CONTROLLED_BETA
        and active_approval(db, bot_instance_id, policy_hash) is not None
        and sections_complete(db, bot_instance_id, policy_hash)
    )


# ── Readiness evidence provenance (§18) ─────────────────────────────────────


def organic_daily_close_days(db: Any, bot_instance_id: str) -> int:
    """Count distinct paper days with daily-close evidence from real runtime.

    Only organic provenance counts. Test fixtures, replay, backtest and the
    Phase 12 validation harness are excluded by construction, so readiness can
    never be satisfied by evidence the runtime did not actually produce.
    """
    placeholders = ",".join("?" for _ in ORGANIC_PROVENANCE)
    with db.connect() as conn:
        row = conn.execute(
            f"""SELECT COUNT(DISTINCT substr(occurred_at, 1, 10))
                FROM position_events
                WHERE bot_instance_id=? AND event_type='DAILY_CLOSE'
                  AND provenance IN ({placeholders})""",
            (bot_instance_id, *sorted(ORGANIC_PROVENANCE)),
        ).fetchone()
    return int(row[0] or 0)


def organic_closed_trades(db: Any, bot_instance_id: str) -> int:
    placeholders = ",".join("?" for _ in ORGANIC_PROVENANCE)
    with db.connect() as conn:
        row = conn.execute(
            f"""SELECT COUNT(*) FROM positions
                WHERE bot_instance_id=? AND status IN ('CLOSED','FLAT')
                  AND provenance IN ({placeholders})""",
            (bot_instance_id, *sorted(ORGANIC_PROVENANCE)),
        ).fetchone()
    return int(row[0] or 0)
