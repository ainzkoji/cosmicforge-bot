from __future__ import annotations

import json
import uuid
from typing import Any

from shared_lib.persistence.db import DB, utc_now_iso


def active_readiness_approval(*, db: DB, bot_instance_id: str) -> dict[str, Any] | None:
    with db.connect() as conn:
        row = conn.execute(
            """SELECT * FROM readiness_approvals
               WHERE bot_instance_id=? AND revoked_at IS NULL
               ORDER BY approved_at DESC LIMIT 1""",
            (bot_instance_id,),
        ).fetchone()
    return dict(row) if row else None


def approve_readiness(
    *, db: DB, bot_instance_id: str, reviewer_admin_id: str,
    policy_hash: str, evidence_snapshot: dict[str, Any],
    source_commit_sha: str | None = None, notes: str | None = None,
) -> dict[str, Any]:
    approval_id = str(uuid.uuid4())
    approved_at = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            "UPDATE readiness_approvals SET revoked_at=?, revocation_reason=? WHERE bot_instance_id=? AND revoked_at IS NULL",
            (approved_at, "SUPERSEDED", bot_instance_id),
        )
        conn.execute(
            """INSERT INTO readiness_approvals
               (id,bot_instance_id,reviewer_admin_id,approved_at,source_commit_sha,
                policy_hash,evidence_snapshot_json,notes)
               VALUES (?,?,?,?,?,?,?,?)""",
            (approval_id, bot_instance_id, reviewer_admin_id, approved_at,
             source_commit_sha, policy_hash, json.dumps(evidence_snapshot, sort_keys=True), notes),
        )
    return {"id": approval_id, "bot_instance_id": bot_instance_id, "approved_at": approved_at, "policy_hash": policy_hash}


def invalidate_readiness_approval(
    *, db: DB, bot_instance_id: str, reason: str,
    current_policy_hash: str | None = None, revoked_by: str = "SYSTEM",
) -> int:
    now = utc_now_iso()
    with db.connect() as conn:
        cur = conn.execute(
            """UPDATE readiness_approvals
               SET revoked_at=?, revoked_by=?, revocation_reason=?
               WHERE bot_instance_id=? AND revoked_at IS NULL
                 AND (? IS NULL OR policy_hash <> ?)""",
            (now, revoked_by, reason, bot_instance_id, current_policy_hash, current_policy_hash),
        )
        return int(cur.rowcount or 0)


def sections_a_to_e_confirmed(*, db: DB) -> bool:
    with db.connect() as conn:
        row = conn.execute(
            """SELECT 1 FROM deployment_confirmations
               WHERE confirmation_type='SECTIONS_A_TO_E' AND revoked_at IS NULL
               ORDER BY confirmed_at DESC LIMIT 1"""
        ).fetchone()
    return bool(row)


def confirm_sections_a_to_e(
    *, db: DB, confirmed_by: str, source_commit_sha: str | None,
    release_ref: str | None, evidence_reference: str,
) -> dict[str, Any]:
    confirmation_id = str(uuid.uuid4())
    confirmed_at = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO deployment_confirmations
               (id,confirmation_type,confirmed_by,confirmed_at,source_commit_sha,release_ref,evidence_reference)
               VALUES (?,?,?,?,?,?,?)""",
            (confirmation_id, "SECTIONS_A_TO_E", confirmed_by, confirmed_at,
             source_commit_sha, release_ref, evidence_reference),
        )
    return {"id": confirmation_id, "confirmed_at": confirmed_at}
