"""
KYC API Endpoints
Complete API for KYC verification flow
"""
import json
import logging
import uuid
import time
from datetime import datetime, timedelta
from typing import Optional, List, Any, Set, Tuple
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from shared_lib.persistence.db import DB
from app.api.auth import get_current_user_id, get_current_active_user, audit_event
from shared_lib.core.policy.kyc_policy import (
    is_kyc_required, get_full_kyc_status, KYCStatus, KYCAction, check_kyc_gate
)
from app.core.kyc_encryption import (
    encrypt_pii, decrypt_pii, mask_name, mask_pii, hash_document_number,
    KYCConfigError, assert_kyc_encryption_configured,
)
from app.core.kyc_storage import (
    MAX_FILE_SIZE, IMAGE_EXTENSIONS, InvalidFileRef,
    generate_upload_url, build_upload_url, save_uploaded_file,
    verify_upload_signature, delete_file, generate_selfie_ref,
    assert_kyc_storage_configured, document_ref_matches, is_selfie_ref,
    file_exists, file_ref_belongs_to, read_stored_file, stored_file_extension,
    extension_for_content_type, list_user_document_files, UPLOAD_URL_TTL_SECONDS,
)

logger = logging.getLogger(__name__)


def _require_kyc_configured():
    """Fail closed (503) when the KYC secrets are missing/unsafe for this environment."""
    try:
        assert_kyc_encryption_configured()
        assert_kyc_storage_configured()
    except KYCConfigError as exc:
        logger.critical("KYC is not configured: %s", exc)
        raise HTTPException(503, "KYC verification is temporarily unavailable")


router = APIRouter(prefix="/kyc", tags=["KYC"], dependencies=[Depends(_require_kyc_configured)])

# Case statuses in which the user may no longer change their submission.
LOCKED_CASE_STATUSES = ("submitted", "under_review", "approved")
# Case statuses waiting for a manual (admin) decision.
REVIEWABLE_CASE_STATUSES = ("submitted", "under_review")
# Selfie/document status meaning "received from the user, awaiting manual review".
STATUS_PENDING_REVIEW = "pending_review"
# Upload references a user may have outstanding (issued or uploaded, but not
# yet confirmed on a document) at any one time. Every /documents/upload-url
# call mints a fresh reference, and each may receive a file of MAX_FILE_SIZE.
MAX_UNCONFIRMED_UPLOADS = 10
# Uploaded-but-never-confirmed files older than this are removed the next time
# the user asks for an upload URL.
UNCONFIRMED_UPLOAD_MAX_AGE_SECONDS = 24 * 3600
UPLOAD_INITIATED_EVENT = "kyc_id_upload_initiated"


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def log_kyc_event(
    user_id: str,
    event_type: str,
    kyc_case_id: Optional[str] = None,
    event_data: Optional[dict] = None,
    actor_id: Optional[str] = None,
    actor_type: str = "user",
    conn: Optional[Any] = None
):
    """Log a KYC audit event"""
    
    # Define INSERT query
    sql = """INSERT INTO kyc_audit_log 
               (id, user_id, kyc_case_id, event_type, event_data, actor_id, actor_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
    
    params = (
        f"kyclog_{uuid.uuid4().hex[:12]}",
        user_id,
        kyc_case_id,
        event_type,
        json.dumps(event_data) if event_data else None,
        actor_id or user_id,
        actor_type,
        utc_now_iso(),
    )

    if conn:
        conn.execute(sql, params)
    else:
        db = DB()
        with db.connect() as c:
            c.execute(sql, params)
    
    # Notification Triggers
    # In a production system, this would enqueue an email/push notification
    if event_type in ["kyc_submitted", "kyc_approved", "kyc_rejected", "kyc_needs_resubmission"]:
        logger.info("[NOTIFICATION] Triggering notification for user %s: KYC Event '%s'", user_id, event_type)


def _require_editable_case(conn, user_id: str) -> dict:
    """The user's KYC case, only while the user is still allowed to change it.

    Once a case is submitted (or approved) its evidence is frozen: otherwise
    the documents could be swapped after -- or during -- the manual review.
    """
    case = conn.execute(
        "SELECT * FROM kyc_cases WHERE user_id = ?", (user_id,)
    ).fetchone()
    if not case:
        raise HTTPException(400, "Please start KYC process first")
    case = dict(case)
    if case["status"] in LOCKED_CASE_STATUSES:
        raise HTTPException(
            409,
            "Your verification has already been approved" if case["status"] == "approved"
            else "Your verification is under review and can no longer be changed",
        )
    return case


def _confirmed_document_refs(conn, user_id: str) -> Set[str]:
    rows = conn.execute(
        "SELECT front_file_ref, back_file_ref FROM kyc_documents WHERE user_id = ?", (user_id,)
    ).fetchall()
    return {ref for row in rows for ref in (row[0], row[1]) if ref}


def _unconfirmed_document_files(conn, user_id: str) -> List[Tuple[str, float]]:
    """``(file_ref, modified_at)`` of stored document files no document row points to."""
    confirmed = _confirmed_document_refs(conn, user_id)
    return [(ref, mtime) for ref, mtime in list_user_document_files(user_id) if ref not in confirmed]


def _purge_stale_unconfirmed_uploads(conn, user_id: str) -> int:
    """Delete this user's unconfirmed uploads older than the retention window (best effort)."""
    cutoff = time.time() - UNCONFIRMED_UPLOAD_MAX_AGE_SECONDS
    removed = 0
    for ref, mtime in _unconfirmed_document_files(conn, user_id):
        if mtime < cutoff and delete_file(ref):
            removed += 1
    if removed:
        logger.info("Removed %d stale unconfirmed KYC upload(s) for user %s", removed, user_id)
    return removed


def _outstanding_upload_refs(conn, user_id: str) -> Set[str]:
    """Unconfirmed upload references that still cost (or can still cost) storage:
    files already uploaded, plus references whose signed URL has not expired."""
    confirmed = _confirmed_document_refs(conn, user_id)
    outstanding = {ref for ref, _mtime in _unconfirmed_document_files(conn, user_id)}
    issued_after = (datetime.utcnow() - timedelta(seconds=UPLOAD_URL_TTL_SECONDS)).isoformat() + "Z"
    rows = conn.execute(
        "SELECT event_data FROM kyc_audit_log WHERE user_id = ? AND event_type = ? AND created_at > ?",
        (user_id, UPLOAD_INITIATED_EVENT, issued_after)
    ).fetchall()
    for row in rows:
        try:
            ref = (json.loads(row[0]) if row[0] else {}).get("file_ref")
        except (TypeError, ValueError, AttributeError):
            continue
        if isinstance(ref, str) and ref not in confirmed:
            outstanding.add(ref)
    return outstanding


def _user_owns_file_ref(conn, user_id: str, file_ref: str) -> bool:
    """True only when the reference is recorded in the database for this user."""
    if not file_ref_belongs_to(file_ref, user_id):
        return False
    row = conn.execute(
        "SELECT 1 FROM kyc_documents WHERE user_id = ? AND (front_file_ref = ? OR back_file_ref = ?)",
        (user_id, file_ref, file_ref)
    ).fetchone()
    if row:
        return True
    row = conn.execute(
        "SELECT 1 FROM kyc_selfie_checks WHERE user_id = ? AND selfie_file_ref = ?",
        (user_id, file_ref)
    ).fetchone()
    return bool(row)


def _document_is_complete(doc: dict) -> bool:
    """Front always required; back required for everything except a passport. Files must exist."""
    user_id = doc["user_id"]
    front = doc.get("front_file_ref")
    if not front or not file_ref_belongs_to(front, user_id) or not file_exists(front):
        return False
    if doc.get("doc_type") != "passport":
        back = doc.get("back_file_ref")
        if not back or not file_ref_belongs_to(back, user_id) or not file_exists(back):
            return False
    return True


def case_evidence_gaps(conn, case: dict) -> dict:
    """What is missing before a case can be submitted/approved, keyed by step (empty = complete)."""
    problems: dict = {}
    profile = conn.execute(
        "SELECT id FROM kyc_profiles WHERE kyc_case_id = ?", (case["id"],)
    ).fetchone()
    if not profile:
        problems["personal_info"] = "personal information is missing"

    docs = conn.execute(
        "SELECT * FROM kyc_documents WHERE kyc_case_id = ? AND user_id = ?",
        (case["id"], case["user_id"])
    ).fetchall()
    if not any(_document_is_complete(dict(d)) for d in docs):
        problems["id_document"] = "identity document upload is missing or incomplete"

    selfie = conn.execute(
        "SELECT * FROM kyc_selfie_checks WHERE kyc_case_id = ? AND user_id = ?",
        (case["id"], case["user_id"])
    ).fetchone()
    selfie_ref = dict(selfie).get("selfie_file_ref") if selfie else None
    if (
        not selfie_ref
        or not is_selfie_ref(selfie_ref, case["user_id"])
        or not file_exists(selfie_ref)
        or dict(selfie)["status"] not in (STATUS_PENDING_REVIEW, "passed")
    ):
        problems["face_verification"] = "selfie upload is missing"
    return problems


def case_evidence_problems(conn, case: dict) -> List[str]:
    """Human-readable list of what is missing (empty list = complete)."""
    return list(case_evidence_gaps(conn, case).values())


def apply_review_decision(
    conn,
    case_id: str,
    decision: str,
    *,
    reviewer_id: str,
    reviewer_email: Optional[str] = None,
    reason: Optional[str] = None,
    reason_codes: Optional[List[str]] = None,
) -> dict:
    """Record a manual review decision on a KYC case (the only path to 'approved').

    Shared by every reviewer-facing endpoint so that the rules are identical:
    only cases awaiting review can be decided, a rejection needs a reason, an
    approval needs the evidence to actually be on file, and every decision is
    written to the review table and both audit logs.
    """
    if decision not in ("approved", "rejected", "needs_resubmission"):
        raise HTTPException(400, "Invalid decision")

    row = conn.execute("SELECT * FROM kyc_cases WHERE id = ?", (case_id,)).fetchone()
    if not row:
        raise HTTPException(404, "KYC case not found")
    case = dict(row)

    if case["status"] not in REVIEWABLE_CASE_STATUSES:
        raise HTTPException(409, f"KYC case is '{case['status']}' and is not awaiting review")

    reason = (reason or "").strip() or None
    if decision != "approved" and not reason:
        raise HTTPException(400, "A reason is required")

    if decision == "approved":
        problems = case_evidence_problems(conn, case)
        if problems:
            raise HTTPException(409, "Cannot approve: " + "; ".join(problems))

    now = utc_now_iso()
    codes_json = json.dumps(reason_codes) if reason_codes else None

    if decision == "approved":
        conn.execute(
            """UPDATE kyc_cases SET status = 'approved', approved_at = ?, rejected_at = NULL,
               rejection_reason = NULL, rejection_codes = NULL, updated_at = ?
               WHERE id = ?""",
            (now, now, case_id)
        )
        conn.execute(
            "UPDATE kyc_documents SET status = 'accepted', reviewed_at = ?, updated_at = ? "
            "WHERE kyc_case_id = ? AND front_file_ref IS NOT NULL",
            (now, now, case_id)
        )
        conn.execute(
            "UPDATE kyc_selfie_checks SET status = 'passed', updated_at = ? WHERE kyc_case_id = ?",
            (now, case_id)
        )
    elif decision == "rejected":
        conn.execute(
            """UPDATE kyc_cases SET status = 'rejected', rejected_at = ?, approved_at = NULL,
               rejection_reason = ?, rejection_codes = ?, updated_at = ?
               WHERE id = ?""",
            (now, reason, codes_json, now, case_id)
        )
    else:
        conn.execute(
            """UPDATE kyc_cases SET status = 'needs_resubmission', approved_at = NULL,
               rejection_reason = ?, rejection_codes = ?, updated_at = ?
               WHERE id = ?""",
            (reason, codes_json, now, case_id)
        )

    # Create review record
    review_id = f"kycrev_{uuid.uuid4().hex[:10]}"
    conn.execute(
        """INSERT INTO kyc_reviews
           (id, kyc_case_id, reviewer_id, reviewer_type, decision, reason_codes, notes_encrypted, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            review_id, case_id, reviewer_id, "admin", decision,
            codes_json,
            encrypt_pii(reason) if reason else None,
            now
        )
    )

    log_kyc_event(
        case["user_id"], f"kyc_{decision}", case_id,
        event_data={"review_id": review_id, "reason": reason, "reason_codes": reason_codes},
        actor_id=reviewer_id, actor_type="admin", conn=conn,
    )
    audit_event(
        conn, f"kyc_{decision}", user_id=case["user_id"],
        details={
            "case_id": case_id,
            "review_id": review_id,
            "reviewer_id": reviewer_id,
            "reviewer_email": reviewer_email,
            "reason": reason,
            "reason_codes": reason_codes,
        },
    )

    return {
        "success": True,
        "case_id": case_id,
        "user_id": case["user_id"],
        "new_status": decision,
        "review_id": review_id,
    }



# ============================================================================
# Request/Response Models
# ============================================================================

class PersonalInfoRequest(BaseModel):
    full_legal_name: str = Field(..., min_length=2, max_length=200)
    date_of_birth: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    nationality: str = Field(..., min_length=2, max_length=2)  # ISO country code
    country_of_residence: str = Field(..., min_length=2, max_length=2)
    address_line1: str = Field(..., min_length=5, max_length=200)
    address_city: str = Field(..., min_length=2, max_length=100)
    address_state: Optional[str] = Field(None, max_length=100)
    address_postal_code: str = Field(..., min_length=2, max_length=20)
    phone: Optional[str] = Field(None, max_length=20)


class DocumentUploadRequest(BaseModel):
    doc_type: str = Field(..., pattern=r"^(passport|national_id|drivers_license)$")
    side: str = Field(default="front", pattern=r"^(front|back)$")
    issuing_country: Optional[str] = Field(None, min_length=2, max_length=2)


class DocumentConfirmRequest(BaseModel):
    doc_id: str
    file_ref: str
    side: str = Field(default="front", pattern=r"^(front|back)$")
    file_size_bytes: int
    content_type: str


class FaceVerificationStartRequest(BaseModel):
    provider: str = Field(default="internal")


class FaceVerificationCompleteRequest(BaseModel):
    # The server decides the outcome. A client-sent ``passed`` flag is NOT part
    # of this model and is ignored: face verification is completed only by an
    # uploaded selfie, and then only as "pending manual review".
    selfie_file_ref: Optional[str] = None
    provider_session_id: Optional[str] = None


class ReviewDecisionRequest(BaseModel):
    decision: str = Field(..., pattern=r"^(approved|rejected|needs_resubmission)$")
    reason_codes: Optional[List[str]] = None
    notes: Optional[str] = None


# ============================================================================
# Policy & Case Management Endpoints
# ============================================================================

@router.get("/requirements")
def get_kyc_requirements(
    action: Optional[str] = Query(None, description="Specific action to check"),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get KYC requirements for the current user.
    Optionally check requirements for a specific action.
    """
    if action:
        req = is_kyc_required(user_id, action)
        return {
            "action": action,
            "is_required": req.is_required,
            "is_satisfied": req.is_satisfied,
            "reason": req.reason,
            "required_status": req.required_status.value,
            "current_status": req.current_status.value if req.current_status else None,
            "required_steps": req.required_steps,
            "completed_steps": req.completed_steps,
        }
    
    # Return general requirements
    status = get_full_kyc_status(user_id)
    
    # Check a default action
    default_req = is_kyc_required(user_id, KYCAction.START_LIVE_TRADING.value)
    
    return {
        "kyc_required_for_trading": default_req.is_required,
        "is_satisfied": default_req.is_satisfied,
        "current_status": status["status"],
        "required_steps": status["required_steps"],
        "completed_steps": status["completed_steps"],
        "blocked_actions": default_req.blocked_actions,
        "allowed_actions": default_req.allowed_actions,
    }


@router.post("/start")
def start_kyc_case(user_id: str = Depends(get_current_user_id)):
    """
    Start a new KYC verification case.
    Creates a case if one doesn't exist, or returns existing case.
    """
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        # Check for existing case
        existing = conn.execute(
            "SELECT * FROM kyc_cases WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        if existing:
            case = dict(existing)
            # If rejected or needs resubmission, allow restart
            if case["status"] in ["rejected", "needs_resubmission"]:
                conn.execute(
                    "UPDATE kyc_cases SET status = 'in_progress', updated_at = ? WHERE id = ?",
                    (now, case["id"])
                )
                log_kyc_event(user_id, "kyc_restarted", case["id"], conn=conn)
                case["status"] = "in_progress"
            
            return {
                "case_id": case["id"],
                "status": case["status"],
                "message": "KYC case already exists",
                "created_at": case["created_at"],
            }
        
        # Create new case
        case_id = f"kyc_{uuid.uuid4().hex[:12]}"
        required_steps = json.dumps(["personal_info", "id_document", "face_verification"])
        
        conn.execute(
            """INSERT INTO kyc_cases 
               (id, user_id, status, required_steps, completed_steps, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (case_id, user_id, "in_progress", required_steps, "[]", now, now)
        )
        
        log_kyc_event(user_id, "kyc_started", case_id, conn=conn)
        
        return {
            "case_id": case_id,
            "status": "in_progress",
            "message": "KYC case started",
            "created_at": now,
        }


@router.get("/status")
def get_kyc_status(user_id: str = Depends(get_current_user_id)):
    """Get current KYC status summary"""
    status = get_full_kyc_status(user_id)
    return status


@router.get("/checklist")
def get_kyc_checklist(user_id: str = Depends(get_current_user_id)):
    """Get detailed KYC checklist with step-by-step progress"""
    status = get_full_kyc_status(user_id)

    # While the case is still editable, a step only counts as done when its
    # evidence is really on file (e.g. a selfie marked "passed" by the old
    # client-side flow, with no uploaded file, has to be redone).
    gaps: dict = {}
    if status.get("has_case") and status["status"] not in LOCKED_CASE_STATUSES:
        db = DB()
        with db.connect() as conn:
            case = conn.execute("SELECT * FROM kyc_cases WHERE user_id = ?", (user_id,)).fetchone()
            if case:
                gaps = case_evidence_gaps(conn, dict(case))
    
    checklist = []
    for step in status["required_steps"]:
        step_info = status["steps"].get(step, {"status": "not_started"})
        step_status = step_info["status"]
        # "pending_review" = the user's part is done; a reviewer has not decided yet.
        is_complete = step_status in ["completed", "accepted", "passed", STATUS_PENDING_REVIEW]
        if step in gaps:
            is_complete = False
            if step_status in ["completed", "accepted", "passed", STATUS_PENDING_REVIEW]:
                step_status = "not_started"
        checklist.append({
            "step": step,
            "label": step.replace("_", " ").title(),
            "status": step_status,
            "is_complete": is_complete,
            "awaiting_review": is_complete and step_status == STATUS_PENDING_REVIEW,
            "data": step_info.get("data"),
        })
    
    return {
        "case_status": status["status"],
        "checklist": checklist,
        "can_submit": bool(status["can_submit"]) and not gaps,
        "rejection_reason": status.get("rejection_reason"),
    }


# ============================================================================
# Personal Info Endpoints
# ============================================================================

@router.post("/personal-info")
def submit_personal_info(
    data: PersonalInfoRequest,
    user_id: str = Depends(get_current_user_id)
):
    """Submit or update personal information"""
    db = DB()
    now = utc_now_iso()
    
    # Validate age (18+)
    from datetime import date
    try:
        dob = date.fromisoformat(data.date_of_birth)
        today = date.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        if age < 18:
            raise HTTPException(400, "Must be 18 or older")
    except ValueError:
        raise HTTPException(400, "Invalid date of birth format")
    
    with db.connect() as conn:
        # Get KYC case (must still be editable)
        case = _require_editable_case(conn, user_id)
        
        case_id = case["id"]
        
        # Check for existing profile
        existing = conn.execute(
            "SELECT id FROM kyc_profiles WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        # Encrypt PII fields
        encrypted_data = {
            "full_legal_name_encrypted": encrypt_pii(data.full_legal_name),
            "date_of_birth_encrypted": encrypt_pii(data.date_of_birth),
            "address_line1_encrypted": encrypt_pii(data.address_line1),
            "address_city_encrypted": encrypt_pii(data.address_city),
            "address_postal_code_encrypted": encrypt_pii(data.address_postal_code),
            "phone_encrypted": encrypt_pii(data.phone) if data.phone else None,
        }
        
        if existing:
            # Update existing
            conn.execute(
                """UPDATE kyc_profiles SET
                   full_legal_name_encrypted = ?,
                   date_of_birth_encrypted = ?,
                   nationality = ?,
                   country_of_residence = ?,
                   address_line1_encrypted = ?,
                   address_city_encrypted = ?,
                   address_state = ?,
                   address_postal_code_encrypted = ?,
                   phone_encrypted = ?,
                   updated_at = ?
                   WHERE id = ?""",
                (
                    encrypted_data["full_legal_name_encrypted"],
                    encrypted_data["date_of_birth_encrypted"],
                    data.nationality,
                    data.country_of_residence,
                    encrypted_data["address_line1_encrypted"],
                    encrypted_data["address_city_encrypted"],
                    data.address_state,
                    encrypted_data["address_postal_code_encrypted"],
                    encrypted_data["phone_encrypted"],
                    now,
                    existing["id"],
                )
            )
            profile_id = existing["id"]
        else:
            # Create new
            profile_id = f"kycpro_{uuid.uuid4().hex[:10]}"
            conn.execute(
                """INSERT INTO kyc_profiles
                   (id, user_id, kyc_case_id, full_legal_name_encrypted, date_of_birth_encrypted,
                    nationality, country_of_residence, address_line1_encrypted, address_city_encrypted,
                    address_state, address_postal_code_encrypted, phone_encrypted, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    profile_id, user_id, case_id,
                    encrypted_data["full_legal_name_encrypted"],
                    encrypted_data["date_of_birth_encrypted"],
                    data.nationality,
                    data.country_of_residence,
                    encrypted_data["address_line1_encrypted"],
                    encrypted_data["address_city_encrypted"],
                    data.address_state,
                    encrypted_data["address_postal_code_encrypted"],
                    encrypted_data["phone_encrypted"],
                    now, now,
                )
            )
        
        # Update completed steps
        case_row = conn.execute("SELECT completed_steps FROM kyc_cases WHERE id = ?", (case_id,)).fetchone()
        completed = json.loads(case_row["completed_steps"])
        if "personal_info" not in completed:
            completed.append("personal_info")
            conn.execute(
                "UPDATE kyc_cases SET completed_steps = ?, updated_at = ? WHERE id = ?",
                (json.dumps(completed), now, case_id)
            )
        
        log_kyc_event(user_id, "kyc_personal_info_submitted", case_id, conn=conn)
        
        return {
            "success": True,
            "profile_id": profile_id,
            "message": "Personal information saved",
        }


@router.get("/personal-info")
def get_personal_info(user_id: str = Depends(get_current_user_id)):
    """Get personal information (masked for security)"""
    db = DB()
    
    with db.connect() as conn:
        profile = conn.execute(
            "SELECT * FROM kyc_profiles WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        if not profile:
            return {"has_profile": False}
        
        profile = dict(profile)
        
        # Decrypt and mask for display
        full_name = decrypt_pii(profile["full_legal_name_encrypted"])
        
        return {
            "has_profile": True,
            "full_legal_name_masked": mask_name(full_name),
            "nationality": profile["nationality"],
            "country_of_residence": profile["country_of_residence"],
            "address_state": profile["address_state"],
            "created_at": profile["created_at"],
            "updated_at": profile["updated_at"],
        }


# ============================================================================
# Document Upload Endpoints
# ============================================================================

@router.post("/documents/upload-url")
def request_upload_url(
    data: DocumentUploadRequest,
    user_id: str = Depends(get_current_user_id)
):
    """Request a presigned URL for document upload"""
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        # Get KYC case (must still be editable)
        case = _require_editable_case(conn, user_id)
        
        case_id = case["id"]

        # Housekeeping, then the cap: abandoned uploads are removed, and no
        # more references are handed out while too many are still unconfirmed.
        _purge_stale_unconfirmed_uploads(conn, user_id)
        if len(_outstanding_upload_refs(conn, user_id)) >= MAX_UNCONFIRMED_UPLOADS:
            raise HTTPException(
                429, "Too many uploads are waiting to be confirmed. Finish or confirm them, then try again later."
            )
        
        # Check for existing document of this type
        existing = conn.execute(
            "SELECT id FROM kyc_documents WHERE kyc_case_id = ? AND doc_type = ?",
            (case_id, data.doc_type)
        ).fetchone()
        
        if not existing:
            # Create document record
            doc_id = f"kycdoc_{uuid.uuid4().hex[:10]}"
            conn.execute(
                """INSERT INTO kyc_documents
                   (id, user_id, kyc_case_id, doc_type, issuing_country, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (doc_id, user_id, case_id, data.doc_type, data.issuing_country, "pending_upload", now, now)
            )
        else:
            doc_id = existing["id"]
        
        # Generate upload URL (server-generated file reference, signed for this user)
        try:
            upload_info = generate_upload_url(user_id, data.doc_type, data.side)
        except InvalidFileRef:
            raise HTTPException(400, "Unable to create an upload reference")
        
        # The reference is recorded so outstanding (unconfirmed) ones can be counted.
        log_kyc_event(
            user_id, UPLOAD_INITIATED_EVENT, case_id,
            {"doc_type": data.doc_type, "side": data.side, "file_ref": upload_info["file_ref"]}, conn=conn)
        
        return {
            "doc_id": doc_id,
            **upload_info,
        }


@router.post("/documents/confirm")
def confirm_document_upload(
    data: DocumentConfirmRequest,
    user_id: str = Depends(get_current_user_id)
):
    """Confirm that a document upload is complete"""
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        # One write transaction from the editability check to the update, so a
        # concurrent /kyc/submit cannot slip in between and have its evidence
        # swapped afterwards.
        conn.execute("BEGIN IMMEDIATE")
        _require_editable_case(conn, user_id)

        # Verify document belongs to user
        doc = conn.execute(
            "SELECT * FROM kyc_documents WHERE id = ? AND user_id = ?",
            (data.doc_id, user_id)
        ).fetchone()
        
        if not doc:
            raise HTTPException(404, "Document not found")
        
        doc = dict(doc)

        # The reference is never trusted as a path. It must be a reference this
        # server generated for THIS user, THIS document type and THIS side, and
        # the file must really have been uploaded through the signed,
        # authenticated upload endpoint.
        if not document_ref_matches(data.file_ref, user_id, doc["doc_type"], data.side):
            raise HTTPException(400, "Invalid file reference")
        stored = read_stored_file(data.file_ref)
        if stored is None:
            raise HTTPException(400, "File has not been uploaded")
        stored_content, stored_content_type = stored
        stored_size = len(stored_content)
        del stored_content

        # A re-upload replaces the previous file for that side.
        previous_ref = doc["front_file_ref"] if data.side == "front" else doc["back_file_ref"]
        if previous_ref and previous_ref != data.file_ref and file_ref_belongs_to(previous_ref, user_id):
            delete_file(previous_ref)
        
        # Update file reference (size and type come from the stored file, not the client)
        if data.side == "front":
            conn.execute(
                """UPDATE kyc_documents SET 
                   front_file_ref = ?, file_content_type = ?, file_size_bytes = ?,
                   status = 'pending_review', uploaded_at = ?, updated_at = ?
                   WHERE id = ?""",
                (data.file_ref, stored_content_type, stored_size, now, now, data.doc_id)
            )
        else:
            conn.execute(
                """UPDATE kyc_documents SET 
                   back_file_ref = ?, updated_at = ?
                   WHERE id = ?""",
                (data.file_ref, now, data.doc_id)
            )
        
        # Check if document is complete (front required, back optional for passport)
        doc = conn.execute("SELECT * FROM kyc_documents WHERE id = ?", (data.doc_id,)).fetchone()
        doc = dict(doc)
        
        is_complete = doc["front_file_ref"] is not None
        if doc["doc_type"] != "passport" and doc["back_file_ref"] is None:
            is_complete = False
        
        # Update completed steps if document is complete
        if is_complete:
            case = conn.execute("SELECT * FROM kyc_cases WHERE id = ?", (doc["kyc_case_id"],)).fetchone()
            completed = json.loads(case["completed_steps"])
            if "id_document" not in completed:
                completed.append("id_document")
                conn.execute(
                    "UPDATE kyc_cases SET completed_steps = ?, updated_at = ? WHERE id = ?",
                    (json.dumps(completed), now, doc["kyc_case_id"])
                )
        
        log_kyc_event(user_id, "kyc_id_uploaded", doc["kyc_case_id"], {"doc_id": data.doc_id, "side": data.side}, conn=conn)
        
        return {
            "success": True,
            "doc_id": data.doc_id,
            "is_complete": is_complete,
        }


@router.get("/documents")
def list_documents(user_id: str = Depends(get_current_user_id)):
    """List all uploaded documents"""
    db = DB()
    
    with db.connect() as conn:
        docs = conn.execute(
            "SELECT id, doc_type, issuing_country, status, front_file_ref, back_file_ref, uploaded_at FROM kyc_documents WHERE user_id = ?",
            (user_id,)
        ).fetchall()
        
        return {
            "documents": [
                {
                    "id": d["id"],
                    "doc_type": d["doc_type"],
                    "issuing_country": d["issuing_country"],
                    "status": d["status"],
                    "has_front": d["front_file_ref"] is not None,
                    "has_back": d["back_file_ref"] is not None,
                    "uploaded_at": d["uploaded_at"],
                }
                for d in docs
            ]
        }


@router.delete("/documents/{doc_id}")
def delete_document(
    doc_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """Delete a document (for re-upload)"""
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        doc = conn.execute(
            "SELECT * FROM kyc_documents WHERE id = ? AND user_id = ?",
            (doc_id, user_id)
        ).fetchone()
        
        if not doc:
            raise HTTPException(404, "Document not found")
        
        doc = dict(doc)

        _require_editable_case(conn, user_id)
        
        # Delete files from storage. The references come from this user's own
        # database row, and are only honoured if they are well-formed
        # references inside this user's storage area (a legacy row could hold
        # a client-supplied path).
        for ref in (doc["front_file_ref"], doc["back_file_ref"]):
            if ref and file_ref_belongs_to(ref, user_id):
                delete_file(ref)
        
        # Reset document record
        conn.execute(
            """UPDATE kyc_documents SET 
               front_file_ref = NULL, back_file_ref = NULL,
               status = 'pending_upload', uploaded_at = NULL, updated_at = ?
               WHERE id = ?""",
            (now, doc_id)
        )
        
        # Remove from completed steps
        case = conn.execute("SELECT * FROM kyc_cases WHERE id = ?", (doc["kyc_case_id"],)).fetchone()
        completed = json.loads(case["completed_steps"])
        if "id_document" in completed:
            completed.remove("id_document")
            conn.execute(
                "UPDATE kyc_cases SET completed_steps = ?, updated_at = ? WHERE id = ?",
                (json.dumps(completed), now, doc["kyc_case_id"])
            )
        
        log_kyc_event(user_id, "kyc_document_deleted", doc["kyc_case_id"], {"doc_id": doc_id}, conn=conn)
        
        return {"success": True, "message": "Document deleted"}


async def _read_limited_body(request: Request, limit: int) -> bytes:
    """Read the request body, refusing anything larger than ``limit`` bytes."""
    declared = request.headers.get("content-length")
    if declared:
        try:
            if int(declared) > limit:
                raise HTTPException(413, f"File too large. Max size: {limit // 1024 // 1024}MB")
        except ValueError:
            raise HTTPException(400, "Invalid Content-Length")
    chunks = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise HTTPException(413, f"File too large. Max size: {limit // 1024 // 1024}MB")
        chunks.append(chunk)
    return b"".join(chunks)


@router.put("/documents/upload/{file_ref:path}")
async def upload_document_file(
    file_ref: str,
    request: Request,
    expires: int = Query(...),
    sig: str = Query(...),
    user_id: str = Depends(get_current_user_id),
):
    """
    Handle direct file upload (raw request body, like an S3 presigned PUT).

    Requires an authenticated user AND a signed URL issued to that same user
    for exactly this file reference. The content is validated (size, magic
    bytes) and stored encrypted.
    """
    # 1. Verify URL signature (bound to user id + PUT + file ref + expiry)
    if not verify_upload_signature(user_id, file_ref, expires, sig):
        raise HTTPException(403, "Invalid or expired upload signature")

    is_selfie = is_selfie_ref(file_ref, user_id)

    # 2. The case must still be editable; a selfie must be the reference this
    #    server issued for the user's current face-verification session.
    db = DB()
    with db.connect() as conn:
        _require_editable_case(conn, user_id)
        if is_selfie:
            issued = conn.execute(
                "SELECT 1 FROM kyc_selfie_checks WHERE user_id = ? AND selfie_file_ref = ?",
                (user_id, file_ref)
            ).fetchone()
            if not issued:
                raise HTTPException(403, "Invalid or expired upload signature")

    # 3. Extract content (bounded)
    content = await _read_limited_body(request, MAX_FILE_SIZE)
    if not content:
        raise HTTPException(400, "Empty file content")

    # 4. Save. The stored type comes from the file's magic bytes; a declared
    #    Content-Type that contradicts the content is rejected.
    #
    #    The client decides how long the body takes to arrive, so the check in
    #    step 2 may be arbitrarily old by now: a request held open across
    #    /kyc/submit would otherwise replace a file that is already under
    #    review. Everything is therefore checked AGAIN here, inside a write
    #    transaction that is held until the file is on disk (/kyc/submit takes
    #    the same lock, so the two cannot interleave).
    declared_extension = extension_for_content_type(request.headers.get("content-type"))
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        _require_editable_case(conn, user_id)
        if is_selfie:
            issued = conn.execute(
                "SELECT 1 FROM kyc_selfie_checks WHERE user_id = ? AND selfie_file_ref = ?",
                (user_id, file_ref)
            ).fetchone()
            if not issued:
                raise HTTPException(403, "Invalid or expired upload signature")
        elif not file_exists(file_ref):
            # A new file: bounded per user however many URLs were collected.
            if len(_unconfirmed_document_files(conn, user_id)) >= MAX_UNCONFIRMED_UPLOADS:
                raise HTTPException(
                    429, "Too many uploads are waiting to be confirmed. Finish or confirm them, then try again later."
                )
        success, result = save_uploaded_file(file_ref, content, declared_extension, images_only=is_selfie)

    if not success:
        raise HTTPException(400, result)

    return {"success": True, "file_ref": file_ref}


@router.get("/documents/download/{file_ref:path}")
def download_document_file(
    file_ref: str,
    user_id: str = Depends(get_current_user_id),
):
    """
    Download one of the caller's own KYC files.

    Requires an authenticated user; the reference must be recorded in the
    database for that user. Reviewers use the admin API instead.
    """
    db = DB()
    with db.connect() as conn:
        owned = _user_owns_file_ref(conn, user_id, file_ref)
    if not owned:
        raise HTTPException(404, "File not found")

    stored = read_stored_file(file_ref)
    if stored is None:
        raise HTTPException(404, "File not found")
    content, content_type = stored

    return Response(
        content=content,
        media_type=content_type,
        headers={
            "Cache-Control": "no-store, private",
            "X-Content-Type-Options": "nosniff",
            "Content-Disposition": "inline",
        },
    )


# ============================================================================
# Face Verification Endpoints
# ============================================================================

@router.post("/face/start")
def start_face_verification(
    data: FaceVerificationStartRequest,
    user_id: str = Depends(get_current_user_id)
):
    """Start a face verification session"""
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        case = _require_editable_case(conn, user_id)
        
        case_id = case["id"]
        
        # Check for existing check
        existing = conn.execute(
            "SELECT * FROM kyc_selfie_checks WHERE kyc_case_id = ? AND user_id = ?",
            (case_id, user_id)
        ).fetchone()

        # The selfie reference is generated here, by the server, and recorded
        # in the database. /face/complete and the upload endpoint only ever
        # use this recorded reference.
        selfie_ref = None
        if existing:
            existing = dict(existing)
            previous_ref = existing.get("selfie_file_ref")
            if (
                existing["status"] == "pending"
                and previous_ref
                and is_selfie_ref(previous_ref, user_id)
            ):
                # Session already open (e.g. the page was reloaded): keep its reference.
                selfie_ref = previous_ref
            else:
                # Reset for retry: new reference, previous selfie (if any) is removed
                if previous_ref and is_selfie_ref(previous_ref, user_id):
                    delete_file(previous_ref)
                selfie_ref = generate_selfie_ref(user_id)
            conn.execute(
                """UPDATE kyc_selfie_checks SET status = 'pending', selfie_file_ref = ?,
                   confidence_score = NULL, failure_reason = NULL, completed_at = NULL, updated_at = ?
                   WHERE id = ?""",
                (selfie_ref, now, existing["id"])
            )
            check_id = existing["id"]
        else:
            check_id = f"kycface_{uuid.uuid4().hex[:10]}"
            session_id = f"session_{uuid.uuid4().hex[:16]}"
            selfie_ref = generate_selfie_ref(user_id)
            
            conn.execute(
                """INSERT INTO kyc_selfie_checks
                   (id, user_id, kyc_case_id, provider, provider_session_id, status, selfie_file_ref, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (check_id, user_id, case_id, "internal", session_id, "pending", selfie_ref, now, now)
            )

        # A face-verification restart invalidates the completed step until a new selfie is uploaded
        completed = json.loads(case.get("completed_steps") or "[]")
        if "face_verification" in completed:
            completed.remove("face_verification")
            conn.execute(
                "UPDATE kyc_cases SET completed_steps = ?, updated_at = ? WHERE id = ?",
                (json.dumps(completed), now, case_id)
            )
        
        # Signed, user-bound upload URL for the selfie (same upload endpoint as documents)
        upload_info = build_upload_url(user_id, selfie_ref)
        
        log_kyc_event(user_id, "kyc_face_started", case_id, conn=conn)
        
        return {
            "check_id": check_id,
            "session_id": check_id,  # For provider-based, this would be provider's session ID
            "selfie_upload_ref": selfie_ref,
            "upload_url": upload_info["upload_url"],
            "expires_at": upload_info["expires_at"],
            "method": upload_info["method"],
            "max_size_bytes": upload_info["max_size_bytes"],
            "allowed_types": sorted(IMAGE_EXTENSIONS - {"jpeg"}),
            "instructions": [
                "Look directly at the camera",
                "Ensure good lighting",
                "Remove glasses and hats",
                "Keep a neutral expression",
            ],
        }


@router.post("/face/complete")
def complete_face_verification(
    data: FaceVerificationCompleteRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Record that the selfie was submitted.

    The outcome is never taken from the client. This endpoint only succeeds
    when the selfie issued by /face/start has really been uploaded, and it
    records the check as "pending_review": a human reviewer decides.
    """
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        case = _require_editable_case(conn, user_id)
        
        case_id = case["id"]
        
        check = conn.execute(
            "SELECT * FROM kyc_selfie_checks WHERE kyc_case_id = ? AND user_id = ?",
            (case_id, user_id)
        ).fetchone()
        
        if not check:
            raise HTTPException(400, "No face verification session found")
        check = dict(check)

        # Only the server-issued reference stored for this user counts.
        selfie_ref = check.get("selfie_file_ref")
        if not selfie_ref or not is_selfie_ref(selfie_ref, user_id):
            raise HTTPException(400, "No face verification session found")
        if data.selfie_file_ref and data.selfie_file_ref != selfie_ref:
            raise HTTPException(400, "Selfie reference does not match the active verification session")
        if stored_file_extension(selfie_ref) not in IMAGE_EXTENSIONS:
            raise HTTPException(400, "Selfie has not been uploaded")
        
        # Update check status: submitted, awaiting manual review
        status = STATUS_PENDING_REVIEW
        conn.execute(
            """UPDATE kyc_selfie_checks SET 
               status = ?, confidence_score = NULL, failure_reason = NULL, completed_at = ?, updated_at = ?
               WHERE id = ?""",
            (status, now, now, check["id"])
        )
        
        # The user's part of this step is done
        completed = json.loads(case.get("completed_steps") or "[]")
        if "face_verification" not in completed:
            completed.append("face_verification")
            conn.execute(
                "UPDATE kyc_cases SET completed_steps = ?, updated_at = ? WHERE id = ?",
                (json.dumps(completed), now, case_id)
            )
        
        log_kyc_event(user_id, "kyc_face_submitted", case_id, conn=conn)
        
        return {
            "success": True,
            "status": status,
            "message": "Selfie submitted for review",
        }


# ============================================================================
# Submit & Review Endpoints
# ============================================================================

@router.post("/submit")
def submit_kyc_for_review(user_id: str = Depends(get_current_user_id)):
    """Submit KYC for review after all steps complete"""
    db = DB()
    now = utc_now_iso()
    
    status = get_full_kyc_status(user_id)
    
    if not status["has_case"]:
        raise HTTPException(400, "No KYC case found")
    
    if not status["can_submit"]:
        raise HTTPException(400, "Please complete all required steps before submitting")
    
    with db.connect() as conn:
        # Take the write lock before looking: uploads / confirmations re-check
        # editability under the same lock, so evidence cannot change between
        # the checks below and the status update.
        conn.execute("BEGIN IMMEDIATE")
        case = _require_editable_case(conn, user_id)

        # Never rely on step flags alone: the evidence must really be on file.
        problems = case_evidence_problems(conn, case)
        if problems:
            raise HTTPException(400, "Please complete all required steps before submitting: " + "; ".join(problems))

        # Update case status. Submission NEVER approves: the case waits in the
        # review queue until a reviewer approves or rejects it.
        conn.execute(
            """UPDATE kyc_cases SET 
               status = 'submitted', submitted_at = ?, approved_at = NULL, rejected_at = NULL,
               rejection_reason = NULL, rejection_codes = NULL, updated_at = ?
               WHERE id = ?""",
            (now, now, case["id"])
        )
        
        log_kyc_event(user_id, "kyc_submitted", case["id"], conn=conn)
        
        return {
            "success": True,
            "status": "submitted",
            "message": "Your verification has been submitted and is awaiting manual review.",
        }


@router.post("/review")
def submit_review_decision(
    case_id: str,
    data: ReviewDecisionRequest,
    user: dict = Depends(get_current_active_user)
):
    """Admin: Submit a review decision (internal endpoint)"""
    # Check admin role
    if user.get("role") != "admin":
        raise HTTPException(403, "Admin access required")
    
    db = DB()
    
    with db.connect() as conn:
        result = apply_review_decision(
            conn,
            case_id,
            data.decision,
            reviewer_id=user["id"],
            reviewer_email=user.get("email"),
            reason=data.notes,
            reason_codes=data.reason_codes,
        )

    return {
        "success": True,
        "case_id": case_id,
        "new_status": result["new_status"],
    }
