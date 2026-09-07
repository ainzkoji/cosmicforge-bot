"""
KYC API Endpoints
Complete API for KYC verification flow
"""
import json
import uuid
from datetime import datetime
from typing import Optional, List, Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from shared_lib.persistence.db import DB
from app.api.auth import get_current_user_id, get_current_active_user
from shared_lib.core.policy.kyc_policy import (
    is_kyc_required, get_full_kyc_status, KYCStatus, KYCAction, check_kyc_gate
)
from app.core.kyc_encryption import encrypt_pii, decrypt_pii, mask_name, mask_pii, hash_document_number
from app.core.kyc_storage import (
    generate_upload_url, save_uploaded_file, generate_download_url,
    verify_upload_signature, delete_file, validate_file_type, generate_selfie_ref, get_file_path
)

router = APIRouter(prefix="/kyc", tags=["KYC"])


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
        print(f"[NOTIFICATION] Triggering notification for user {user_id}: KYC Event '{event_type}'")



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
    selfie_file_ref: Optional[str] = None
    provider_session_id: Optional[str] = None
    passed: bool = True


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
    
    checklist = []
    for step in status["required_steps"]:
        step_info = status["steps"].get(step, {"status": "not_started"})
        checklist.append({
            "step": step,
            "label": step.replace("_", " ").title(),
            "status": step_info["status"],
            "is_complete": step_info["status"] in ["completed", "accepted", "passed"],
            "data": step_info.get("data"),
        })
    
    return {
        "case_status": status["status"],
        "checklist": checklist,
        "can_submit": status["can_submit"],
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
        # Get or create KYC case
        case = conn.execute(
            "SELECT id FROM kyc_cases WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        if not case:
            raise HTTPException(400, "Please start KYC process first")
        
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
        # Get KYC case
        case = conn.execute(
            "SELECT id FROM kyc_cases WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        if not case:
            raise HTTPException(400, "Please start KYC process first")
        
        case_id = case["id"]
        
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
        
        # Generate upload URL
        upload_info = generate_upload_url(user_id, data.doc_type, data.side)
        
        log_kyc_event(user_id, "kyc_id_upload_initiated", case_id, {"doc_type": data.doc_type, "side": data.side}, conn=conn)
        
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
        # Verify document belongs to user
        doc = conn.execute(
            "SELECT * FROM kyc_documents WHERE id = ? AND user_id = ?",
            (data.doc_id, user_id)
        ).fetchone()
        
        if not doc:
            raise HTTPException(404, "Document not found")
        
        doc = dict(doc)
        
        # Update file reference
        if data.side == "front":
            conn.execute(
                """UPDATE kyc_documents SET 
                   front_file_ref = ?, file_content_type = ?, file_size_bytes = ?,
                   status = 'pending_review', uploaded_at = ?, updated_at = ?
                   WHERE id = ?""",
                (data.file_ref, data.content_type, data.file_size_bytes, now, now, data.doc_id)
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
        
        # Delete files from storage
        if doc["front_file_ref"]:
            delete_file(doc["front_file_ref"])
        if doc["back_file_ref"]:
            delete_file(doc["back_file_ref"])
        
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


@router.put("/documents/upload/{file_path:path}")
async def upload_document_file(
    file_path: str,
    request: Request,
    expires: int = Query(...),
    sig: str = Query(...),
):
    """
    Handle direct file upload (simulating S3 PUT).
    Validates signature and saves raw body content.
    """
    # 1. Verify URL signature
    if not verify_upload_signature(file_path, expires, sig):
        raise HTTPException(403, "Invalid or expired upload signature")
    
    # 2. Extract content
    content = await request.body()
    if not content:
        raise HTTPException(400, "Empty file content")
        
    # 3. Determine extension (naive)
    # In a real S3 signed URL scenario, the content-type is strictly enforced
    # Here we just try to guess or use a default, but better to detect from signature or path?
    # Actually, the file_path already contains the intended structure, but not extension?
    # generate_file_ref returns 'documents/{user_id}/{doc_type}_{timestamp}_{unique_id}_{side}'
    # But when we actually save, we need an extension.
    # The client usually PUTs to the URL.
    # We can peek at magic bytes or rely on client headers? 
    # For now, let's trust Content-Type or magic bytes validation in save_uploaded_file?
    # Actually save_uploaded_file expects 'extension'.
    # We can try to infer from content-type header.
    
    content_type = request.headers.get("content-type", "")
    extension = "png" # default
    if "jpeg" in content_type or "jpg" in content_type:
        extension = "jpg"
    elif "pdf" in content_type:
        extension = "pdf"
    
    # 4. Save
    success, result = save_uploaded_file(file_path, content, extension)
    
    if not success:
        raise HTTPException(400, result)
        
    return {"success": True, "path": result}


@router.get("/documents/download/{file_path:path}")
async def download_document_file(
    file_path: str,
    expires: int = Query(...),
    sig: str = Query(...),
):
    """
    Handle secure file download (simulating S3 GET).
    """
    if not verify_upload_signature(file_path, expires, sig):
        raise HTTPException(403, "Invalid or expired download signature")
        
    path_obj = get_file_path(file_path)
    if not path_obj or not path_obj.exists():
        raise HTTPException(404, "File not found")
        
    return FileResponse(path_obj)


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
        case = conn.execute(
            "SELECT id FROM kyc_cases WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        if not case:
            raise HTTPException(400, "Please start KYC process first")
        
        case_id = case["id"]
        
        # Check for existing check
        existing = conn.execute(
            "SELECT id FROM kyc_selfie_checks WHERE kyc_case_id = ?",
            (case_id,)
        ).fetchone()
        
        if existing:
            # Reset for retry
            conn.execute(
                "UPDATE kyc_selfie_checks SET status = 'pending', updated_at = ? WHERE id = ?",
                (now, existing["id"])
            )
            check_id = existing["id"]
        else:
            check_id = f"kycface_{uuid.uuid4().hex[:10]}"
            session_id = f"session_{uuid.uuid4().hex[:16]}"
            
            conn.execute(
                """INSERT INTO kyc_selfie_checks
                   (id, user_id, kyc_case_id, provider, provider_session_id, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (check_id, user_id, case_id, data.provider, session_id, "pending", now, now)
            )
        
        # Generate selfie upload URL
        selfie_ref = generate_selfie_ref(user_id)
        
        log_kyc_event(user_id, "kyc_face_started", case_id, conn=conn)
        
        return {
            "check_id": check_id,
            "session_id": check_id,  # For provider-based, this would be provider's session ID
            "selfie_upload_ref": selfie_ref,
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
    """Mark face verification as complete"""
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        case = conn.execute(
            "SELECT id FROM kyc_cases WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        if not case:
            raise HTTPException(400, "No KYC case found")
        
        case_id = case["id"]
        
        check = conn.execute(
            "SELECT id FROM kyc_selfie_checks WHERE kyc_case_id = ?",
            (case_id,)
        ).fetchone()
        
        if not check:
            raise HTTPException(400, "No face verification session found")
        
        # Update check status
        status = "passed" if data.passed else "failed"
        conn.execute(
            """UPDATE kyc_selfie_checks SET 
               status = ?, selfie_file_ref = ?, completed_at = ?, updated_at = ?
               WHERE id = ?""",
            (status, data.selfie_file_ref, now, now, check["id"])
        )
        
        # Update completed steps if passed
        if data.passed:
            case_row = conn.execute("SELECT completed_steps FROM kyc_cases WHERE id = ?", (case_id,)).fetchone()
            completed = json.loads(case_row["completed_steps"])
            if "face_verification" not in completed:
                completed.append("face_verification")
                conn.execute(
                    "UPDATE kyc_cases SET completed_steps = ?, updated_at = ? WHERE id = ?",
                    (json.dumps(completed), now, case_id)
                )
        
        event_type = "kyc_face_passed" if data.passed else "kyc_face_failed"
        log_kyc_event(user_id, event_type, case_id, conn=conn)
        
        return {
            "success": True,
            "status": status,
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
        # Update case status
        conn.execute(
            """UPDATE kyc_cases SET 
               status = 'submitted', submitted_at = ?, updated_at = ?
               WHERE id = ?""",
            (now, now, status["case_id"])
        )
        
        log_kyc_event(user_id, "kyc_submitted", status["case_id"], conn=conn)
        
        # Auto-approve for demo (in production, this would go to review queue)
        # For now, auto-approve after 5 seconds delay (simulated)
        conn.execute(
            """UPDATE kyc_cases SET 
               status = 'approved', approved_at = ?, updated_at = ?
               WHERE id = ?""",
            (now, now, status["case_id"])
        )
        
        # Create review record
        review_id = f"kycrev_{uuid.uuid4().hex[:10]}"
        conn.execute(
            """INSERT INTO kyc_reviews
               (id, kyc_case_id, reviewer_type, decision, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (review_id, status["case_id"], "system", "approved", now)
        )
        
        log_kyc_event(user_id, "kyc_approved", status["case_id"], actor_type="system", conn=conn)
        
        return {
            "success": True,
            "status": "approved",  # For demo, instant approval
            "message": "Your verification has been approved!",
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
    now = utc_now_iso()
    
    with db.connect() as conn:
        case = conn.execute("SELECT * FROM kyc_cases WHERE id = ?", (case_id,)).fetchone()
        
        if not case:
            raise HTTPException(404, "KYC case not found")
        
        case = dict(case)
        
        # Update case status
        new_status = data.decision
        update_fields = {
            "status": new_status,
            "updated_at": now,
        }
        
        if new_status == "approved":
            update_fields["approved_at"] = now
        elif new_status == "rejected":
            update_fields["rejected_at"] = now
            update_fields["rejection_reason"] = data.notes
            update_fields["rejection_codes"] = json.dumps(data.reason_codes) if data.reason_codes else None
        
        set_clause = ", ".join(f"{k} = ?" for k in update_fields.keys())
        conn.execute(
            f"UPDATE kyc_cases SET {set_clause} WHERE id = ?",
            (*update_fields.values(), case_id)
        )
        
        # Create review record
        review_id = f"kycrev_{uuid.uuid4().hex[:10]}"
        conn.execute(
            """INSERT INTO kyc_reviews
               (id, kyc_case_id, reviewer_id, reviewer_type, decision, reason_codes, notes_encrypted, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                review_id, case_id, user["id"], "admin", data.decision,
                json.dumps(data.reason_codes) if data.reason_codes else None,
                encrypt_pii(data.notes) if data.notes else None,
                now
            )
        )
        
        event_type = f"kyc_{data.decision}"
        log_kyc_event(case["user_id"], event_type, case_id, actor_id=user["id"], actor_type="admin", conn=conn)
        
        return {
            "success": True,
            "case_id": case_id,
            "new_status": new_status,
        }


# ============================================================================
# File Upload/Download (for local dev storage)
# ============================================================================

@router.put("/documents/upload/{file_ref:path}")
async def upload_document_file(
    file_ref: str,
    file: UploadFile = File(...),
    expires: int = Query(...),
    sig: str = Query(...),
    user_id: str = Depends(get_current_user_id)
):
    """Direct file upload endpoint (for local dev)"""
    # Verify signature
    if not verify_upload_signature(file_ref, expires, sig):
        raise HTTPException(403, "Invalid or expired upload URL")
    
    # Validate file
    if not validate_file_type(file.filename or ""):
        raise HTTPException(400, "Invalid file type")
    
    content = await file.read()
    extension = (file.filename or "").rsplit(".", 1)[-1].lower()
    
    success, result = save_uploaded_file(file_ref, content, extension)
    
    if not success:
        raise HTTPException(400, result)
    
    return {"success": True, "file_ref": file_ref}


@router.get("/documents/download/{file_ref:path}")
def download_document_file(
    file_ref: str,
    expires: int = Query(...),
    sig: str = Query(...),
    user_id: str = Depends(get_current_user_id)
):
    """Document download endpoint (for local dev)"""
    from fastapi.responses import FileResponse
    from app.core.kyc_storage import get_file_path
    
    # Verify signature
    if not verify_upload_signature(file_ref, expires, sig):
        raise HTTPException(403, "Invalid or expired download URL")
    
    file_path = get_file_path(file_ref)
    if not file_path:
        raise HTTPException(404, "File not found")
    
    return FileResponse(str(file_path))
