"""
KYC Policy Engine
Determines when KYC is required and what actions are gated
"""
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from app.persistence.db import DB


class KYCAction(str, Enum):
    """Actions that may require KYC"""
    START_LIVE_TRADING = "start_live_trading"
    BECOME_SIGNAL_PROVIDER = "become_signal_provider"
    WITHDRAW_FUNDS = "withdraw_funds"
    DEVELOPER_API_ACCESS = "developer_api_access"
    INCREASE_LIMITS = "increase_limits"
    LINK_BROKER = "link_broker"


class KYCStatus(str, Enum):
    """KYC case statuses"""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    SUBMITTED = "submitted"
    UNDER_REVIEW = "under_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_RESUBMISSION = "needs_resubmission"
    EXPIRED = "expired"


@dataclass
class KYCRequirement:
    """Result of KYC policy check"""
    is_required: bool
    reason: str
    required_status: KYCStatus
    current_status: Optional[KYCStatus]
    is_satisfied: bool
    blocked_actions: List[str]
    allowed_actions: List[str]
    required_steps: List[str]
    completed_steps: List[str]


# Countries that require KYC for all actions
HIGH_RISK_COUNTRIES = {"US", "GB", "DE", "FR", "AU", "CA", "JP", "SG"}

# Countries exempt from KYC (for demo trading only)
EXEMPT_COUNTRIES = set()  # Empty for now, can add countries that don't require KYC


def get_user_kyc_status(user_id: str) -> Optional[Dict[str, Any]]:
    """Get user's current KYC case status"""
    db = DB()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM kyc_cases WHERE user_id = ?",
            (user_id,)
        ).fetchone()
        return dict(row) if row else None


def get_kyc_requirements_config(action: str) -> Optional[Dict[str, Any]]:
    """Get KYC requirements config for an action"""
    db = DB()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM kyc_requirements_config WHERE action_name = ?",
            (action,)
        ).fetchone()
        return dict(row) if row else None


def is_kyc_required(
    user_id: str,
    action: str,
    user_country: Optional[str] = None,
    user_tier: Optional[str] = None
) -> KYCRequirement:
    """
    Main policy engine function.
    Determines if KYC is required for a user to perform an action.
    
    Args:
        user_id: The user's ID
        action: The action being attempted (from KYCAction enum)
        user_country: User's country code (optional, fetched if not provided)
        user_tier: User's account tier (optional)
    
    Returns:
        KYCRequirement with full details
    """
    # Get action requirements from config
    config = get_kyc_requirements_config(action)
    
    # Default: action not configured means not required
    if not config:
        return KYCRequirement(
            is_required=False,
            reason="Action not in KYC requirements",
            required_status=KYCStatus.APPROVED,
            current_status=None,
            is_satisfied=True,
            blocked_actions=[],
            allowed_actions=[action],
            required_steps=[],
            completed_steps=[],
        )
    
    # Check if action requires KYC
    requires_kyc = bool(config.get("requires_kyc", 1))
    required_status = config.get("required_status", "approved")
    
    # Check for country exceptions
    if user_country:
        import json
        country_exceptions = config.get("country_exceptions")
        if country_exceptions:
            exempt_countries = json.loads(country_exceptions) if isinstance(country_exceptions, str) else country_exceptions
            if user_country in exempt_countries:
                requires_kyc = False
    
    # Check for tier exceptions
    if user_tier:
        import json
        tier_exceptions = config.get("tier_exceptions")
        if tier_exceptions:
            exempt_tiers = json.loads(tier_exceptions) if isinstance(tier_exceptions, str) else tier_exceptions
            if user_tier in exempt_tiers:
                requires_kyc = False
    
    # Get user's current KYC status
    kyc_case = get_user_kyc_status(user_id)
    current_status = KYCStatus(kyc_case["status"]) if kyc_case else None
    
    # Parse steps
    import json
    required_steps = json.loads(kyc_case.get("required_steps", "[]")) if kyc_case else ["personal_info", "id_document", "face_verification"]
    completed_steps = json.loads(kyc_case.get("completed_steps", "[]")) if kyc_case else []
    
    # Determine if satisfied
    is_satisfied = True
    if requires_kyc:
        if not current_status:
            is_satisfied = False
        elif current_status != KYCStatus.APPROVED:
            is_satisfied = False
    
    # Get all blocked actions for this user
    blocked_actions = []
    allowed_actions = []
    
    db = DB()
    with db.connect() as conn:
        all_configs = conn.execute("SELECT action_name, requires_kyc FROM kyc_requirements_config").fetchall()
        for row in all_configs:
            action_name = row["action_name"]
            if row["requires_kyc"] and not is_satisfied:
                blocked_actions.append(action_name)
            else:
                allowed_actions.append(action_name)
    
    reason = "KYC approved" if is_satisfied else (
        f"KYC required for {action}" if requires_kyc else "KYC not required"
    )
    
    return KYCRequirement(
        is_required=requires_kyc,
        reason=reason,
        required_status=KYCStatus(required_status),
        current_status=current_status,
        is_satisfied=is_satisfied,
        blocked_actions=blocked_actions,
        allowed_actions=allowed_actions,
        required_steps=required_steps,
        completed_steps=completed_steps,
    )


def get_full_kyc_status(user_id: str) -> Dict[str, Any]:
    """
    Get complete KYC status for a user including all steps.
    Used for the status/checklist endpoint.
    """
    import json
    
    db = DB()
    with db.connect() as conn:
        # Get KYC case
        case = conn.execute(
            "SELECT * FROM kyc_cases WHERE user_id = ?", (user_id,)
        ).fetchone()
        
        if not case:
            return {
                "has_case": False,
                "status": KYCStatus.NOT_STARTED.value,
                "required_steps": ["personal_info", "id_document", "face_verification"],
                "completed_steps": [],
                "steps": {
                    "personal_info": {"status": "not_started", "data": None},
                    "id_document": {"status": "not_started", "data": None},
                    "face_verification": {"status": "not_started", "data": None},
                },
                "can_submit": False,
                "rejection_reason": None,
            }
        
        case = dict(case)
        required_steps = json.loads(case.get("required_steps", "[]"))
        completed_steps = json.loads(case.get("completed_steps", "[]"))
        
        # Get personal info status
        profile = conn.execute(
            "SELECT id, created_at FROM kyc_profiles WHERE kyc_case_id = ?",
            (case["id"],)
        ).fetchone()
        
        # Get documents status
        docs = conn.execute(
            "SELECT id, doc_type, status, front_file_ref, back_file_ref FROM kyc_documents WHERE kyc_case_id = ?",
            (case["id"],)
        ).fetchall()
        
        # Get selfie status
        selfie = conn.execute(
            "SELECT id, status FROM kyc_selfie_checks WHERE kyc_case_id = ?",
            (case["id"],)
        ).fetchone()
        
        # Build steps status
        steps = {
            "personal_info": {
                "status": "completed" if profile else "not_started",
                "data": {"submitted_at": profile["created_at"]} if profile else None,
            },
            "id_document": {
                "status": "not_started",
                "data": None,
            },
            "face_verification": {
                "status": "not_started",
                "data": None,
            },
        }
        
        # Check document status
        if docs:
            doc = dict(docs[0])
            if doc["front_file_ref"]:
                steps["id_document"]["status"] = "completed" if doc["status"] == "accepted" else doc["status"]
                steps["id_document"]["data"] = {"doc_type": doc["doc_type"]}
        
        # Check selfie status
        if selfie:
            selfie = dict(selfie)
            steps["face_verification"]["status"] = selfie["status"]
        
        # Determine if can submit
        can_submit = all(
            steps.get(step, {}).get("status") in ["completed", "pending_review", "accepted", "passed"]
            for step in required_steps
        )
        
        return {
            "has_case": True,
            "case_id": case["id"],
            "status": case["status"],
            "required_steps": required_steps,
            "completed_steps": completed_steps,
            "steps": steps,
            "can_submit": can_submit,
            "rejection_reason": case.get("rejection_reason"),
            "rejection_codes": json.loads(case.get("rejection_codes", "[]")) if case.get("rejection_codes") else [],
            "created_at": case["created_at"],
            "submitted_at": case.get("submitted_at"),
            "approved_at": case.get("approved_at"),
        }


def check_kyc_gate(user_id: str, action: str) -> Tuple[bool, str]:
    """
    Gate function to check if action is allowed.
    Use this as a guard before sensitive operations.
    
    Returns:
        Tuple of (allowed: bool, reason: str)
    """
    req = is_kyc_required(user_id, action)
    
    if req.is_required and not req.is_satisfied:
        return False, f"KYC verification required. Current status: {req.current_status.value if req.current_status else 'not started'}"
    
    return True, "OK"



