"""
Admin API Endpoints
- Dashboard stats
- User management
- Revenue analytics
- Commission settings
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from app.persistence.db import DB, utc_now_iso
from app.api.auth import get_current_active_user
from app.schemas.auth import UserResponse
from typing import List, Optional
import uuid

router = APIRouter(prefix="/admin", tags=["admin"])

# Middleware: Require admin role
async def require_admin(current_user: dict = Depends(get_current_active_user)):
    """Middleware to ensure user has admin role"""
    db = DB()
    with db.connect() as conn:
        admin_role = conn.execute(
            "SELECT * FROM admin_roles WHERE user_id = ? AND revoked_at IS NULL",
            (current_user["id"],)
        ).fetchone()
        
        if not admin_role:
            raise HTTPException(status_code=403, detail="Admin access required")
    
    return current_user

# =====================
# Dashboard Stats
# =====================

@router.get("/dashboard/stats")
async def get_dashboard_stats(_admin: dict = Depends(require_admin)):
    """Get overview statistics for admin dashboard"""
    db = DB()
    with db.connect() as conn:
        # Total users
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        
        # Active subscriptions (mock data)
        active_subscriptions = 142890
        
        # Total revenue (sum from revenue_snapshots)
        total_revenue_row = conn.execute(
            "SELECT SUM(total_revenue) FROM revenue_snapshots"
        ).fetchone()
        total_revenue = total_revenue_row[0] if total_revenue_row and total_revenue_row[0] else 0
        
        # Platform trades (sum from users)
        total_trades_row = conn.execute(
            "SELECT SUM(total_trades) FROM users"
        ).fetchone()
        total_trades = total_trades_row[0] if total_trades_row and total_trades_row[0] else 0
        
    return {
        "total_users": total_users,
        "active_subscriptions": active_subscriptions,
        "total_revenue": total_revenue,
        "platform_trades": total_trades
    }

@router.get("/dashboard/revenue-overview")
async def get_revenue_overview(_admin: dict = Depends(require_admin)):
    """Get revenue chart data for last 12 months"""
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("""
            SELECT date, subscription_revenue, commission_revenue, total_revenue
            FROM revenue_snapshots
            ORDER BY date DESC
            LIMIT 12
        """).fetchall()
        
    return {
        "data": [dict(row) for row in rows]
    }

# =====================
# User Management
# =====================

@router.get("/users")
async def list_users(
    _admin: dict = Depends(require_admin),
    status: Optional[str] = None,
    limit: int = Query(50, le=100)
):
    """List all users with optional filters"""
    db = DB()
    with db.connect() as conn:
        query = """
            SELECT id, email, status, role, created_at, last_login_at, 
                   total_trades, total_commission,
                   CASE WHEN is_verified THEN 'verified' ELSE 'unverified' END as verification_status
            FROM users
        """
        params = []
        
        if status:
            query += " WHERE status = ?"
            params.append(status)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        rows = conn.execute(query, params).fetchall()
    
    return {
        "users": [dict(row) for row in rows],
        "count": len(rows)
    }

@router.get("/users/{user_id}")
async def get_user_details(
    user_id: str,
    _admin: dict = Depends(require_admin)
):
    """Get detailed user information"""
    db = DB()
    with db.connect() as conn:
        user = conn.execute("""
            SELECT id, email, status, role, created_at, last_login_at,
                   total_trades, total_commission, is_verified
            FROM users
            WHERE id = ?
        """, (user_id,)).fetchone()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
    
    return dict(user)

@router.post("/users/{user_id}/suspend")
async def suspend_user(
    user_id: str,
    _admin: dict = Depends(require_admin)
):
    """Suspend a user account"""
    db = DB()
    with db.connect() as conn:
        conn.execute(
            "UPDATE users SET status = 'suspended' WHERE id = ?",
            (user_id,)
        )
        
        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'user_suspended', ?, ?, ?)
        """, (str(uuid.uuid4()), user_id, f"Suspended by admin", utc_now_iso()))
    
    return {"message": "User suspended successfully"}

@router.post("/users/{user_id}/activate")
async def activate_user(
    user_id: str,
    _admin: dict = Depends(require_admin)
):
    """Activate a suspended user account"""
    db = DB()
    with db.connect() as conn:
        conn.execute(
            "UPDATE users SET status = 'active' WHERE id = ?",
            (user_id,)
        )
        
        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'user_activated', ?, ?, ?)
        """, (str(uuid.uuid4()), user_id, f"Activated by admin", utc_now_iso()))
    
    return {"message": "User activated successfully"}

# =====================
# Revenue Analytics
# =====================

@router.get("/revenue/overview")
async def get_revenue_overview(_admin: dict = Depends(require_admin)):
    """Get detailed revenue breakdown"""
    db = DB()
    with db.connect() as conn:
        # Get latest metrics
        latest = conn.execute("""
            SELECT SUM(subscription_revenue) as sub_rev,
                   SUM(commission_revenue) as comm_rev,
                   SUM(total_revenue) as total_rev
            FROM revenue_snapshots
        """).fetchone()
        
        # Revenue by plan (mock data for now)
        by_plan = [
            {"plan": "Enterprise", "revenue": 450000, "percentage": 51},
            {"plan": "Pro", "revenue": 320000, "percentage": 36},
            {"plan": "Free", "revenue": 142000, "percentage": 13}
        ]
    
    return {
        "total_revenue": latest["total_rev"] if latest else 0,
        "subscription_revenue": latest["sub_rev"] if latest else 0,
        "commission_revenue": latest["comm_rev"] if latest else 0,
        "revenue_by_plan": by_plan
    }

# =====================
# Commission Settings
# =====================

@router.get("/commissions/tiers")
async def get_commission_tiers(_admin: dict = Depends(require_admin)):
    """Get all commission tiers"""
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("""
            SELECT id, name, min_volume, max_volume, rate, is_active
            FROM commission_tiers
            ORDER BY min_volume ASC
        """).fetchall()
    
    return {"tiers": [dict(row) for row in rows]}

@router.put("/commissions/tiers/{tier_id}")
async def update_commission_tier(
    tier_id: str,
    rate: float,
    is_active: bool,
    _admin: dict = Depends(require_admin)
):
    """Update a commission tier"""
    db = DB()
    with db.connect() as conn:
        conn.execute("""
            UPDATE commission_tiers
            SET rate = ?, is_active = ?, updated_at = ?
            WHERE id = ?
        """, (rate, is_active, utc_now_iso(), tier_id))
    
    return {"message": "Tier updated successfully"}

# =====================
# Audit Logs
# =====================

@router.get("/audit/logs")
async def get_audit_logs(
    _admin: dict = Depends(require_admin),
    event_type: Optional[str] = None,
    limit: int = Query(50, le=200)
):
    """Get audit logs with optional filtering"""
    db = DB()
    with db.connect() as conn:
        query = """
            SELECT id, event_type, user_id, email, ip, details, created_at
            FROM auth_audit_log
        """
        params = []
        
        if event_type:
            query += " WHERE event_type = ?"
            params.append(event_type)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        rows = conn.execute(query, params).fetchall()
    
    return {
        "logs": [dict(row) for row in rows],
        "count": len(rows)
    }

# =====================
# Compliance
# =====================

@router.get("/compliance/kyc-pending")
async def get_pending_kyc(_admin: dict = Depends(require_admin)):
    """Get pending KYC submissions"""
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("""
            SELECT k.id, k.user_id, u.email as user_email, k.document_type,
                   k.risk_level, k.status, k.submitted_at
            FROM kyc_submissions k
            JOIN users u ON k.user_id = u.id
            WHERE k.status = 'pending'
            ORDER BY k.submitted_at DESC
        """).fetchall()
    
    return {
        "submissions": [dict(row) for row in rows],
        "count": len(rows)
    }

@router.get("/compliance/aml-flags")
async def get_aml_flags(_admin: dict = Depends(require_admin)):
    """Get AML alerts"""
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("""
            SELECT a.id, a.user_id, u.email as user_email, a.alert_type,
                   a.severity, a.description, a.status, a.created_at
            FROM aml_alerts a
            JOIN users u ON a.user_id = u.id
            WHERE a.status = 'open'
            ORDER BY a.created_at DESC
        """).fetchall()
    
    return {
        "alerts": [dict(row) for row in rows],
        "count": len(rows)
    }

# =====================
# Admin Role Management
# =====================

@router.post("/roles/grant")
async def grant_admin_role(
    user_id: str,
    _admin: dict = Depends(require_admin)
):
    """Grant admin role to a user"""
    db = DB()
    with db.connect() as conn:
        # Check if user exists
        user = conn.execute("SELECT id, email FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Check if already has admin role
        existing = conn.execute(
            "SELECT * FROM admin_roles WHERE user_id = ? AND revoked_at IS NULL",
            (user_id,)
        ).fetchone()
        
        if existing:
            raise HTTPException(status_code=400, detail="User already has admin role")
        
        # Grant admin role
        role_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO admin_roles (id, user_id, role, granted_by, granted_at)
            VALUES (?, ?, 'admin', ?, ?)
        """, (role_id, user_id, _admin["id"], utc_now_iso()))
        
        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'admin_role_granted', ?, ?, ?)
        """, (str(uuid.uuid4()), user_id, f"Admin role granted by {_admin['email']}", utc_now_iso()))
    
    return {
        "message": "Admin role granted successfully",
        "user_id": user_id,
        "user_email": user["email"]
    }

@router.post("/roles/revoke")
async def revoke_admin_role(
    user_id: str,
    _admin: dict = Depends(require_admin)
):
    """Revoke admin role from a user"""
    db = DB()
    with db.connect() as conn:
        # Check if user has admin role
        role = conn.execute(
            "SELECT * FROM admin_roles WHERE user_id = ? AND revoked_at IS NULL",
            (user_id,)
        ).fetchone()
        
        if not role:
            raise HTTPException(status_code=404, detail="User does not have admin role")
        
        # Revoke admin role
        conn.execute(
            "UPDATE admin_roles SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
            (utc_now_iso(), user_id)
        )
        
        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'admin_role_revoked', ?, ?, ?)
        """, (str(uuid.uuid4()), user_id, f"Admin role revoked by {_admin['email']}", utc_now_iso()))
    
    return {
        "message": "Admin role revoked successfully",
        "user_id": user_id
    }

# =====================
# KYC Actions
# =====================

@router.post("/compliance/kyc/{submission_id}/approve")
async def approve_kyc_submission(
    submission_id: str,
    _admin: dict = Depends(require_admin)
):
    """Approve a KYC submission"""
    db = DB()
    with db.connect() as conn:
        # Update submission status
        conn.execute("""
            UPDATE kyc_submissions
            SET status = 'approved', reviewed_by = ?, reviewed_at = ?
            WHERE id = ?
        """, (_admin["id"], utc_now_iso(), submission_id))
        
        # Get submission details for logging
        submission = conn.execute(
            "SELECT user_id FROM kyc_submissions WHERE id = ?",
            (submission_id,)
        ).fetchone()
        
        if not submission:
            raise HTTPException(status_code=404, detail="Submission not found")
        
        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'kyc_approved', ?, ?, ?)
        """, (str(uuid.uuid4()), submission["user_id"], 
              f"KYC approved by admin {_admin['email']}", utc_now_iso()))
    
    return {"message": "KYC submission approved"}

@router.post("/compliance/kyc/{submission_id}/reject")
async def reject_kyc_submission(
    submission_id: str,
    reason: str,
    _admin: dict = Depends(require_admin)
):
    """Reject a KYC submission"""
    db = DB()
    with db.connect() as conn:
        # Update submission status
        conn.execute("""
            UPDATE kyc_submissions
            SET status = 'rejected', reviewed_by = ?, reviewed_at = ?
            WHERE id = ?
        """, (_admin["id"], utc_now_iso(), submission_id))
        
        # Get submission details
        submission = conn.execute(
            "SELECT user_id FROM kyc_submissions WHERE id = ?",
            (submission_id,)
        ).fetchone()
        
        if not submission:
            raise HTTPException(status_code=404, detail="Submission not found")
        
        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'kyc_rejected', ?, ?, ?)
        """, (str(uuid.uuid4()), submission["user_id"], 
              f"KYC rejected by admin {_admin['email']}. Reason: {reason}", utc_now_iso()))
    
    return {"message": "KYC submission rejected"}

