"""
Enhanced Admin Monitoring Endpoints
- System health monitoring
- Bot activity tracking
- Real-time activity feed
"""
from fastapi import APIRouter, Depends, HTTPException
from app.persistence.db import DB, utc_now_iso
from app.api.auth import get_current_active_user
from app.schemas.auth import UserResponse
from app.api.admin import require_admin
import psutil
import uuid
import json
from datetime import datetime, timedelta

router = APIRouter(prefix="/admin/monitoring", tags=["admin-monitoring"])

# =====================
# System Health
# =====================

@router.get("/system/health")
async def get_system_health(_admin: UserResponse = Depends(require_admin)):
    """Get overall system health status"""
    try:
        # CPU and Memory usage
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Response time from recent API requests
        db = DB()
        with db.connect() as conn:
            avg_response = conn.execute("""
                SELECT AVG(response_time_ms) as avg_time
                FROM api_requests
                WHERE created_at > datetime('now', '-5 minutes')
            """).fetchone()
            
            error_count = conn.execute("""
                SELECT COUNT(*) as errors
                FROM api_requests
                WHERE status_code >= 500 AND created_at > datetime('now', '-5 minutes')
            """).fetchone()
        
        health_status = "healthy" if cpu_percent < 80 and memory.percent < 85 else "degraded"
        
        return {
            "status": health_status,
            "cpu_percent": round(cpu_percent, 2),
            "memory_percent": round(memory.percent, 2),
            "disk_percent": round(disk.percent, 2),
            "avg_response_time_ms": round(avg_response['avg_time'], 2) if avg_response['avg_time'] else 0,
            "recent_errors": error_count['errors'] if error_count else 0,
            "timestamp": utc_now_iso()
        }
    except Exception as e:
        return {
            "status": "unknown",
            "error": str(e),
            "timestamp": utc_now_iso()
        }

@router.get("/system/metrics")
async def get_system_metrics(_admin: UserResponse = Depends(require_admin)):
    """Get detailed system metrics"""
    db = DB()
    with db.connect() as conn:
        # Get latest metrics from database
        metrics = conn.execute("""
            SELECT metric_name, metric_value, metric_unit, recorded_at
            FROM system_metrics
            ORDER BY recorded_at DESC
            LIMIT 50
        """).fetchall()
        
        # Get API stats
        api_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_requests,
                AVG(response_time_ms) as avg_response_time,
                COUNT(CASE WHEN status_code >= 400 THEN 1 END) as error_count
            FROM api_requests
            WHERE created_at > datetime('now', '-1 hour')
        """).fetchone()
    
    return {
        "metrics": [dict(m) for m in metrics],
        "api_stats": dict(api_stats) if api_stats else {},
        "timestamp": utc_now_iso()
    }

@router.post("/system/record-metric")
async def record_metric(
    metric_name: str,
    metric_value: float,
    metric_unit: str = None,
    _admin: UserResponse = Depends(require_admin)
):
    """Record a system metric"""
    db = DB()
    with db.connect() as conn:
        conn.execute("""
            INSERT INTO system_metrics (id, metric_name, metric_value, metric_unit, recorded_at)
            VALUES (?, ?, ?, ?, ?)
        """, (str(uuid.uuid4()), metric_name, metric_value, metric_unit, utc_now_iso()))
    
    return {"message": "Metric recorded successfully"}

# =====================
# Bot Activity Monitor
# =====================

@router.get("/bots/overview")
async def get_bots_overview(_admin: UserResponse = Depends(require_admin)):
    """Get overview of all bots and their activity"""
    db = DB()
    with db.connect() as conn:
        # Total bots
        total_bots = conn.execute("SELECT COUNT(*) FROM bots").fetchone()[0]
        
        # Active bots (those with recent executions)
        active_bots = conn.execute("""
            SELECT COUNT(DISTINCT bot_id)
            FROM bot_executions
            WHERE executed_at > datetime('now', '-1 hour')
        """).fetchone()[0]
        
        # Recent executions
        recent_executions = conn.execute("""
            SELECT COUNT(*)
            FROM bot_executions
            WHERE executed_at > datetime('now', '-24 hours')
        """).fetchone()[0]
        
        # Success rate
        success_rate_row = conn.execute("""
            SELECT 
                COUNT(CASE WHEN status = 'success' THEN 1 END) * 100.0 / COUNT(*) as rate
            FROM bot_executions
            WHERE executed_at > datetime('now', '-24 hours')
        """).fetchone()
        
        # Total PnL
        total_pnl_row = conn.execute("""
            SELECT SUM(pnl) as total_pnl
            FROM bot_executions
            WHERE pnl IS NOT NULL AND executed_at > datetime('now', '-24 hours')
        """).fetchone()
    
    return {
        "total_bots": total_bots,
        "active_bots": active_bots,
        "executions_24h": recent_executions,
        "success_rate": round(success_rate_row['rate'], 2) if success_rate_row['rate'] else 0,
        "total_pnl_24h": round(total_pnl_row['total_pnl'], 2) if total_pnl_row['total_pnl'] else 0
    }

@router.get("/bots/executions")
async def get_bot_executions(
    limit: int = 50,
    bot_id: str = None,
    _admin: UserResponse = Depends(require_admin)
):
    """Get recent bot executions"""
    db = DB()
    with db.connect() as conn:
        query = """
            SELECT be.*, b.name as bot_name
            FROM bot_executions be
            LEFT JOIN bots b ON be.bot_id = b.id
        """
        params = []
        
        if bot_id:
            query += " WHERE be.bot_id = ?"
            params.append(bot_id)
        
        query += " ORDER BY be.executed_at DESC LIMIT ?"
        params.append(limit)
        
        executions = conn.execute(query, params).fetchall()
    
    return {
        "executions": [dict(e) for e in executions],
        "count": len(executions)
    }

@router.post("/bots/emergency-stop")
async def emergency_stop_all_bots(_admin: UserResponse = Depends(require_admin)):
    """Emergency stop all active bots"""
    db = DB()
    with db.connect() as conn:
        # Update all active bots to paused
        conn.execute("UPDATE bots SET status = 'paused' WHERE status = 'active'")
        
        # Log activity event
        conn.execute("""
            INSERT INTO activity_events (id, event_type, event_category, user_id, description, severity, created_at)
            VALUES (?, 'emergency_stop', 'bot_control', ?, 'All bots emergency stopped by admin', 'critical', ?)
        """, (str(uuid.uuid4()), _admin.id, utc_now_iso()))
    
    return {"message": "All bots stopped successfully"}

# =====================
# Activity Feed
# =====================

@router.get("/activity/events")
async def get_activity_events(
    limit: int = 100,
    event_type: str = None,
    severity: str = None,
    _admin: UserResponse = Depends(require_admin)
):
    """Get recent activity events"""
    db = DB()
    with db.connect() as conn:
        query = "SELECT * FROM activity_events WHERE 1=1"
        params = []
        
        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)
        
        if severity:
            query += " AND severity = ?"
            params.append(severity)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        events = conn.execute(query, params).fetchall()
    
    return {
        "events": [dict(e) for e in events],
        "count": len(events)
    }

@router.post("/activity/log-event")
async def log_activity_event(
    event_type: str,
    event_category: str,
    description: str,
    severity: str = "info",
    user_id: str = None,
    bot_id: str = None,
    metadata: dict = None,
    _admin: UserResponse = Depends(require_admin)
):
    """Log a new activity event"""
    db = DB()
    with db.connect() as conn:
        conn.execute("""
            INSERT INTO activity_events (id, event_type, event_category, user_id, bot_id, description, severity, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid.uuid4()),
            event_type,
            event_category,
            user_id,
            bot_id,
            description,
            severity,
            json.dumps(metadata) if metadata else None,
            utc_now_iso()
        ))
    
    return {"message": "Event logged successfully"}

# =====================
# Transactions
# =====================

@router.get("/transactions")
async def get_transactions(
    limit: int = 50,
    status: str = None,
    transaction_type: str = None,
    _admin: UserResponse = Depends(require_admin)
):
    """Get all transactions with filters"""
    db = DB()
    with db.connect() as conn:
        query = """
            SELECT t.*, u.email as user_email
            FROM transactions t
            LEFT JOIN users u ON t.user_id = u.id
            WHERE 1=1
        """
        params = []
        
        if status:
            query += " AND t.status = ?"
            params.append(status)
        
        if transaction_type:
            query += " AND t.type = ?"
            params.append(transaction_type)
        
        query += " ORDER BY t.created_at DESC LIMIT ?"
        params.append(limit)
        
        transactions = conn.execute(query, params).fetchall()
    
    return {
        "transactions": [dict(t) for t in transactions],
        "count": len(transactions)
    }

@router.post("/transactions/{transaction_id}/approve")
async def approve_transaction(
    transaction_id: str,
    _admin: UserResponse = Depends(require_admin)
):
    """Approve a pending transaction"""
    db = DB()
    with db.connect() as conn:
        conn.execute("""
            UPDATE transactions
            SET status = 'approved', completed_at = ?
            WHERE id = ? AND status = 'pending'
        """, (utc_now_iso(), transaction_id))
        
        # Log activity
        conn.execute("""
            INSERT INTO activity_events (id, event_type, event_category, user_id, description, severity, created_at)
            VALUES (?, 'transaction_approved', 'finance', ?, ?, 'info', ?)
        """, (str(uuid.uuid4()), _admin.id, f"Transaction {transaction_id} approved", utc_now_iso()))
    
    return {"message": "Transaction approved successfully"}

# =====================
# Feature Flags
# =====================

@router.get("/feature-flags")
async def get_feature_flags(_admin: UserResponse = Depends(require_admin)):
    """Get all feature flags"""
    db = DB()
    with db.connect() as conn:
        flags = conn.execute("SELECT * FROM feature_flags ORDER BY name").fetchall()
    
    return {"flags": [dict(f) for f in flags]}

@router.put("/feature-flags/{flag_id}/toggle")
async def toggle_feature_flag(
    flag_id: str,
    enabled: bool,
    _admin: UserResponse = Depends(require_admin)
):
    """Toggle a feature flag"""
    db = DB()
    with db.connect() as conn:
        conn.execute("""
            UPDATE feature_flags
            SET is_enabled = ?, updated_at = ?
            WHERE id = ?
        """, (enabled, utc_now_iso(), flag_id))
    
    return {"message": "Feature flag updated successfully"}
