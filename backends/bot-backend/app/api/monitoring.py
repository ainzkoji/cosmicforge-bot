"""
Enhanced Admin Monitoring Endpoints
- System health monitoring
- Bot activity tracking
- Real-time activity feed
"""
from fastapi import APIRouter, Depends, HTTPException
from shared_lib.persistence.db import DB, utc_now_iso
from app.core.auth import get_current_active_user, require_admin, require_permission
from app.schemas.auth import UserResponse
from app.execution.entry_recovery import EntryRecoveryService
import psutil
import uuid
import json
from datetime import datetime, timedelta

router = APIRouter(tags=["Monitoring"])

# =====================
# System Health
# =====================

@router.get("/system/health")
async def get_system_health(
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("system:monitor"))
):
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
async def get_system_metrics(
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("system:monitor"))
):
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


@router.post("/admin/bots/{bot_instance_id}/test/daily-close")
async def test_daily_close_validation(
    bot_instance_id: str,
    symbol: str = "BTCUSDT",
    side: str = "LONG",
    entry_price: float = 100.0,
    close_price: float = 101.0,
    quantity: float = 1.0,
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("system:monitor")),
):
    """
    Section F-3: Admin-only paper-mode daily close end-to-end validation hook.

    Safety:
    - Admin-only
    - Rejects live mode
    - Audited and persists a validation report
    """
    from app.core.bot_instance_service import get_bot_instance_service
    service = get_bot_instance_service()
    inst = service.get_bot_instance(bot_instance_id)
    if not inst:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if str(inst.mode).lower() == "live":
        raise HTTPException(status_code=403, detail="Daily close validation is paper/testnet-only")

    db = DB()
    from app.product_safety.daily_close_validation import validate_daily_close_paper

    report = validate_daily_close_paper(
        db=db,
        bot_instance_id=bot_instance_id,
        run_id=f"daily_close_validation_{bot_instance_id}",
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        close_price=close_price,
        quantity=quantity,
        estimated_fees=0.0,
        estimated_slippage_pct=0.0,
    )
    return report.to_dict()

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
async def emergency_stop_all_bots(
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("bot:control"))
):
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


@router.get("/entry-protection/{bot_id}")
async def get_entry_protection_rows(
    bot_id: str,
    symbol: str | None = None,
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("system:monitor"))
):
    service = EntryRecoveryService(DB())
    rows = service.list_entries(bot_id=bot_id, symbol=symbol)
    return {"rows": rows, "count": len(rows)}


@router.get("/entry-protection/{bot_id}/events")
async def get_entry_protection_events(
    bot_id: str,
    symbol: str | None = None,
    limit: int = 200,
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("system:monitor"))
):
    service = EntryRecoveryService(DB())
    rows = service.list_events(bot_id=bot_id, symbol=symbol, limit=limit)
    return {"events": rows, "count": len(rows)}


@router.get("/entry-protection/report")
async def get_entry_protection_report(
    bot_id: str | None = None,
    symbol: str | None = None,
    since_ts_ms: int | None = None,
    limit: int = 5000,
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("system:monitor"))
):
    service = EntryRecoveryService(DB())
    return service.get_forensic_report(
        bot_id=bot_id,
        symbol=symbol,
        since_ts_ms=since_ts_ms,
        limit=limit,
    )


@router.post("/entry-protection/{bot_id}/release")
async def release_entry_protection_row(
    bot_id: str,
    symbol: str,
    side: str,
    exchange_flat_confirmed: bool,
    note: str = "",
    force: bool = False,
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("system:monitor"))
):
    service = EntryRecoveryService(DB())
    try:
        result = service.release_entry(
            bot_id=bot_id,
            symbol=symbol,
            side=side,
            actor_user_id=_admin.id,
            exchange_flat_confirmed=exchange_flat_confirmed,
            note=note,
            force=force,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result

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
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("billing:write"))
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
        """, (int(enabled), utc_now_iso(), flag_id))
    
    return {"message": "Feature flag updated successfully"}

# =====================
# Circuit Breaker Management
# =====================

@router.post("/admin/circuit-breaker/reset")
async def reset_circuit_breakers(
    bot_id: str = None,
    symbol: str = None,
    broker_id: str = None,
    _admin: UserResponse = Depends(require_admin),
    _perm: str = Depends(require_permission("bot:control"))
):
    """
    Reset circuit breaker state for paused symbols or brokers.
    
    Clears both database state (order_failures table) and in-memory circuit breakers.
    
    Query Parameters:
        bot_id: Reset all symbols for specific bot (config_id)
        symbol: Reset specific symbol for bot_id (requires bot_id)
        broker_id: Reset in-memory broker circuit breaker
    
    Examples:
        - Reset all symbols for bot: /reset?bot_id=bot_abc123
        - Reset specific symbol: /reset?bot_id=bot_abc123&symbol=BTCUSDT
        - Reset broker circuit: /reset?broker_id=binance
        - Reset everything: /reset (no parameters)
    """
    from app.risk.safety_engine import SafetyEngine
    from app.risk.circuit import get_circuit_registry
    from app.core.safety_factory import get_safety_engine
    
    reset_count = 0
    details = {}
    
    # Reset symbol-level circuit breakers (DB state)
    if bot_id:
        try:
            safety = get_safety_engine(bot_id)
            if safety:
                result = safety.reset_symbol_circuit_breaker(bot_id, symbol=symbol)
                details.update(result)
                reset_count += len(result)
            else:
                return {"error": f"Safety engine not found for bot_id={bot_id}"}, 404
        except Exception as e:
            return {"error": f"Failed to reset symbol circuit breakers: {str(e)}"}, 500
    
    # Reset broker-level circuit breakers (in-memory state)
    if broker_id:
        try:
            registry = get_circuit_registry()
            old_state = registry.get_state(broker_id).value
            registry.reset(broker_id)
            details[f"broker:{broker_id}"] = {
                "old_state": old_state,
                "new_state": "NORMAL"
            }
            reset_count += 1
        except Exception as e:
            return {"error": f"Failed to reset broker circuit breaker: {str(e)}"}, 500
    
    # Reset ALL if no parameters provided
    if not bot_id and not broker_id:
        # This is a global reset - should be used with caution
        try:
            registry = get_circuit_registry()
            brokers = registry.list_brokers()
            registry.reset_all()
            for broker in brokers:
                details[f"broker:{broker}"] = {
                    "old_state": "UNKNOWN",
                    "new_state": "NORMAL"
                }
            reset_count += len(brokers)
        except Exception as e:
            return {"error": f"Failed to reset all circuit breakers: {str(e)}"}, 500
    
    return {
        "status": "success",
        "reset_count": reset_count,
        "details": details,
        "timestamp": utc_now_iso()
    }

