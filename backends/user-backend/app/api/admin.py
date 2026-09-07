"""
Admin API Endpoints
- Dashboard stats
- User management
- Revenue analytics
- Commission settings
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from shared_lib.persistence.db import DB, utc_now_iso
from app.core.deps import require_admin
from app.schemas.auth import UserResponse
from typing import List, Optional
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import math
import uuid
import json
import logging
import time
from pydantic import BaseModel, EmailStr, Field
from app.core.security import get_password_hash

class OperatorCreate(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)
    name: str
    role: str = Field(..., pattern="^(admin|operator|support)$")


router = APIRouter(prefix="/admin", tags=["admin"])
logger = logging.getLogger(__name__)


def _log_admin_timing(endpoint: str, started_at: float, *, row_count: int | None = None, status: str = "success", error: str | None = None) -> None:
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    payload = {
        "endpoint": endpoint,
        "duration_ms": duration_ms,
        "status": status,
    }
    if row_count is not None:
        payload["row_count"] = row_count
    if error:
        payload["error"] = error
    log_fn = logger.warning if status != "success" or duration_ms >= 1000 else logger.info
    log_fn("admin_endpoint_timing %s", payload)

# Dashboard Stats
# =====================

@router.get("/dashboard/stats")
async def get_dashboard_stats(_admin: dict = Depends(require_admin)):
    """Get overview statistics for admin dashboard"""
    started_at = time.perf_counter()
    try:
        db = DB()
        with db.connect() as conn:
            # Total users
            total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

            # Active subscriptions
            active_subscriptions = conn.execute(
                "SELECT COUNT(*) FROM subscriptions WHERE status = 'active'"
            ).fetchone()[0]

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

        _log_admin_timing("GET /api/admin/dashboard/stats", started_at, row_count=1)
        return {
            "total_users": total_users,
            "active_subscriptions": active_subscriptions,
            "total_revenue": total_revenue,
            "platform_trades": total_trades
        }
    except Exception as exc:
        _log_admin_timing("GET /api/admin/dashboard/stats", started_at, status="failed", error=type(exc).__name__)
        raise

@router.get("/dashboard/revenue-overview")
async def get_revenue_overview(
    timeframe: str = Query("12m", pattern="^(30d|6m|12m)$"),
    _admin: dict = Depends(require_admin)
):
    """Get revenue chart data for selected timeframe"""
    started_at = time.perf_counter()
    try:
        db = DB()

        # Calculate cutoff date
        now = datetime.now(timezone.utc)
        if timeframe == "30d":
            cutoff_date = (now - timedelta(days=30)).date().isoformat()
        elif timeframe == "6m":
            cutoff_date = (now - timedelta(days=180)).date().isoformat()
        else:  # "12m"
            cutoff_date = (now - timedelta(days=365)).date().isoformat()

        with db.connect() as conn:
            rows = conn.execute("""
                SELECT date, subscription_revenue, commission_revenue, total_revenue
                FROM revenue_snapshots
                WHERE date >= ?
                ORDER BY date ASC
            """, (cutoff_date,)).fetchall()

        _log_admin_timing("GET /api/admin/dashboard/revenue-overview", started_at, row_count=len(rows))
        return {
            "data": [dict(row) for row in rows]
        }
    except Exception as exc:
        _log_admin_timing("GET /api/admin/dashboard/revenue-overview", started_at, status="failed", error=type(exc).__name__)
        raise

@router.get("/dashboard/top-trading-pairs")
async def get_top_trading_pairs(_admin: dict = Depends(require_admin)):
    """Aggregate and return the top 5 traded symbols from active trade fills"""
    started_at = time.perf_counter()
    try:
        db = DB()
        with db.connect() as conn:
            rows = conn.execute("""
                SELECT symbol, COUNT(*) as trade_count
                FROM trade_fills
                WHERE COALESCE(account_id, '') != 'backfill'
                  AND COALESCE(initiator_type, '') != 'SHADOW'
                GROUP BY symbol
                ORDER BY trade_count DESC
                LIMIT 5
            """).fetchall()

        data = [dict(row) for row in rows]
        total = sum(d["trade_count"] for d in data)
        for d in data:
            d["percentage"] = round((d["trade_count"] / total * 100), 1) if total > 0 else 0

        _log_admin_timing("GET /api/admin/dashboard/top-trading-pairs", started_at, row_count=len(data))
        return {"data": data}
    except Exception as exc:
        _log_admin_timing("GET /api/admin/dashboard/top-trading-pairs", started_at, status="failed", error=type(exc).__name__)
        raise

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
    started_at = time.perf_counter()
    try:
        db = DB()
        with db.connect() as conn:
            query = """
                SELECT u.id, u.email, u.status, u.role, u.created_at, u.last_login_at,
                       u.total_trades, u.total_commission,
                       CASE WHEN u.is_verified THEN 'verified' ELSE 'unverified' END as verification_status,
                       (SELECT COUNT(*) FROM bot_instances b WHERE b.user_id = u.id AND b.status = 'active') as active_bots,
                       (SELECT COUNT(*) FROM bot_instances b WHERE b.user_id = u.id) as total_bots
                FROM users u
            """
            params = []

            if status:
                query += " WHERE u.status = ?"
                params.append(status)

            query += " ORDER BY u.created_at DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()

        _log_admin_timing("GET /api/admin/users", started_at, row_count=len(rows))
        return {
            "users": [dict(row) for row in rows],
            "count": len(rows)
        }
    except Exception as exc:
        _log_admin_timing("GET /api/admin/users", started_at, status="failed", error=type(exc).__name__)
        raise

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
async def get_revenue_detail(_admin: dict = Depends(require_admin)):
    """Get detailed revenue breakdown"""
    db = DB()
    with db.connect() as conn:
        # 1. Total Subscription Revenue (From Invoices)
        sub_rev_row = conn.execute("""
            SELECT SUM(amount) FROM invoices WHERE status = 'paid'
        """).fetchone()
        sub_rev = sub_rev_row[0] if sub_rev_row and sub_rev_row[0] else 0.0

        # 2. Total Commission Revenue
        comm_rev_row = conn.execute("""
            SELECT SUM(commission_amount) FROM commission_ledger
        """).fetchone()
        comm_rev = comm_rev_row[0] if comm_rev_row and comm_rev_row[0] else 0.0
        
        total_rev = sub_rev + comm_rev
        
        # 3. Revenue by Plan (Join Invoices + Subscriptions)
        # Fallback to User selected_plan_id if subscription missing?
        # Only count paid invoices within current year/month? The chart implies "overview" (total or TTM?)
        # Let's do All Time for "Overview"
        
        by_plan_rows = conn.execute("""
            SELECT s.plan_id, SUM(i.amount) as rev, count(distinct s.user_id) as users
            FROM invoices i
            JOIN subscriptions s ON i.user_id = s.user_id
            WHERE i.status = 'paid'
            GROUP BY s.plan_id
        """).fetchall()
        
        by_plan_data = {row["plan_id"]: row["rev"] for row in by_plan_rows}
        
        # If no invoices, use active subscription counts as proxied percentage
        if not by_plan_rows and sub_rev == 0:
             sub_counts = conn.execute("SELECT plan_id, count(*) as cnt FROM subscriptions WHERE status='active' GROUP BY plan_id").fetchall()
             total_subs = sum(r["cnt"] for r in sub_counts)
             by_plan = []
             for r in sub_counts:
                 pct = (r["cnt"] / total_subs * 100) if total_subs > 0 else 0
                 by_plan.append({"plan": r["plan_id"] or "Unknown", "revenue": 0, "percentage": round(pct, 1)})
        else:
             # Calculate percentage of revenue
             by_plan = []
             for pid, rev in by_plan_data.items():
                 pct = (rev / sub_rev * 100) if sub_rev > 0 else 0
                 by_plan.append({"plan": pid or "Unknown", "revenue": rev, "percentage": round(pct, 1)})
                 
        # Ensure default structure if empty
        if not by_plan:
             by_plan = [{"plan": "No Data", "revenue": 0, "percentage": 0}]

    return {
        "total_revenue": total_rev,
        "subscription_revenue": sub_rev,
        "commission_revenue": comm_rev,
        "revenue_by_plan": by_plan
    }


# =====================
# Trading Profitability
# =====================

def _admin_safe_float(value):
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
        if math.isnan(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _admin_avg(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def _admin_sum(values):
    return float(sum(v for v in values if v is not None))


def _admin_pct(numerator: int, denominator: int):
    if denominator <= 0:
        return None
    return float(numerator * 100.0 / denominator)


def _admin_profit_factor(pnls):
    wins = sum(v for v in pnls if v is not None and v > 0)
    losses = abs(sum(v for v in pnls if v is not None and v < 0))
    if losses == 0:
        return None if wins == 0 else "inf"
    return float(wins / losses)


def _admin_parse_ts(value):
    if not value:
        return None
    raw = str(value).strip()
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _admin_exit_bucket(row):
    reason = str(row.get("exit_reason") or row.get("trigger_source") or "").upper()
    if "TIME_EXIT" in reason or "TIME" in reason:
        return "TIME_EXIT"
    if "TP" in reason or "TAKE_PROFIT" in reason:
        return "TP"
    if "SL" in reason or "STOP" in reason:
        return "SL"
    return "OTHER"


def _admin_trade_view(row):
    if not row:
        return None
    return {
        "id": row.get("id"),
        "timestamp_utc": row.get("timestamp_utc"),
        "symbol": row.get("symbol"),
        "side": row.get("side"),
        "position_id": row.get("position_id"),
        "realized_pnl": row.get("realized_pnl"),
        "r_multiple": row.get("r_multiple"),
        "exit_reason": row.get("exit_reason"),
        "trigger_source": row.get("trigger_source"),
    }


def _admin_parse_json(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _admin_sizing_cap_message(sizing):
    if not sizing:
        return None
    if sizing.get("admin_message"):
        return sizing.get("admin_message")
    if sizing.get("risk_warning"):
        final_margin = _admin_safe_float(sizing.get("final_margin_usdt") or sizing.get("final_margin"))
        risk_pct = _admin_safe_float(sizing.get("theoretical_risk_pct"))
        risk_level = sizing.get("risk_level_label") or sizing.get("risk_level") or "configured"
        account_risk_pct = _admin_safe_float(sizing.get("account_risk_pct"))
        if final_margin is not None and risk_pct is not None and account_risk_pct is not None:
            return (
                f"Your {final_margin:.2f} USDT fixed margin was respected. "
                f"Estimated risk is {risk_pct:.2f}% of equity, above selected {risk_level} "
                f"risk limit {account_risk_pct:.2f}%. Risk cap was NOT applied because "
                "fixed_amount strict mode is active."
            )
    if not sizing.get("cap_applied"):
        return None
    base_margin = _admin_safe_float(sizing.get("base_margin_usdt") or sizing.get("base_margin"))
    final_margin = _admin_safe_float(sizing.get("final_margin_usdt") or sizing.get("final_margin"))
    risk_level = sizing.get("risk_level_label") or sizing.get("risk_level") or "configured"
    account_risk_pct = _admin_safe_float(sizing.get("account_risk_pct"))
    stop_distance_pct = _admin_safe_float(sizing.get("stop_distance_pct"))
    if base_margin is None or final_margin is None:
        return sizing.get("cap_reason")
    risk_text = f"{account_risk_pct:.2f}%" if account_risk_pct is not None else "the configured"
    stop_text = f"{stop_distance_pct:.2f}%" if stop_distance_pct is not None else "this trade's"
    return (
        f"Your {base_margin:.2f} USDT fixed margin was reduced to {final_margin:.2f} USDT "
        f"because {risk_level} risk allows max {risk_text} account risk and this trade's "
        f"ATR stop distance was {stop_text}."
    )


def _admin_sizing_cap_view(row):
    sizing = _admin_parse_json(row.get("sizing_json"))
    if not (
        sizing.get("cap_applied")
        or sizing.get("risk_warning")
        or sizing.get("sizing_method") == "fixed_amount_strict"
    ):
        return None
    return {
        "trace_id": row.get("trace_id"),
        "timestamp_utc": row.get("ts"),
        "symbol": row.get("symbol"),
        "position_id": row.get("position_id"),
        "signal": row.get("signal"),
        "confidence": row.get("confidence"),
        "allocation_type": sizing.get("allocation_type") or sizing.get("allocation_mode"),
        "user_fixed_margin_usdt": sizing.get("user_fixed_margin_usdt"),
        "base_margin_usdt": sizing.get("base_margin_usdt") or sizing.get("base_margin"),
        "final_margin_usdt": sizing.get("final_margin_usdt") or sizing.get("final_margin"),
        "base_notional_usdt": sizing.get("base_notional_usdt"),
        "final_notional_usdt": sizing.get("final_notional_usdt"),
        "leverage": sizing.get("leverage") or sizing.get("leverage_used_for_cap"),
        "cap_applied": bool(sizing.get("cap_applied")),
        "cap_reason": sizing.get("cap_reason"),
        "atr_cap_margin_usdt": sizing.get("atr_cap_margin_usdt") or sizing.get("margin_cap_usdt"),
        "account_risk_pct": sizing.get("account_risk_pct"),
        "stop_distance_pct": sizing.get("stop_distance_pct"),
        "theoretical_risk_usdt": sizing.get("theoretical_risk_usdt"),
        "theoretical_risk_pct": sizing.get("theoretical_risk_pct"),
        "risk_warning": bool(sizing.get("risk_warning")),
        "max_risk_capital": sizing.get("max_risk_capital"),
        "risk_level": sizing.get("risk_level"),
        "risk_level_label": sizing.get("risk_level_label"),
        "sizing_method": sizing.get("sizing_method"),
        "admin_message": _admin_sizing_cap_message(sizing),
    }


@router.get("/profitability/report")
async def get_profitability_report(_admin: dict = Depends(require_admin)):
    """Read-only profitability dashboard from actual executed trade_fills."""
    db = DB()
    generated_at = datetime.now(timezone.utc)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM trade_fills
            WHERE COALESCE(account_id, '') != 'backfill'
              AND COALESCE(initiator_type, '') != 'SHADOW'
            ORDER BY timestamp_utc ASC, id ASC
            """
        ).fetchall()
        cap_rows = conn.execute(
            """
            SELECT trace_id, ts, symbol, signal, confidence, position_id, sizing_json
            FROM decision_traces
            WHERE sizing_json IS NOT NULL
              AND sizing_json != ''
              AND sizing_json LIKE '%"cap_applied"%'
            ORDER BY ts DESC
            LIMIT 25
            """
        ).fetchall()

    fills = [dict(row) for row in rows]
    open_fills = [row for row in fills if str(row.get("action")).upper() == "OPEN"]
    close_fills = [row for row in fills if str(row.get("action")).upper() == "CLOSE"]
    closed_with_pnl = [row for row in close_fills if _admin_safe_float(row.get("realized_pnl")) is not None]
    pnls = [_admin_safe_float(row.get("realized_pnl")) for row in close_fills]
    r_values = [_admin_safe_float(row.get("r_multiple")) for row in close_fills]
    wins = [pnl for pnl in pnls if pnl is not None and pnl > 0]
    losses = [pnl for pnl in pnls if pnl is not None and pnl < 0]
    closed_position_ids = {row.get("position_id") for row in close_fills if row.get("position_id")}
    open_trades = [
        row
        for row in open_fills
        if not row.get("position_id") or row.get("position_id") not in closed_position_ids
    ]
    position_linked_closed_trades = len(
        {
            row.get("position_id")
            for row in open_fills
            if row.get("position_id") and row.get("position_id") in closed_position_ids
        }
    )
    best = max(closed_with_pnl, key=lambda row: _admin_safe_float(row.get("realized_pnl")) or 0, default=None)
    worst = min(closed_with_pnl, key=lambda row: _admin_safe_float(row.get("realized_pnl")) or 0, default=None)

    per_symbol = []
    by_symbol = defaultdict(list)
    for row in close_fills:
        by_symbol[str(row.get("symbol") or "UNKNOWN")].append(row)
    for symbol, symbol_rows in sorted(by_symbol.items()):
        symbol_pnls = [_admin_safe_float(row.get("realized_pnl")) for row in symbol_rows]
        symbol_r = [_admin_safe_float(row.get("r_multiple")) for row in symbol_rows]
        symbol_wins = [pnl for pnl in symbol_pnls if pnl is not None and pnl > 0]
        exit_counts = Counter(_admin_exit_bucket(row) for row in symbol_rows)
        per_symbol.append(
            {
                "symbol": symbol,
                "trades": len(symbol_rows),
                "win_rate_pct": _admin_pct(len(symbol_wins), len([p for p in symbol_pnls if p is not None])),
                "total_pnl": _admin_sum(symbol_pnls),
                "average_pnl": _admin_avg(symbol_pnls),
                "average_r_multiple": _admin_avg(symbol_r),
                "sl_count": exit_counts.get("SL", 0),
                "tp_count": exit_counts.get("TP", 0),
                "time_exit_count": exit_counts.get("TIME_EXIT", 0),
                "other_count": exit_counts.get("OTHER", 0),
            }
        )
    per_symbol.sort(key=lambda row: row["total_pnl"], reverse=True)

    recent = {}
    for label, delta in {
        "last_24h": timedelta(hours=24),
        "last_48h": timedelta(hours=48),
        "last_7d": timedelta(days=7),
    }.items():
        cutoff = generated_at - delta
        window_rows = [
            row
            for row in close_fills
            if (_admin_parse_ts(row.get("timestamp_utc")) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
        ]
        window_pnls = [_admin_safe_float(row.get("realized_pnl")) for row in window_rows]
        window_wins = [pnl for pnl in window_pnls if pnl is not None and pnl > 0]
        recent[label] = {
            "closed_trades": len(window_rows),
            "total_realized_pnl": _admin_sum(window_pnls),
            "win_rate_pct": _admin_pct(len(window_wins), len([p for p in window_pnls if p is not None])),
            "average_pnl": _admin_avg(window_pnls),
            "profit_factor": _admin_profit_factor(window_pnls),
        }

    order_dupes = Counter(
        (row.get("order_id"), row.get("action"), row.get("symbol"))
        for row in fills
        if row.get("order_id")
    )
    exact_dupes = Counter(
        (
            row.get("position_id"),
            row.get("action"),
            row.get("symbol"),
            row.get("side"),
            row.get("timestamp_utc"),
            row.get("qty"),
            row.get("price"),
        )
        for row in fills
    )
    slippages = [_admin_safe_float(row.get("slippage_pct")) for row in fills]
    slippages_non_null = [value for value in slippages if value is not None]
    biggest_slip_row = max(
        (row for row in fills if _admin_safe_float(row.get("slippage_pct")) is not None),
        key=lambda row: abs(_admin_safe_float(row.get("slippage_pct")) or 0),
        default=None,
    )

    return {
        "generated_at": generated_at.isoformat(),
        "scope": "actual trade_fills only; excludes account_id='backfill' and initiator_type='SHADOW'",
        "overall": {
            "total_fills": len(fills),
            "total_trades": len(close_fills) + len(open_trades),
            "closed_trades": len(close_fills),
            "open_trades": len(open_trades),
            "raw_open_fills": len(open_fills),
            "raw_close_fills": len(close_fills),
            "position_linked_closed_trades": position_linked_closed_trades,
            "total_realized_pnl": _admin_sum(pnls),
            "win_rate_pct": _admin_pct(len(wins), len(closed_with_pnl)),
            "average_win": _admin_avg(wins),
            "average_loss": _admin_avg(losses),
            "profit_factor": _admin_profit_factor(pnls),
            "average_r_multiple": _admin_avg(r_values),
            "best_trade": _admin_trade_view(best),
            "worst_trade": _admin_trade_view(worst),
        },
        "per_symbol": per_symbol,
        "recent": recent,
        "risk_execution_quality": {
            "duplicate_order_id_action_symbol_groups": sum(1 for count in order_dupes.values() if count > 1),
            "duplicate_exact_fill_groups": sum(1 for count in exact_dupes.values() if count > 1),
            "missing_run_id_count": sum(1 for row in fills if not row.get("run_id")),
            "missing_position_id_count": sum(1 for row in fills if not row.get("position_id")),
            "closed_fills_null_exit_reason": sum(1 for row in close_fills if row.get("exit_reason") is None),
            "closed_fills_null_r_multiple": sum(1 for row in close_fills if row.get("r_multiple") is None),
            "average_slippage_pct": _admin_avg(slippages_non_null),
            "biggest_abs_slippage_pct": max((abs(value) for value in slippages_non_null), default=None),
            "biggest_slippage_fill": _admin_trade_view(biggest_slip_row),
        },
        "sizing_cap_events": [
            event for event in (_admin_sizing_cap_view(dict(row)) for row in cap_rows) if event is not None
        ],
    }

# =====================
# Affiliate & Broker Revenue Settings
# =====================

_AFFILIATE_DEFAULTS = {
    "id": "global",
    "referral_commission_rate": 15.0,
    "cookie_duration_days": 30,
    "minimum_payout_amount": 100.0,
    "minimum_payout_currency": "USD",
    "tiered_referrals_enabled": False,
    "attribution_model": "last_click",
    "is_enabled": False,
    "updated_at": None,
}

class AffiliateSettingsUpdate(BaseModel):
    referral_commission_rate: float = Field(..., ge=0, le=100)
    cookie_duration_days: int = Field(..., gt=0)
    minimum_payout_amount: float = Field(..., ge=0)
    minimum_payout_currency: str = "USD"
    tiered_referrals_enabled: bool = False
    attribution_model: str = Field(..., pattern="^(first_click|last_click|fixed_partner|manual_review)$")
    is_enabled: bool = False


def _row_to_affiliate_settings(row) -> dict:
    data = dict(row)
    data["tiered_referrals_enabled"] = bool(data.get("tiered_referrals_enabled", 0))
    data["is_enabled"] = bool(data.get("is_enabled", 0))
    return data


@router.get("/affiliate/settings")
async def get_affiliate_settings(_admin: dict = Depends(require_admin)):
    """Return persisted affiliate/broker revenue settings, or safe defaults if none saved yet."""
    db = DB()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM affiliate_settings WHERE id = 'global'"
        ).fetchone()
    if row:
        return _row_to_affiliate_settings(row)
    return dict(_AFFILIATE_DEFAULTS)


@router.put("/affiliate/settings")
async def update_affiliate_settings(
    body: AffiliateSettingsUpdate,
    _admin: dict = Depends(require_admin),
):
    """Upsert affiliate/broker revenue settings."""
    db = DB()
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO affiliate_settings (
                id, referral_commission_rate, cookie_duration_days,
                minimum_payout_amount, minimum_payout_currency,
                tiered_referrals_enabled, attribution_model, is_enabled, updated_at
            ) VALUES ('global', ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                referral_commission_rate  = excluded.referral_commission_rate,
                cookie_duration_days      = excluded.cookie_duration_days,
                minimum_payout_amount     = excluded.minimum_payout_amount,
                minimum_payout_currency   = excluded.minimum_payout_currency,
                tiered_referrals_enabled  = excluded.tiered_referrals_enabled,
                attribution_model         = excluded.attribution_model,
                is_enabled                = excluded.is_enabled,
                updated_at                = excluded.updated_at
            """,
            (
                body.referral_commission_rate,
                body.cookie_duration_days,
                body.minimum_payout_amount,
                body.minimum_payout_currency,
                int(body.tiered_referrals_enabled),
                body.attribution_model,
                int(body.is_enabled),
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM affiliate_settings WHERE id = 'global'"
        ).fetchone()
    return _row_to_affiliate_settings(row)


# =====================
# Commission Tiers (legacy — inert scaffolding, kept for DB compatibility)
# =====================

@router.get("/commissions/tiers")
async def get_commission_tiers(_admin: dict = Depends(require_admin)):
    db = DB()
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT id, name, min_volume, max_volume, rate, is_active FROM commission_tiers ORDER BY min_volume ASC"
        ).fetchall()
    return {"tiers": [dict(row) for row in rows]}

# =====================
# Audit Logs
# =====================

@router.get("/audit-logs")
async def get_audit_logs(
    _admin: dict = Depends(require_admin),
    event_type: Optional[str] = None,
    source: Optional[str] = Query(None, pattern="^(all|auth|bot)$"),
    limit: int = Query(50, le=200)
):
    """
    Get unified audit logs from both:
    1. auth_audit_log (Admin/User actions)
    2. events (Bot trading/system events)
    
    Params:
    - event_type: Filter by event type (supports wildcards)
    - source: Filter by source ('all', 'auth', 'bot'). Default: 'all'
    - limit: Max results to return
    """
    db = DB()
    with db.connect() as conn:
        # Base query combining both tables
        query = """
            SELECT * FROM (
                SELECT 
                    id, 
                    event_type, 
                    user_id, 
                    email, 
                    ip, 
                    details, 
                    created_at,
                    'auth' as source
                FROM auth_audit_log
                
                UNION ALL
                
                SELECT 
                    CAST(id AS TEXT) as id,
                    event_type,
                    COALESCE(run_id, 'BOT') as user_id,
                    'System' as email,
                    'Internal' as ip,
                    details_json as details,
                    timestamp_utc as created_at,
                    'bot' as source
                FROM events
            ) as combined_logs
        """
        
        params = []
        where_clauses = []
        
        # Filter by event_type
        if event_type:
            where_clauses.append("event_type LIKE ?")
            params.append(f"%{event_type}%")
        
        # Filter by source
        if source and source != "all":
            where_clauses.append("source = ?")
            params.append(source)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        rows = conn.execute(query, params).fetchall()
    
    return {
        "logs": [dict(row) for row in rows],
        "count": len(rows)
    }

# =====================
# Bot Monitoring
# =====================

@router.get("/bot/overview")
async def get_bot_overview(_admin: dict = Depends(require_admin)):
    """Get high-level bot status and metrics"""
    from datetime import datetime, timezone
    db = DB()
    with db.connect() as conn:
        # Get latest run info
        latest_run = conn.execute("""
            SELECT run_id, started_at, stopped_at, status, mode
            FROM runs
            ORDER BY started_at DESC
            LIMIT 1
        """).fetchone()
        
        # Get today's PnL
        today = datetime.now(timezone.utc).date().isoformat()
        daily_pnl = conn.execute(
            "SELECT realized_pnl, trade_count FROM daily_state WHERE day = ?",
            (today,)
        ).fetchone()
        
        # Count active positions
        active_positions = conn.execute(
            "SELECT COUNT(*) FROM symbol_state WHERE position != 'NONE'"
        ).fetchone()[0]
        
        # Get recent events count
        recent_events = conn.execute("""
            SELECT COUNT(*) FROM events 
            WHERE timestamp_utc > datetime('now', '-1 hour')
        """).fetchone()[0]
        
        # Calculate uptime if running
        status = "stopped"
        uptime_seconds = 0
        if latest_run and not latest_run["stopped_at"]:
            status = "running"
            started = datetime.fromisoformat(latest_run["started_at"].replace('Z', '+00:00'))
            uptime_seconds = int((datetime.now(timezone.utc) - started).total_seconds())
        
        return {
            "status": status,
            "uptime_seconds": uptime_seconds,
            "active_run_id": latest_run["run_id"] if latest_run else None,
            "active_positions": active_positions,
            "daily_pnl": daily_pnl["realized_pnl"] if daily_pnl else 0,
            "daily_trades": daily_pnl["trade_count"] if daily_pnl else 0,
            "recent_events_1h": recent_events
        }

@router.get("/bot/runs")
async def get_bot_runs(
    _admin: dict = Depends(require_admin),
    limit: int = Query(20, le=100)
):
    """Get historical bot run sessions"""
    db = DB()
    with db.connect() as conn:
        runs = conn.execute("""
            SELECT 
                r.run_id,
                r.started_at,
                r.stopped_at,
                r.mode,
                r.status,
                r.config_json,
                rs.cycles,
                rs.trades,
                rs.realized_pnl,
                rs.win_trades,
                rs.loss_trades
            FROM runs r
            LEFT JOIN run_summary rs ON r.run_id = rs.run_id
            ORDER BY r.started_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        
        return {
            "runs": [dict(r) for r in runs],
            "count": len(runs)
        }

@router.get("/bot/runs/{run_id}")
async def get_bot_run_details(
    run_id: str,
    _admin: dict = Depends(require_admin)
):
    """Get detailed information about a specific bot run"""
    db = DB()
    with db.connect() as conn:
        # Get run metadata
        run = conn.execute(
            "SELECT * FROM runs WHERE run_id = ?",
            (run_id,)
        ).fetchone()
        
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        
        # Get summary
        summary = conn.execute(
            "SELECT * FROM run_summary WHERE run_id = ?",
            (run_id,)
        ).fetchone()
        
        # Get events (sample, not all)
        events = conn.execute("""
            SELECT event_type, symbol, action, timestamp_utc, details_json
            FROM events
            WHERE run_id = ?
            ORDER BY timestamp_utc DESC
            LIMIT 100
        """, (run_id,)).fetchall()
        
        # Get decision traces (if available)
        traces = conn.execute("""
            SELECT trace_id, symbol, signal, confidence, intended_action, 
                   execution_status, final_position, ts
            FROM decision_traces
            WHERE run_id = ?
            ORDER BY ts DESC
            LIMIT 50
        """, (run_id,)).fetchall()
        
        return {
            "run": dict(run),
            "summary": dict(summary) if summary else None,
            "events": [dict(e) for e in events],
            "traces": [dict(t) for t in traces]
        }

@router.get("/bot/live")
async def get_bot_live_status(_admin: dict = Depends(require_admin)):
    """Get real-time bot telemetry (positions, latest signals)"""
    db = DB()
    with db.connect() as conn:
        # Get current positions
        positions = conn.execute("""
            SELECT symbol, position, entry_price, entry_qty, last_signal, 
                   last_action, updated_at
            FROM symbol_state
            WHERE position != 'NONE'
            ORDER BY updated_at DESC
        """).fetchall()
        
        # Get latest decision traces (last 10)
        latest_decisions = conn.execute("""
            SELECT trace_id, symbol, signal, confidence, intended_action,
                   execution_status, ts, sizing_json
            FROM decision_traces
            ORDER BY ts DESC
            LIMIT 10
        """).fetchall()
        
        # Get latest events (last 20)
        latest_events = conn.execute("""
            SELECT event_type, symbol, action, timestamp_utc, details_json
            FROM events
            ORDER BY timestamp_utc DESC
            LIMIT 20
        """).fetchall()
        
        return {
            "positions": [dict(p) for p in positions],
            "latest_decisions": [
                {
                    **dict(d),
                    "sizing_cap_event": _admin_sizing_cap_view(dict(d)),
                }
                for d in latest_decisions
            ],
            "latest_events": [dict(e) for e in latest_events]
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
# Admin User Provisioning
# =====================

@router.post("/users/create")
async def create_operator(
    user_data: OperatorCreate,
    _admin: dict = Depends(require_admin)
):
    """Create a new operator/admin account (Super Admin only)"""
    db = DB()
    with db.connect() as conn:
        # Check if user exists
        existing = conn.execute("SELECT id FROM users WHERE email = ?", (user_data.email,)).fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="User with this email already exists")

        uid = str(uuid.uuid4())
        hashed = get_password_hash(user_data.password)
        now = utc_now_iso()

        # Create user
        conn.execute("""
            INSERT INTO users (id, email, hashed_password, full_name, status, role, is_verified, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'active', 'user', 1, ?, ?)""",
            (uid, user_data.email, hashed, user_data.name, now, now)
        )
        
        # Grant specific role permissions
        if user_data.role in ['admin', 'operator', 'support']:
            role_id = str(uuid.uuid4())
            conn.execute("""
                INSERT INTO admin_roles (id, user_id, role, granted_by, granted_at)
                VALUES (?, ?, ?, ?, ?)
            """, (role_id, uid, user_data.role, _admin["id"], now))

        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'admin_user_created', ?, ?, ?)
        """, (str(uuid.uuid4()), uid, f"Created by {_admin['email']} with role {user_data.role}", now))

    return {
        "message": "User created successfully",
        "user_id": uid,
        "email": user_data.email,
        "role": user_data.role
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

# =====================
# System Settings
# =====================

@router.get("/settings")
async def get_system_settings(_admin: dict = Depends(require_admin)):
    """Get all system settings"""
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM system_settings").fetchall()
        
    # Convert list to dict for easier frontend consumption, or return list
    # Let's return a list for now, or map to a structured object if we had one.
    settings = {row["key"]: row["value"] for row in rows}
    
    # Ensure defaults if missing
    defaults = {
        "max_bots_per_user": "5",
        "maintenance_mode": "false",
        "signup_enabled": "true",
        "default_risk_cap": "0.05"
    }
    
    for k, v in defaults.items():
        if k not in settings:
            settings[k] = v
            
    return settings

@router.put("/settings")
async def update_system_settings(
    settings: dict,
    _admin: dict = Depends(require_admin)
):
    """Update system settings"""
    db = DB()
    with db.connect() as conn:
        for key, value in settings.items():
            # Upsert
            conn.execute("""
                INSERT INTO system_settings (key, value, updated_at, updated_by)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
            """, (key, str(value), utc_now_iso(), _admin["email"]))
            
        # Log audit event
        conn.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, details, created_at)
            VALUES (?, 'system_settings_updated', ?, ?, ?)
        """, (str(uuid.uuid4()), _admin["id"], f"Updated settings: {list(settings.keys())}", utc_now_iso()))
    
    return {"message": "Settings updated successfully"}

# =====================
# Audit Export
# =====================

@router.get("/audit-logs/export")
async def export_audit_logs(
    _admin: dict = Depends(require_admin),
    event_type: Optional[str] = None,
    source: Optional[str] = "all"
):
    """Export audit logs as CSV"""
    from fastapi.responses import StreamingResponse
    import io
    import csv
    
    db = DB()
    with db.connect() as conn:
        # Reuse the query logic from get_audit_logs but no limit
        query = """
            SELECT * FROM (
                SELECT 
                    id, event_type, user_id, email, ip, details, created_at, 'auth' as source
                FROM auth_audit_log
                UNION ALL
                SELECT 
                    CAST(id AS TEXT) as id, event_type, COALESCE(run_id, 'BOT') as user_id, 'System' as email, 'Internal' as ip, details_json as details, timestamp_utc as created_at, 'bot' as source
                FROM events
            ) as combined_logs
        """
        params = []
        where = []
        if event_type:
            where.append("event_type LIKE ?")
            params.append(f"%{event_type}%")
        if source and source != "all":
            where.append("source = ?")
            params.append(source)
            
        if where:
            query += " WHERE " + " AND ".join(where)
            
        query += " ORDER BY created_at DESC LIMIT 5000" # Cap export size for safety
        
        rows = conn.execute(query, params).fetchall()
        
    stream = io.StringIO()
    writer = csv.writer(stream)
    
    # Header
    writer.writerow(["ID", "Timestamp", "Source", "Event Type", "User ID", "Email", "IP", "Details"])
    
    for row in rows:
        writer.writerow([
            row["id"], 
            row["created_at"], 
            row["source"], 
            row["event_type"], 
            row["user_id"], 
            row["email"], 
            row["ip"], 
            row["details"]
        ])
        
    stream.seek(0)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=audit_logs.csv"
    return response
