"""
Reporting API Endpoints - Bot Backend

Implements Phase 8 backend logic for user-facing reporting.
Uses trade_fills, equity_snapshots, and benchmark_prices tables.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from typing import Optional, Dict, Any, List
import logging
import csv
import io
from datetime import datetime, timezone, timedelta
from shared_lib.persistence.db import DB
from app.api.deps import get_current_active_user
from app.analytics.pdf_report_service import get_pdf_report_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analytics/reporting", tags=["Analytics Reporting"])

def _build_trade_fills_user_scope_sql() -> str:
    """
    Build a safe user-scope predicate for trade_fills.

    Primary scope: trade_fills.user_id == current user.
    Fallback scope: legacy fills may have NULL/empty user_id; in that case we
    scope via bot_instances.user_id joined on trade_fills.bot_instance_id.

    IMPORTANT: This preserves user isolation. It does NOT broaden scope beyond
    fills belonging to bot instances owned by the authenticated user.
    """
    return "(f.user_id = ? OR ((f.user_id IS NULL OR f.user_id = '') AND bi.user_id = ?))"


def get_timeframe_filter(timeframe: str) -> Optional[str]:
    """Convert timeframe to ISO datetime for SQL filtering"""
    now = datetime.now(timezone.utc)
    
    if timeframe == "7d":
        since = now - timedelta(days=7)
    elif timeframe == "30d":
        since = now - timedelta(days=30)
    elif timeframe == "90d":
        since = now - timedelta(days=90)
    elif timeframe == "1M":
        since = now - timedelta(days=30)
    elif timeframe == "3M":
        since = now - timedelta(days=90)
    elif timeframe == "YTD":
        since = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    elif timeframe == "ALL":
        return None
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    return since.isoformat()


@router.get("/overview")
def get_reporting_overview(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """
    Get comprehensive analytics overview.
    Data source: trade_fills
    """
    user_id = user.get("id")
    db = DB()
    
    try:
        since_date = get_timeframe_filter(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Build WHERE clauses
    where_clauses = ["user_id = ?"]
    params = [user_id]
    
    if bot_instance_id:
        where_clauses.append("bot_instance_id = ?")
        params.append(bot_instance_id)
    
    if broker_account_id:
        where_clauses.append("broker_account_id = ?")
        params.append(broker_account_id)
    
    if since_date:
        where_clauses.append("timestamp_utc >= ?")
        params.append(since_date)
    
    where_sql = " AND ".join(where_clauses)
    
    # Initialize Trade Stats Service
    from app.analytics.trade_stats_service import get_trade_stats_service
    
    trade_stats_svc = get_trade_stats_service()
    
    since_dt = None
    if since_date:
        since_dt = datetime.fromisoformat(since_date.replace('Z', '+00:00'))

    trade_stats = trade_stats_svc.get_trade_summary(
        user_id=user_id,
        broker_account_id=broker_account_id,
        bot_instance_id=bot_instance_id,
        start_date=since_dt,
        end_date=None
    )
    
    total_trades = trade_stats["total_trades"]
    wins = trade_stats["winning_trades"]
    losses = trade_stats["losing_trades"]
    breakevens = total_trades - wins - losses if total_trades > 0 else 0
    
    pnl_gross = trade_stats["total_pnl"]
    fees_total = trade_stats["total_fees"]
    pnl_net = pnl_gross - fees_total
    
    win_rate = trade_stats["win_rate"]
    profit_factor = trade_stats["profit_factor"]
    expectancy = trade_stats["expectancy"]
    
    with db.connect() as conn:
        
        # Get equity start/end from equity_snapshots
        equity_where = "user_id = ?"
        equity_params = [user_id]
        if broker_account_id:
            equity_where += " AND broker_account_id = ?"
            equity_params.append(broker_account_id)
        if bot_instance_id:
            equity_where += " AND bot_instance_id = ?"
            equity_params.append(bot_instance_id)
        if since_date:
            equity_where += " AND timestamp_utc >= ?"
            equity_params.append(since_date)
        
        equity_start_row = conn.execute(f"""
            SELECT equity FROM equity_snapshots
            WHERE {equity_where}
            ORDER BY timestamp_utc ASC LIMIT 1
        """, equity_params).fetchone()
        
        equity_end_row = conn.execute(f"""
            SELECT equity, unrealized_pnl FROM equity_snapshots
            WHERE {equity_where}
            ORDER BY timestamp_utc DESC LIMIT 1
        """, equity_params).fetchone()
        
        equity_start = (equity_start_row["equity"] or 0.0) if equity_start_row else 0.0
        equity_end = (equity_end_row["equity"] or 0.0) if equity_end_row else 0.0
        unrealized_pnl_current = equity_end_row["unrealized_pnl"] if equity_end_row else None
        
        # Calculate max drawdown from equity snapshots
        max_drawdown = 0.0
        equity_rows = conn.execute(f"""
            SELECT equity FROM equity_snapshots
            WHERE {equity_where}
            ORDER BY timestamp_utc ASC
        """, equity_params).fetchall()
        
        peak = 0.0
        for row in equity_rows:
            equity = row["equity"] or 0.0
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_drawdown:
                max_drawdown = dd
        
        # Calculate Sharpe ratio (delegated to canonical BenchmarkService)
        try:
            from app.analytics.benchmark_service import get_benchmark_service
            days_lookback = 30 if timeframe in ("1M", "30d") else (90 if timeframe in ("3M", "90d") else (7 if timeframe == "7d" else 365))
            sharpe_data = get_benchmark_service().get_sharpe_ratio(user_id=user_id, broker_account_id=broker_account_id, days=days_lookback)
            sharpe_ratio = sharpe_data.get("sharpe_ratio", 0.0)
        except Exception:
            sharpe_ratio = 0.0
        
        return {
            "pnl_net": round(pnl_net, 2),
            "pnl_gross": round(pnl_gross, 2),
            "fees_total": round(fees_total, 2),
            "funding_total": 0.0,  # Future implementation
            "win_rate": round(win_rate, 2),
            "profit_factor": round(profit_factor, 2),
            "expectancy": round(expectancy, 2),
            "max_drawdown": round(max_drawdown, 2),
            "equity_start": round(equity_start, 2),
            "equity_end": round(equity_end, 2),
            "unrealized_pnl_current": round(unrealized_pnl_current, 2) if unrealized_pnl_current else None,
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "breakevens": breakevens,
            "sharpe_ratio": round(sharpe_ratio, 2),
            "metadata": {
                "grouping_mode": "execution_based",
                "currency": "USDT",  # Future: detect from data
                "timeframe_applied": timeframe,
                "limitations": []
            }
        }


@router.get("/trades")
def get_reporting_trades(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    symbol: Optional[str] = Query(None, description="Filter by symbol (case-insensitive)"),
    action: Optional[str] = Query(None, description="Filter by action: OPEN or CLOSE"),
    result: Optional[str] = Query(None, description="Filter by result: winner or loser (CLOSE fills only)"),
    user: dict = Depends(get_current_active_user)
):
    """
    Get paginated list of trade executions.
    Data source: trade_fills
    Filters: symbol, action (OPEN|CLOSE), result (winner|loser, CLOSE-only)
    """
    user_id = user.get("id")
    db = DB()

    try:
        since_date = get_timeframe_filter(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Build WHERE clauses — all parameterized, no string interpolation of user input
    where_clauses = ["user_id = ?"]
    params = [user_id]

    if bot_instance_id:
        where_clauses.append("bot_instance_id = ?")
        params.append(bot_instance_id)

    if broker_account_id:
        where_clauses.append("broker_account_id = ?")
        params.append(broker_account_id)

    if since_date:
        where_clauses.append("timestamp_utc >= ?")
        params.append(since_date)

    if symbol:
        where_clauses.append("UPPER(symbol) = UPPER(?)")
        params.append(symbol.strip())

    # result filter implies CLOSE action
    result_normalized = result.lower() if result else None
    if result_normalized in ("winner", "loser"):
        where_clauses.append("action = 'CLOSE'")
        if result_normalized == "winner":
            where_clauses.append("realized_pnl > 0")
        else:
            where_clauses.append("realized_pnl < 0")
    elif action:
        action_upper = action.upper()
        if action_upper in ("OPEN", "CLOSE"):
            where_clauses.append("action = ?")
            params.append(action_upper)

    where_sql = " AND ".join(where_clauses)
    
    offset = (page - 1) * page_size
    
    with db.connect() as conn:
        # Get total count
        count_row = conn.execute(f"""
            SELECT COUNT(*) as total FROM trade_fills WHERE {where_sql}
        """, params).fetchone()
        total_items = count_row["total"] or 0
        
        # Get paginated trades
        trades_rows = conn.execute(f"""
            SELECT 
                id, timestamp_utc, symbol, side, action, qty, price,
                realized_pnl, fee, broker_id, broker_account_id, bot_instance_id,
                quote_currency, base_currency
            FROM trade_fills
            WHERE {where_sql}
            ORDER BY timestamp_utc DESC
            LIMIT ? OFFSET ?
        """, params + [page_size, offset]).fetchall()
        
        trades = []
        for row in trades_rows:
            realized_pnl = row["realized_pnl"] or 0.0
            fee = row["fee"] or 0.0
            trades.append({
                "id": row["id"],
                "timestamp_utc": row["timestamp_utc"],
                "symbol": row["symbol"],
                "side": row["side"],
                "action": row["action"],
                "qty": row["qty"],
                "price": row["price"],
                "realized_pnl": round(realized_pnl, 2),
                "fee": round(fee, 2),
                "net_pnl": round(realized_pnl - fee, 2),
                "broker_id": row["broker_id"],
                "broker_account_id": row["broker_account_id"],
                "bot_instance_id": row["bot_instance_id"],
                "quote_currency": row["quote_currency"],
                "base_currency": row["base_currency"]
            })
        
        total_pages = (total_items + page_size - 1) // page_size
        
        return {
            "trades": trades,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "total_pages": total_pages
            }
        }


@router.get("/equity-curve")
def get_reporting_equity_curve(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """
    Get equity curve series.
    Data source: equity_snapshots
    """
    user_id = user.get("id")
    db = DB()
    
    try:
        since_date = get_timeframe_filter(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Build WHERE clauses
    where_clauses = ["user_id = ?"]
    params = [user_id]
    
    if bot_instance_id:
        where_clauses.append("bot_instance_id = ?")
        params.append(bot_instance_id)
    
    if broker_account_id:
        where_clauses.append("broker_account_id = ?")
        params.append(broker_account_id)
    
    if since_date:
        where_clauses.append("timestamp_utc >= ?")
        params.append(since_date)
    
    where_sql = " AND ".join(where_clauses)
    
    with db.connect() as conn:
        rows = conn.execute(f"""
            SELECT timestamp_utc, equity, unrealized_pnl
            FROM equity_snapshots
            WHERE {where_sql}
            ORDER BY timestamp_utc ASC
        """, params).fetchall()
        
        equity_curve = []
        for row in rows:
            equity_curve.append({
                "timestamp": row["timestamp_utc"],
                "equity": round(row["equity"] or 0.0, 2),
                "unrealized_pnl": round(row["unrealized_pnl"] or 0.0, 2)
            })
        
        return {
            "equity_curve": equity_curve,
            "metadata": {
                "source": "equity_snapshots",
                "currency": "USDT",
                "resolution": "variable"  # Based on snapshot frequency
            }
        }


@router.get("/drawdown")
def get_reporting_drawdown(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """
    Get drawdown series and statistics.
    Data source: equity_snapshots
    """
    user_id = user.get("id")
    db = DB()
    
    try:
        since_date = get_timeframe_filter(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Build WHERE clauses
    where_clauses = ["user_id = ?"]
    params = [user_id]
    
    if bot_instance_id:
        where_clauses.append("bot_instance_id = ?")
        params.append(bot_instance_id)
    
    if broker_account_id:
        where_clauses.append("broker_account_id = ?")
        params.append(broker_account_id)
    
    if since_date:
        where_clauses.append("timestamp_utc >= ?")
        params.append(since_date)
    
    where_sql = " AND ".join(where_clauses)
    
    with db.connect() as conn:
        rows = conn.execute(f"""
            SELECT timestamp_utc, equity
            FROM equity_snapshots
            WHERE {where_sql}
            ORDER BY timestamp_utc ASC
        """, params).fetchall()
        
        drawdown_series = []
        peak_equity = 0.0
        max_dd_amount = 0.0
        max_dd_percent = 0.0
        peak_timestamp = None
        trough_timestamp = None
        
        for row in rows:
            timestamp = row["timestamp_utc"]
            equity = row["equity"] or 0.0
            
            if equity > peak_equity:
                peak_equity = equity
                peak_timestamp = timestamp
            
            dd_amount = peak_equity - equity
            dd_percent = (dd_amount / peak_equity * 100) if peak_equity > 0 else 0.0
            
            if dd_amount > max_dd_amount:
                max_dd_amount = dd_amount
                max_dd_percent = dd_percent
                trough_timestamp = timestamp
            
            drawdown_series.append({
                "timestamp": timestamp,
                "equity": round(equity, 2),
                "peak_equity": round(peak_equity, 2),
                "drawdown_amount": round(dd_amount, 2),
                "drawdown_percent": round(dd_percent, 2)
            })
        
        # Calculate duration (in days) if we have peak and trough
        duration_days = None
        if peak_timestamp and trough_timestamp:
            try:
                peak_dt = datetime.fromisoformat(peak_timestamp.replace('Z', '+00:00'))
                trough_dt = datetime.fromisoformat(trough_timestamp.replace('Z', '+00:00'))
                duration_days = (trough_dt - peak_dt).days
            except:
                pass
        
        return {
            "drawdown_series": drawdown_series,
            "stats": {
                "max_drawdown_amount": round(max_dd_amount, 2),
                "max_drawdown_percent": round(max_dd_percent, 2),
                "peak_timestamp": peak_timestamp,
                "trough_timestamp": trough_timestamp,
                "recovery_timestamp": None,  # Future: detect when equity recovers to peak
                "duration_days": duration_days
            },
            "metadata": {
                "source": "equity_snapshots",
                "currency": "USDT"
            }
        }


@router.get("/benchmarks")
def get_reporting_benchmarks(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    benchmark: str = Query("BTCUSDT"),
    user: dict = Depends(get_current_active_user)
):
    """
    Get benchmark comparison.
    Data sources: equity_snapshots (bot), benchmark_prices (benchmark)
    """
    user_id = user.get("id")
    db = DB()
    
    try:
        since_date = get_timeframe_filter(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Build WHERE clauses for equity
    where_clauses = ["user_id = ?"]
    params = [user_id]
    
    if bot_instance_id:
        where_clauses.append("bot_instance_id = ?")
        params.append(bot_instance_id)
    
    if broker_account_id:
        where_clauses.append("broker_account_id = ?")
        params.append(broker_account_id)
    
    if since_date:
        where_clauses.append("timestamp_utc >= ?")
        params.append(since_date)
    
    where_sql = " AND ".join(where_clauses)
    
    with db.connect() as conn:
        # Get bot equity curve
        equity_rows = conn.execute(f"""
            SELECT timestamp_utc, equity
            FROM equity_snapshots
            WHERE {where_sql}
            ORDER BY timestamp_utc ASC
        """, params).fetchall()
        
        if not equity_rows:
            # Return empty structure instead of 404
            return {
                "bot_return": 0.0,
                "benchmark_return": 0.0,
                "outperformance": 0.0,
                "correlation": 0.0,
                "aligned_series": [],
                "metadata": {
                    "benchmark_symbol": benchmark,
                    "bot_currency": "USDT",
                    "benchmark_source": "benchmark_prices",
                    "warning": "No equity data available for specified filters"
                }
            }
        
        bot_start_equity = equity_rows[0]["equity"] or 0.0
        bot_end_equity = equity_rows[-1]["equity"] or 0.0
        bot_return = ((bot_end_equity - bot_start_equity) / bot_start_equity * 100) if bot_start_equity > 0 else 0.0
        
        # Get benchmark prices
        benchmark_where = "symbol = ?"
        benchmark_params = [benchmark]
        if since_date:
            benchmark_where += " AND date >= ?"
            benchmark_params.append(since_date[:10])  # Extract date part
        
        benchmark_rows = conn.execute(f"""
            SELECT date, close_price
            FROM benchmark_prices
            WHERE {benchmark_where}
            ORDER BY date ASC
        """, benchmark_params).fetchall()
        
        if not benchmark_rows:
            return {
                "bot_return": round(bot_return, 2),
                "benchmark_return": 0.0,
                "outperformance": round(bot_return, 2),
                "correlation": 0.0,
                "aligned_series": [],
                "metadata": {
                    "benchmark_symbol": benchmark,
                    "bot_currency": "USDT",
                    "benchmark_source": "benchmark_prices",
                    "warning": "No benchmark data available for specified timeframe"
                }
            }
        
        benchmark_start_price = benchmark_rows[0]["close_price"] or 0.0
        benchmark_end_price = benchmark_rows[-1]["close_price"] or 0.0
        benchmark_return = ((benchmark_end_price - benchmark_start_price) / benchmark_start_price * 100) if benchmark_start_price > 0 else 0.0
        
        outperformance = bot_return - benchmark_return
        
        # Build aligned series (normalize both to 100 at start)
        aligned_series = []
        equity_map = {row["timestamp_utc"][:10]: row["equity"] for row in equity_rows}  # Map by date
        
        for b_row in benchmark_rows:
            date = b_row["date"]
            benchmark_normalized = (b_row["close_price"] / benchmark_start_price * 100) if benchmark_start_price > 0 else 100.0
            
            # Find closest equity point
            equity_val = equity_map.get(date, bot_start_equity)
            bot_normalized = (equity_val / bot_start_equity * 100) if bot_start_equity > 0 else 100.0
            
            aligned_series.append({
                "timestamp": f"{date}T00:00:00Z",
                "bot_equity_normalized": round(bot_normalized, 2),
                "benchmark_price_normalized": round(benchmark_normalized, 2)
            })
        
        # Calculate correlation (simplified - Pearson correlation)
        correlation = 0.0
        if len(aligned_series) > 1:
            try:
                bot_vals = [p["bot_equity_normalized"] for p in aligned_series]
                bench_vals = [p["benchmark_price_normalized"] for p in aligned_series]
                
                import math
                n = len(bot_vals)
                mean_bot = sum(bot_vals) / n
                mean_bench = sum(bench_vals) / n
                
                num = sum((bot_vals[i] - mean_bot) * (bench_vals[i] - mean_bench) for i in range(n))
                den_bot = math.sqrt(sum((x - mean_bot) ** 2 for x in bot_vals))
                den_bench = math.sqrt(sum((x - mean_bench) ** 2 for x in bench_vals))
                
                if den_bot > 0 and den_bench > 0:
                    correlation = num / (den_bot * den_bench)
            except:
                pass
        
        return {
            "bot_return": round(bot_return, 2),
            "benchmark_return": round(benchmark_return, 2),
            "outperformance": round(outperformance, 2),
            "correlation": round(correlation, 2),
            "aligned_series": aligned_series,
            "metadata": {
                "benchmark_symbol": benchmark,
                "bot_currency": "USDT",
                "benchmark_source": "benchmark_prices"
            }
        }


@router.get("/export/pdf")
def export_analytics_pdf(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Export analytics overview as PDF"""
    user_id = user.get("id")
    
    try:
        service = get_pdf_report_service()
        pdf_bytes = service.generate_analytics_overview_pdf(
            user_id=user_id,
            timeframe=timeframe,
            bot_instance_id=bot_instance_id,
            broker_account_id=broker_account_id
        )
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"analytics_overview_{timeframe}_{timestamp}.pdf"
        
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except Exception as e:
        logger.error(f"Failed to export analytics PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")


@router.get("/positions/history")
def get_positions_history(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="open | closed"),
    result: Optional[str] = Query(None, description="winner | loser | breakeven"),
    sort_by: Optional[str] = Query("opened_at"),
    sort_order: Optional[str] = Query("DESC", description="ASC | DESC"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """
    Position-level trade history: groups OPEN/CLOSE fill pairs by position_id.
    Read-only — does not touch execution or position management.
    Fills without a position_id are included as synthetic positions (position_id = 'fill_<id>'),
    and also counted in metadata for diagnostics.
    """
    user_id = user.get("id")
    db = DB()

    try:
        since_date = get_timeframe_filter(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    scope_sql = _build_trade_fills_user_scope_sql()

    # Base fill WHERE (applies to both diagnostics + grouped data)
    base_where: List[str] = [scope_sql]
    base_params: List[Any] = [user_id, user_id]

    if since_date:
        base_where.append("f.timestamp_utc >= ?")
        base_params.append(since_date)
    if start_date:
        base_where.append("f.timestamp_utc >= ?")
        base_params.append(start_date)
    if end_date:
        base_where.append("f.timestamp_utc <= ?")
        base_params.append(end_date)
    if bot_instance_id:
        base_where.append("f.bot_instance_id = ?")
        base_params.append(bot_instance_id)
    if broker_account_id:
        base_where.append("f.broker_account_id = ?")
        base_params.append(broker_account_id)
    if symbol:
        base_where.append("UPPER(f.symbol) = UPPER(?)")
        base_params.append(symbol.strip())

    side_normalized = side.upper() if side else None
    if side_normalized in ("LONG", "SHORT", "BUY", "SELL"):
        base_where.append("f.side = ?")
        base_params.append(side_normalized)

    # Fill-level WHERE (applied inside fill_data CTE)
    # NOTE: We do NOT require position_id here. Some legacy/external fills have
    # realized_pnl but missing position_id; excluding them makes realized/net PnL
    # totals disagree with raw CLOSE fill totals and broker balance movement.
    fill_where: List[str] = list(base_where)
    fill_params: List[Any] = list(base_params)
    fill_where_sql = " AND ".join(fill_where)

    # Post-group WHERE (applied after CTE computes status/result)
    group_where: List[str] = []
    group_params: List[Any] = []

    status_normalized = status.lower() if status else None
    if status_normalized in ("open", "closed"):
        group_where.append("status = ?")
        group_params.append(status_normalized.upper())

    result_normalized = result.lower() if result else None
    if result_normalized in ("winner", "loser", "breakeven"):
        group_where.append("result = ?")
        group_params.append(result_normalized)

    group_where_sql = " AND ".join(group_where) if group_where else "1=1"

    _VALID_SORT = {"opened_at", "closed_at", "realized_pnl", "net_pnl", "duration_seconds"}
    sort_col = sort_by if sort_by in _VALID_SORT else "opened_at"
    sort_dir = "ASC" if (sort_order or "DESC").upper() == "ASC" else "DESC"
    if sort_col in ("opened_at", "closed_at"):
        order_expr = f"COALESCE({sort_col}, '') {sort_dir}"
    else:
        order_expr = f"COALESCE({sort_col}, 0) {sort_dir}"

    offset = (page - 1) * page_size

    cte_sql = f"""
        WITH fill_data AS (
            SELECT
                f.id,
                CASE
                    WHEN f.position_id IS NULL OR f.position_id = '' THEN ('fill_' || CAST(f.id AS TEXT))
                    ELSE f.position_id
                END AS position_id,
                f.symbol, f.side, f.action, f.qty, f.price,
                COALESCE(f.fee, 0) AS fee,
                f.realized_pnl, f.timestamp_utc, f.bot_instance_id, f.broker_account_id,
                f.run_id, f.r_multiple
            FROM trade_fills f
            LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE {fill_where_sql}
        ),
        position_groups AS (
            SELECT
                position_id,
                MAX(symbol) AS symbol,
                MAX(side) AS side,
                MAX(bot_instance_id) AS bot_instance_id,
                MAX(broker_account_id) AS broker_account_id,
                MAX(run_id) AS run_id,
                MIN(timestamp_utc) AS opened_at,
                MAX(CASE WHEN action = 'CLOSE' THEN timestamp_utc END) AS closed_at,
                CASE WHEN SUM(CASE WHEN action='OPEN' THEN qty ELSE 0 END) > 0
                     THEN SUM(CASE WHEN action='OPEN' THEN qty * price ELSE 0 END) /
                          SUM(CASE WHEN action='OPEN' THEN qty ELSE 0 END)
                     ELSE NULL END AS entry_price,
                CASE WHEN SUM(CASE WHEN action='CLOSE' THEN qty ELSE 0 END) > 0
                     THEN SUM(CASE WHEN action='CLOSE' THEN qty * price ELSE 0 END) /
                          SUM(CASE WHEN action='CLOSE' THEN qty ELSE 0 END)
                     ELSE NULL END AS exit_price,
                SUM(CASE WHEN action='OPEN' THEN qty ELSE 0 END) AS open_qty,
                SUM(CASE WHEN action='CLOSE' THEN qty ELSE 0 END) AS close_qty,
                SUM(fee) AS total_fees,
                SUM(CASE WHEN action='CLOSE' THEN COALESCE(realized_pnl, 0) ELSE 0 END) AS realized_pnl,
                MAX(CASE WHEN action='CLOSE' THEN r_multiple END) AS r_multiple,
                SUM(CASE WHEN action='CLOSE' THEN 1 ELSE 0 END) AS close_count,
                SUM(CASE WHEN action='OPEN' THEN 1 ELSE 0 END) AS open_count,
                GROUP_CONCAT(CASE WHEN action='OPEN' THEN CAST(id AS TEXT) END) AS open_fill_ids_raw,
                GROUP_CONCAT(CASE WHEN action='CLOSE' THEN CAST(id AS TEXT) END) AS close_fill_ids_raw
            FROM fill_data
            GROUP BY position_id
        ),
        with_computed AS (
            SELECT
                position_id, symbol, side, bot_instance_id, broker_account_id, run_id,
                opened_at, closed_at, entry_price, exit_price,
                open_qty, close_qty, total_fees, realized_pnl, r_multiple,
                open_count, close_count, open_fill_ids_raw, close_fill_ids_raw,
                CASE WHEN close_count > 0 THEN 'CLOSED' ELSE 'OPEN' END AS status,
                CASE
                    WHEN close_count > 0 AND realized_pnl > 0 THEN 'winner'
                    WHEN close_count > 0 AND realized_pnl < 0 THEN 'loser'
                    WHEN close_count > 0 THEN 'breakeven'
                    ELSE NULL
                END AS result,
                CASE WHEN close_count > 0 THEN realized_pnl - total_fees ELSE NULL END AS net_pnl,
                CASE WHEN close_count > 0 AND entry_price > 0 AND open_qty > 0
                     THEN (realized_pnl / (entry_price * open_qty)) * 100
                     ELSE NULL END AS realized_pnl_percent,
                CASE WHEN opened_at IS NOT NULL AND closed_at IS NOT NULL
                     THEN CAST((julianday(closed_at) - julianday(opened_at)) * 86400 AS INTEGER)
                     ELSE NULL END AS duration_seconds
            FROM position_groups
        )
    """

    with db.connect() as conn:
        # Diagnostics for debugging empty history safely (no sensitive IDs returned)
        base_where_sql = " AND ".join(base_where)
        total_fills_seen = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM trade_fills f
            LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE {base_where_sql}
            """,
            base_params,
        ).fetchone()["cnt"]

        fills_without_position_id = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM trade_fills f
            LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE {base_where_sql}
              AND (f.position_id IS NULL OR f.position_id = '')
            """,
            base_params,
        ).fetchone()["cnt"]

        direct_user_id_fills = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM trade_fills f
            LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE {base_where_sql}
              AND f.user_id = ?
            """,
            base_params + [user_id],
        ).fetchone()["cnt"]

        fallback_bot_instance_fills = total_fills_seen - direct_user_id_fills

        summary_row = conn.execute(
            cte_sql + f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_count,
                SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_count,
                SUM(CASE WHEN result = 'winner' THEN 1 ELSE 0 END) AS winners,
                SUM(CASE WHEN result = 'loser' THEN 1 ELSE 0 END) AS losers,
                SUM(CASE WHEN result = 'breakeven' THEN 1 ELSE 0 END) AS breakevens,
                COALESCE(SUM(CASE WHEN status = 'CLOSED' THEN realized_pnl ELSE 0 END), 0) AS total_realized_pnl,
                COALESCE(SUM(total_fees), 0) AS total_fees,
                COALESCE(SUM(CASE WHEN net_pnl IS NOT NULL THEN net_pnl ELSE 0 END), 0) AS total_net_pnl
            FROM with_computed
            WHERE {group_where_sql}
            """,
            fill_params + group_params
        ).fetchone()

        total_items = (summary_row["total"] or 0) if summary_row else 0
        closed_count = (summary_row["closed_count"] or 0) if summary_row else 0
        winners = (summary_row["winners"] or 0) if summary_row else 0
        win_rate = round(winners / closed_count * 100, 2) if closed_count > 0 else None

        summary = {
            "total": total_items,
            "open_count": (summary_row["open_count"] or 0) if summary_row else 0,
            "closed_count": closed_count,
            "winners": winners,
            "losers": (summary_row["losers"] or 0) if summary_row else 0,
            "breakevens": (summary_row["breakevens"] or 0) if summary_row else 0,
            "total_realized_pnl": round((summary_row["total_realized_pnl"] or 0), 2) if summary_row else 0.0,
            "total_fees": round((summary_row["total_fees"] or 0), 2) if summary_row else 0.0,
            "total_net_pnl": round((summary_row["total_net_pnl"] or 0), 2) if summary_row else 0.0,
            "win_rate": win_rate,
        }

        items_rows = conn.execute(
            cte_sql + f"""
            SELECT * FROM with_computed
            WHERE {group_where_sql}
            ORDER BY {order_expr}
            LIMIT ? OFFSET ?
            """,
            fill_params + group_params + [page_size, offset]
        ).fetchall()

        items = []
        for row in items_rows:
            open_fills = [x for x in (row["open_fill_ids_raw"] or "").split(",") if x]
            close_fills = [x for x in (row["close_fill_ids_raw"] or "").split(",") if x]
            items.append({
                "position_id": row["position_id"],
                "symbol": row["symbol"],
                "side": row["side"],
                "status": row["status"],
                "result": row["result"],
                "opened_at": row["opened_at"],
                "closed_at": row["closed_at"],
                "entry_price": round(row["entry_price"], 8) if row["entry_price"] is not None else None,
                "exit_price": round(row["exit_price"], 8) if row["exit_price"] is not None else None,
                "open_qty": round(row["open_qty"] or 0, 8),
                "close_qty": round(row["close_qty"] or 0, 8),
                "realized_pnl": round(row["realized_pnl"] or 0, 2),
                "total_fees": round(row["total_fees"] or 0, 2),
                "net_pnl": round(row["net_pnl"], 2) if row["net_pnl"] is not None else None,
                "realized_pnl_percent": round(row["realized_pnl_percent"], 4) if row["realized_pnl_percent"] is not None else None,
                "duration_seconds": row["duration_seconds"],
                "r_multiple": round(row["r_multiple"], 4) if row["r_multiple"] is not None else None,
                "bot_instance_id": row["bot_instance_id"],
                "broker_account_id": row["broker_account_id"],
                "run_id": row["run_id"],
                "open_count": row["open_count"] or 0,
                "close_count": row["close_count"] or 0,
                "open_fill_ids": open_fills,
                "close_fill_ids": close_fills,
            })

        total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 1

        user_scope_mode = "trade_fills.user_id" if direct_user_id_fills > 0 else "bot_instances.user_id_via_bot_instance_id"
        warning = None
        if direct_user_id_fills == 0 and fallback_bot_instance_fills > 0:
            warning = "trade_fills.user_id missing for this user; scoping via bot_instances.user_id"

        return {
            "items": items,
            "summary": summary,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "total_pages": total_pages,
            },
            "metadata": {
                "source": "trade_fills",
                "total_fills_seen": int(total_fills_seen or 0),
                "fills_without_position_id": fills_without_position_id,
                "grouped_positions_total": int(total_items or 0),
                "closed_positions_total": int(closed_count or 0),
                "user_scope_mode": user_scope_mode,
                "warning": warning,
                "timeframe_applied": timeframe,
                "grouping_mode": "position_id",
            },
        }


@router.get("/positions/history/export")
def export_positions_history_csv(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None),
    result: Optional[str] = Query(None, description="winner | loser | breakeven"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """
    Export completed position history as CSV. Auth required, user-scoped.
    No pagination — exports the full filtered dataset (closed positions only).
    """
    user_id = user.get("id")
    db = DB()

    try:
        since_date = get_timeframe_filter(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    scope_sql = _build_trade_fills_user_scope_sql()

    base_where: List[str] = [scope_sql]
    base_params: List[Any] = [user_id, user_id]

    if since_date:
        base_where.append("f.timestamp_utc >= ?")
        base_params.append(since_date)
    if start_date:
        base_where.append("f.timestamp_utc >= ?")
        base_params.append(start_date)
    if end_date:
        base_where.append("f.timestamp_utc <= ?")
        base_params.append(end_date)
    if bot_instance_id:
        base_where.append("f.bot_instance_id = ?")
        base_params.append(bot_instance_id)
    if broker_account_id:
        base_where.append("f.broker_account_id = ?")
        base_params.append(broker_account_id)
    if symbol:
        base_where.append("UPPER(f.symbol) = UPPER(?)")
        base_params.append(symbol.strip())

    side_normalized = side.upper() if side else None
    if side_normalized in ("LONG", "SHORT", "BUY", "SELL"):
        base_where.append("f.side = ?")
        base_params.append(side_normalized)

    # NOTE: Export must match /positions/history behaviour: include CLOSE fills even when
    # position_id is missing (synthetic grouping) so CSV totals reconcile with raw fills.
    fill_where: List[str] = list(base_where)
    fill_params: List[Any] = list(base_params)
    fill_where_sql = " AND ".join(fill_where)

    # Always export only CLOSED positions
    group_where: List[str] = ["status = 'CLOSED'"]
    group_params: List[Any] = []

    result_normalized = result.lower() if result else None
    if result_normalized in ("winner", "loser", "breakeven"):
        group_where.append("result = ?")
        group_params.append(result_normalized)

    group_where_sql = " AND ".join(group_where)

    cte_sql = f"""
        WITH fill_data AS (
            SELECT
                f.id,
                CASE
                    WHEN f.position_id IS NULL OR f.position_id = '' THEN ('fill_' || CAST(f.id AS TEXT))
                    ELSE f.position_id
                END AS position_id,
                f.symbol, f.side, f.action, f.qty, f.price,
                COALESCE(f.fee, 0) AS fee,
                f.realized_pnl, f.timestamp_utc, f.bot_instance_id, f.broker_account_id,
                f.run_id, f.r_multiple
            FROM trade_fills f
            LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE {fill_where_sql}
        ),
        position_groups AS (
            SELECT
                position_id,
                MAX(symbol) AS symbol,
                MAX(side) AS side,
                MAX(bot_instance_id) AS bot_instance_id,
                MAX(broker_account_id) AS broker_account_id,
                MAX(run_id) AS run_id,
                MIN(timestamp_utc) AS opened_at,
                MAX(CASE WHEN action = 'CLOSE' THEN timestamp_utc END) AS closed_at,
                CASE WHEN SUM(CASE WHEN action='OPEN' THEN qty ELSE 0 END) > 0
                     THEN SUM(CASE WHEN action='OPEN' THEN qty * price ELSE 0 END) /
                          SUM(CASE WHEN action='OPEN' THEN qty ELSE 0 END)
                     ELSE NULL END AS entry_price,
                CASE WHEN SUM(CASE WHEN action='CLOSE' THEN qty ELSE 0 END) > 0
                     THEN SUM(CASE WHEN action='CLOSE' THEN qty * price ELSE 0 END) /
                          SUM(CASE WHEN action='CLOSE' THEN qty ELSE 0 END)
                     ELSE NULL END AS exit_price,
                SUM(CASE WHEN action='OPEN' THEN qty ELSE 0 END) AS open_qty,
                SUM(CASE WHEN action='CLOSE' THEN qty ELSE 0 END) AS close_qty,
                SUM(fee) AS total_fees,
                SUM(CASE WHEN action='CLOSE' THEN COALESCE(realized_pnl, 0) ELSE 0 END) AS realized_pnl,
                MAX(CASE WHEN action='CLOSE' THEN r_multiple END) AS r_multiple,
                SUM(CASE WHEN action='CLOSE' THEN 1 ELSE 0 END) AS close_count,
                SUM(CASE WHEN action='OPEN' THEN 1 ELSE 0 END) AS open_count,
                GROUP_CONCAT(CASE WHEN action='OPEN' THEN CAST(id AS TEXT) END) AS open_fill_ids_raw,
                GROUP_CONCAT(CASE WHEN action='CLOSE' THEN CAST(id AS TEXT) END) AS close_fill_ids_raw
            FROM fill_data
            GROUP BY position_id
        ),
        with_computed AS (
            SELECT
                position_id, symbol, side, bot_instance_id, broker_account_id, run_id,
                opened_at, closed_at, entry_price, exit_price,
                open_qty, close_qty, total_fees, realized_pnl, r_multiple,
                open_count, close_count, open_fill_ids_raw, close_fill_ids_raw,
                CASE WHEN close_count > 0 THEN 'CLOSED' ELSE 'OPEN' END AS status,
                CASE
                    WHEN close_count > 0 AND realized_pnl > 0 THEN 'winner'
                    WHEN close_count > 0 AND realized_pnl < 0 THEN 'loser'
                    WHEN close_count > 0 THEN 'breakeven'
                    ELSE NULL
                END AS result,
                CASE WHEN close_count > 0 THEN realized_pnl - total_fees ELSE NULL END AS net_pnl,
                CASE WHEN close_count > 0 AND entry_price > 0 AND open_qty > 0
                     THEN (realized_pnl / (entry_price * open_qty)) * 100
                     ELSE NULL END AS realized_pnl_percent,
                CASE WHEN opened_at IS NOT NULL AND closed_at IS NOT NULL
                     THEN CAST((julianday(closed_at) - julianday(opened_at)) * 86400 AS INTEGER)
                     ELSE NULL END AS duration_seconds
            FROM position_groups
        )
    """

    with db.connect() as conn:
        rows = conn.execute(
            cte_sql + f"""
            SELECT * FROM with_computed
            WHERE {group_where_sql}
            ORDER BY COALESCE(closed_at, '') DESC
            """,
            fill_params + group_params
        ).fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "position_id", "symbol", "side", "status", "result",
            "broker_account_id", "bot_instance_id", "run_id",
            "opened_at", "closed_at", "duration_seconds",
            "entry_price", "exit_price", "open_qty", "close_qty",
            "total_fees", "realized_pnl", "net_pnl",
            "realized_pnl_percent", "r_multiple",
            "open_count", "close_count",
            "open_fill_ids", "close_fill_ids",
        ])

        for row in rows:
            open_fills = [x for x in (row["open_fill_ids_raw"] or "").split(",") if x]
            close_fills = [x for x in (row["close_fill_ids_raw"] or "").split(",") if x]
            writer.writerow([
                row["position_id"],
                row["symbol"],
                row["side"],
                row["status"],
                row["result"] or "",
                row["broker_account_id"] or "",
                row["bot_instance_id"] or "",
                row["run_id"] or "",
                row["opened_at"] or "",
                row["closed_at"] or "",
                row["duration_seconds"] if row["duration_seconds"] is not None else "",
                round(row["entry_price"], 8) if row["entry_price"] is not None else "",
                round(row["exit_price"], 8) if row["exit_price"] is not None else "",
                round(row["open_qty"] or 0, 8),
                round(row["close_qty"] or 0, 8),
                round(row["total_fees"] or 0, 2),
                round(row["realized_pnl"] or 0, 2),
                round(row["net_pnl"], 2) if row["net_pnl"] is not None else "",
                round(row["realized_pnl_percent"], 4) if row["realized_pnl_percent"] is not None else "",
                round(row["r_multiple"], 4) if row["r_multiple"] is not None else "",
                row["open_count"] or 0,
                row["close_count"] or 0,
                "|".join(open_fills),
                "|".join(close_fills),
            ])

        csv_content = output.getvalue()
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"cosmicforge-completed-trades-{date_str}.csv"

        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/csv; charset=utf-8",
            }
        )

