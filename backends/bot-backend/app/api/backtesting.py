from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import JSONResponse, StreamingResponse
from typing import List, Optional
import json
import uuid
import csv
import io
from datetime import datetime, timezone

from shared_lib.persistence.db import DB, utc_now_iso
from app.api.deps import get_current_active_user
from app.schemas.backtest import (
    BacktestCreate, BacktestRun, BacktestListResponse, 
    BacktestMetrics, EquityCurveResponse, FillListResponse,
    EquityPoint, FillItem
)

import logging
import traceback
import os

# Configure logging to file
log_path = os.path.join(os.path.dirname(__file__), "../../../debug_error.log")
handler = logging.FileHandler(log_path)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)

router = APIRouter()
db = DB()

def _get_metrics(row) -> BacktestMetrics:
    """Helper to extract metrics from row"""
    # Ensure row is a dict or supports .get()
    # If it's sqlite3.Row, convert to dict
    r = dict(row)
    return BacktestMetrics(
        total_trades=r.get("total_trades", 0) or 0,
        win_rate=r.get("win_rate", 0.0) or 0.0,
        net_pnl=r.get("net_pnl", 0.0) or 0.0,
        gross_pnl=r.get("gross_pnl", 0.0) or 0.0,
        total_fees=r.get("total_fees", 0.0) or 0.0,
        max_drawdown=r.get("max_drawdown", 0.0) or 0.0,
        sharpe_ratio=r.get("sharpe_ratio"),
        return_pct=None # Calculated field if needed, or added to DB
    )

@router.post("/", response_model=dict, status_code=201)
def create_backtest(
    payload: BacktestCreate,
    user: dict = Depends(get_current_active_user)
):
    """
    Queue a new backtest run.
    """
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    now = utc_now_iso()
    user_id = user["id"]
    
    
    try:
        with db.connect() as conn:
            # 1. Create Run Record
            conn.execute(
                """
                INSERT INTO backtest_runs (
                    id, user_id, name, strategy_id, 
                    symbols_json, timeframe, start_date, end_date, 
                    initial_capital, status, 
                    strategy_params_json, risk_params_json,
                    slippage_bps, fee_bps, market_type, data_source,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id, user_id, payload.name, payload.strategy_id,
                    json.dumps(payload.symbols), payload.interval, 
                    payload.start_date, payload.end_date,
                    payload.initial_capital, "pending",
                    json.dumps(payload.strategy_params or {}),
                    json.dumps(payload.risk_params or {}),
                    payload.slippage_bps, payload.fee_bps,
                    payload.market_type, payload.data_source,
                    now, now
                )
            )
            
            # 2. Enqueue Job
            # We store minimal config in job since full config is in run
            conn.execute(
                """
                INSERT INTO backtest_jobs (
                    id, run_id, user_id, status, priority, 
                    config_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id, run_id, user_id, "pending", 5,
                    "{}", # Config handled by worker reading run table
                    now, now
                )
            )
            
        return {
            "run_id": run_id,
            "job_id": job_id,
            "status": "pending",
            "message": "Backtest queued successfully"
        }
    except Exception as e:
        logger.error(f"Error creating backtest: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@router.get("/", response_model=BacktestListResponse)
def list_backtests(
    status: Optional[str] = None,
    page: int = 1,
    size: int = 20,
    user: dict = Depends(get_current_active_user)
):
    """
    List backtest history for user.
    """
    offset = (page - 1) * size
    user_id = user["id"]
    
    query = "SELECT * FROM backtest_runs WHERE user_id = ?"
    params = [user_id]
    
    if status:
        query += " AND status = ?"
        params.append(status)
        
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([size, offset])
    
    count_query = "SELECT COUNT(*) as count FROM backtest_runs WHERE user_id = ?"
    count_params = [user_id]
    if status:
        count_query += " AND status = ?"
        count_params.append(status)
    
    with db.connect() as conn:
        total = conn.execute(count_query, count_params).fetchone()["count"]
        rows = conn.execute(query, params).fetchall()
        
    items = []
    for row in rows:
        try:
            items.append(BacktestRun(
                id=row["id"],
                user_id=row["user_id"],
                name=row["name"],
                strategy_id=row["strategy_id"],
                status=row["status"],
                created_at=row["created_at"],
                completed_at=row["completed_at"],
                symbols=json.loads(row["symbols_json"]),
                timeframe=row["timeframe"],
                start_date=row["start_date"],
                end_date=row["end_date"],
                initial_capital=row["initial_capital"],
                metrics=_get_metrics(row),
                error_message=row["error_message"],
                progress_pct=row["progress_pct"] or 0.0
            ))
        except Exception as e:
            # Skip malformed/legacy rows if any, log error
            print(f"Error parsing run {row['id']}: {e}")
            continue
            
    return BacktestListResponse(
        items=items,
        total=total,
        page=page,
        size=size
    )

@router.get("/{run_id}", response_model=BacktestRun)
def get_backtest_details(
    run_id: str,
    user: dict = Depends(get_current_active_user)
):
    """
    Get details for a specific run.
    """
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM backtest_runs WHERE id = ? AND user_id = ?",
            (run_id, user["id"])
        ).fetchone()
        
    if not row:
        raise HTTPException(status_code=404, detail="Backtest not found")
        
    return BacktestRun(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        strategy_id=row["strategy_id"],
        status=row["status"],
        created_at=row["created_at"],
        completed_at=row["completed_at"],
        symbols=json.loads(row["symbols_json"]),
        timeframe=row["timeframe"],
        start_date=row["start_date"],
        end_date=row["end_date"],
        initial_capital=row["initial_capital"],
        metrics=_get_metrics(row),
        error_message=row["error_message"],
        progress_pct=row["progress_pct"] or 0.0
    )

@router.get("/{run_id}/equity", response_model=EquityCurveResponse)
def get_equity_curve(
    run_id: str,
    user: dict = Depends(get_current_active_user)
):
    """
    Get equity curve time series.
    """
    # First verify ownership
    with db.connect() as conn:
        run = conn.execute(
            "SELECT id FROM backtest_runs WHERE id = ? AND user_id = ?",
            (run_id, user["id"])
        ).fetchone()
        
        if not run:
            raise HTTPException(status_code=404, detail="Backtest not found")
            
        rows = conn.execute(
            """
            SELECT timestamp_utc, equity, balance, drawdown_pct, unrealized_pnl 
            FROM backtest_equity_curve 
            WHERE run_id = ? 
            ORDER BY timestamp_utc ASC
            """,
            (run_id,)
        ).fetchall()
        
    datapoints = []
    for r in rows:
        datapoints.append(EquityPoint(
            timestamp=r["timestamp_utc"],
            equity=r["equity"],
            balance=r["balance"],
            drawdown_pct=r["drawdown_pct"] or 0.0,
            unrealized_pnl=r["unrealized_pnl"] or 0.0
        ))
        
    return EquityCurveResponse(run_id=run_id, datapoints=datapoints)

@router.get("/{run_id}/fills", response_model=FillListResponse)
def get_fills(
    run_id: str,
    page: int = 1,
    size: int = 50,
    user: dict = Depends(get_current_active_user)
):
    """
    Get paginated execution history.
    """
    offset = (page - 1) * size
    
    with db.connect() as conn:
        run = conn.execute(
            "SELECT id FROM backtest_runs WHERE id = ? AND user_id = ?",
            (run_id, user["id"])
        ).fetchone()
        
        if not run:
            raise HTTPException(status_code=404, detail="Backtest not found")
            
        total = conn.execute(
            "SELECT COUNT(*) as count FROM backtest_fills WHERE run_id = ?",
            (run_id,)
        ).fetchone()["count"]
        
        rows = conn.execute(
            """
            SELECT timestamp_utc, symbol, side, fill_price, quantity, fee_usdt, pnl
            FROM backtest_fills 
            WHERE run_id = ? 
            ORDER BY timestamp_utc DESC
            LIMIT ? OFFSET ?
            """,
            (run_id, size, offset)
        ).fetchall()
        
    items = []
    for r in rows:
        items.append(FillItem(
            timestamp=r["timestamp_utc"],
            symbol=r["symbol"],
            side=r["side"],
            price=r["fill_price"], # normalized to 'price' for API
            quantity=r["quantity"],
            fee_usdt=r["fee_usdt"],
            pnl=r["pnl"]
        ))
        
    return FillListResponse(
        items=items,
        total=total,
        page=page,
        size=size
    )

@router.post("/{run_id}/cancel")
def cancel_backtest(
    run_id: str,
    user: dict = Depends(get_current_active_user)
):
    """
    Cancel a pending or running backtest.
    """
    with db.connect() as conn:
        run = conn.execute(
            "SELECT status FROM backtest_runs WHERE id = ? AND user_id = ?",
            (run_id, user["id"])
        ).fetchone()
        
        if not run:
            raise HTTPException(status_code=404, detail="Backtest not found")
        
        if run["status"] in ("completed", "failed", "cancelled"):
            return {"status": run["status"], "message": "Run already finished"}
            
        now = utc_now_iso()
        conn.execute(
            "UPDATE backtest_runs SET status = 'cancelled', updated_at = ? WHERE id = ?",
            (now, run_id)
        )
        
        # Also cancel pending tokens if any in worker queue 
        # (Worker logic handles existing jobs by checking run status)
        
    return {"status": "cancelled", "message": "Cancellation requested"}

@router.get("/{run_id}/export")
def export_backtest(
    run_id: str,
    format: str = "csv",
    user: dict = Depends(get_current_active_user)
):
    """
    Export backtest results as CSV or JSON.
    """
    if format not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="Invalid format. Use 'csv' or 'json'")
        
    with db.connect() as conn:
        run = conn.execute(
            "SELECT * FROM backtest_runs WHERE id = ? AND user_id = ?",
            (run_id, user["id"])
        ).fetchone()
        
        if not run:
            raise HTTPException(status_code=404, detail="Backtest not found")
            
        fills = conn.execute(
            "SELECT * FROM backtest_fills WHERE run_id = ? ORDER BY timestamp_utc ASC",
            (run_id,)
        ).fetchall()
        
        metrics = _get_metrics(run)
        
    if format == "json":
        data = {
            "run_id": run_id,
            "config": {
                "strategy": run["strategy_id"],
                "symbols": json.loads(run["symbols_json"]),
                "timeframe": run["timeframe"],
                "start": run["start_date"],
                "end": run["end_date"]
            },
            "metrics": metrics.dict(),
            "fills": [dict(f) for f in fills]
        }
        return JSONResponse(data)
        
    elif format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header section
        writer.writerow(["Backtest Report", run["name"]])
        writer.writerow(["Strategy", run["strategy_id"]])
        writer.writerow(["Status", run["status"]])
        writer.writerow([])
        
        # Metrics section
        writer.writerow(["Metrics"])
        for k, v in metrics.dict().items():
            writer.writerow([k, v])
        writer.writerow([])
        
        # Fills section
        writer.writerow(["Execution History"])
        writer.writerow(["Timestamp", "Symbol", "Side", "Price", "Qty", "Fee", "PnL"])
        
        for f in fills:
            writer.writerow([
                f["timestamp_utc"],
                f["symbol"],
                f["side"],
                f["fill_price"],
                f["quantity"],
                f["fee_usdt"],
                f["pnl"]
            ])
            
        output.seek(0)
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=backtest_{run_id}.csv"}
        )
