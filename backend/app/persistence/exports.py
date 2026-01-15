"""
Export System - D Persistence

CSV exports for:
- Trades (one row per trade_id)
- Fills
- Events
- Strategy performance
"""
from __future__ import annotations
import csv
import io
from typing import Optional, List
from datetime import datetime
import sqlite3

from app.persistence.trade_tracker import get_trade_tracker


def export_trades_csv(run_id: str, db_path: str = "data/bot.db") -> str:
    """
    Export trades for a run to CSV format.
    Returns CSV string.
    """
    tracker = get_trade_tracker(db_path)
    trades = tracker.get_trades_by_run(run_id)
    
    if not trades:
        return "No trades found"
    
    output = io.StringIO()
    fieldnames = [
        "trade_id", "symbol", "side", "strategy", "mode", "timeframe",
        "entry_time", "entry_price", "entry_qty", "entry_confidence",
        "exit_time", "exit_price", "exit_reason",
        "realized_pnl", "fees", "r_multiple",
        "tp1_hit", "add_count", "status"
    ]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for trade in trades:
        row = {k: trade.get(k, "") for k in fieldnames}
        writer.writerow(row)
    
    return output.getvalue()


def export_events_csv(
    run_id: str,
    event_types: Optional[List[str]] = None,
    db_path: str = "data/bot.db"
) -> str:
    """
    Export events for a run to CSV format.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if event_types:
            placeholders = ",".join("?" * len(event_types))
            cursor = conn.execute(f"""
                SELECT * FROM events
                WHERE run_id = ? AND event_type IN ({placeholders})
                ORDER BY ts
            """, [run_id] + event_types)
        else:
            cursor = conn.execute("""
                SELECT * FROM events WHERE run_id = ? ORDER BY ts
            """, (run_id,))
        
        rows = cursor.fetchall()
    finally:
        conn.close()
    
    if not rows:
        return "No events found"
    
    output = io.StringIO()
    fieldnames = ["event_id", "ts", "event_type", "level", "symbol", "trade_id", "payload_json"]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in rows:
        writer.writerow({k: row[k] for k in fieldnames})
    
    return output.getvalue()


def export_strategy_performance_csv(run_id: str, db_path: str = "data/bot.db") -> str:
    """
    Export per-strategy performance for a run.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("""
            SELECT
                strategy,
                mode,
                COUNT(*) as total_trades,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losses,
                SUM(realized_pnl) as net_pnl,
                AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END) as avg_win,
                AVG(CASE WHEN realized_pnl < 0 THEN ABS(realized_pnl) END) as avg_loss,
                AVG(r_multiple) as avg_r
            FROM trades
            WHERE run_id = ? AND status = 'CLOSED'
            GROUP BY strategy, mode
            ORDER BY net_pnl DESC
        """, (run_id,))
        
        rows = cursor.fetchall()
    finally:
        conn.close()
    
    if not rows:
        return "No strategy data found"
    
    output = io.StringIO()
    fieldnames = ["strategy", "mode", "total_trades", "wins", "losses", 
                  "win_rate", "net_pnl", "avg_win", "avg_loss", "avg_r", "profit_factor"]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in rows:
        strategy, mode, total, wins, losses, net_pnl, avg_win, avg_loss, avg_r = row
        wins = wins or 0
        losses = losses or 0
        avg_win = avg_win or 0
        avg_loss = avg_loss or 0
        
        pf = (avg_win * wins) / (avg_loss * losses) if losses > 0 and avg_loss > 0 else 0
        
        writer.writerow({
            "strategy": strategy,
            "mode": mode,
            "total_trades": total,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
            "net_pnl": round(net_pnl or 0, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "avg_r": round(avg_r or 0, 2),
            "profit_factor": round(pf, 2),
        })
    
    return output.getvalue()


def get_run_report(run_id: str, db_path: str = "data/bot.db") -> dict:
    """
    Generate a complete run report (JSON-friendly).
    """
    tracker = get_trade_tracker(db_path)
    summary = tracker.get_trade_summary(run_id)
    
    # Get run info
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        run_row = cursor.fetchone()
        run_info = dict(run_row) if run_row else {}
        
        # Get by-strategy breakdown
        cursor = conn.execute("""
            SELECT strategy, mode,
                   COUNT(*) as trades,
                   SUM(realized_pnl) as pnl
            FROM trades
            WHERE run_id = ? AND status = 'CLOSED'
            GROUP BY strategy, mode
        """, (run_id,))
        by_strategy = [dict(row) for row in cursor.fetchall()]
        
        # Get by-symbol breakdown
        cursor = conn.execute("""
            SELECT symbol,
                   COUNT(*) as trades,
                   SUM(realized_pnl) as pnl
            FROM trades
            WHERE run_id = ? AND status = 'CLOSED'
            GROUP BY symbol
        """, (run_id,))
        by_symbol = [dict(row) for row in cursor.fetchall()]
        
        # Risk events
        cursor = conn.execute("""
            SELECT COUNT(*) as count
            FROM events
            WHERE run_id = ? AND event_type = 'RISK_GATE_BLOCK'
        """, (run_id,))
        risk_blocks = cursor.fetchone()[0]
        
    finally:
        conn.close()
    
    return {
        "run_id": run_id,
        "run_info": run_info,
        "summary": summary,
        "by_strategy": by_strategy,
        "by_symbol": by_symbol,
        "risk_blocks": risk_blocks,
    }


# =============================================================================
# LEGACY EXPORTS - Works with existing trade_fills table
# =============================================================================

def export_legacy_fills_csv(run_id: str = None, db_path: str = "data/bot.db") -> str:
    """
    Export fills from legacy trade_fills table.
    If run_id is None, exports all fills.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Check what columns exist
        cursor = conn.execute("PRAGMA table_info(trade_fills)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if not columns:
            return "trade_fills table not found"
        
        # Build query
        select_cols = ", ".join(columns)
        if run_id:
            cursor = conn.execute(f"SELECT {select_cols} FROM trade_fills WHERE run_id = ? ORDER BY ts", (run_id,))
        else:
            cursor = conn.execute(f"SELECT {select_cols} FROM trade_fills ORDER BY ts DESC LIMIT 1000")
        
        rows = cursor.fetchall()
    finally:
        conn.close()
    
    if not rows:
        return "No fills found"
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    
    for row in rows:
        writer.writerow({k: row[k] for k in columns})
    
    return output.getvalue()


def export_legacy_performance_csv(db_path: str = "data/bot.db") -> str:
    """
    Export strategy performance from legacy trade_fills table (all runs).
    """
    conn = sqlite3.connect(db_path)
    try:
        # Check columns exist
        cursor = conn.execute("PRAGMA table_info(trade_fills)")
        columns = {row[1] for row in cursor.fetchall()}
        
        # Determine pnl column
        pnl_col = None
        for col_name in ["realized_pnl", "pnl_realized", "pnl"]:
            if col_name in columns:
                pnl_col = col_name
                break
        
        if not pnl_col:
            return "No PnL column found"
        
        if "strategy" not in columns:
            # Use a simpler aggregation
            cursor = conn.execute(f"""
                SELECT
                    symbol,
                    side,
                    COUNT(*) as total_fills,
                    SUM(CASE WHEN {pnl_col} > 0 THEN {pnl_col} ELSE 0 END) as gross_profit,
                    SUM(CASE WHEN {pnl_col} < 0 THEN ABS({pnl_col}) ELSE 0 END) as gross_loss,
                    SUM({pnl_col}) as net_pnl,
                    SUM(fee) as total_fees
                FROM trade_fills
                GROUP BY symbol, side
                ORDER BY net_pnl DESC
            """)
        else:
            cursor = conn.execute(f"""
                SELECT
                    strategy,
                    symbol,
                    COUNT(*) as total_fills,
                    SUM(CASE WHEN {pnl_col} > 0 THEN {pnl_col} ELSE 0 END) as gross_profit,
                    SUM(CASE WHEN {pnl_col} < 0 THEN ABS({pnl_col}) ELSE 0 END) as gross_loss,
                    SUM({pnl_col}) as net_pnl,
                    SUM(fee) as total_fees
                FROM trade_fills
                GROUP BY strategy, symbol
                ORDER BY net_pnl DESC
            """)
        
        rows = cursor.fetchall()
    finally:
        conn.close()
    
    if not rows:
        return "No performance data found"
    
    output = io.StringIO()
    
    if "strategy" not in columns:
        fieldnames = ["symbol", "side", "total_fills", "gross_profit", "gross_loss", "net_pnl", "total_fees", "profit_factor"]
    else:
        fieldnames = ["strategy", "symbol", "total_fills", "gross_profit", "gross_loss", "net_pnl", "total_fees", "profit_factor"]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in rows:
        row_dict = {fieldnames[i]: row[i] for i in range(len(row))}
        gross_profit = row_dict.get("gross_profit", 0) or 0
        gross_loss = row_dict.get("gross_loss", 0) or 0
        row_dict["profit_factor"] = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0
        writer.writerow(row_dict)
    
    return output.getvalue()


def get_legacy_summary(db_path: str = "data/bot.db") -> dict:
    """
    Get summary from legacy trade_fills table.
    """
    conn = sqlite3.connect(db_path)
    try:
        # Check what columns exist
        cursor = conn.execute("PRAGMA table_info(trade_fills)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        
        if not existing_cols:
            return {"error": "trade_fills table not found"}
        
        # Determine correct column name for pnl
        pnl_col = None
        for col_name in ["realized_pnl", "pnl_realized", "pnl"]:
            if col_name in existing_cols:
                pnl_col = col_name
                break
        fee_col = "fee" if "fee" in existing_cols else "fees" if "fees" in existing_cols else None
        
        if not pnl_col:
            return {"error": "No pnl column found", "columns": list(existing_cols)}
        
        # Build dynamic query
        query = f"""
            SELECT
                COUNT(*) as total_fills,
                SUM(CASE WHEN {pnl_col} > 0 THEN 1 ELSE 0 END) as winning_fills,
                SUM(CASE WHEN {pnl_col} < 0 THEN 1 ELSE 0 END) as losing_fills,
                SUM({pnl_col}) as net_pnl
        """
        if fee_col:
            query += f", SUM({fee_col}) as total_fees"
        query += " FROM trade_fills"
        
        cursor = conn.execute(query)
        row = cursor.fetchone()
        
        if not row or row[0] == 0:
            return {"total_fills": 0}
        
        total = row[0]
        wins = row[1] or 0
        losses = row[2] or 0
        net_pnl = row[3] or 0
        fees = row[4] if fee_col and len(row) > 4 else 0
        
        return {
            "total_fills": total,
            "winning_fills": wins,
            "losing_fills": losses,
            "net_pnl": round(net_pnl, 4),
            "total_fees": round(fees or 0, 4),
            "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
        }
    finally:
        conn.close()
