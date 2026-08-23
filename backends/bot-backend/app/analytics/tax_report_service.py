"""
Tax Report Generator - Execution-Based Tax Reports for Crypto Trading

Generates tax reports based on trade_fills execution records.
WARNING: This is NOT accounting advice. Users should consult tax professionals.

Phase 1: Execution-based chronological reports
Phase 2 (Future): FIFO/LIFO matching when position lifecycle IDs exist
"""
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone
from shared_lib.persistence.db import DB
import logging
import csv
import json
from io import StringIO

logger = logging.getLogger(__name__)


class TaxReportService:
    """Generate tax reports for realized gains/losses"""
    
    def __init__(self, db: DB):
        self.db = db
    
    def get_execution_based_tax_report(
        self,
        user_id: str,
        tax_year: int,
        broker_account_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate execution-based tax report for a tax year.
        
        WARNING: This is a simplified execution-based report.
        Consult a tax professional for proper tax reporting.
        
        Args:
            user_id: User ID (required)
            tax_year: Tax year (e.g., 2024)
            broker_account_id: Filter by specific broker account
            
        Returns:
            {
                "tax_year": int,
                "disclaimer": str,
                "summary": dict,
                "trades": List[dict],  # All CLOSE trades
                "by_symbol": List[dict],  # Grouped by symbol
                "currency": str
            }
        """
        start_date = f"{tax_year}-01-01T00:00:00Z"
        end_date = f"{tax_year}-12-31T23:59:59Z"
        
        where_clauses = [
            "user_id = ?",
            "action = 'CLOSE'",
            "timestamp_utc >= ?",
            "timestamp_utc <= ?"
        ]
        params = [user_id, start_date, end_date]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            # Get all close trades
            trades = conn.execute(
                f"""
                SELECT 
                    timestamp_utc,
                    symbol,
                    side,
                    qty,
                    price,
                    fee,
                    realized_pnl,
                    quote_currency,
                    base_currency,
                    broker_id,
                    broker_account_id
                FROM trade_fills
                WHERE {where_sql}
                ORDER BY timestamp_utc ASC
                """,
                params
            ).fetchall()
            
            # Summary stats
            summary_row = conn.execute(
                f"""
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as gains_count,
                    SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losses_count,
                    SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) as total_gains,
                    SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE 0 END) as total_losses,
                    SUM(realized_pnl) as net_pnl,
                    SUM(fee) as total_fees
                FROM trade_fills
                WHERE {where_sql}
                """,
                params
            ).fetchone()
        
        # Format trades
        trade_list = []
        by_symbol = {}
        
        for row in trades:
            trade = {
                "date": row["timestamp_utc"][:10] if row["timestamp_utc"] else "",
                "timestamp": row["timestamp_utc"],
                "symbol": row["symbol"],
                "side": row["side"],
                "qty": float(row["qty"]),
                "price": float(row["price"]),
                "proceeds": float(row["qty"]) * float(row["price"]),
                "fee": float(row["fee"] or 0.0),
                "realized_pnl": float(row["realized_pnl"] or 0.0),
                "quote_currency": row["quote_currency"] or "USDT",
                "base_currency": row["base_currency"] or "",
                "broker": row["broker_id"],
                "category": "short_term"  # Crypto is typically short-term
            }
            trade_list.append(trade)
            
            # Group by symbol
            symbol = row["symbol"]
            if symbol not in by_symbol:
                by_symbol[symbol] = {
                    "symbol": symbol,
                    "trade_count": 0,
                    "total_pnl": 0.0,
                    "total_fees": 0.0
                }
            by_symbol[symbol]["trade_count"] += 1
            by_symbol[symbol]["total_pnl"] += float(row["realized_pnl"] or 0.0)
            by_symbol[symbol]["total_fees"] += float(row["fee"] or 0.0)
        
        # Summary
        summary = {
            "total_trades": summary_row["total_trades"] if summary_row else 0,
            "gains_count": summary_row["gains_count"] or 0 if summary_row else 0,
            "losses_count": summary_row["losses_count"] or 0 if summary_row else 0,
            "total_gains": float(summary_row["total_gains"] or 0.0) if summary_row else 0.0,
            "total_losses": float(summary_row["total_losses"] or 0.0) if summary_row else 0.0,
            "net_pnl": float(summary_row["net_pnl"] or 0.0) if summary_row else 0.0,
            "total_fees": float(summary_row["total_fees"] or 0.0) if summary_row else 0.0
        }
        
        disclaimer = (
            "⚠️ DISCLAIMER: This is an execution-based tax report for INFORMATIONAL PURPOSES ONLY. "
            "It does NOT use FIFO, LIFO, or specific lot identification methods required by many tax authorities. "
            "This report shows chronological trade executions and realized P&L as reported by the broker. "
            "Consult a qualified tax professional or accountant for proper tax reporting. "
            "Cryptocurrency tax regulations vary by jurisdiction and are subject to change."
        )
        
        return {
            "tax_year": tax_year,
            "disclaimer": disclaimer,
            "summary": summary,
            "trades": trade_list,
            "by_symbol": list(by_symbol.values()),
            "currency": "USDT",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "method": "execution_based"
        }
    
    def export_tax_report_csv(
        self,
        user_id: str,
        tax_year: int,
        broker_account_id: Optional[str] = None
    ) -> str:
        """
        Export tax report as CSV string.
        
        Returns:
            CSV string with trade data
        """
        report = self.get_execution_based_tax_report(
            user_id=user_id,
            tax_year=tax_year,
            broker_account_id=broker_account_id
        )
        
        output = StringIO()
        
        # Write disclaimer
        output.write(f"# Tax Report {tax_year}\n")
        output.write(f"# {report['disclaimer']}\n\n")
        
        # Write summary
        output.write("# SUMMARY\n")
        summary = report["summary"]
        output.write(f"# Total Trades: {summary['total_trades']}\n")
        output.write(f"# Gains: {summary['gains_count']} trades, {summary['total_gains']:.2f} {report['currency']}\n")
        output.write(f"# Losses: {summary['losses_count']} trades, {summary['total_losses']:.2f} {report['currency']}\n")
        output.write(f"# Net P&L: {summary['net_pnl']:.2f} {report['currency']}\n")
        output.write(f"# Total Fees: {summary['total_fees']:.2f} {report['currency']}\n\n")
        
        # Write trades
        writer = csv.DictWriter(output, fieldnames=[
            "date", "symbol", "side", "qty", "price", "proceeds",
            "fee", "realized_pnl", "quote_currency", "broker", "category"
        ])
        writer.writeheader()
        
        for trade in report["trades"]:
            writer.writerow({
                "date": trade["date"],
                "symbol": trade["symbol"],
                "side": trade["side"],
                "qty": f"{trade['qty']:.8f}",
                "price": f"{trade['price']:.2f}",
                "proceeds": f"{trade['proceeds']:.2f}",
                "fee": f"{trade['fee']:.2f}",
                "realized_pnl": f"{trade['realized_pnl']:.2f}",
                "quote_currency": trade["quote_currency"],
                "broker": trade["broker"],
                "category": trade["category"]
            })
        
        return output.getvalue()
    
    def export_tax_report_json(
        self,
        user_id: str,
        tax_year: int,
        broker_account_id: Optional[str] = None
    ) -> str:
        """
        Export tax report as JSON string.
        
        Returns:
            JSON string with complete tax report
        """
        report = self.get_execution_based_tax_report(
            user_id=user_id,
            tax_year=tax_year,
            broker_account_id=broker_account_id
        )
        
        return json.dumps(report, indent=2)
    
    def get_fifo_tax_report(
        self,
        user_id: str,
        tax_year: int
    ) -> Dict[str, Any]:
        """
        FUTURE: Generate FIFO-matched tax report.
        
        Requires position lifecycle IDs to match OPEN/CLOSE pairs.
        Not implemented in Phase 1.
        
        Raises:
            NotImplementedError
        """
        raise NotImplementedError(
            "FIFO tax reporting requires position lifecycle IDs. "
            "This feature will be implemented when position tracking includes lifecycle IDs. "
            "For now, use get_execution_based_tax_report()."
        )


# Singleton instance
_tax_report_service_instance = None

def get_tax_report_service() -> TaxReportService:
    """Get singleton tax report service instance"""
    global _tax_report_service_instance
    if _tax_report_service_instance is None:
        _tax_report_service_instance = TaxReportService(DB())
    return _tax_report_service_instance
