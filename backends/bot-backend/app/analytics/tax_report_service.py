"""
Tax Report Generator - Execution-Based Tax Reports for Crypto Trading

Generates tax reports based on trade_fills execution records.
WARNING: This is NOT accounting advice. Users should consult tax professionals.

Phase 1: Execution-based chronological reports
Phase 2 (Future): FIFO/LIFO matching when position lifecycle IDs exist
"""
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone
from shared_lib.core.money_math import ZERO, dsum, mul, to_dec, to_float
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
            
        # Format trades. Money is summed in exact decimal over the same rows (every CLOSE fill of the year) and
        # reported as the float nearest each exact total; a NULL fee or PnL counts as zero, as SQL SUM skipped it.
        trade_list = []
        by_symbol = {}
        pnl_values = [to_dec(row["realized_pnl"], default=ZERO) for row in trades]
        fee_values = [to_dec(row["fee"], default=ZERO) for row in trades]
        symbol_pnl: Dict[str, List[Any]] = {}
        symbol_fees: Dict[str, List[Any]] = {}
        
        for row, row_pnl, row_fee in zip(trades, pnl_values, fee_values):
            trade = {
                "date": row["timestamp_utc"][:10] if row["timestamp_utc"] else "",
                "timestamp": row["timestamp_utc"],
                "symbol": row["symbol"],
                "side": row["side"],
                "qty": float(row["qty"]),
                "price": float(row["price"]),
                "proceeds": to_float(mul(row["qty"], row["price"])),
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
            symbol_pnl.setdefault(symbol, []).append(row_pnl)
            symbol_fees.setdefault(symbol, []).append(row_fee)
        for symbol, entry in by_symbol.items():
            entry["total_pnl"] = to_float(dsum(symbol_pnl[symbol]))
            entry["total_fees"] = to_float(dsum(symbol_fees[symbol]))
        
        # Summary
        summary = {
            "total_trades": len(trades),
            "gains_count": sum(1 for v in pnl_values if v > 0),
            "losses_count": sum(1 for v in pnl_values if v < 0),
            "total_gains": to_float(dsum(v for v in pnl_values if v > 0)),
            "total_losses": to_float(dsum(v for v in pnl_values if v < 0)),
            "net_pnl": to_float(dsum(pnl_values)),
            "total_fees": to_float(dsum(fee_values))
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
