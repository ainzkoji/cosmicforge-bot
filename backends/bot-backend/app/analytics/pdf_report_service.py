"""
PDF Report Generator - Professional PDF Reports for Analytics

Generates PDF reports using reportlab for tax reports and analytics dashboards.
"""
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from io import BytesIO
import logging

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

from shared_lib.persistence.db import DB
from app.analytics.tax_report_service import TaxReportService

logger = logging.getLogger(__name__)


class PDFReportService:
    """Generate professional PDF reports for analytics and tax data"""
    
    def __init__(self, db: DB):
        self.db = db
        self.tax_service = TaxReportService(db)
    
    def generate_tax_report_pdf(
        self,
        user_id: str,
        tax_year: int,
        broker_account_id: Optional[str] = None
    ) -> bytes:
        """
        Generate PDF tax report.
        
        Args:
            user_id: User ID
            tax_year: Tax year (e.g., 2024)
            broker_account_id: Optional broker account filter
            
        Returns:
            PDF file content as bytes
        """
        # Get tax report data
        report = self.tax_service.get_execution_based_tax_report(
            user_id=user_id,
            tax_year=tax_year,
            broker_account_id=broker_account_id
        )
        
        # Create PDF buffer
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter)
        story = []
        
        # Styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1a1a1a'),
            spaceAfter=12,
            alignment=TA_CENTER
        )
        heading_style = styles['Heading2']
        disclaimer_style = ParagraphStyle(
            'Disclaimer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#666666'),
            spaceAfter=12,
            spaceBefore=12
        )
        
        # Title
        story.append(Paragraph(f"Tax Report {tax_year}", title_style))
        story.append(Spacer(1, 0.2*inch))
        
        # Disclaimer
        story.append(Paragraph(report['disclaimer'], disclaimer_style))
        story.append(Spacer(1, 0.3*inch))
        
        # Summary Section
        story.append(Paragraph("Summary", heading_style))
        summary = report['summary']
        summary_data = [
            ['Metric', 'Value'],
            ['Total Trades', str(summary['total_trades'])],
            ['Gains Count', str(summary['gains_count'])],
            ['Losses Count', str(summary['losses_count'])],
            ['Total Gains', f"${summary['total_gains']:.2f}"],
            ['Total Losses', f"${summary['total_losses']:.2f}"],
            ['Net P&L', f"${summary['net_pnl']:.2f}"],
            ['Total Fees', f"${summary['total_fees']:.2f}"],
        ]
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a5568')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 0.3*inch))
        
        # Trades Section
        story.append(Paragraph("Trade Details", heading_style))
        story.append(Spacer(1, 0.1*inch))
        
        if report['trades']:
            # Trades table header
            trades_data = [['Date', 'Symbol', 'Side', 'Qty', 'Price', 'P&L', 'Fee']]
            
            # Add trade rows (limit to prevent huge PDFs)
            for trade in report['trades'][:100]:  # Limit to first 100 trades
                trades_data.append([
                    trade['date'],
                    trade['symbol'],
                    trade['side'],
                    f"{trade['qty']:.4f}",
                    f"${trade['price']:.2f}",
                    f"${trade['realized_pnl']:.2f}",
                    f"${trade['fee']:.2f}",
                ])
            
            trades_table = Table(trades_data, colWidths=[0.9*inch, 0.9*inch, 0.6*inch, 0.8*inch, 0.9*inch, 0.9*inch, 0.8*inch])
            trades_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a5568')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f7fafc')]),
            ]))
            story.append(trades_table)
            
            if len(report['trades']) > 100:
                story.append(Spacer(1, 0.2*inch))
                story.append(Paragraph(
                    f"Note: Showing first 100 of {len(report['trades'])} trades. Download CSV for complete data.",
                    disclaimer_style
                ))
        else:
            story.append(Paragraph("No trades found for this period.", styles['Normal']))
        
        # Build PDF
        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        return pdf_bytes
    
    def generate_analytics_overview_pdf(
        self,
        user_id: str,
        timeframe: str = "ALL",
        bot_instance_id: Optional[str] = None,
        broker_account_id: Optional[str] = None
    ) -> bytes:
        """
        Generate PDF analytics overview report.
        
        Args:
            user_id: User ID
            timeframe: Time period filter
            bot_instance_id: Optional bot instance filter
            broker_account_id: Optional broker account filter
            
        Returns:
            PDF file content as bytes
        """
        # Get analytics data by querying the database directly
        db = DB()
        
        # Build filters
        where_clauses = ["user_id = ?"]
        params = [user_id]
        
        if bot_instance_id:
            where_clauses.append("bot_instance_id = ?")
            params.append(bot_instance_id)
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        # Timeframe filtering (simplified)
        since_date = None
        if timeframe == "1M":
            since_date = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        elif timeframe == "3M":
            since_date = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        elif timeframe == "YTD":
            since_date = f"{datetime.now(timezone.utc).year}-01-01T00:00:00Z"
        
        if since_date:
            where_clauses.append("timestamp_utc >= ?")
            params.append(since_date)
        
        where_sql = " AND ".join(where_clauses)
        
        with db.get_connection() as conn:
            # Get basic stats
            stats_row = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN action = 'CLOSE' AND realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN action = 'CLOSE' AND realized_pnl < 0 THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN action = 'CLOSE' THEN realized_pnl ELSE 0 END) as pnl_net,
                    SUM(fee) as fees_total
                FROM trade_fills
                WHERE {where_sql}
            """, params).fetchone()
        
        total_trades = stats_row["total_trades"] if stats_row else 0
        wins = stats_row["wins"] or 0 if stats_row else 0
        losses = stats_row["losses"] or 0 if stats_row else 0
        pnl_net = float(stats_row["pnl_net"] or 0.0) if stats_row else 0.0
        fees_total = float(stats_row["fees_total"] or 0.0) if stats_row else 0.0
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
        
        # Create PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter)
        story = []
        
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1a1a1a'),
            spaceAfter=12,
            alignment=TA_CENTER
        )
        
        # Title
        story.append(Paragraph(f"Analytics Overview - {timeframe}", title_style))
        story.append(Paragraph(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}", styles['Normal']))
        story.append(Spacer(1, 0.3*inch))
        
        # Metrics Table
        metrics_data = [
            ['Metric', 'Value'],
            ['Net P&L', f"${pnl_net:.2f}"],
            ['Total Fees', f"${fees_total:.2f}"],
            ['Total Trades', str(total_trades)],
            ['Wins', str(wins)],
            ['Losses', str(losses)],
            ['Win Rate', f"{win_rate:.1f}%"],
        ]
        
        metrics_table = Table(metrics_data, colWidths=[3*inch, 2*inch])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a5568')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(metrics_table)
        
        # Build PDF
        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        return pdf_bytes


# Singleton instance
_pdf_service_instance = None

def get_pdf_report_service() -> PDFReportService:
    """Get singleton PDF report service instance"""
    global _pdf_service_instance
    if _pdf_service_instance is None:
        _pdf_service_instance = PDFReportService(DB())
    return _pdf_service_instance
