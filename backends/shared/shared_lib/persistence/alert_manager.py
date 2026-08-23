"""
AlertManager - Manages monitoring alerts.

Detects alert conditions and persists alerts to database.
Supports acknowledgment and severity filtering.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import sqlite3


@dataclass
class Alert:
    """A monitoring alert."""
    alert_type: str
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW, INFO
    symbol: Optional[str]
    message: str
    trace_id: Optional[str] = None
    details: Dict[str, Any] = None
    id: Optional[int] = None
    ts: Optional[str] = None


class AlertType:
    """Standard alert types."""
    UNEXPECTED_OPEN = "UNEXPECTED_OPEN"
    OVERTRADING = "OVERTRADING"
    RISK_BREACH_NEAR_MISS = "RISK_BREACH_NEAR_MISS"
    EXECUTION_INSTABILITY = "EXECUTION_INSTABILITY"
    STATE_MISMATCH = "STATE_MISMATCH"
    INVARIANT_VIOLATION = "INVARIANT_VIOLATION"
    KILL_SWITCH_ACTIVATED = "KILL_SWITCH_ACTIVATED"
    MARGIN_WARNING = "MARGIN_WARNING"
    CONNECTION_ERROR = "CONNECTION_ERROR"


class Severity:
    """Alert severity levels."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class AlertManager:
    """
    Manages monitoring alerts.
    
    Usage:
        manager = AlertManager(db_path)
        
        # Create alert
        manager.emit(
            alert_type=AlertType.UNEXPECTED_OPEN,
            severity=Severity.CRITICAL,
            symbol="BTCUSDT",
            message="Position opened without intent trace",
            trace_id=trace_id,
            details={"order_id": "123"}
        )
        
        # Get unacknowledged alerts
        alerts = manager.get_alerts(unacked_only=True)
        
        # Acknowledge
        manager.acknowledge(alert_id, acknowledged_by="operator")
    """
    
    def __init__(self, db_path: str = "data/bot.db"):
        self._db_path = db_path
    
    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=1)
    
    def emit(
        self,
        alert_type: str,
        severity: str,
        message: str,
        symbol: Optional[str] = None,
        trace_id: Optional[str] = None,
        details: Optional[Dict] = None,
    ) -> Optional[int]:
        """
        Emit a new alert.
        
        Returns alert ID if successful.
        """
        try:
            conn = self._conn()
            try:
                ts = datetime.now(timezone.utc).isoformat()
                cursor = conn.execute(
                    """
                    INSERT INTO alerts (
                        ts, alert_type, severity, trace_id, symbol,
                        message, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ts, alert_type, severity, trace_id, symbol,
                        message, json.dumps(details) if details else None,
                    )
                )
                conn.commit()
                return cursor.lastrowid
            finally:
                conn.close()
        except Exception as e:
            print(f"[AlertManager] Failed to emit alert: {e}")
            return None
    
    def get_alerts(
        self,
        unacked_only: bool = False,
        severity: Optional[str] = None,
        alert_type: Optional[str] = None,
        symbol: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """Get alerts with filtering."""
        try:
            conn = self._conn()
            conn.row_factory = sqlite3.Row
            try:
                query = "SELECT * FROM alerts WHERE 1=1"
                params = []
                
                if unacked_only:
                    query += " AND acknowledged = 0"
                if severity:
                    query += " AND severity = ?"
                    params.append(severity)
                if alert_type:
                    query += " AND alert_type = ?"
                    params.append(alert_type)
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
                
                query += " ORDER BY id DESC LIMIT ?"
                params.append(limit)
                
                rows = conn.execute(query, params).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()
        except Exception:
            return []
    
    def acknowledge(
        self,
        alert_id: int,
        acknowledged_by: str = "system",
    ) -> bool:
        """Acknowledge an alert."""
        try:
            conn = self._conn()
            try:
                ts = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """
                    UPDATE alerts SET
                        acknowledged = 1,
                        acknowledged_at = ?,
                        acknowledged_by = ?
                    WHERE id = ?
                    """,
                    (ts, acknowledged_by, alert_id)
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception:
            return False
    
    def acknowledge_all(
        self,
        alert_type: Optional[str] = None,
        symbol: Optional[str] = None,
        acknowledged_by: str = "system",
    ) -> int:
        """Acknowledge multiple alerts. Returns count."""
        try:
            conn = self._conn()
            try:
                ts = datetime.now(timezone.utc).isoformat()
                query = """
                    UPDATE alerts SET
                        acknowledged = 1,
                        acknowledged_at = ?,
                        acknowledged_by = ?
                    WHERE acknowledged = 0
                """
                params = [ts, acknowledged_by]
                
                if alert_type:
                    query += " AND alert_type = ?"
                    params.append(alert_type)
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
                
                cursor = conn.execute(query, params)
                conn.commit()
                return cursor.rowcount
            finally:
                conn.close()
        except Exception:
            return 0
    
    def count_unacked(
        self,
        severity: Optional[str] = None,
    ) -> int:
        """Count unacknowledged alerts."""
        try:
            conn = self._conn()
            try:
                query = "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
                params = []
                
                if severity:
                    query += " AND severity = ?"
                    params.append(severity)
                
                return conn.execute(query, params).fetchone()[0]
            finally:
                conn.close()
        except Exception:
            return 0
    
    # --- Alert condition checks ---
    
    def check_overtrading(
        self,
        run_id: str,
        max_trades_per_hour: int = 20,
    ) -> Optional[Alert]:
        """Check for overtrading condition."""
        try:
            conn = self._conn()
            try:
                # Count trades in last hour
                count = conn.execute(
                    """
                    SELECT COUNT(*) FROM events
                    WHERE run_id = ?
                    AND action = 'ORDER_PLACED'
                    AND ts > datetime('now', '-1 hour')
                    """,
                    (run_id,)
                ).fetchone()[0]
                
                if count > max_trades_per_hour:
                    alert = Alert(
                        alert_type=AlertType.OVERTRADING,
                        severity=Severity.HIGH,
                        symbol=None,
                        message=f"Placed {count} trades in last hour (limit: {max_trades_per_hour})",
                        details={"count": count, "limit": max_trades_per_hour},
                    )
                    self.emit(
                        alert.alert_type, alert.severity,
                        alert.message, details=alert.details
                    )
                    return alert
            finally:
                conn.close()
        except Exception:
            pass
        return None
    
    def check_margin_warning(
        self,
        margin_level: float,
        warning_threshold: float = 150.0,  # 150%
    ) -> Optional[Alert]:
        """Check for low margin level."""
        if margin_level < warning_threshold:
            alert = Alert(
                alert_type=AlertType.MARGIN_WARNING,
                severity=Severity.HIGH if margin_level < 120 else Severity.MEDIUM,
                symbol=None,
                message=f"Margin level at {margin_level:.1f}% (warning at {warning_threshold}%)",
                details={"margin_level": margin_level, "threshold": warning_threshold},
            )
            self.emit(
                alert.alert_type, alert.severity,
                alert.message, details=alert.details
            )
            return alert
        return None


# Global instance
_manager: Optional[AlertManager] = None


def get_alert_manager(db_path: str = "data/bot.db") -> AlertManager:
    """Get or create global alert manager."""
    global _manager
    if _manager is None:
        _manager = AlertManager(db_path)
    return _manager
