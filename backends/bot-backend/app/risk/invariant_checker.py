"""
InvariantChecker - Detects critical safety violations.

Checks for conditions that should NEVER happen:
- Order placed while kill-switch active
- Position opened while exposure freeze active
- Missing SL on new position
- Order without matching intent trace
- Double-open (new position while pending order exists)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import sqlite3


@dataclass
class InvariantViolation:
    """A detected invariant violation."""
    violation_type: str
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    symbol: str
    trace_id: Optional[str]
    details: Dict[str, Any]
    auto_action: Optional[str] = None  # HARD_KILL, CLOSE_POSITION, etc.


class InvariantChecker:
    """
    Checks for critical safety invariants.
    
    Usage:
        checker = InvariantChecker(db_path)
        violations = checker.check_all(
            symbol="BTCUSDT",
            trace_id=trace_id,
            kill_switch_active=False,
            exposure_freeze=False,
            has_intent=True,
            has_sl=True,
            pending_order_exists=False,
            action="OPEN_LONG"
        )
        
        for v in violations:
            if v.severity == "CRITICAL":
                # Trigger auto-kill
                pass
    """
    
    def __init__(self, db_path: str = "data/bot.db"):
        self._db_path = db_path
    
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=10000;")
        except Exception:
            pass
        return conn
    
    def check_all(
        self,
        symbol: str,
        trace_id: Optional[str] = None,
        run_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        action: str = "",
        kill_switch_active: bool = False,
        exposure_freeze: bool = False,
        gate_blocked: bool = False,
        has_intent: bool = True,
        has_sl: bool = True,
        pending_order_exists: bool = False,
        local_position: str = "NONE",
        exchange_position: str = "NONE",
    ) -> List[InvariantViolation]:
        """
        Run all invariant checks.
        
        Returns list of violations (empty if all checks pass).
        """
        violations = []
        is_open_action = action in ("OPEN_LONG", "OPEN_SHORT", "BUY", "SELL")
        
        # 1. Kill-switch bypass
        if kill_switch_active and is_open_action:
            violations.append(InvariantViolation(
                violation_type="KILL_SWITCH_BYPASS",
                severity="CRITICAL",
                symbol=symbol,
                trace_id=trace_id,
                details={
                    "action": action,
                    "kill_switch_active": True,
                },
                auto_action="HARD_KILL",
            ))
        
        # 2. Exposure freeze bypass
        if exposure_freeze and is_open_action:
            violations.append(InvariantViolation(
                violation_type="EXPOSURE_FREEZE_BYPASS",
                severity="CRITICAL",
                symbol=symbol,
                trace_id=trace_id,
                details={
                    "action": action,
                    "exposure_freeze_active": True,
                },
                auto_action="HARD_KILL",
            ))
        
        # 3. Missing intent trace
        if is_open_action and not has_intent:
            violations.append(InvariantViolation(
                violation_type="MISSING_INTENT_TRACE",
                severity="CRITICAL",
                symbol=symbol,
                trace_id=trace_id,
                details={
                    "action": action,
                    "has_intent_event": False,
                },
                auto_action="HARD_KILL",
            ))
        
        # 4. Missing SL on open
        if is_open_action and action in ("OPEN_LONG", "OPEN_SHORT") and not has_sl:
            violations.append(InvariantViolation(
                violation_type="MISSING_STOP_LOSS",
                severity="HIGH",
                symbol=symbol,
                trace_id=trace_id,
                details={
                    "action": action,
                    "has_sl": False,
                },
                auto_action="CLOSE_POSITION",
            ))
        
        # 5. Double-open (pending order exists)
        if is_open_action and pending_order_exists:
            violations.append(InvariantViolation(
                violation_type="DOUBLE_OPEN_ATTEMPT",
                severity="HIGH",
                symbol=symbol,
                trace_id=trace_id,
                details={
                    "action": action,
                    "pending_order_exists": True,
                },
                auto_action=None,  # Just block, don't kill
            ))
        
        # 6. Gate said BLOCK but order was placed
        if gate_blocked and is_open_action:
            violations.append(InvariantViolation(
                violation_type="GATE_BLOCK_BYPASS",
                severity="CRITICAL",
                symbol=symbol,
                trace_id=trace_id,
                details={
                    "action": action,
                    "gate_blocked": True,
                },
                auto_action="HARD_KILL",
            ))

        # 7. State mismatch (local vs exchange)
        if local_position != exchange_position:
            # Only flag if it's a significant mismatch
            if (local_position == "NONE" and exchange_position != "NONE") or \
               (local_position != "NONE" and exchange_position == "NONE"):
                violations.append(InvariantViolation(
                    violation_type="STATE_MISMATCH",
                    severity="HIGH",
                    symbol=symbol,
                    trace_id=trace_id,
                    details={
                        "local_position": local_position,
                        "exchange_position": exchange_position,
                    },
                    auto_action=None,  # Trigger reconciliation
                ))
        
        # Persist violations to database
        if violations:
            self._persist_violations(violations, run_id=run_id, bot_instance_id=bot_instance_id)
        
        return violations
    
    def _persist_violations(
        self,
        violations: List[InvariantViolation],
        run_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
    ):
        """Save violations to database."""
        try:
            conn = self._conn()
            try:
                ts = datetime.now(timezone.utc).isoformat()
                # Support both new and legacy schemas (bot_instance_id column may not exist).
                cols = []
                try:
                    c = conn.execute("PRAGMA table_info(invariant_violations)")
                    cols = [r[1] for r in c.fetchall()]
                except Exception:
                    cols = []

                has_bot_col = "bot_instance_id" in cols
                for v in violations:
                    if has_bot_col:
                        conn.execute(
                            """
                            INSERT INTO invariant_violations (
                                ts, trace_id, run_id, bot_instance_id, symbol,
                                violation_type, severity, details_json, auto_action_taken
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                ts, v.trace_id, run_id, bot_instance_id, v.symbol,
                                v.violation_type, v.severity,
                                json.dumps(v.details), v.auto_action,
                            )
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO invariant_violations (
                                ts, trace_id, run_id, symbol,
                                violation_type, severity, details_json, auto_action_taken
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                ts, v.trace_id, run_id, v.symbol,
                                v.violation_type, v.severity,
                                json.dumps(v.details), v.auto_action,
                            )
                        )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            print(f"[InvariantChecker] Failed to persist violations: {e}")
    
    def get_recent_violations(
        self,
        symbol: Optional[str] = None,
        severity: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """Get recent violations from database."""
        try:
            conn = self._conn()
            conn.row_factory = sqlite3.Row
            try:
                query = "SELECT * FROM invariant_violations WHERE 1=1"
                params = []
                
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
                if severity:
                    query += " AND severity = ?"
                    params.append(severity)
                
                query += " ORDER BY id DESC LIMIT ?"
                params.append(limit)
                
                rows = conn.execute(query, params).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()
        except Exception:
            return []
    
    def has_critical_violations(
        self,
        bot_instance_id: Optional[str] = None,
        run_id: Optional[str] = None,
        since_minutes: int = 5,
    ) -> bool:
        """Check if there are recent critical violations."""
        # Global invariant checks are admin-only. Runtime bot checks must pass bot_instance_id or run_id.
        try:
            conn = self._conn()
            try:
                query = """
                    SELECT COUNT(*) FROM invariant_violations 
                    WHERE severity = 'CRITICAL'
                    AND ts > datetime('now', '-' || ? || ' minutes')
                """
                params = [since_minutes]
                 
                if bot_instance_id:
                    query += " AND bot_instance_id = ?"
                    params.append(bot_instance_id)
                if run_id:
                    query += " AND run_id = ?"
                    params.append(run_id)
                 
                count = conn.execute(query, params).fetchone()[0]
                return count > 0
            finally:
                conn.close()
        except Exception:
            return False


# Global instance
_checker: Optional[InvariantChecker] = None


def get_invariant_checker(db_path: str = "data/bot.db") -> InvariantChecker:
    """Get or create global invariant checker."""
    global _checker
    if _checker is None:
        _checker = InvariantChecker(db_path)
    return _checker
