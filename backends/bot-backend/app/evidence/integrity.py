"""Evidence integrity checks (§53) and provenance filtering (§36).

These are the invariants that make the evidence trustworthy enough to build a
readiness case — or later, an AI dataset — on top of. Each check returns the
offending rows rather than a boolean, so a failure names its subjects.

None of these checks repair anything. They report, and the caller decides.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from shared_lib.persistence.evidence_schema import (
    NON_ORGANIC_PROVENANCE,
    ORGANIC_PROVENANCE,
)

#: A decision may legitimately be mid-flight for a short window. Beyond this it
#: is an abandoned trace, not work in progress.
INCOMPLETE_GRACE_SECONDS = 120


def _rows(conn: Any, sql: str, args: tuple = ()) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(sql, args).fetchall()]


def _provenance_filter(alias: str = "") -> tuple[str, tuple]:
    """SQL fragment restricting rows to organic runtime evidence."""
    prefix = f"{alias}." if alias else ""
    placeholders = ",".join("?" for _ in ORGANIC_PROVENANCE)
    return f"{prefix}provenance IN ({placeholders})", tuple(sorted(ORGANIC_PROVENANCE))


# ── §5 / §53: no abandoned decisions ────────────────────────────────────────


def find_incomplete_decisions(db: Any, *, grace_seconds: int = INCOMPLETE_GRACE_SECONDS) -> list[dict]:
    """Decisions that never finalized, older than the transient window."""
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=grace_seconds)).isoformat()
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT decision_id, bot_instance_id, symbol, evaluated_at, primary_reason
               FROM trading_decisions
               WHERE complete = 0 AND evaluated_at < ?""",
            (cutoff,),
        )


def find_decisions_without_primary_reason(db: Any) -> list[dict]:
    """A complete decision must say why. NONE/NULL is not a reason."""
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT decision_id, bot_instance_id, symbol, final_action
               FROM trading_decisions
               WHERE complete = 1
                 AND (primary_reason IS NULL OR primary_reason = ''
                      OR UPPER(primary_reason) IN ('NONE', 'UNFINALIZED'))""",
        )


def find_duplicate_candle_decisions(db: Any) -> list[dict]:
    """Same bot + symbol + timeframe + candle must yield one entry decision."""
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT bot_instance_id, symbol, timeframe, closed_candle_close_time,
                      COUNT(*) AS n
               FROM trading_decisions
               WHERE complete = 1 AND closed_candle_close_time IS NOT NULL
               GROUP BY bot_instance_id, symbol, timeframe, closed_candle_close_time
               HAVING COUNT(*) > 1""",
        )


# ── §53: approved decision -> attempt -> order/fill -> position ─────────────


def find_approved_decisions_without_attempt(db: Any) -> list[dict]:
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT d.decision_id, d.bot_instance_id, d.symbol
               FROM trading_decisions d
               LEFT JOIN execution_attempts a ON a.decision_id = d.decision_id
               WHERE d.final_action = 'APPROVED' AND d.complete = 1
                 AND a.execution_attempt_id IS NULL""",
        )


def find_successful_attempts_without_evidence(db: Any) -> list[dict]:
    """A successful attempt must have produced an order id or a position."""
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT execution_attempt_id, bot_instance_id, symbol, result
               FROM execution_attempts
               WHERE result = 'SUCCESS'
                 AND (broker_order_id IS NULL OR broker_order_id = '')
                 AND (position_id IS NULL OR position_id = '')""",
        )


def find_open_fills_without_position(db: Any) -> list[dict]:
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT f.id, f.bot_instance_id, f.symbol, f.position_id
               FROM trade_fills f
               LEFT JOIN positions p ON p.position_id = f.position_id
               WHERE f.action IN ('OPEN', 'BUY', 'SELL')
                 AND f.position_id IS NOT NULL AND f.position_id <> ''
                 AND p.position_id IS NULL""",
        )


def find_flat_positions_without_close_evidence(db: Any) -> list[dict]:
    """FLAT must be backed by a close or reconciliation event, not just state."""
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT p.position_id, p.bot_instance_id, p.symbol, p.status
               FROM positions p
               WHERE p.status IN ('CLOSED', 'FLAT')
                 AND NOT EXISTS (
                     SELECT 1 FROM position_events e
                     WHERE e.position_id = p.position_id
                       AND e.event_type IN ('FINAL_CLOSE','DAILY_CLOSE',
                                            'KILL_SWITCH_CLOSE','RECONCILIATION')
                 )""",
        )


def find_position_quantity_mismatches(db: Any, *, tolerance: float = 1e-9) -> list[dict]:
    """original - realized - remaining must be zero within rounding tolerance."""
    with db.connect() as conn:
        rows = _rows(
            conn,
            "SELECT position_id, bot_instance_id, symbol, original_qty, realized_qty, remaining_qty "
            "FROM positions",
        )
    bad = []
    for row in rows:
        residual = (
            float(row["original_qty"] or 0.0)
            - float(row["realized_qty"] or 0.0)
            - float(row["remaining_qty"] or 0.0)
        )
        if abs(residual) > tolerance:
            bad.append({**row, "residual": residual})
    return bad


# ── §36: provenance separation ──────────────────────────────────────────────


def find_non_organic_in_readiness_scope(db: Any, bot_instance_id: str) -> list[dict]:
    """Rows that must never count toward readiness for this bot."""
    placeholders = ",".join("?" for _ in NON_ORGANIC_PROVENANCE)
    with db.connect() as conn:
        return _rows(
            conn,
            f"""SELECT decision_id, provenance FROM trading_decisions
                WHERE bot_instance_id = ? AND provenance IN ({placeholders})""",
            (bot_instance_id, *sorted(NON_ORGANIC_PROVENANCE)),
        )


def organic_decision_count(db: Any, bot_instance_id: str) -> int:
    """Count only evidence eligible to support a readiness claim."""
    clause, args = _provenance_filter()
    with db.connect() as conn:
        row = conn.execute(
            f"SELECT COUNT(*) FROM trading_decisions WHERE bot_instance_id = ? AND {clause}",
            (bot_instance_id, *args),
        ).fetchone()
    return int(row[0])


def find_mode_environment_contradictions(db: Any) -> list[dict]:
    """paper execution must never be recorded against a mainnet environment."""
    with db.connect() as conn:
        return _rows(
            conn,
            """SELECT decision_id, bot_instance_id, execution_mode, broker_environment
               FROM trading_decisions
               WHERE execution_mode = 'paper'
                 AND LOWER(COALESCE(broker_environment,'')) IN ('live','mainnet','production')""",
        )


# ── Aggregate ───────────────────────────────────────────────────────────────

INTEGRITY_CHECKS = (
    ("INCOMPLETE_DECISION", find_incomplete_decisions),
    ("MISSING_PRIMARY_REASON", find_decisions_without_primary_reason),
    ("DUPLICATE_DECISION", find_duplicate_candle_decisions),
    ("MISSING_EXECUTION_ATTEMPT", find_approved_decisions_without_attempt),
    ("ORPHAN_ORDER", find_successful_attempts_without_evidence),
    ("ORPHAN_FILL", find_open_fills_without_position),
    ("LIFECYCLE_FLAT_WITHOUT_CLOSE", find_flat_positions_without_close_evidence),
    ("POSITION_QTY_MISMATCH", find_position_quantity_mismatches),
    ("MODE_ENVIRONMENT_CONTRADICTION", find_mode_environment_contradictions),
)


def run_integrity_checks(db: Any, *, record: bool = False) -> dict[str, list[dict]]:
    """Run every integrity check. Optionally log findings as data-quality events."""
    findings: dict[str, list[dict]] = {}
    for name, check in INTEGRITY_CHECKS:
        try:
            rows = check(db)
        except Exception as exc:  # a missing legacy table must not hide the rest
            rows = [{"error": f"{type(exc).__name__}: {exc}"}]
        if rows:
            findings[name] = rows

    if record and findings:
        from app.evidence.writers import record_data_quality_event

        for name, rows in findings.items():
            record_data_quality_event(
                db,
                event_type=name,
                severity="ERROR" if name != "INCOMPLETE_DECISION" else "WARNING",
                detail=f"{len(rows)} row(s): {rows[:5]}",
            )
    return findings


def integrity_is_clean(db: Any) -> bool:
    return not run_integrity_checks(db)
