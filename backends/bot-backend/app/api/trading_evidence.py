"""Phase 9 §10 — operator diagnostics over canonical trading evidence.

The question these routes exist to answer, without correlating log files:

    "Why did this bot not trade between 18:00 and 22:00 UTC?"

and its counterpart:

    "Show me exactly why it did trade."

Everything is served from the canonical tables. No credentials, keys or broker
secrets are exposed by any route here.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.auth import require_admin
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import ORGANIC_PROVENANCE

router = APIRouter(prefix="/api/v1/admin/trading/evidence", tags=["Trading Evidence"])


def get_db() -> DB:
    return DB()


def _rows(conn: Any, sql: str, args: tuple = ()) -> list[dict]:
    return [dict(r) for r in conn.execute(sql, args).fetchall()]


def _loads(value: Any) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def _window(hours: int | None, since: str | None, until: str | None) -> tuple[str, str]:
    """Resolve an explicit window, or default to the last N hours."""
    end = until or datetime.now(timezone.utc).isoformat()
    if since:
        return since, end
    span = timedelta(hours=hours or 24)
    return (datetime.now(timezone.utc) - span).isoformat(), end


@router.get("/bots/{bot_id}")
def bot_evidence_summary(
    bot_id: str,
    hours: int = Query(24, ge=1, le=24 * 90),
    since: str | None = None,
    until: str | None = None,
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """Runs, cycles and decision counts for one bot over a window."""
    start, end = _window(hours, since, until)
    with db.connect() as conn:
        runs = _rows(
            conn,
            """SELECT run_id, runtime_session_id, policy_hash, provenance, execution_mode,
                      broker_environment, started_at, ended_at, status, cycles, decisions,
                      attempts, fills, positions_opened, positions_closed, primary_failure_reason
               FROM bot_runs WHERE bot_instance_id=? AND started_at BETWEEN ? AND ?
               ORDER BY started_at DESC""",
            (bot_id, start, end),
        )
        cycles = _rows(
            conn,
            """SELECT cycle_id, run_id, started_at, completed_at, new_candle_evaluations,
                      no_new_candle_count, approved_count, execution_attempt_count, error_count
               FROM trading_cycles WHERE bot_instance_id=? AND started_at BETWEEN ? AND ?
               ORDER BY started_at DESC LIMIT 200""",
            (bot_id, start, end),
        )
        totals = conn.execute(
            """SELECT COUNT(*) AS decisions,
                      SUM(CASE WHEN final_action='APPROVED' THEN 1 ELSE 0 END) AS approved
               FROM trading_decisions
               WHERE bot_instance_id=? AND evaluated_at BETWEEN ? AND ?""",
            (bot_id, start, end),
        ).fetchone()

    return {
        "bot_instance_id": bot_id,
        "window": {"since": start, "until": end},
        "runs": runs,
        "cycles": cycles,
        "decisions": int(totals["decisions"] or 0),
        "approved": int(totals["approved"] or 0),
    }


@router.get("/bots/{bot_id}/diagnostics")
def bot_diagnostics(
    bot_id: str,
    hours: int = Query(24, ge=1, le=24 * 90),
    since: str | None = None,
    until: str | None = None,
    organic_only: bool = Query(False, description="Exclude replay/test/validation evidence"),
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """Answer 'why did this bot not trade?' from canonical evidence alone."""
    start, end = _window(hours, since, until)

    clause = "bot_instance_id=? AND evaluated_at BETWEEN ? AND ?"
    args: list[Any] = [bot_id, start, end]
    if organic_only:
        clause += f" AND provenance IN ({','.join('?' for _ in ORGANIC_PROVENANCE)})"
        args.extend(sorted(ORGANIC_PROVENANCE))

    with db.connect() as conn:
        reason_rows = _rows(
            conn,
            f"""SELECT primary_reason, COUNT(*) AS n FROM trading_decisions
                WHERE {clause} GROUP BY primary_reason ORDER BY n DESC""",
            tuple(args),
        )
        stage_rows = _rows(
            conn,
            f"""SELECT
                  SUM(CASE WHEN quality_result='FAIL' THEN 1 ELSE 0 END) AS quality_rejections,
                  SUM(CASE WHEN risk_result='FAIL' THEN 1 ELSE 0 END) AS risk_rejections,
                  SUM(CASE WHEN execution_feasibility_result='FAIL' THEN 1 ELSE 0 END)
                      AS execution_rejections,
                  SUM(CASE WHEN entry_protection_result='FAIL' THEN 1 ELSE 0 END)
                      AS protection_rejections,
                  SUM(CASE WHEN final_action='APPROVED' THEN 1 ELSE 0 END) AS approved,
                  COUNT(*) AS total
                FROM trading_decisions WHERE {clause}""",
            tuple(args),
        )
        attempts = conn.execute(
            "SELECT COUNT(*) FROM execution_attempts WHERE bot_instance_id=? AND started_at BETWEEN ? AND ?",
            (bot_id, start, end),
        ).fetchone()[0]
        fills = conn.execute(
            "SELECT COUNT(*) FROM trade_fills WHERE bot_instance_id=? AND created_at BETWEEN ? AND ?",
            (bot_id, start, end),
        ).fetchone()[0] if _has_column(conn, "trade_fills", "created_at") else None
        policy = conn.execute(
            "SELECT policy_hash FROM bot_runs WHERE bot_instance_id=? ORDER BY started_at DESC LIMIT 1",
            (bot_id,),
        ).fetchone()

    reason_counts = {r["primary_reason"] or "UNKNOWN": int(r["n"]) for r in reason_rows}
    stages = stage_rows[0] if stage_rows else {}
    evaluations = int(stages.get("total") or 0)
    non_evaluations = reason_counts.get("NO_NEW_CANDLE", 0)

    return {
        "bot_instance_id": bot_id,
        "window": {"since": start, "until": end},
        "organic_only": organic_only,
        "evaluations": evaluations,
        "new_candle_evaluations": evaluations - non_evaluations,
        "no_new_candle": non_evaluations,
        "reason_counts": reason_counts,
        "stage_rejections": {
            "quality": int(stages.get("quality_rejections") or 0),
            "risk": int(stages.get("risk_rejections") or 0),
            "execution": int(stages.get("execution_rejections") or 0),
            "entry_protection": int(stages.get("protection_rejections") or 0),
        },
        "approved": int(stages.get("approved") or 0),
        "execution_attempts": int(attempts or 0),
        "fills": fills,
        "current_policy_hash": policy["policy_hash"] if policy else None,
    }


def _has_column(conn: Any, table: str, column: str) -> bool:
    try:
        return column in {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    except Exception:
        return False


@router.get("/decisions/{decision_id}")
def decision_detail(
    decision_id: str,
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """The full causal chain for one decision — OLD-R4's acceptance test.

    Answers in one response: what market was seen, what strategy evidence
    existed, what regime, what confidence, what threshold, what veto, what risk
    result, what execution result, and why the final action was taken.
    """
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM trading_decisions WHERE decision_id=?", (decision_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Decision not found")
        decision = dict(row)

        snapshot = None
        if decision.get("market_snapshot_id"):
            snap_row = conn.execute(
                "SELECT * FROM market_snapshots WHERE market_snapshot_id=?",
                (decision["market_snapshot_id"],),
            ).fetchone()
            snapshot = dict(snap_row) if snap_row else None

        attempts = _rows(
            conn, "SELECT * FROM execution_attempts WHERE decision_id=?", (decision_id,)
        )
        position = None
        events: list[dict] = []
        if decision.get("position_id"):
            pos_row = conn.execute(
                "SELECT * FROM positions WHERE position_id=?", (decision["position_id"],)
            ).fetchone()
            position = dict(pos_row) if pos_row else None
            events = _rows(
                conn,
                "SELECT * FROM position_events WHERE position_id=? ORDER BY occurred_at",
                (decision["position_id"],),
            )

    for field in (
        "component_signals_json", "active_strategies_json", "supporting_strategies_json",
        "opposing_strategies_json", "component_metadata_json", "secondary_reasons_json",
        "fill_ids_json",
    ):
        decision[field.removesuffix("_json")] = _loads(decision.pop(field, None))

    return {
        "decision": decision,
        "market_snapshot": snapshot,
        "execution_attempts": attempts,
        "position": position,
        "position_events": events,
    }


@router.get("/runs/{run_id}")
def run_detail(
    run_id: str,
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    with db.connect() as conn:
        run = conn.execute("SELECT * FROM bot_runs WHERE run_id=?", (run_id,)).fetchone()
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        cycles = _rows(
            conn, "SELECT * FROM trading_cycles WHERE run_id=? ORDER BY started_at", (run_id,)
        )
        reasons = _rows(
            conn,
            """SELECT primary_reason, COUNT(*) AS n FROM trading_decisions
               WHERE run_id=? GROUP BY primary_reason ORDER BY n DESC""",
            (run_id,),
        )
    return {
        "run": dict(run),
        "cycles": cycles,
        "reason_counts": {r["primary_reason"] or "UNKNOWN": int(r["n"]) for r in reasons},
    }


@router.get("/cycles/{cycle_id}")
def cycle_detail(
    cycle_id: str,
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    with db.connect() as conn:
        cycle = conn.execute(
            "SELECT * FROM trading_cycles WHERE cycle_id=?", (cycle_id,)
        ).fetchone()
        if not cycle:
            raise HTTPException(status_code=404, detail="Cycle not found")
        decisions = _rows(
            conn,
            """SELECT decision_id, symbol, final_action, primary_reason, raw_confidence,
                      effective_entry_threshold, quality_result, risk_result
               FROM trading_decisions WHERE cycle_id=? ORDER BY evaluated_at""",
            (cycle_id,),
        )
    payload = dict(cycle)
    payload["reason_counts"] = _loads(payload.pop("reason_counts_json", None))
    return {"cycle": payload, "decisions": decisions}


@router.get("/integrity")
def evidence_integrity(
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """Run the §53 integrity invariants and report any violations."""
    from app.evidence.integrity import run_integrity_checks

    findings = run_integrity_checks(db)
    return {
        "clean": not findings,
        "violations": {name: len(rows) for name, rows in findings.items()},
        "detail": {name: rows[:20] for name, rows in findings.items()},
    }
