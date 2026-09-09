"""Operator diagnostics for the AdaptiveEntryThresholdEngine.

The question these routes exist to answer:

    "What is the entry bar right now, and what made it that?"

and the one nobody could answer for months:

    "Is the engine actually adapting, or is something pinning it?"

Every number is served from the persisted threshold decisions, so the answer is
what the runtime recorded rather than what a recomputation would produce today.
No credentials, keys or broker secrets are exposed by any route here.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.auth import require_admin
from app.threshold.diagnostics import (
    detect_inert_engine,
    health_check,
    threshold_stats,
)
from app.threshold.migration import legacy_inventory
from app.threshold.persistence import (
    decisions_from_rows,
    load_expert_evaluations,
    load_threshold_decisions,
)
from app.threshold.runtime import get_threshold_policy
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/api/v1/admin/trading/threshold", tags=["Entry Threshold"])


def get_db() -> DB:
    return DB()


@router.get("/status/{bot_id}")
def threshold_status(
    bot_id: str,
    limit: int = Query(200, ge=1, le=2000),
    db: DB = Depends(get_db),
    _: Any = Depends(require_admin),
) -> dict[str, Any]:
    """Current mode, policy, per-symbol threshold, and adaptation health."""
    policy = get_threshold_policy()
    rows = load_threshold_decisions(db, bot_instance_id=bot_id, limit=limit)
    decisions = decisions_from_rows(rows)

    current: dict[str, Any] = {}
    for row in rows:  # rows are newest-first, so the first hit per key wins
        key = f"{row['symbol']}:{row['timeframe']}"
        if key in current:
            continue
        current[key] = {
            "status": row["status"],
            # NULL, not 0.0, when nothing was evaluated on that candle.
            "final_threshold": row["final_threshold"],
            "decided_at": row["decided_at"],
            "regime": row["regime"],
            "mode": row["threshold_mode"],
            "engine_version": row["threshold_engine_version"],
            "policy_hash": row["policy_hash"],
        }

    stats = threshold_stats(decisions, total_bound=policy.total_adjustment_bound)
    inert = detect_inert_engine(decisions)

    return {
        "bot_instance_id": bot_id,
        "policy": policy.summary(),
        "active_threshold_authorities": 1,
        "current_by_symbol": current,
        "distribution": stats.to_dict(),
        "adaptation": inert.to_dict(),
        "health": health_check(decisions, policy=policy),
        "sample": len(decisions),
    }


@router.get("/decisions/{threshold_decision_id}")
def threshold_decision_detail(
    threshold_decision_id: str,
    db: DB = Depends(get_db),
    _: Any = Depends(require_admin),
) -> dict[str, Any]:
    """One threshold calculation, every component, plus the expert evidence."""
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM threshold_decisions WHERE threshold_decision_id = ?",
            (threshold_decision_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="threshold decision not found")

    payload = dict(row)
    rebuilt = decisions_from_rows([payload])
    payload["reconciles"] = rebuilt[0].reconcile() if rebuilt else None
    payload["reconciliation_error"] = (
        rebuilt[0].reconciliation_error() if rebuilt else None
    )
    payload["experts"] = load_expert_evaluations(
        db, threshold_decision_id=threshold_decision_id
    )
    return payload


@router.get("/history/{bot_id}")
def threshold_history(
    bot_id: str,
    symbol: str | None = None,
    limit: int = Query(200, ge=1, le=2000),
    db: DB = Depends(get_db),
    _: Any = Depends(require_admin),
) -> dict[str, Any]:
    """Threshold over time, with the adjustment components that moved it."""
    rows = load_threshold_decisions(
        db, bot_instance_id=bot_id, symbol=symbol, limit=limit
    )
    return {
        "bot_instance_id": bot_id,
        "symbol": symbol,
        "count": len(rows),
        "history": [
            {
                "threshold_decision_id": r["threshold_decision_id"],
                "decided_at": r["decided_at"],
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "closed_candle_time": r["closed_candle_time"],
                "status": r["status"],
                "regime": r["regime"],
                "opportunity_confidence": r["opportunity_confidence"],
                "base_threshold": r["base_threshold"],
                "adjustments": {
                    "regime": r["regime_adjustment"],
                    "volatility": r["volatility_adjustment"],
                    "agreement": r["agreement_adjustment"],
                    "htf": r["htf_adjustment"],
                    "market_quality": r["market_quality_adjustment"],
                    "performance": r["performance_adjustment"],
                    "distribution": r["distribution_adjustment"],
                },
                "raw_unclamped_threshold": r["raw_unclamped_threshold"],
                "final_threshold": r["final_threshold"],
                "previous_threshold": r["previous_threshold"],
                "smoothing_applied": bool(r["smoothing_applied"]),
                "rate_limit_applied": bool(r["rate_limit_applied"]),
                "clamp_applied": bool(r["clamp_applied"]),
                "passed": r["passed"],
                "reason": r["reason"],
            }
            for r in rows
        ],
    }


@router.get("/policy")
def threshold_policy(
    symbol: str | None = None,
    venue: str | None = None,
    market_type: str | None = None,
    _: Any = Depends(require_admin),
) -> dict[str, Any]:
    """The resolved policy for a scope, and what happened to the old settings."""
    policy = get_threshold_policy(symbol=symbol, venue=venue, market_type=market_type)
    return {
        "policy": policy.summary(),
        "legacy_settings": legacy_inventory(),
        "active_threshold_authorities": 1,
    }
