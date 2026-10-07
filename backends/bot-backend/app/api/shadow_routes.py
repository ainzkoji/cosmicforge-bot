"""
Shadow Trading System — REST API Routes

All endpoints are read-only. They expose shadow analytics for research.
Shadow capture and evaluation are internal concerns (runner + evaluator).

Access control: every route requires an authenticated user. Shadow trades
belong to a bot, and a bot belongs to a user:

* a regular user must name a ``bot_instance_id`` they own and only ever sees
  that bot's shadow data (a bot that is missing or someone else's is a 404);
* system-wide views that the store cannot scope to one bot (status counts,
  outcomes list, regime and symbol breakdowns) are admin-only;
* an admin may query any bot, or all bots by omitting ``bot_instance_id``.

Prefix: /api/v1/shadow
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from app.core.auth import get_current_user_id, oauth2_scheme, require_admin
from app.core.security import decode_token
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)
# Authentication is declared on the router so a route added later cannot be
# public by omission.
router = APIRouter(
    prefix="/api/v1/shadow",
    tags=["shadow"],
    dependencies=[Depends(get_current_user_id)],
)


def _caller_is_admin(token: str = Depends(oauth2_scheme)) -> bool:
    """True when the (already validated) access token carries the admin role."""
    payload = decode_token(token) or {}
    return payload.get("type") == "access" and payload.get("role") == "admin"


def _bot_owner_id(bot_instance_id: str) -> Optional[str]:
    """Owner (user id) of a bot instance, or None when the bot does not exist."""
    with DB().connect() as conn:
        row = conn.execute(
            "SELECT user_id FROM bot_instances WHERE id = ?", (bot_instance_id,)
        ).fetchone()
    if not row:
        return None
    return str(row[0]) if row[0] is not None else None


def _authorize_bot_scope(bot_instance_id: Optional[str], user_id: str, is_admin: bool) -> Optional[str]:
    """Return the bot filter the caller is allowed to query with.

    Admins may query one bot or all of them (``None``). Everyone else must
    name a bot they own; a missing bot and another user's bot both answer 404
    so bot ids cannot be probed.
    """
    if is_admin:
        return bot_instance_id
    if not bot_instance_id:
        raise HTTPException(
            status_code=403,
            detail="bot_instance_id is required: shadow data is scoped to your own bots",
        )
    try:
        owner = _bot_owner_id(bot_instance_id)
    except Exception as exc:
        # Fail closed: without a verified owner nothing is returned.
        logger.error("[ShadowAPI] bot ownership lookup failed: %s", exc)
        raise HTTPException(status_code=503, detail="Bot ownership could not be verified")
    if owner is None or owner != str(user_id):
        raise HTTPException(status_code=404, detail="Bot instance not found")
    return bot_instance_id


def _get_analytics():
    from app.shadow.analytics import get_shadow_analytics
    return get_shadow_analytics()


def _get_store():
    from app.shadow.store import get_shadow_store
    return get_shadow_store()


# ── Health / Status ───────────────────────────────────────────────────────────

@router.get("/status")
def get_shadow_status(_admin: str = Depends(require_admin)) -> Dict[str, Any]:
    """
    Shadow system status — config flags and current DB record counts.

    Admin-only: the counts span every user's bots.
    """
    from app.shadow.config import get_shadow_config
    cfg = get_shadow_config()
    store = _get_store()
    try:
        status_counts = store.count_by_status()
        stage_counts = store.count_by_stage()
    except Exception as exc:
        logger.warning("[ShadowAPI] status query failed: %s", exc)
        status_counts = {}
        stage_counts = {}

    return {
        "enabled": cfg.enabled,
        "config": {
            "capture_ml_blocked": cfg.capture_ml_blocked,
            "capture_threshold_blocked": cfg.capture_threshold_blocked,
            "capture_correlation_blocked": cfg.capture_correlation_blocked,
            "capture_already_open": cfg.capture_already_open,
            "capture_executor_rejected": cfg.capture_executor_rejected,
            "capture_safety_blocked": cfg.capture_safety_blocked,
            "capture_near_miss": cfg.capture_near_miss,
            "expiry_bars": cfg.expiry_bars,
            "max_eval_per_pass": cfg.max_eval_per_pass,
        },
        "record_counts": {
            "by_status": status_counts,
            "by_stage": stage_counts,
        },
    }


# ── Category Summary ──────────────────────────────────────────────────────────

@router.get("/summary")
def get_summary(
    days: int = Query(default=30, ge=1, le=365),
    bot_instance_id: Optional[str] = Query(default=None),
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(_caller_is_admin),
) -> List[Dict[str, Any]]:
    """
    Per-rejection-stage summary of evaluated shadow trades.

    Returns total, win_rate, avg_pnl_pct, total_pnl_net grouped by rejection stage.
    """
    bot_instance_id = _authorize_bot_scope(bot_instance_id, user_id, is_admin)
    try:
        return _get_analytics().get_category_summary(days=days, bot_instance_id=bot_instance_id)
    except Exception as exc:
        logger.error("[ShadowAPI] /summary failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Trade List ────────────────────────────────────────────────────────────────

@router.get("/trades")
def list_trades(
    rejection_stage: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    bot_instance_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(_caller_is_admin),
) -> List[Dict[str, Any]]:
    """
    List shadow trades with optional filters (stage, status, symbol, bot).
    """
    bot_instance_id = _authorize_bot_scope(bot_instance_id, user_id, is_admin)
    try:
        return _get_store().list_trades(
            rejection_stage=rejection_stage,
            status=status,
            symbol=symbol,
            bot_instance_id=bot_instance_id,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:
        logger.error("[ShadowAPI] /trades failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/trades/{shadow_trade_id}")
def get_trade(
    shadow_trade_id: str,
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(_caller_is_admin),
) -> Dict[str, Any]:
    """Get a specific shadow trade by ID (includes outcome if evaluated)."""
    trade = _get_store().get_shadow_trade(shadow_trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Shadow trade not found")
    if not is_admin:
        # The trade must belong to one of the caller's own bots. Someone
        # else's trade is reported exactly like a missing one.
        try:
            owner = _bot_owner_id(str(trade.get("bot_instance_id") or ""))
        except Exception as exc:
            logger.error("[ShadowAPI] bot ownership lookup failed: %s", exc)
            raise HTTPException(status_code=503, detail="Bot ownership could not be verified")
        if owner is None or owner != str(user_id):
            raise HTTPException(status_code=404, detail="Shadow trade not found")
    outcome = _get_store().get_outcome(shadow_trade_id)
    return {"trade": trade, "outcome": outcome}


# ── Outcomes List ─────────────────────────────────────────────────────────────

@router.get("/outcomes")
def list_outcomes(
    outcome: Optional[str] = Query(default=None, description="TP_HIT | SL_HIT | EXPIRED | DATA_MISSING"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _admin: str = Depends(require_admin),
) -> List[Dict[str, Any]]:
    """
    List shadow trade outcomes, joined with trade metadata.

    Admin-only: the store cannot scope this list to one bot.
    """
    try:
        return _get_store().list_outcomes(outcome=outcome, limit=limit, offset=offset)
    except Exception as exc:
        logger.error("[ShadowAPI] /outcomes failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── ML Gate Analysis ──────────────────────────────────────────────────────────

@router.get("/analytics/ml-gate")
def ml_gate_analysis(
    days: int = Query(default=30, ge=1, le=365),
    bot_instance_id: Optional[str] = Query(default=None),
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(_caller_is_admin),
) -> Dict[str, Any]:
    """
    ML gate analysis — what happens to ML-blocked trades?

    Returns hypothetical win rate + PnL for trades the ML model blocked,
    with per-score-bucket breakdown. Used to evaluate ML gate efficacy.
    """
    bot_instance_id = _authorize_bot_scope(bot_instance_id, user_id, is_admin)
    try:
        return _get_analytics().get_ml_gate_analysis(days=days, bot_instance_id=bot_instance_id)
    except Exception as exc:
        logger.error("[ShadowAPI] /analytics/ml-gate failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Threshold Gap Analysis ────────────────────────────────────────────────────

@router.get("/analytics/threshold")
def threshold_gap_analysis(
    days: int = Query(default=30, ge=1, le=365),
    bot_instance_id: Optional[str] = Query(default=None),
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(_caller_is_admin),
) -> List[Dict[str, Any]]:
    """
    Threshold gap analysis — near-miss vs far-below-threshold trade performance.

    Bucketed by how far the confidence was below the threshold.
    Useful for deciding whether to tighten or relax the dynamic threshold.
    """
    bot_instance_id = _authorize_bot_scope(bot_instance_id, user_id, is_admin)
    try:
        return _get_analytics().get_threshold_gap_analysis(days=days, bot_instance_id=bot_instance_id)
    except Exception as exc:
        logger.error("[ShadowAPI] /analytics/threshold failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Shadow vs Real Comparison ─────────────────────────────────────────────────

@router.get("/analytics/compare")
def compare_vs_real(
    days: int = Query(default=30, ge=1, le=365),
    bot_instance_id: Optional[str] = Query(default=None),
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(_caller_is_admin),
) -> Dict[str, Any]:
    """
    Compare shadow (hypothetical) vs real executed trade performance.

    Answers: "Are the trades we're taking better or worse than the ones we're blocking?"
    """
    bot_instance_id = _authorize_bot_scope(bot_instance_id, user_id, is_admin)
    try:
        return _get_analytics().compare_vs_real_trades(days=days, bot_instance_id=bot_instance_id)
    except Exception as exc:
        logger.error("[ShadowAPI] /analytics/compare failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Regime Breakdown ──────────────────────────────────────────────────────────

@router.get("/analytics/regime")
def regime_breakdown(
    days: int = Query(default=30, ge=1, le=365),
    _admin: str = Depends(require_admin),
) -> List[Dict[str, Any]]:
    """Shadow trade performance broken down by market regime (admin-only: spans all bots)."""
    try:
        return _get_analytics().get_regime_breakdown(days=days)
    except Exception as exc:
        logger.error("[ShadowAPI] /analytics/regime failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Symbol Breakdown ──────────────────────────────────────────────────────────

@router.get("/analytics/symbol")
def symbol_breakdown(
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=20, ge=1, le=100),
    _admin: str = Depends(require_admin),
) -> List[Dict[str, Any]]:
    """Per-symbol shadow trade summary sorted by total trades (admin-only: spans all bots)."""
    try:
        return _get_analytics().get_symbol_breakdown(days=days, limit=limit)
    except Exception as exc:
        logger.error("[ShadowAPI] /analytics/symbol failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
