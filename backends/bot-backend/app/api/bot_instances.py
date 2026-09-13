"""
Bot Instances API

Manages bot instance lifecycle (list, get, start, stop, pause, delete).
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import List, Optional
import json
import logging

from app.core.auth import get_current_active_user, require_permission
from app.core.bot_instance_service import get_bot_instance_service, BotInstanceService
from app.models.bot_instance_models import BotInstance
from shared_lib.persistence.db import DB

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/bot-instances", response_model=List[BotInstance])
def get_user_bot_instances(
    status: Optional[str] = None,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read"))
):
    """Get all bot instances for the current user (excludes deleted/archived by default unless requested)."""
    return service.get_user_bot_instances(user["id"], status_filter=status)


@router.get("/bot-instances/inventory", response_model=List[BotInstance])
def get_bot_inventory(
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read"))
):
    """Get all bot instances in the system including archived and deleted (admin diagnostic)."""
    return service.get_all_bot_instances()


@router.get("/bot-instances/{instance_id}", response_model=BotInstance)
def get_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read"))
):
    """Get a specific bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    return instance


@router.get("/bot-instances/{instance_id}/effective-policy")
def get_effective_policy(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read")),
):
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    db = service.db
    with db.connect() as conn:
        row = conn.execute("SELECT environment FROM broker_accounts WHERE id=?", (instance.broker_account_id,)).fetchone()
    from app.runner.effective_policy import EffectivePolicyError, resolve_effective_bot_policy
    try:
        policy = resolve_effective_bot_policy(
            instance=instance,
            risk_params=service.get_risk_profile_preset(instance.risk_level),
            broker_environment=(row["environment"] if row else "unknown"),
        )
    except EffectivePolicyError as exc:
        # Fail closed and say why, rather than returning an invented policy.
        raise HTTPException(
            status_code=409,
            detail={
                "status": "CONFIGURATION_INVALID",
                "reason": exc.reason_code,
                "message": str(exc),
            },
        )

    payload = policy.to_public_dict()

    # Phase 2 §28: surface whether the live runner is still on this policy.
    runner_policy_hash = None
    try:
        from app.main import runner_service

        multi = getattr(runner_service, "multi_runner", None)
        cached = (getattr(multi, "_runners", {}) or {}).get(instance_id) if multi else None
        runner_policy_hash = getattr(cached, "effective_policy_hash", None) if cached else None
    except Exception:
        runner_policy_hash = None

    payload["runner_policy_hash"] = runner_policy_hash
    payload["policy_stale"] = bool(
        runner_policy_hash is not None and runner_policy_hash != policy.policy_hash
    )
    payload["requested_vs_effective"] = {
        "risk_per_trade": {
            "requested": policy.requested_risk_per_trade,
            "effective": policy.risk_per_trade,
            "hard_ceiling": policy.risk_per_trade_ceiling,
        },
        "max_leverage": {
            "requested": policy.requested_max_leverage,
            "effective": policy.max_leverage,
            "hard_ceiling": policy.max_leverage_ceiling,
        },
        "max_open_positions": {
            "requested": policy.requested_max_open_positions,
            "effective": policy.max_open_positions,
        },
        "max_daily_trades": {
            "requested": policy.requested_max_daily_trades,
            "effective": policy.max_daily_trades,
        },
    }
    # The allocation is per trade; keep it apart from capital_allocation, the
    # position count, committed margin and account affordability.
    try:
        from app.risk.capital_ledger import capital_diagnostics

        payload["capital"] = capital_diagnostics(
            db,
            bot_instance_id=instance_id,
            allocation_type=policy.position_allocation_type,
            allocation_value=policy.position_allocation_value,
            capital_allocation=policy.capital_budget,
            max_open_positions=policy.max_open_positions,
            account_key=str(instance.broker_account_id or "") or None,
        )
    except Exception as exc:
        payload["capital"] = {"error": f"{type(exc).__name__}: {exc}"}
    return payload


@router.get("/bot-instances/{instance_id}/decision-diagnostics")
def get_decision_diagnostics(
    instance_id: str,
    lookback: int = 200,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read")),
):
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    limit = max(1, min(int(lookback), 5000))
    with service.db.connect() as conn:
        rows = conn.execute(
            """SELECT final_action,primary_reason_code,confidence,effective_entry_threshold,
                      executor_status,executor_error,decision_json
               FROM canonical_trade_decisions WHERE bot_instance_id=?
               ORDER BY decision_timestamp DESC LIMIT ?""",
            (instance_id, limit),
        ).fetchall()
        fills = conn.execute(
            """SELECT action,COUNT(*) AS n FROM trade_fills
               WHERE bot_instance_id=? GROUP BY action""", (instance_id,),
        ).fetchall()
    reason_counts: dict[str, int] = {}
    regime_counts: dict[str, int] = {}
    confidences, thresholds = [], []
    errors = 0
    for row in rows:
        reason = str(row["primary_reason_code"] or "UNKNOWN")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        payload = json.loads(row["decision_json"] or "{}")
        regime = str(payload.get("regime") or "UNKNOWN")
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
        if row["confidence"] is not None:
            confidences.append(float(row["confidence"]))
        if row["effective_entry_threshold"] is not None:
            thresholds.append(float(row["effective_entry_threshold"]))
        errors += 1 if row["executor_error"] else 0
    fill_counts = {str(r["action"]): int(r["n"]) for r in fills}
    return {
        "bot_instance_id": instance_id,
        "evaluation_count": len(rows),
        "new_candle_count": sum(1 for r in rows if r["primary_reason_code"] != "NO_NEW_CANDLE"),
        "regime_distribution": regime_counts,
        "buy_count": sum(1 for r in rows if str(r["final_action"]).upper() == "BUY"),
        "sell_count": sum(1 for r in rows if str(r["final_action"]).upper() == "SELL"),
        "hold_count": sum(1 for r in rows if str(r["final_action"]).upper() in {"HOLD", "NONE"}),
        "average_confidence": sum(confidences) / len(confidences) if confidences else None,
        "average_effective_threshold": sum(thresholds) / len(thresholds) if thresholds else None,
        "average_confidence_gap": (
            sum(c - t for c, t in zip(confidences, thresholds)) / min(len(confidences), len(thresholds))
            if confidences and thresholds else None
        ),
        "top_rejection_reasons": sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:10],
        "execution_attempts": sum(1 for r in rows if r["executor_status"] not in (None, "None")),
        "fills": sum(fill_counts.values()),
        "positions_opened": fill_counts.get("OPEN", 0),
        "partial_fills": fill_counts.get("PARTIAL_CLOSE", 0),
        "closes": fill_counts.get("CLOSE", 0),
        "executor_errors": errors,
    }


@router.post("/bot-instances/{instance_id}/start", response_model=BotInstance)
def start_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:control"))
):
    """Start a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    try:
        return service.start_bot_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bot-instances/{instance_id}/pause", response_model=BotInstance)
def pause_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:control"))
):
    """Pause a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        return service.pause_bot_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bot-instances/{instance_id}/stop", response_model=BotInstance)
def stop_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:control"))
):
    """Stop a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        return service.stop_bot_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/bot-instances/{instance_id}")
def delete_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:write"))
):
    """Delete a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        service.delete_bot_instance(instance_id)
        return {"status": "deleted", "id": instance_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bot-instances/{instance_id}/archive")
def archive_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:write"))
):
    """Archive a bot instance (preserve record but remove from active flows)."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        service.archive_bot_instance(instance_id)
        return {"status": "archived", "id": instance_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
