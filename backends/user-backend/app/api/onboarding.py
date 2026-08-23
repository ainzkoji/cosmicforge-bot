from fastapi import APIRouter, Depends, HTTPException, Body
from typing import List, Dict, Any

from app.api.auth import get_current_active_user
from app.core import onboarding_service
from app.schemas.onboarding import (
    OnboardingStateResponse,
    SaveStepRequest,
    StrategyCatalogResponse,
    NextStepDecision,
    BotSetupBlueprint
)

router = APIRouter()

@router.get("/state", response_model=OnboardingStateResponse)
async def get_state(
    current_user: dict = Depends(get_current_active_user)
):
    """
    Get current onboarding progress and answers.
    """
    return onboarding_service.get_onboarding_state(current_user["id"])

@router.post("/step")
async def save_step(
    req: SaveStepRequest,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Save answers for a specific step.
    Validates data against strict schemas for that step.
    """
    try:
        onboarding_service.update_onboarding_step(
            current_user["id"], 
            req.step, 
            req.data
        )
        return {"status": "saved", "step": req.step}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/complete", response_model=BotSetupBlueprint)
async def complete_onboarding(
    current_user: dict = Depends(get_current_active_user)
):
    """
    Finalize onboarding, generate clamped defaults, and save the profile.
    Returns the final Bot Setup Blueprint.
    """
    try:
        blueprint = onboarding_service.complete_onboarding(current_user["id"])
        return blueprint
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/strategies", response_model=StrategyCatalogResponse)
async def get_strategies():
    """
    Get list of available strategies for the wizard.
    """
    strategies = onboarding_service.get_strategy_catalog()
    return {"strategies": strategies}

@router.get("/next-steps", response_model=NextStepDecision)
async def get_next_steps(
    current_user: dict = Depends(get_current_active_user)
):
    """
    Get actionable advice based on platform gates (KYC, Broker, Plan).
    """
    return onboarding_service.get_next_steps(current_user["id"])
