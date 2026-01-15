from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from app.api.auth import get_current_active_user as get_current_user
from app.persistence.db import DB
from app.core.strategy_service import StrategyService

router = APIRouter()

# Dependency
def get_strategy_service():
    return StrategyService(DB())

# --- Models ---
class StrategyCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    market_types: List[str] = []
    tags: List[str] = []
    spec: Optional[Dict[str, Any]] = {} # The initial logic
    schema: Optional[Dict[str, Any]] = {} # The inputs

class StrategyUpdate(BaseModel):
    logic: Dict[str, Any]
    schema: Optional[Dict[str, Any]] = {}
    changelog: Optional[str] = "Update"

# --- Endpoints ---

@router.get("/catalog")
def get_strategy_catalog(
    current_user: dict = Depends(get_current_user),
    service: StrategyService = Depends(get_strategy_service)
):
    """
    Get all available strategies (Marketplace + Private).
    """
    return {"strategies": service.list_strategies(user_id=current_user["id"])}

@router.get("/{strategy_id}")
def get_strategy_details(
    strategy_id: str,
    current_user: dict = Depends(get_current_user),
    service: StrategyService = Depends(get_strategy_service)
):
    strategy = service.get_strategy(strategy_id, user_id=current_user["id"])
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy

@router.post("/")
def create_strategy(
    payload: StrategyCreate,
    current_user: dict = Depends(get_current_user),
    service: StrategyService = Depends(get_strategy_service)
):
    """
    Create a new private strategy draft.
    """
    strategy_id = service.create_strategy(current_user["id"], payload.dict())
    return {"id": strategy_id, "status": "draft"}

@router.post("/{strategy_id}/versions")
def save_strategy_version(
    strategy_id: str,
    payload: StrategyUpdate,
    current_user: dict = Depends(get_current_user),
    service: StrategyService = Depends(get_strategy_service)
):
    """
    Save a new version of the strategy logic.
    """
    try:
        new_version = service.update_strategy_spec(strategy_id, current_user["id"], payload.dict())
        return {"version": new_version, "status": "saved"}
    except ValueError:
        raise HTTPException(status_code=403, detail="Unauthorized")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
