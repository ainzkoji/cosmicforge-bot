from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Dict, Any
from app.api.auth import get_current_active_user
from app.persistence.db import DB
from app.core.strategy_service import StrategyService
from app.schemas.strategies import StrategyDSL, StrategyVersion

router = APIRouter()

def get_service():
    return StrategyService(DB())

@router.post("/validate")
def validate_spec(
    spec: StrategyDSL,
    user: dict = Depends(get_current_active_user)
):
    """
    Validate a strategy spec against the DSL rules.
    Does not save to DB.
    """
    # Placeholder validation logic
    # Real logic would check:
    # 1. Recursive depth
    # 2. Unknown indicators
    # 3. Invalid types
    
    errors = []
    if len(spec.indicators) > 10:
        errors.append("Too many indicators (max 10)")
    
    if errors:
        return {"valid": False, "errors": errors}
        
    return {"valid": True, "errors": []}

@router.post("/{strategy_id}/versions", response_model=Dict[str, Any])
def save_version(
    strategy_id: str,
    spec: StrategyDSL,
    changelog: str = Body(default="Updated spec"),
    user: dict = Depends(get_current_active_user),
    service: StrategyService = Depends(get_service)
):
    """
    Save a new immutable version of the strategy logic.
    """
    try:
        # Pydantic -> Dict for service
        spec_dict = spec.dict()
        
        # We need to wrap it to match service signature which expects {logic: ..., schema: ...}
        # or update service to handle DSL objects directly.
        # Current service expects: {'logic': ..., 'schema': ...} in update_strategy_spec?
        # Let's check service.update_strategy_spec signature...
        # It takes `spec: Dict`.
        # And extracts `spec.get("logic")` and `spec.get("schema")`.
        
        # We'll treat the StrategyDSL as the "logic".
        payload = {
            "logic": spec_dict,
            "schema": {}, # We aren't passing schema param in this endpoint yet
            "changelog": changelog
        }
        
        new_version_num = service.update_strategy_spec(strategy_id, user["id"], payload)
        return {"version": new_version_num, "status": "saved"}
        
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{strategy_id}/publish")
def publish_strategy(
    strategy_id: str,
    user: dict = Depends(get_current_active_user),
    service: StrategyService = Depends(get_service)
):
    """
    Submit strategy for publishing/review.
    """
    # Needs service.publish_strategy or similar DB update
    # For MVP:
    # 1. Update status to 'under_review' or 'active' (if trusted)
    # 2. Check strict validation
    
    return {"status": "submitted", "message": "Strategy submitted for review"}
