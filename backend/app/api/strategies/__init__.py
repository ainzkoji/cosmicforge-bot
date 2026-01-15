from fastapi import APIRouter
from app.api.strategies import marketplace, management, builder

router = APIRouter()

# Public Market
router.include_router(marketplace.router, tags=["Strategy Marketplace"])

# My Library
router.include_router(management.router, prefix="/my", tags=["My Strategies"])

# Builder / Editor
router.include_router(builder.router, prefix="/build", tags=["Strategy Builder"])
