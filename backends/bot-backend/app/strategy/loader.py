from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from app.strategy.base import Strategy
from app.strategy.registry import get_strategy_class

logger = logging.getLogger(__name__)

_SAFE_FALLBACK = "master_ensemble"
_UNSAFE_STRATEGIES = {"sma_cross"}


def _parse_params(params_json: Optional[str]) -> Dict[str, Any]:
    if not params_json:
        return {}
    try:
        obj = json.loads(params_json)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def build_strategy(
    *, name: str, client: Any, interval: str, params_json: Optional[str] = None
) -> Strategy:
    execution_mode = os.getenv("EXECUTION_MODE", "paper").lower()

    # A-9: Reject sma_cross in live/user-capital mode.
    if name in _UNSAFE_STRATEGIES:
        if execution_mode == "live":
            logger.error(
                "STRATEGY_REJECTED_SMA_CROSS_UNSAFE_FOR_LIVE — "
                "sma_cross is not allowed in live/user-capital mode. "
                "Falling back to %s.",
                _SAFE_FALLBACK,
            )
            name = _SAFE_FALLBACK
        else:
            logger.warning(
                "STRATEGY_WARNING_SMA_CROSS_TEST_ONLY — "
                "sma_cross selected in paper/testnet mode. "
                "This strategy is for manual testing only and must not be used in production."
            )

    # If name is empty/None, default to master_ensemble.
    if not name:
        logger.warning(
            "STRATEGY_FALLBACK_MASTER_ENSEMBLE — no strategy name provided; "
            "defaulting to %s.",
            _SAFE_FALLBACK,
        )
        name = _SAFE_FALLBACK

    cls = get_strategy_class(name)
    if cls is None:
        logger.error(
            "Unknown strategy '%s'; falling back to %s.", name, _SAFE_FALLBACK
        )
        cls = get_strategy_class(_SAFE_FALLBACK)
        if cls is None:
            raise ValueError(f"Unknown strategy: {name} (fallback {_SAFE_FALLBACK} also unavailable)")

    params = _parse_params(params_json)

    try:
        return cls(client=client, interval=interval, **params)  # type: ignore
    except TypeError:
        return cls(client=client, interval=interval)  # type: ignore
