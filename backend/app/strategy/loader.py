from __future__ import annotations

import json
from typing import Any, Dict, Optional

from app.strategy.base import Strategy
from app.strategy.registry import get_strategy_class


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
    cls = get_strategy_class(name)
    if cls is None:
        raise ValueError(f"Unknown strategy: {name}")

    params = _parse_params(params_json)

    # Most of your existing strategies accept (client, interval, **params)
    try:
        return cls(client=client, interval=interval, **params)  # type: ignore
    except TypeError:
        return cls(client=client, interval=interval)  # type: ignore
