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
    # Stored strategy names are historical metadata, never engine selection.
    from app.trading_intelligence.integration.runtime_binding import CatiRuntimeBinding
    return CatiRuntimeBinding(client=client, interval=interval)


def _signature_rejects(callable_: Any, /, **kwargs: Any) -> bool:
    """True only when ``callable_``'s signature cannot bind ``kwargs``.

    Decided from the signature, never from the exception message. When the
    signature cannot be inspected the answer is False, so the original error
    propagates: failing closed.
    """
    import inspect

    try:
        signature = inspect.signature(callable_)
    except (TypeError, ValueError):
        return False
    try:
        signature.bind(**kwargs)
    except TypeError:
        return True
    return False
